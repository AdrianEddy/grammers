// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A session error carries the session's error in its message, and the error chain goes on from
//! that error's own cause: nothing of it is lost, and no reporter prints it twice. An I/O error
//! does the same with the error its `io::Error` carries, such as a dialer's, and so does the
//! error that ends a connection, in every request the connection held.

use std::error::Error;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::{fmt, io, iter};

use grammers_mtproto::mtp::Plain;
use grammers_mtproto::transport::Intermediate;
use grammers_mtsender::{
    ConnKind, DialFuture, DialTarget, Dialer, InvocationError, MtStream, ReadError, Sender,
};
use grammers_tl_types as tl;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A session storage's error that names what it was doing and leaves its cause to `source()`.
#[derive(Debug)]
struct StoreError(io::Error);

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("storing the session failed")
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

/// The message of every error in `error`'s chain, outermost first, as a chain-aware reporter
/// (`anyhow`'s `{:#}`, `std::error::Report`) prints them.
fn chain(error: &(dyn Error + 'static)) -> Vec<String> {
    iter::successors(Some(error), |&error| error.source())
        .map(ToString::to_string)
        .collect()
}

#[test]
fn session_error_keeps_its_cause() {
    let error = InvocationError::from(
        Box::new(StoreError(io::Error::other("disk full"))) as Box<dyn Error + Send + Sync>
    );
    assert_eq!(
        chain(&error),
        ["session error: storing the session failed", "disk full"]
    );
    // The cause is the session error's own, so a caller can still look for it by type.
    let cause = error.source().unwrap();
    assert!(cause.downcast_ref::<io::Error>().is_some());
}

#[test]
fn session_error_that_renders_its_cause_says_it_once() {
    // As grammers' own errors do: the cause in the message, and no `source()`.
    let error = InvocationError::Session("no datacenter 9 in the session".into());
    assert_eq!(
        chain(&error),
        ["session error: no datacenter 9 in the session"]
    );
}

/// A dialer's error that names what it was doing and leaves its cause to `source()`.
#[derive(Debug)]
struct DialError(io::Error);

impl fmt::Display for DialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("opening the tunnel failed")
    }
}

impl Error for DialError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

#[test]
fn io_error_keeps_its_cause() {
    // The `io::Error` a dialer returns renders the error it carries and goes on from that error's
    // source, and so does the request's error.
    let error = InvocationError::Io(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        DialError(io::Error::other("the relay hung up")),
    ));
    assert_eq!(
        chain(&error),
        [
            "request error: opening the tunnel failed",
            "the relay hung up"
        ]
    );
    let cause = error.source().unwrap();
    assert!(cause.downcast_ref::<io::Error>().is_some());
}

#[test]
fn io_error_without_a_cause_says_it_once() {
    // An operating system's error, and one whose message is all it carries.
    let error = InvocationError::Io(io::ErrorKind::ConnectionAborted.into());
    assert_eq!(chain(&error), ["request error: connection aborted"]);
    let error = InvocationError::Io(io::Error::other("the proxy hung up"));
    assert_eq!(chain(&error), ["request error: the proxy hung up"]);
}

/// A stream's error that names what failed and leaves its cause to `source()`.
#[derive(Debug)]
struct TunnelError(io::Error);

impl fmt::Display for TunnelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the tunnel failed")
    }
}

impl Error for TunnelError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

/// [`TunnelError`] as a stream's I/O error.
fn tunnel_failed() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionReset,
        TunnelError(io::Error::other("the relay hung up")),
    )
}

#[test]
fn read_error_says_its_cause_once() {
    let error = ReadError::from(tunnel_failed());
    assert_eq!(
        chain(&error),
        [
            "read error, IO failed: the tunnel failed",
            "the relay hung up"
        ]
    );
}

#[test]
fn connection_error_keeps_its_cause_in_every_request() {
    // The error that ends a connection fails every request the connection held, each with a copy.
    let error = ReadError::from(tunnel_failed());
    for copy in [error.clone(), error.clone()].map(InvocationError::from) {
        assert_eq!(
            chain(&copy),
            ["request error: the tunnel failed", "the relay hung up"]
        );
        let InvocationError::Io(io_error) = &copy else {
            panic!("{copy:?}")
        };
        assert_eq!(io_error.kind(), io::ErrorKind::ConnectionReset);
        // The payload is the shared error, whose own payload is the stream's.
        let shared = io_error.get_ref().unwrap().downcast_ref::<Arc<io::Error>>();
        let payload = shared.unwrap().get_ref().unwrap();
        assert!(payload.downcast_ref::<TunnelError>().is_some());
    }

    // With no copy left, the error itself.
    let InvocationError::Io(io_error) = InvocationError::from(error) else {
        panic!("not an I/O error")
    };
    assert!(io_error.into_inner().unwrap().is::<TunnelError>());
}

#[test]
fn connection_error_of_the_operating_system_keeps_its_code_in_every_request() {
    // Which error the code is doesn't matter: an operating system's error is all in its code.
    const CODE: i32 = 2;
    let error = ReadError::from(io::Error::from_raw_os_error(CODE));
    let copy = InvocationError::from(error.clone());
    let InvocationError::Io(io_error) = &copy else {
        panic!("{copy:?}")
    };
    assert_eq!(io_error.raw_os_error(), Some(CODE));
    let expected = format!("request error: {}", io::Error::from_raw_os_error(CODE));
    assert_eq!(chain(&copy), [expected]);
}

/// A stream that takes every write, and whose first read fails with the error it holds.
struct FailsReading(Option<io::Error>);

impl AsyncRead for FailsReading {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(self.0.take().expect("one read")))
    }
}

impl AsyncWrite for FailsReading {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Dials the one stream it holds.
struct Dials(Mutex<Option<Box<dyn MtStream>>>);

impl Dialer for Dials {
    fn dial<'a>(&'a self, _: &'a DialTarget) -> DialFuture<'a> {
        let stream = self.0.lock().unwrap().take().unwrap();
        Box::pin(async move { Ok(stream) })
    }
}

#[tokio::test]
async fn connection_error_while_invoking_is_the_error_itself() {
    // `Sender::invoke`, as a connection's setup runs it: the stream fails under the one request.
    let dialer = Dials(Mutex::new(Some(Box::new(FailsReading(Some(
        tunnel_failed(),
    ))))));
    let target = DialTarget {
        dc_id: 2,
        kind: ConnKind::Main,
        ipv4: None,
        ipv6: None,
    };
    let mut sender = Sender::connect(Intermediate::new(), Plain::new(), &dialer, &target)
        .await
        .unwrap();
    let error = sender
        .invoke(&tl::functions::Ping { ping_id: 1 })
        .await
        .unwrap_err();
    assert_eq!(
        chain(&error),
        ["request error: the tunnel failed", "the relay hung up"]
    );
    // No other request shares it, so the payload is the stream's own.
    let InvocationError::Io(io_error) = error else {
        panic!("{error:?}")
    };
    assert!(io_error.into_inner().unwrap().is::<TunnelError>());
}
