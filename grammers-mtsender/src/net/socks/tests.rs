// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The SOCKS5 dialer's errors name the proxy hop and say each cause once, as the request's error
//! chain reads them: `tokio-socks` and hickory render the error they wrap and return it from
//! `source()` as well, and `InvocationError::Io` goes on from the dial's error's source.

use std::error::Error;
use std::iter;

use hickory_resolver::net::DnsError;
use hickory_resolver::proto::op::ResponseCode;

use super::*;
use crate::InvocationError;

/// The message of every error in `error`'s chain, outermost first, as a chain-aware reporter
/// (`anyhow`'s `{:#}`, `std::error::Report`) prints them.
fn chain(error: &(dyn Error + 'static)) -> Vec<String> {
    iter::successors(Some(error), |&error| error.source())
        .map(ToString::to_string)
        .collect()
}

#[test]
fn proxy_io_error_is_said_once() {
    // The proxy hung up during the handshake.
    let error = socks_error(tokio_socks::Error::Io(ErrorKind::UnexpectedEof.into()));
    assert_eq!(error.kind(), ErrorKind::ConnectionAborted);
    // The I/O error itself is in the payload, out of `tokio-socks`' error.
    assert!(matches!(
        error.get_ref().unwrap().downcast_ref::<ProxyError>(),
        Some(ProxyError::Io(io_error)) if io_error.kind() == ErrorKind::UnexpectedEof
    ));
    assert_eq!(
        chain(&InvocationError::Io(error)),
        ["request error: SOCKS5 proxy: unexpected end of file"]
    );
}

/// An error that names what failed and leaves its cause to `source()`.
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

#[test]
fn proxy_io_error_keeps_its_own_cause() {
    // An I/O error whose payload leaves its cause to `source()`: the chain goes on from it.
    let io_error = io::Error::new(
        ErrorKind::ConnectionReset,
        TunnelError(io::Error::other("the relay hung up")),
    );
    let error = socks_error(tokio_socks::Error::Io(io_error));
    assert_eq!(
        chain(&InvocationError::Io(error)),
        [
            "request error: SOCKS5 proxy: the tunnel failed",
            "the relay hung up"
        ]
    );
}

#[test]
fn proxy_refusal_is_said_once() {
    // The proxy answered that the target refused the connection (RFC 1928's reply 5).
    let error = socks_error(tokio_socks::Error::ConnectionRefused);
    assert_eq!(error.kind(), ErrorKind::ConnectionAborted);
    assert_eq!(
        chain(&InvocationError::Io(error)),
        ["request error: SOCKS5 proxy: Connection refused"]
    );
}

#[test]
fn proxy_lookup_error_is_said_once() {
    let error = lookup_error(NetError::from(DnsError::ResponseCode(
        ResponseCode::ServFail,
    )));
    assert_eq!(error.kind(), ErrorKind::Other);
    assert_eq!(
        chain(&InvocationError::Io(error)),
        [
            "request error: looking up the SOCKS5 proxy's host: DNS error: error response: Server Failure"
        ]
    );
}
