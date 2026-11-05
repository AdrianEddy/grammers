// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The byte streams connections run over. grammers opens none itself: a [`Dialer`] supplies each
//! one, for the [`DialTarget`] the connection is for.

#[cfg(feature = "proxy")]
mod socks;
mod tcp;

use std::io;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::pin::Pin;
use std::time::Duration;

use grammers_session::types::DcOption;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;

#[cfg(feature = "proxy")]
pub use socks::Socks5Dialer;
pub use tcp::TokioTcpDialer;

/// How long one of a target's addresses has to connect before [`DialTarget::connect_any`] drops
/// the attempt and tries the next.
const ADDRESS_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A bidirectional byte stream a connection runs over, such as a TCP stream.
pub trait MtStream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> MtStream for T {}

/// What a connection carries, which a [`Dialer`] may route or count by.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConnKind {
    /// API requests and updates.
    Main,
    /// File transfers.
    Media,
    /// File transfers from a CDN datacenter.
    Cdn,
}

/// The datacenter a connection is for, and its addresses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DialTarget {
    /// The datacenter's identifier.
    pub dc_id: i32,
    /// What the connection carries.
    pub kind: ConnKind,
    /// The datacenter's IPv4 address, if it has one.
    pub ipv4: Option<SocketAddrV4>,
    /// The datacenter's IPv6 address, if it has one of its own.
    pub ipv6: Option<SocketAddrV6>,
}

impl DialTarget {
    /// The target for a `kind` connection to the datacenter of `dc_option`.
    ///
    /// An unspecified address is no address. Nor is an IPv6 address that embeds an IPv4 one,
    /// which the session's options hold when the datacenter has no IPv6 address of its own: its
    /// traffic would be IPv4.
    pub(crate) fn new(dc_option: &DcOption, kind: ConnKind) -> Self {
        Self {
            dc_id: dc_option.id,
            kind,
            ipv4: Some(dc_option.ipv4).filter(|address| !address.ip().is_unspecified()),
            ipv6: Some(dc_option.ipv6).filter(|address| {
                !address.ip().is_unspecified() && address.ip().to_ipv4_mapped().is_none()
            }),
        }
    }

    /// The target's addresses in the order to try them: IPv4 first, then IPv6.
    pub fn addresses(&self) -> impl Iterator<Item = SocketAddr> + use<> {
        self.ipv4
            .map(SocketAddr::V4)
            .into_iter()
            .chain(self.ipv6.map(SocketAddr::V6))
    }

    /// Run `connect` on each of the target's [`DialTarget::addresses`] in turn until one
    /// succeeds, and return that one's stream, or the last error. A target with no address is
    /// an [`io::ErrorKind::AddrNotAvailable`] error.
    ///
    /// An attempt that hasn't finished within 10 seconds is dropped and counts as an
    /// [`io::ErrorKind::TimedOut`] error, so an address that never answers, or a proxy that
    /// takes the connection and says nothing, doesn't keep the next address from being tried.
    pub async fn connect_any<S, F>(&self, mut connect: impl FnMut(SocketAddr) -> F) -> io::Result<S>
    where
        F: Future<Output = io::Result<S>>,
    {
        let mut last_error = None;
        for address in self.addresses() {
            match timeout(ADDRESS_CONNECT_TIMEOUT, connect(address)).await {
                Ok(Ok(stream)) => return Ok(stream),
                Ok(Err(error)) => last_error = Some(error),
                Err(elapsed) => last_error = Some(elapsed.into()),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                format!("datacenter {} has no address", self.dc_id),
            )
        }))
    }
}

/// The future a [`Dialer`] returns.
pub type DialFuture<'a> = Pin<Box<dyn Future<Output = io::Result<Box<dyn MtStream>>> + Send + 'a>>;

/// Opens the byte streams connections run over.
///
/// Every connection a [`crate::SenderPool`] makes is dialed through its
/// [`crate::ConnectionParams::dialer`], which decides how the bytes travel: directly, through a
/// proxy, over a WebSocket, through an application's own network stack, or to a test server.
pub trait Dialer: Send + Sync + 'static {
    /// Open a stream to `target`.
    fn dial<'a>(&'a self, target: &'a DialTarget) -> DialFuture<'a>;
}

#[cfg(test)]
mod tests;
