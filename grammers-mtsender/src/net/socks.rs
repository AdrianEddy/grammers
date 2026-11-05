// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::borrow::Cow;
use std::fmt;
use std::io::{self, ErrorKind};
use std::net::{IpAddr, SocketAddr};

use hickory_resolver::Resolver;
use percent_encoding::percent_decode_str;
use tokio_socks::tcp::Socks5Stream;
use tracing::info;
use url::{Host, Url};

use super::{DialFuture, DialTarget, Dialer, MtStream};

/// Dials through a SOCKS5 proxy, to the target's IPv4 address first and to its IPv6 address if
/// that fails ([`DialTarget::connect_any`]), with Nagle's algorithm off.
#[derive(Clone)]
pub struct Socks5Dialer {
    host: Host<String>,
    port: u16,
    /// The username and password, decoded, if the proxy wants them.
    credentials: Option<(String, String)>,
}

impl Socks5Dialer {
    /// A dialer through the proxy at `proxy_url`, whose scheme must be `socks5`. Username and
    /// password are optional, e.g.:
    /// - socks5://127.0.0.1:1234
    /// - socks5://username:password@example.com:5678
    ///
    /// The username and password are percent-decoded, as a URL's are, so an `@` in either is
    /// written `%40`; one that isn't UTF-8 once decoded is an error.
    ///
    /// Both a host and port must be provided. If a domain is used for the host, its address is
    /// looked up at each dial, and the first IP address found is used. If a different IP address
    /// should be used, consider resolving the host manually and selecting an IP address of your
    /// choice.
    pub fn new(proxy_url: &str) -> io::Result<Self> {
        let proxy =
            Url::parse(proxy_url).map_err(|err| io::Error::new(ErrorKind::InvalidInput, err))?;
        if proxy.scheme() != "socks5" {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("proxy scheme not supported: {}", proxy.scheme()),
            ));
        }
        // No error says what the URL was: it may hold a password.
        let host = proxy
            .host()
            .map(|host| host.to_owned())
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "proxy url has no host"))?;
        let port = proxy
            .port()
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "proxy url has no port"))?;
        let credentials = if proxy.username().is_empty() {
            None
        } else {
            Some((
                decoded(proxy.username())?,
                decoded(proxy.password().unwrap_or_default())?,
            ))
        };
        Ok(Self {
            host,
            port,
            credentials,
        })
    }

    /// The proxy's socket address, its domain looked up if it has one.
    async fn proxy_address(&self) -> io::Result<SocketAddr> {
        let ip = match &self.host {
            Host::Ipv4(ip) => IpAddr::V4(*ip),
            Host::Ipv6(ip) => IpAddr::V6(*ip),
            // A `socks5` URL's host isn't parsed as an IP address: its scheme isn't special.
            Host::Domain(domain) => match domain.parse() {
                Ok(ip) => ip,
                Err(_) => Resolver::builder_tokio()
                    .and_then(|builder| builder.build())
                    .map_err(io::Error::other)?
                    .lookup_ip(domain.as_str())
                    .await
                    .map_err(io::Error::other)?
                    .iter()
                    .next()
                    .ok_or_else(|| {
                        io::Error::new(
                            ErrorKind::NotFound,
                            format!("proxy host did not return any ip address: {domain}"),
                        )
                    })?,
            },
        };
        Ok(SocketAddr::new(ip, self.port))
    }
}

/// A username or password from the proxy's URL, where it is percent-encoded, as the proxy wants
/// it: decoded, and UTF-8, since `tokio-socks` takes it as a `&str`.
fn decoded(part: &str) -> io::Result<String> {
    percent_decode_str(part)
        .decode_utf8()
        .map(Cow::into_owned)
        .map_err(|err| {
            io::Error::new(
                ErrorKind::InvalidInput,
                format!("proxy url's username or password isn't UTF-8 once decoded: {err}"),
            )
        })
}

/// Says whether the dialer has credentials, never what they are.
impl fmt::Debug for Socks5Dialer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Socks5Dialer")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("authenticated", &self.credentials.is_some())
            .finish()
    }
}

impl Dialer for Socks5Dialer {
    fn dial<'a>(&'a self, target: &'a DialTarget) -> DialFuture<'a> {
        Box::pin(async move {
            info!(dc_id = target.dc_id, kind = ?target.kind, "connecting through a proxy...");
            let proxy = self.proxy_address().await?;
            let stream = target
                .connect_any(|address| async move {
                    match &self.credentials {
                        None => Socks5Stream::connect(proxy, address).await,
                        Some((username, password)) => {
                            Socks5Stream::connect_with_password(proxy, address, username, password)
                                .await
                        }
                    }
                    .map_err(|err| io::Error::new(ErrorKind::ConnectionAborted, err))
                })
                .await?;
            // As in `TokioTcpDialer`: a request is written whole.
            stream.set_nodelay(true)?;
            Ok(Box::new(stream) as Box<dyn MtStream>)
        })
    }
}
