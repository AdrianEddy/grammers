// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use tokio::net::TcpStream;
use tracing::info;

use super::{DialFuture, DialTarget, Dialer, MtStream};

/// Dials a direct TCP connection with tokio, to the target's IPv4 address first and to its IPv6
/// address if that fails ([`DialTarget::connect_any`]), with Nagle's algorithm off.
#[derive(Clone, Copy, Debug, Default)]
pub struct TokioTcpDialer;

impl Dialer for TokioTcpDialer {
    fn dial<'a>(&'a self, target: &'a DialTarget) -> DialFuture<'a> {
        Box::pin(async move {
            info!(dc_id = target.dc_id, kind = ?target.kind, "connecting...");
            let stream = target.connect_any(TcpStream::connect).await?;
            // A request is written whole: holding a small one back until the server acknowledges
            // the last would only delay it.
            stream.set_nodelay(true)?;
            Ok(Box::new(stream) as Box<dyn MtStream>)
        })
    }
}
