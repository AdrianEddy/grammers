// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The connection state reports what it couldn't unpack by the layer that refused it: a transport
//! error code from the server is a transport error, as callers match it (404 asks for a new
//! authorization key), not a deserialization error.

use grammers_mtproto::connection::{State, UnpackError};
use grammers_mtproto::mtp::Plain;
use grammers_mtproto::transport::{self, Intermediate};

#[test]
fn transport_error_code_is_a_transport_error() {
    let mut state = State::new(Intermediate::new(), Plain::new());
    // A packet of one `int`, the code negated, as the intermediate transport frames it.
    let frame = [4i32.to_le_bytes(), (-404i32).to_le_bytes()].concat();
    let (read, _) = state.io_buffers();
    read[..frame.len()].copy_from_slice(&frame);
    state.handle_net_read(frame.len()).unwrap();

    let unpacked = state.unpack_next();
    assert!(
        matches!(
            unpacked,
            Some(Err(UnpackError::Transport(transport::Error::BadStatus {
                status: 404
            })))
        ),
        "{unpacked:?}"
    );
}
