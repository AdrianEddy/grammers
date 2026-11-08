// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This library is an implementation of the [Mobile Transport Protocol].
//!
//! It is capable of efficiently packing enqueued requests into message
//! containers to later be encrypted and transmitted, and processing the
//! server responses to maintain a correct state.
//!
//! [Mobile Transport Protocol]: https://core.telegram.org/mtproto

#![deny(unsafe_code)]

pub mod authentication;
pub mod connection;
mod deque_buffer;
mod manual_tl;
pub mod mtp;
pub mod transport;
mod utils;

pub use deque_buffer::DequeBuffer;

/// The default compression threshold to be used.
pub const DEFAULT_COMPRESSION_THRESHOLD: Option<usize> = Some(512);

/// Maximum serialized request size. Requests above this size
/// are dropped, as they would otherwise reset the connection.
pub const MAX_REQUEST_SIZE: usize = manual_tl::Message::MAX_BODY_SIZE;

/// A Message Identifier.
///
/// When requests are serialized, a new message identifier is attached to them.
/// As server responses get processed, some of them will be RPC responses.
/// The message identifier in the response can be matched with the message identifier
/// that was assigned to the request to determine which request the response is for.
///
/// Identifiers are ordered as the unsigned 64-bit numbers they are, about the time they were
/// made, in seconds since the epoch, times 2^32 (TDLib's `MessageId` is a `uint64`). From
/// 2038-01-19T03:14:08Z their top bit is set, so the `long` the protocol carries them in, and
/// [`MsgId::into_inner`], read negative.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct MsgId(i64);

impl MsgId {
    /// Returns the raw value of the message identifier.
    pub fn into_inner(self) -> i64 {
        self.0
    }
}

impl Ord for MsgId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cast_unsigned().cmp(&other.0.cast_unsigned())
    }
}

impl PartialOrd for MsgId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
