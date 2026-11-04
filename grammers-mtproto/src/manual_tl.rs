// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This module contains additional, manual structures for some TL types.

// The messages are read from the server's bytes: a bad length is an error, never a panic.
#![cfg_attr(
    not(test),
    deny(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing
    )
)]

use std::io::{self, Read, Write};

use flate2::Compression;
use flate2::bufread::GzDecoder;
use flate2::write::GzEncoder;
use grammers_crypto::MAX_PADDING_LEN;
use grammers_tl_types::{self as tl, Cursor, Deserializable, Identifiable, Serializable};

use crate::mtp::DeserializeError;
use crate::transport::MAX_TRANSPORT_BUFFER_SIZE;

#[cfg(test)]
mod server_data_tests;

/// This struct represents the following TL definition:
///
/// ```tl
/// message msg_id:long seqno:int bytes:int body:Object = Message;
/// ```
///
/// Messages are what's ultimately sent to Telegram.
///
/// Each message has its own unique identifier, and the body is simply
/// the serialized request that should be executed on the server, or
/// the response object from Telegram.
pub(crate) struct Message {
    pub msg_id: i64,
    pub seq_no: i32,
    pub body: Vec<u8>,
}

// https://core.telegram.org/mtproto/description#encrypted-message
// Subtracting key_id (i64) and msg_key (i128).
const MAX_ENCRYPTED_MESSAGE_SIZE: usize = MAX_TRANSPORT_BUFFER_SIZE - 8 - 16;

// https://core.telegram.org/mtproto/description#encrypted-message-encrypted-data
// Subtracting salt (i64), session_id (i64) and padding's upper bound.
pub(crate) const MAX_ENCRYPTED_DATA_SIZE: usize =
    MAX_ENCRYPTED_MESSAGE_SIZE - 8 - 8 - MAX_PADDING_LEN;

impl Message {
    // msg_id (8 bytes), seq_no (4 bytes), bytes (4 len)
    pub const SIZE_OVERHEAD: usize = 16;

    /// Maximum size a single request can be before it would be rejected by Telegram.
    /// Pushing a request above this size will panic.
    pub const MAX_BODY_SIZE: usize =
        MAX_ENCRYPTED_DATA_SIZE - MessageContainer::SIZE_OVERHEAD - Message::SIZE_OVERHEAD;

    /// Peek the constructor ID from the body.
    pub fn constructor_id(&self) -> Result<u32, tl::deserialize::Error> {
        u32::from_bytes(&self.body)
    }

    /// Determines whether this server message needs acknowledgement.
    pub fn requires_ack(&self) -> bool {
        // > Content-related Message
        // >   A message requiring an explicit acknowledgment.
        // > [...]
        // > (msg_seqno) [...] twice the number of "content-related" messages
        // > [...] and subsequently incremented by one if the current message
        // > is a content-related message.
        // https://core.telegram.org/mtproto/description#content-related-message
        self.seq_no % 2 == 1
    }
}

impl Serializable for Message {
    fn serialize(&self, sink: &mut impl FnMut(&[u8])) {
        self.msg_id.serialize(sink);
        self.seq_no.serialize(sink);
        (self.body.len() as i32).serialize(sink);
        sink(&self.body);
    }
}

impl Message {
    /// Reads the message at the start of `bytes`, and returns it with the bytes after it.
    ///
    /// The body's length is checked against the bytes there are, before anything is allocated.
    pub fn split_from(bytes: &[u8]) -> Result<(Self, &[u8]), DeserializeError> {
        let (msg_id, bytes) = split_i64(bytes)?;
        let (seq_no, bytes) = split_i32(bytes)?;
        let (len, bytes) = split_i32(bytes)?;
        let len = usize::try_from(len)
            .map_err(|_| DeserializeError::NegativeMessageLength { got: len })?;
        let (body, rest) =
            bytes
                .split_at_checked(len)
                .ok_or(DeserializeError::TooLongMessageLength {
                    got: len,
                    max_length: bytes.len(),
                })?;

        Ok((
            Message {
                msg_id,
                seq_no,
                body: body.to_vec(),
            },
            rest,
        ))
    }
}

/// Reads the `long` at the start of `bytes`, and returns it with the bytes after it.
pub(crate) fn split_i64(bytes: &[u8]) -> Result<(i64, &[u8]), DeserializeError> {
    let (value, rest) = bytes
        .split_first_chunk()
        .ok_or(DeserializeError::MessageBufferTooSmall)?;
    Ok((i64::from_le_bytes(*value), rest))
}

/// Reads the `int` at the start of `bytes`, and returns it with the bytes after it.
pub(crate) fn split_i32(bytes: &[u8]) -> Result<(i32, &[u8]), DeserializeError> {
    let (value, rest) = bytes
        .split_first_chunk()
        .ok_or(DeserializeError::MessageBufferTooSmall)?;
    Ok((i32::from_le_bytes(*value), rest))
}

/// Reads the `bytes` (a TL `string`) at the start of `bytes`, and returns its data with the bytes
/// after it.
///
/// The length (one byte, or 254 and three more) is checked against the bytes there are, padding
/// to a multiple of four included, and the data is borrowed: nothing is allocated for the length
/// the server claims.
fn split_bytes(bytes: &[u8]) -> Result<(&[u8], &[u8]), DeserializeError> {
    let (header_len, len, rest) = match bytes {
        [254, rest @ ..] => {
            let (&[b0, b1, b2], rest) = rest
                .split_first_chunk()
                .ok_or(DeserializeError::MessageBufferTooSmall)?;
            (4, u32::from_le_bytes([b0, b1, b2, 0]) as usize, rest)
        }
        [len, rest @ ..] => (1, usize::from(*len), rest),
        [] => return Err(DeserializeError::MessageBufferTooSmall),
    };
    let padding = (header_len + len).next_multiple_of(4) - (header_len + len);
    let (data, rest) = rest
        .split_at_checked(len)
        .ok_or(DeserializeError::MessageBufferTooSmall)?;
    let rest = rest
        .get(padding..)
        .ok_or(DeserializeError::MessageBufferTooSmall)?;
    Ok((data, rest))
}

/// This struct represents the following TL definition:
///
/// ```tl
/// rpc_result#f35c6d01 req_msg_id:long result:Object = RpcResult;
/// ```
pub struct RpcResult {
    pub req_msg_id: i64,
    pub result: Vec<u8>,
}

impl RpcResult {
    /// Peek the constructor ID from the body.
    pub fn inner_constructor(&self) -> Result<u32, tl::deserialize::Error> {
        u32::from_bytes(&self.result)
    }
}

impl Identifiable for RpcResult {
    #[allow(clippy::unreadable_literal)]
    const CONSTRUCTOR_ID: u32 = 0xf35c6d01;
}

impl Deserializable for RpcResult {
    fn deserialize(buf: &mut Cursor) -> Result<Self, tl::deserialize::Error> {
        let constructor_id = u32::deserialize(buf)?;
        if constructor_id != Self::CONSTRUCTOR_ID {
            return Err(tl::deserialize::Error::UnexpectedConstructor { id: constructor_id });
        }

        let req_msg_id = i64::deserialize(buf)?;
        let mut result = Vec::new();
        buf.read_to_end(&mut result)?;

        Ok(Self { req_msg_id, result })
    }
}

/// This struct represents the following TL definition:
///
/// ```tl
/// msg_container#73f1f8dc messages:vector<message> = MessageContainer;
/// ```
pub(crate) struct MessageContainer {
    pub messages: Vec<Message>,
}

impl MessageContainer {
    // message around container, constructor id (4 bytes), inner vec len (4 bytes)
    pub const SIZE_OVERHEAD: usize = Message::SIZE_OVERHEAD + 8;

    /// Maximum amount of messages that can't be sent inside a single
    /// container, inclusive. Beyond this limit Telegram will respond
    /// with `BAD_MESSAGE` `64` (invalid container).
    ///
    /// This limit is not 100% accurate and may in some cases be higher.
    /// However, sending up to 100 requests at once in a single container
    /// is a reasonable conservative value, since it could also depend on
    /// other factors like size per request, but we cannot know this.
    pub const MAXIMUM_LENGTH: usize = 100;
}

impl Identifiable for MessageContainer {
    #[allow(clippy::unreadable_literal)]
    const CONSTRUCTOR_ID: u32 = 0x73f1f8dc;
}

impl MessageContainer {
    /// Reads a container from a message's body.
    ///
    /// Each message's length is checked against the bytes left in the container.
    pub fn from_body(body: &[u8]) -> Result<Self, DeserializeError> {
        let (constructor_id, bytes) = split_i32(body)?;
        let constructor_id = constructor_id as u32;
        if constructor_id != Self::CONSTRUCTOR_ID {
            return Err(DeserializeError::UnexpectedConstructor { id: constructor_id });
        }

        // A vector's length is unsigned: a "negative" one claims more messages than any body
        // holds, and fails at the end of the bytes like any other count that is too large.
        let (len, mut bytes) = split_i32(bytes)?;
        let len = len as u32;
        let mut messages = Vec::with_capacity((len as usize).min(Self::MAXIMUM_LENGTH));
        for _ in 0..len {
            let (message, rest) = Message::split_from(bytes)?;
            messages.push(message);
            bytes = rest;
        }

        Ok(Self { messages })
    }
}

/// This struct represents the following TL definition:
///
/// ```tl
/// msg_copy#e06046b2 orig_message:Message = MessageCopy;
/// ```
///
/// Note that this is "not used", in favour of `msg_container`.
// Even though we use `MessageCopy::CONSTRUCTOR_ID, the dead code lint fires.
#[allow(dead_code)]
pub(crate) struct MessageCopy {
    pub orig_message: Vec<Message>,
}

impl Identifiable for MessageCopy {
    #[allow(clippy::unreadable_literal)]
    const CONSTRUCTOR_ID: u32 = 0xe06046b2;
}

/// This struct represents the following TL definition:
///
/// ```tl
/// gzip_packed#3072cfa1 packed_data:string = Object;
/// ```
///
/// The client builds one with [`GzipPacked::new`]; one from the server is read in place by
/// [`GzipPacked::decompress`].
pub(crate) struct GzipPacked {
    pub packed_data: Vec<u8>,
}

/// How many bytes the `gzip_packed` objects in one packet from the server may inflate to in
/// total: tdesktop's `kMaxUnpackedMessageLength`. A larger output is an error, so a small packet
/// can't make the client allocate, or inflate, without bound.
pub(crate) const MAX_UNPACKED_PER_PACKET: usize = 32 * 1024 * 1024;

/// How many `gzip_packed` objects may be nested in one another: tdesktop's `kMaxGzipNesting`.
/// Deeper nesting is an error, so a packet can't exhaust the stack.
pub(crate) const MAX_GZIP_NESTING: u32 = 64;

impl GzipPacked {
    /// Compresses `unpacked_data`.
    ///
    /// Compressing to memory doesn't fail in practice, but the encoder reports errors through
    /// [`io::Error`] all the same; a caller can then send the data uncompressed.
    pub fn new(unpacked_data: &[u8]) -> io::Result<Self> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(unpacked_data)?;
        let packed_data = encoder.finish()?;
        Ok(Self { packed_data })
    }

    /// Reads the `gzip_packed` object `object` from the server (a message's body, or the result in
    /// an `rpc_result`) and inflates its data, drawing what it inflates on `budget`, the bytes the
    /// packet it came in may still inflate to ([`MAX_UNPACKED_PER_PACKET`]).
    ///
    /// The packed data is read in place. The output's buffer grows with what is inflated, never
    /// from the size the gzip trailer states: that is the server's claim, and bytes after the gzip
    /// member aren't read, so the last four bytes needn't even be the trailer. Inflating stops one
    /// byte past the budget, so the buffer holds at most about twice the budget.
    ///
    /// Fails with [`DeserializeError::DecompressionFailed`] if the data isn't valid gzip, or if it
    /// would inflate past the budget. A failure still draws what was inflated before it, and output
    /// past the budget exhausts the budget, so failing objects can't repeat the work in one packet.
    pub fn decompress(object: &[u8], budget: &mut usize) -> Result<Vec<u8>, DeserializeError> {
        let (constructor_id, bytes) = split_i32(object)?;
        let constructor_id = constructor_id as u32;
        if constructor_id != Self::CONSTRUCTOR_ID {
            return Err(DeserializeError::UnexpectedConstructor { id: constructor_id });
        }
        let (packed_data, _) = split_bytes(bytes)?;

        // One byte past the budget, to tell output that fills it exactly from output that is too long.
        let limit = (*budget as u64).saturating_add(1);
        let mut unpacked = Vec::new();
        // On an error, `unpacked` holds what was inflated before it.
        let read = GzDecoder::new(packed_data)
            .take(limit)
            .read_to_end(&mut unpacked);

        let within = unpacked.len() <= *budget;
        *budget = budget.saturating_sub(unpacked.len());
        if read.is_err() || !within {
            return Err(DeserializeError::DecompressionFailed);
        }
        Ok(unpacked)
    }
}

impl Identifiable for GzipPacked {
    #[allow(clippy::unreadable_literal)]
    const CONSTRUCTOR_ID: u32 = 0x3072cfa1;
}

impl Serializable for GzipPacked {
    fn serialize(&self, sink: &mut impl FnMut(&[u8])) {
        Self::CONSTRUCTOR_ID.serialize(sink);
        self.packed_data.serialize(sink);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gzip_decompress() {
        let rpc_result = [
            1, 109, 92, 243, 132, 41, 150, 69, 54, 75, 49, 94, 161, 207, 114, 48, 254, 140, 1, 0,
            31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 149, 147, 61, 75, 195, 80, 20, 134, 79, 62, 20, 84,
            170, 1, 17, 28, 68, 28, 132, 110, 183, 247, 38, 185, 249, 154, 58, 10, 130, 116, 116,
            170, 54, 109, 18, 11, 173, 169, 109, 90, 112, 210, 209, 81, 112, 112, 22, 127, 131, 56,
            232, 232, 224, 143, 112, 112, 238, 159, 168, 55, 31, 234, 77, 91, 82, 26, 184, 57, 36,
            79, 222, 115, 222, 251, 38, 9, 170, 27, 218, 209, 62, 128, 113, 76, 234, 181, 83, 82,
            55, 31, 175, 223, 101, 0, 216, 249, 120, 217, 219, 102, 181, 244, 244, 186, 203, 10, 8,
            108, 109, 18, 221, 70, 132, 234, 136, 152, 20, 81, 13, 222, 132, 148, 43, 11, 184, 144,
            241, 178, 138, 49, 113, 176, 171, 90, 142, 175, 106, 45, 199, 79, 46, 217, 145, 63, 53,
            126, 117, 241, 92, 49, 215, 215, 48, 17, 197, 185, 185, 179, 156, 252, 113, 49, 227,
            91, 60, 39, 148, 240, 190, 196, 127, 95, 134, 217, 116, 176, 238, 89, 177, 47, 181,
            200, 151, 180, 156, 206, 229, 247, 35, 229, 252, 176, 156, 8, 198, 252, 126, 138, 184,
            144, 241, 57, 57, 106, 139, 114, 148, 167, 115, 178, 73, 46, 199, 34, 46, 100, 124,
            206, 126, 245, 162, 185, 98, 166, 227, 242, 55, 16, 81, 49, 159, 227, 18, 125, 93, 222,
            207, 202, 76, 14, 126, 172, 163, 69, 126, 148, 76, 87, 178, 9, 139, 213, 66, 148, 185,
            177, 48, 0, 159, 211, 52, 167, 118, 198, 27, 189, 145, 134, 6, 145, 215, 65, 205, 176,
            11, 240, 201, 158, 171, 150, 36, 104, 177, 90, 211, 37, 184, 99, 63, 11, 30, 2, 124,
            63, 200, 73, 253, 98, 141, 206, 199, 233, 119, 18, 63, 11, 207, 34, 76, 38, 147, 155,
            120, 193, 248, 48, 185, 23, 207, 186, 117, 214, 146, 26, 247, 57, 56, 1, 184, 63, 19,
            18, 189, 82, 102, 51, 47, 162, 168, 55, 112, 42, 149, 8, 117, 189, 10, 123, 247, 65,
            219, 95, 247, 195, 97, 127, 112, 53, 108, 244, 61, 144, 221, 246, 101, 192, 116, 171,
            65, 24, 6, 29, 47, 13, 83, 73, 203, 15, 58, 186, 13, 141, 216, 3, 0, 0,
        ];
        let _rpc_result_id = &rpc_result[0..4];
        let _msg_id = &rpc_result[4..12];
        let gzipped = &rpc_result[12..rpc_result.len()];
        let mut budget = MAX_UNPACKED_PER_PACKET;
        assert_eq!(
            GzipPacked::decompress(gzipped, &mut budget).unwrap().len(),
            984
        );
    }
}
