// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::{Error, Tagged, Transport, UnpackedOffset};
use crate::DequeBuffer;

/// The lightest MTProto transport protocol available.
///
/// This is an implementation of the [abridged transport].
///
/// * Overhead: very small.
/// * Minimum envelope length: 1 byte.
/// * Maximum envelope length: 4 bytes.
///
/// It serializes the input payload as follows, if the length is small enough:
///
/// ```text
/// +-+----...----+
/// |L|  payload  |
/// +-+----...----+
///  ^ 1 byte
/// ```
///
/// Otherwise:
///
/// ```text
/// +----+----...----+
/// | len|  payload  |
/// +----+----...----+
///  ^^^^ 4 bytes
/// ```
///
/// [abridged transport]: https://core.telegram.org/mtproto/mtproto-transports#abridged
pub struct Abridged {
    init: bool,
}

#[allow(clippy::new_without_default)]
impl Abridged {
    pub fn new() -> Self {
        Self { init: false }
    }
}

impl Transport for Abridged {
    fn pack(&mut self, buffer: &mut DequeBuffer<u8>) {
        let len = buffer.len();
        assert_eq!(len % 4, 0);

        let len = len / 4;
        if len < 127 {
            buffer.extend_front(&[len as u8]);
        } else {
            buffer.extend_front(&(0x7f | ((len as u32) << 8)).to_le_bytes());
        }

        if !self.init {
            buffer.extend_front(&[0xef]);
            self.init = true;
        }
    }

    fn unpack(&mut self, buffer: &mut [u8]) -> Result<UnpackedOffset, Error> {
        let Some(&first) = buffer.first() else {
            return Err(Error::MissingBytes);
        };

        // The length in 4-byte words: the first byte below 127, otherwise the three bytes after
        // it, unsigned, so at most 2^24 - 1 words.
        let (header_len, words) = if first < 127 {
            (1, usize::from(first))
        } else {
            let Some(header) = buffer.first_chunk::<4>() else {
                return Err(Error::MissingBytes);
            };
            (4, (u32::from_le_bytes(*header) >> 8) as usize)
        };

        let len = words * 4;
        let Some(data) = buffer.get(header_len..header_len + len) else {
            return Err(Error::MissingBytes);
        };

        // A transport error is a packet of exactly one negative `int`, the error code (shorter
        // than any MTProto message): the first `int` of a longer packet is just data.
        if header_len == 1
            && let Ok(code) = <[u8; 4]>::try_from(data)
        {
            let code = i32::from_le_bytes(code);
            if code < 0 {
                return Err(Error::BadStatus {
                    status: code.wrapping_neg() as u32,
                });
            }
        }

        Ok(UnpackedOffset {
            data_range: header_len..header_len + len,
            next_offset: header_len + len,
        })
    }
}

impl Tagged for Abridged {
    fn init_tag(&mut self) -> [u8; 4] {
        self.init = true;
        [0xef, 0xef, 0xef, 0xef]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns a new abridged transport, and `n` bytes of input data for it.
    fn setup_pack(n: usize) -> (Abridged, DequeBuffer<u8>) {
        let mut buffer = DequeBuffer::with_capacity(n, 0);
        for i in 0..n {
            buffer.push((i & 0xff) as u8);
        }
        (Abridged::new(), buffer)
    }

    #[test]
    fn pack_empty() {
        let (mut transport, mut buffer) = setup_pack(0);
        transport.pack(&mut buffer);
        assert_eq!(&buffer[..], &[0xef, 0]);
    }

    #[test]
    #[should_panic]
    fn pack_non_padded() {
        let (mut transport, mut buffer) = setup_pack(7);
        transport.pack(&mut buffer);
    }

    #[test]
    fn pack_normal() {
        let (mut transport, mut buffer) = setup_pack(128);
        let orig = buffer.clone();
        transport.pack(&mut buffer);
        assert_eq!(&buffer[..2], &[0xef, 32]);
        assert_eq!(&buffer[2..], &orig[..]);
    }

    #[test]
    fn pack_large() {
        let (mut transport, mut buffer) = setup_pack(1024);
        let orig = buffer.clone();
        transport.pack(&mut buffer);
        assert_eq!(&buffer[..5], &[0xef, 127, 0, 1, 0]);
        assert_eq!(&buffer[5..], &orig[..]);
    }

    #[test]
    fn unpack_small() {
        let mut transport = Abridged::new();
        let mut buffer = DequeBuffer::with_capacity(1, 0);
        buffer.extend_back(&[1]);
        assert_eq!(transport.unpack(&mut buffer[..]), Err(Error::MissingBytes));
    }

    #[test]
    fn unpack_normal() {
        let (mut transport, mut buffer) = setup_pack(128);
        let orig = buffer.clone();
        transport.pack(&mut buffer);
        let n = 1; // init byte
        let offset = transport.unpack(&mut buffer[n..][..]).unwrap();
        assert_eq!(&buffer[n..][offset.data_range], &orig[..]);
    }

    #[test]
    fn unpack_two_at_once() {
        let (mut transport, mut buffer) = setup_pack(128);
        let orig = buffer.clone();

        let mut two_buffer = DequeBuffer::with_capacity(0, 0);
        transport.pack(&mut buffer);
        two_buffer.extend_back(&buffer[1..]); // init byte
        let single_size = two_buffer.len();

        buffer = orig.clone();
        transport.pack(&mut buffer);
        two_buffer.extend_back(&buffer[..]);

        let offset = transport.unpack(&mut two_buffer[..]).unwrap();
        assert_eq!(&buffer[offset.data_range], &orig[..]);
        assert_eq!(offset.next_offset, single_size);

        let n = offset.next_offset;
        let offset = transport.unpack(&mut two_buffer[n..]).unwrap();
        assert_eq!(&buffer[offset.data_range], &orig[..]);
    }

    #[test]
    fn unpack_large() {
        let (mut transport, mut buffer) = setup_pack(1024);
        let orig = buffer.clone();
        transport.pack(&mut buffer);
        let n = 1; // init byte
        let offset = transport.unpack(&mut buffer[n..]).unwrap();
        assert_eq!(&buffer[n..][offset.data_range], &orig[..]);
    }

    #[test]
    fn unpack_bad_status() {
        let mut transport = Abridged::new();
        let mut buffer = DequeBuffer::with_capacity(5, 0);
        buffer.push(1u8);
        buffer.extend_back(&(-404_i32).to_le_bytes());

        assert_eq!(
            transport.unpack(&mut buffer[..]),
            Err(Error::BadStatus { status: 404 })
        );
    }
}
