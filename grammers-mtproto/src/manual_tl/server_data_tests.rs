// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Message framing and packed objects from the server, read without panicking.

use super::*;

/// A message with the given header and body bytes.
fn message(len: i32, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(1i64.to_le_bytes());
    bytes.extend(1i32.to_le_bytes());
    bytes.extend(len.to_le_bytes());
    bytes.extend(body);
    bytes
}

/// A container body claiming `count` messages, followed by `messages`.
fn container(count: i32, messages: &[u8]) -> Vec<u8> {
    let mut bytes = MessageContainer::CONSTRUCTOR_ID.to_le_bytes().to_vec();
    bytes.extend(count.to_le_bytes());
    bytes.extend(messages);
    bytes
}

#[test]
fn manual_tl_bad_length_is_error() {
    // A length that is negative, or past the bytes there are, is an error before anything is
    // allocated for it.
    assert_eq!(
        Message::split_from(&message(-1, &[0; 8])).err(),
        Some(DeserializeError::NegativeMessageLength { got: -1 })
    );
    assert_eq!(
        Message::split_from(&message(i32::MAX, &[0; 8])).err(),
        Some(DeserializeError::TooLongMessageLength {
            got: i32::MAX as usize,
            max_length: 8,
        })
    );
    assert_eq!(
        Message::split_from(&message(12, &[0; 8])).err(),
        Some(DeserializeError::TooLongMessageLength {
            got: 12,
            max_length: 8,
        })
    );
    for len in 0..16 {
        assert_eq!(
            Message::split_from(&message(4, &[])[..len]).err(),
            Some(DeserializeError::MessageBufferTooSmall)
        );
    }

    // A message's length takes exactly its bytes, and the rest follows.
    let mut two = message(4, b"Hey!");
    two.extend(message(0, &[]));
    let (first, rest) = Message::split_from(&two).unwrap();
    assert_eq!(first.body, b"Hey!");
    let (second, rest) = Message::split_from(rest).unwrap();
    assert!(second.body.is_empty() && rest.is_empty());

    // A container claiming more messages than it holds, also a "negative" number of them.
    let one = message(4, b"Hey!");
    for count in [2, -1, i32::MIN] {
        assert_eq!(
            MessageContainer::from_body(&container(count, &one)).err(),
            Some(DeserializeError::MessageBufferTooSmall)
        );
    }
    assert_eq!(
        MessageContainer::from_body(&container(1, &one))
            .unwrap()
            .messages
            .len(),
        1
    );
    assert_eq!(
        MessageContainer::from_body(&container(1, &one)[..6]).err(),
        Some(DeserializeError::MessageBufferTooSmall)
    );

    // A `bytes` (the data of a `gzip_packed` object) is read in place, in both length forms and
    // with every padding, as the TL serializer writes it. Cut anywhere, padding included, it is an
    // error, as is a length past the bytes there are.
    for len in 0..300 {
        let data: Vec<u8> = (0..len).map(|byte| byte as u8).collect();
        let mut bytes = data.to_bytes();
        bytes.push(9);
        assert_eq!(
            split_bytes(&bytes).unwrap(),
            (data.as_slice(), [9].as_slice())
        );
        for cut in 0..bytes.len() - 1 {
            assert_eq!(
                split_bytes(&bytes[..cut]).err(),
                Some(DeserializeError::MessageBufferTooSmall)
            );
        }
    }
    assert_eq!(
        split_bytes(&[254, 0xff, 0xff, 0xff, 0]).err(),
        Some(DeserializeError::MessageBufferTooSmall)
    );
}

/// The `gzip_packed` object holding `packed_data`, as the server sends it.
fn gzip_packed(packed_data: Vec<u8>) -> Vec<u8> {
    GzipPacked { packed_data }.to_bytes()
}

#[test]
fn gzip_inflates_within_its_budget() {
    let gzip = GzipPacked::new(&[7; 1000]).unwrap();
    let object = gzip.to_bytes();

    // Exactly the budget is fine, and drains it.
    let mut budget = 1000;
    assert_eq!(
        GzipPacked::decompress(&object, &mut budget).unwrap(),
        vec![7; 1000]
    );
    assert_eq!(budget, 0);

    // One byte less is an error. The bytes were inflated all the same, so it exhausts the budget.
    let mut budget = 999;
    assert_eq!(
        GzipPacked::decompress(&object, &mut budget),
        Err(DeserializeError::DecompressionFailed)
    );
    assert_eq!(budget, 0);

    // A wrong checksum is found after the output, which the error draws on the budget too.
    let mut bad_crc = gzip.packed_data.clone();
    let crc = bad_crc.len() - 8;
    bad_crc[crc] ^= 1;
    let mut budget = MAX_UNPACKED_PER_PACKET;
    assert_eq!(
        GzipPacked::decompress(&gzip_packed(bad_crc), &mut budget),
        Err(DeserializeError::DecompressionFailed)
    );
    assert_eq!(budget, MAX_UNPACKED_PER_PACKET - 1000);

    // Data that stops early is an error.
    let truncated = gzip.packed_data[..gzip.packed_data.len() / 2].to_vec();
    assert_eq!(
        GzipPacked::decompress(&gzip_packed(truncated), &mut budget),
        Err(DeserializeError::DecompressionFailed)
    );

    // So is data that isn't gzip, an object that isn't a whole `gzip_packed`, and another object:
    // they inflate nothing, and cost nothing.
    let mut budget = MAX_UNPACKED_PER_PACKET;
    for packed_data in [b"not gzip".to_vec(), Vec::new()] {
        assert_eq!(
            GzipPacked::decompress(&gzip_packed(packed_data), &mut budget),
            Err(DeserializeError::DecompressionFailed)
        );
    }
    assert_eq!(
        GzipPacked::decompress(&object[..object.len() - 4], &mut budget),
        Err(DeserializeError::MessageBufferTooSmall)
    );
    assert_eq!(
        GzipPacked::decompress(&[0; 8], &mut budget),
        Err(DeserializeError::UnexpectedConstructor { id: 0 })
    );
    assert_eq!(budget, MAX_UNPACKED_PER_PACKET);
}

#[test]
fn gzip_bytes_after_the_member_reserve_nothing() {
    // The last four bytes of a gzip member state its inflated size. Bytes after the member aren't
    // read, so these four claim the whole budget unchecked: the output's buffer must grow with
    // what is inflated, never from the claim.
    let mut packed_data = GzipPacked::new(b"Hey!").unwrap().packed_data;
    packed_data.extend((MAX_UNPACKED_PER_PACKET as u32).to_le_bytes());

    let mut budget = MAX_UNPACKED_PER_PACKET;
    let unpacked = GzipPacked::decompress(&gzip_packed(packed_data), &mut budget).unwrap();
    assert_eq!(unpacked, b"Hey!");
    assert_eq!(budget, MAX_UNPACKED_PER_PACKET - 4);
    assert!(
        unpacked.capacity() < 64 * 1024,
        "{} bytes reserved for 4",
        unpacked.capacity()
    );
}
