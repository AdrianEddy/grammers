// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Bytes from the server that the transports and the plain MTP unpack without panicking: a range
//! they return always lies in the buffer, and a bad length is an error.

use grammers_mtproto::mtp::{DeserializeError, Mtp, Plain};
use grammers_mtproto::transport::{
    Abridged, Error, Full, Intermediate, Obfuscated, Transport, UnpackedOffset,
};

/// Unpack `bytes`, checking that what is returned lies in them.
fn unpack(transport: &mut impl Transport, bytes: &[u8]) -> Result<UnpackedOffset, Error> {
    let mut buffer = bytes.to_vec();
    let offset = transport.unpack(&mut buffer)?;
    assert!(offset.data_range.end <= buffer.len());
    assert!(offset.next_offset <= buffer.len());
    Ok(offset)
}

fn le(values: &[i32]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

#[test]
fn intermediate_partial_packet_needs_more_bytes() {
    // The length counts the bytes after itself: 8 of them, of which 4 arrived.
    let packet = le(&[8, 1, 2]);
    for end in 0..packet.len() {
        assert_eq!(
            unpack(&mut Intermediate::new(), &packet[..end]),
            Err(Error::MissingBytes)
        );
    }
    assert_eq!(
        unpack(&mut Intermediate::new(), &packet),
        Ok(UnpackedOffset {
            data_range: 4..12,
            next_offset: 12,
        })
    );

    // A transport error is one `int`, which must have arrived to be read.
    let status = le(&[4, -404]);
    for end in 4..status.len() {
        assert_eq!(
            unpack(&mut Intermediate::new(), &status[..end]),
            Err(Error::MissingBytes)
        );
    }
    assert_eq!(
        unpack(&mut Intermediate::new(), &status),
        Err(Error::BadStatus { status: 404 })
    );

    // A length below one `int`, also a negative one, is no packet.
    for len in [3, 0, -4, i32::MIN] {
        assert_eq!(
            unpack(&mut Intermediate::new(), &le(&[len, 0, 0])),
            Err(Error::BadLen { got: len })
        );
    }
}

#[test]
fn abridged_long_length_is_unsigned() {
    // `0x7f` and three length bytes, the top one set: 2^24 - 1 words, waiting for them.
    assert_eq!(
        unpack(&mut Abridged::new(), &[0x7f, 0xff, 0xff, 0xff, 0, 0, 0, 0]),
        Err(Error::MissingBytes)
    );
    assert_eq!(
        unpack(&mut Abridged::new(), &[0x7f, 0xff]),
        Err(Error::MissingBytes)
    );
    let mut packet = vec![0x7f, 2, 0, 0];
    packet.extend(le(&[1, 2]));
    assert_eq!(
        unpack(&mut Abridged::new(), &packet),
        Ok(UnpackedOffset {
            data_range: 4..12,
            next_offset: 12,
        })
    );
}

#[test]
fn abridged_negative_first_int_of_a_message_is_data() {
    // Only a packet of exactly one negative `int` is a transport error.
    let mut message = vec![4];
    message.extend(le(&[-1, 2, 3, 4]));
    assert_eq!(
        unpack(&mut Abridged::new(), &message),
        Ok(UnpackedOffset {
            data_range: 1..17,
            next_offset: 17,
        })
    );
    let mut status = vec![1];
    status.extend(le(&[-404]));
    assert_eq!(
        unpack(&mut Abridged::new(), &status),
        Err(Error::BadStatus { status: 404 })
    );
}

#[test]
fn transport_status_of_i32_min_does_not_overflow() {
    let status = 1u32 << 31;
    let mut abridged = vec![1];
    abridged.extend(le(&[i32::MIN]));
    assert_eq!(
        unpack(&mut Abridged::new(), &abridged),
        Err(Error::BadStatus { status })
    );
    assert_eq!(
        unpack(&mut Intermediate::new(), &le(&[4, i32::MIN])),
        Err(Error::BadStatus { status })
    );
    assert_eq!(
        unpack(&mut Full::new(), &le(&[i32::MIN, 0, 0])),
        Err(Error::BadStatus { status })
    );
}

#[test]
fn full_bad_length_is_error() {
    for len in [0, 11] {
        assert_eq!(
            unpack(&mut Full::new(), &le(&[len, 0, 0])),
            Err(Error::BadLen { got: len })
        );
    }
    // A length past the bytes there are waits for them, also the largest one.
    for len in [16, i32::MAX] {
        assert_eq!(
            unpack(&mut Full::new(), &le(&[len, 0, 0])),
            Err(Error::MissingBytes)
        );
    }
    assert_eq!(unpack(&mut Full::new(), &[12, 0]), Err(Error::MissingBytes));
}

#[test]
fn obfuscated_buffer_shorter_than_decrypted_does_not_panic() {
    let mut transport = Obfuscated::new(Intermediate::new());
    let mut buffer = vec![0; 8];
    assert!(transport.unpack(&mut buffer).is_err());

    // Against the trait's contract, a shorter buffer: still no panic.
    let mut shorter = vec![0; 4];
    assert!(transport.unpack(&mut shorter).is_err());
    let mut empty = Vec::new();
    assert_eq!(transport.unpack(&mut empty), Err(Error::MissingBytes));
}

/// A plain message from the server with the given length field and `data`.
fn plain_message(len: i32, data: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(0i64.to_le_bytes()); // auth_key_id
    bytes.extend(1i64.to_le_bytes()); // msg_id, a response's
    bytes.extend(len.to_le_bytes());
    bytes.extend(data);
    bytes
}

#[test]
fn plain_length_past_payload_is_error() {
    for len in [5, i32::MAX] {
        assert_eq!(
            Plain::new()
                .deserialize(&mut plain_message(len, &[1, 2, 3, 4]))
                .unwrap_err(),
            DeserializeError::TooLongMessageLength {
                got: len as usize,
                max_length: 4,
            }
        );
    }
    assert_eq!(
        Plain::new()
            .deserialize(&mut plain_message(-1, &[1, 2, 3, 4]))
            .unwrap_err(),
        DeserializeError::NegativeMessageLength { got: -1 }
    );
    assert_eq!(
        Plain::new()
            .deserialize(&mut plain_message(4, &[1, 2, 3, 4]))
            .unwrap()
            .len(),
        1
    );
}
