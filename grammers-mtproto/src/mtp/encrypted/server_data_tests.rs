// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Packets from the server that the encrypted MTP must handle without panicking: crafted as they
//! are once decrypted, and fed to [`Encrypted::deserialize_plaintext`].

use super::*;
use crate::manual_tl::{
    GzipPacked, MAX_GZIP_NESTING, MAX_UNPACKED_PER_PACKET, MessageContainer, MessageCopy,
};

fn mtp() -> Encrypted {
    Encrypted::build().finish([0; 256])
}

/// A message as the server sends it.
fn message(msg_id: i64, seq_no: i32, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(msg_id.to_le_bytes());
    bytes.extend(seq_no.to_le_bytes());
    bytes.extend((body.len() as i32).to_le_bytes());
    bytes.extend(body);
    bytes
}

/// A decrypted packet: salt, session, the message, padding.
fn packet(session_id: i64, message: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(0i64.to_le_bytes());
    bytes.extend(session_id.to_le_bytes());
    bytes.extend(message);
    bytes.extend([0; 12]);
    bytes
}

/// A `msg_container` body holding `messages`.
fn container(messages: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = MessageContainer::CONSTRUCTOR_ID.to_le_bytes().to_vec();
    bytes.extend((messages.len() as i32).to_le_bytes());
    messages.iter().for_each(|message| bytes.extend(message));
    bytes
}

/// A `gzip_packed` object holding `body`.
fn gzip(body: &[u8]) -> Vec<u8> {
    GzipPacked::new(body).unwrap().to_bytes()
}

/// An `rpc_result` answering the request `req_msg_id` with `result`.
fn rpc_result(req_msg_id: i64, result: &[u8]) -> Vec<u8> {
    let mut bytes = manual_tl::RpcResult::CONSTRUCTOR_ID.to_le_bytes().to_vec();
    bytes.extend(req_msg_id.to_le_bytes());
    bytes.extend(result);
    bytes
}

/// The smallest `Updates`, as the server pushes it.
fn updates_too_long() -> Vec<u8> {
    tl::enums::Updates::TooLong.to_bytes()
}

/// `n` zero bytes in a `gzip_packed` object: they inflate to `n` bytes, from a small packet.
fn gzip_zeros(n: usize) -> Vec<u8> {
    gzip(&vec![0; n])
}

/// A `msgs_ack` body acknowledging `msg_ids`.
fn msgs_ack(msg_ids: Vec<i64>) -> Vec<u8> {
    tl::enums::MsgsAck::Ack(tl::types::MsgsAck { msg_ids }).to_bytes()
}

fn receive(mtp: &mut Encrypted, body: &[u8]) -> Result<Vec<Deserialization>, DeserializeError> {
    let packet = packet(mtp.session_id, &message(1, 1, body));
    mtp.deserialize_plaintext(&packet)
}

#[test]
fn wrong_session_id_is_dropped() {
    let mut mtp = mtp();
    let other_session = packet(
        mtp.session_id.wrapping_add(1),
        &message(1, 1, &updates_too_long()),
    );

    // Neither processed nor acknowledged.
    assert!(
        mtp.deserialize_plaintext(&other_session)
            .unwrap()
            .is_empty()
    );
    assert!(mtp.pending_ack.is_empty());

    // The same message in this session is.
    let this_session = packet(mtp.session_id, &message(5, 1, &updates_too_long()));
    let received = mtp.deserialize_plaintext(&this_session).unwrap();
    assert!(matches!(received.as_slice(), [Deserialization::Update(_)]));
    assert_eq!(mtp.pending_ack, vec![5]);
}

#[test]
fn unsolicited_future_salt_is_ignored() {
    let mut mtp = mtp();
    let mut future_salt = tl::types::FutureSalt::CONSTRUCTOR_ID.to_le_bytes().to_vec();
    future_salt.extend(0i32.to_le_bytes());
    future_salt.extend(i32::MAX.to_le_bytes());
    future_salt.extend(1234i64.to_le_bytes());

    assert!(receive(&mut mtp, &future_salt).unwrap().is_empty());
    assert_eq!(mtp.salts.len(), 1);
    assert_eq!(mtp.get_current_salt(), 0);
}

#[test]
fn future_salts_since_i32_min_sort_last() {
    let mut mtp = mtp();
    mtp.salt_request_msg_id = Some(MsgId(7));
    let salt = |valid_since, salt| tl::types::FutureSalt {
        valid_since,
        valid_until: i32::MAX,
        salt,
    };
    let future_salts = tl::enums::FutureSalts::Salts(tl::types::FutureSalts {
        req_msg_id: 7,
        now: 0,
        salts: tl::RawVec(vec![salt(100, 1), salt(i32::MIN, 2), salt(200, 3)]),
    })
    .to_bytes();

    // The answer to the salts the client asked for, sorted latest first: the salt valid since the
    // earliest time is the one in use.
    assert!(receive(&mut mtp, &future_salts).unwrap().is_empty());
    let valid_since: Vec<_> = mtp.salts.iter().map(|salt| salt.valid_since).collect();
    assert_eq!(valid_since, [200, 100, i32::MIN]);
    assert_eq!(mtp.get_current_salt(), 2);
}

#[test]
fn msg_copy_is_error() {
    let mut mtp = mtp();
    let mut msg_copy = MessageCopy::CONSTRUCTOR_ID.to_le_bytes().to_vec();
    msg_copy.extend(message(3, 1, &updates_too_long()));

    assert_eq!(
        receive(&mut mtp, &msg_copy).unwrap_err(),
        DeserializeError::UnexpectedConstructor {
            id: MessageCopy::CONSTRUCTOR_ID
        }
    );
}

#[test]
fn nested_container_is_error() {
    let nested = DeserializeError::UnexpectedConstructor {
        id: MessageContainer::CONSTRUCTOR_ID,
    };
    let inner = container(&[message(3, 1, &updates_too_long())]);

    // "A container [...] may not carry other simple containers", also when they are packed.
    let mut mtp = mtp();
    let after_an_update = container(&[message(3, 1, &updates_too_long()), message(5, 2, &inner)]);
    assert_eq!(receive(&mut mtp, &after_an_update).unwrap_err(), nested);
    assert_eq!(
        receive(&mut mtp, &container(&[message(5, 2, &gzip(&inner))])).unwrap_err(),
        nested
    );

    // One container, also a packed one, is read; what came before a bad one isn't returned.
    let received = receive(&mut mtp, &inner).unwrap();
    assert!(matches!(received.as_slice(), [Deserialization::Update(_)]));
    let received = receive(&mut mtp, &gzip(&inner)).unwrap();
    assert!(matches!(received.as_slice(), [Deserialization::Update(_)]));
}

#[test]
fn gzip_over_budget_is_error() {
    let mut mtp = mtp();
    assert_eq!(
        receive(&mut mtp, &gzip_zeros(MAX_UNPACKED_PER_PACKET + 1)).unwrap_err(),
        DeserializeError::DecompressionFailed
    );

    // Up to the budget is read, and each packet has its own.
    for _ in 0..2 {
        let received = receive(&mut mtp, &gzip_zeros(MAX_UNPACKED_PER_PACKET)).unwrap();
        assert!(matches!(
            received.as_slice(),
            [Deserialization::Update(update)] if update.len() == MAX_UNPACKED_PER_PACKET
        ));
    }

    // The result of a request draws on it too: a result over it fails that request.
    let received = receive(
        &mut mtp,
        &rpc_result(9, &gzip_zeros(MAX_UNPACKED_PER_PACKET + 1)),
    )
    .unwrap();
    assert!(matches!(
        received.as_slice(),
        [Deserialization::Failure(DeserializationFailure {
            msg_id: MsgId(9),
            error: DeserializeError::DecompressionFailed,
        })]
    ));
}

#[test]
fn gzip_budget_is_shared_across_a_container() {
    let half = MAX_UNPACKED_PER_PACKET / 2;
    let mut mtp = mtp();

    // Each fits alone, both don't.
    let over = container(&[
        message(3, 1, &gzip_zeros(half)),
        message(5, 1, &gzip_zeros(half + 4)),
    ]);
    assert_eq!(
        receive(&mut mtp, &over).unwrap_err(),
        DeserializeError::DecompressionFailed
    );

    // Both fill it exactly.
    let full = container(&[
        message(3, 1, &gzip_zeros(half)),
        message(5, 1, &gzip_zeros(half)),
    ]);
    assert_eq!(receive(&mut mtp, &full).unwrap().len(), 2);

    // A request's result and a packed update share it, in either order: the result's failure
    // is that request's, the update's is the packet's.
    let result_last = container(&[
        message(3, 1, &gzip_zeros(half)),
        message(5, 1, &rpc_result(9, &gzip_zeros(half + 4))),
    ]);
    let received = receive(&mut mtp, &result_last).unwrap();
    assert!(matches!(
        received.as_slice(),
        [
            Deserialization::Update(_),
            Deserialization::Failure(DeserializationFailure {
                msg_id: MsgId(9),
                error: DeserializeError::DecompressionFailed,
            })
        ]
    ));
    let result_first = container(&[
        message(3, 1, &rpc_result(9, &gzip_zeros(half))),
        message(5, 1, &gzip_zeros(half + 4)),
    ]);
    assert_eq!(
        receive(&mut mtp, &result_first).unwrap_err(),
        DeserializeError::DecompressionFailed
    );

    // A result that fails still draws what it inflated, so the packet can't repeat the work. Past
    // the budget, it exhausts it: the next result has nothing left, however small.
    let both_over = container(&[
        message(
            3,
            1,
            &rpc_result(9, &gzip_zeros(MAX_UNPACKED_PER_PACKET + 1)),
        ),
        message(5, 1, &rpc_result(11, &gzip_zeros(1))),
    ]);
    let received = receive(&mut mtp, &both_over).unwrap();
    assert!(matches!(
        received.as_slice(),
        [
            Deserialization::Failure(DeserializationFailure {
                msg_id: MsgId(9),
                error: DeserializeError::DecompressionFailed,
            }),
            Deserialization::Failure(DeserializationFailure {
                msg_id: MsgId(11),
                error: DeserializeError::DecompressionFailed,
            })
        ]
    ));

    // A wrong checksum is found after the output, which is drawn all the same.
    let mut bad_crc = GzipPacked::new(&vec![0; half]).unwrap().packed_data;
    let crc = bad_crc.len() - 8;
    bad_crc[crc] ^= 1;
    let bad_crc = GzipPacked {
        packed_data: bad_crc,
    }
    .to_bytes();
    let after_bad_crc = container(&[
        message(3, 1, &rpc_result(9, &bad_crc)),
        message(5, 1, &gzip_zeros(half + 4)),
    ]);
    assert_eq!(
        receive(&mut mtp, &after_bad_crc).unwrap_err(),
        DeserializeError::DecompressionFailed
    );
}

#[test]
fn gzip_capacity_bounded_across_a_container() {
    // Results whose gzip member is followed by four bytes claiming the whole budget. Bytes after
    // the member aren't read, so nothing checks the claim: each result keeps a buffer sized by
    // what it inflated, and the packet's results together stay small.
    let mut packed_data = GzipPacked::new(b"Hey!").unwrap().packed_data;
    packed_data.extend((MAX_UNPACKED_PER_PACKET as u32).to_le_bytes());
    let result = GzipPacked { packed_data }.to_bytes();
    let results: Vec<_> = (0..8)
        .map(|i| message(3 + 2 * i, 1, &rpc_result(9 + 2 * i, &result)))
        .collect();

    let received = receive(&mut mtp(), &container(&results)).unwrap();
    assert_eq!(received.len(), 8);
    let mut kept = 0;
    for deserialization in &received {
        let Deserialization::RpcResult(RpcResult { body, .. }) = deserialization else {
            panic!("not a result: {deserialization:?}");
        };
        assert_eq!(body, b"Hey!");
        kept += body.capacity();
    }
    assert!(kept < 1024 * 1024, "{kept} bytes kept for 32");
}

#[test]
fn gzip_nesting_limit_is_error() {
    let nested = |levels: u32| (0..levels).fold(updates_too_long(), |body, _| gzip(&body));
    let mut mtp = mtp();

    let received = receive(&mut mtp, &nested(MAX_GZIP_NESTING)).unwrap();
    assert!(matches!(
        received.as_slice(),
        [Deserialization::Update(update)] if *update == updates_too_long()
    ));
    assert_eq!(
        receive(&mut mtp, &nested(MAX_GZIP_NESTING + 1)).unwrap_err(),
        DeserializeError::DecompressionFailed
    );
}

#[test]
fn bad_message_length_is_error() {
    let mut mtp = mtp();
    let mut negative = message(1, 1, &updates_too_long());
    negative[12..16].copy_from_slice(&(-4i32).to_le_bytes());
    assert_eq!(
        mtp.deserialize_plaintext(&packet(mtp.session_id, &negative))
            .unwrap_err(),
        DeserializeError::NegativeMessageLength { got: -4 }
    );

    let mut long = message(1, 1, &updates_too_long());
    long[12..16].copy_from_slice(&i32::MAX.to_le_bytes());
    assert!(matches!(
        mtp.deserialize_plaintext(&packet(mtp.session_id, &long)),
        Err(DeserializeError::TooLongMessageLength { .. })
    ));

    assert_eq!(
        mtp.deserialize_plaintext(&mtp.session_id.to_le_bytes())
            .unwrap_err(),
        DeserializeError::MessageBufferTooSmall
    );
}

#[test]
fn acks_kept_when_their_message_does_not_fit() {
    let mut mtp = mtp();
    mtp.pending_ack = vec![1, 5, 9];

    // A full container takes no message: the acknowledgment waits, and so does the request.
    let mut buffer = DequeBuffer::with_capacity(0, 0);
    mtp.msg_count = manual_tl::MessageContainer::MAXIMUM_LENGTH;
    assert!(mtp.push(&mut buffer, b"Hey!").is_none());
    assert_eq!(mtp.pending_ack, vec![1, 5, 9]);

    // The next packet carries it first.
    mtp.msg_count = 0;
    let mut buffer = DequeBuffer::with_capacity(0, 0);
    assert!(mtp.push(&mut buffer, b"Hey!").is_some());
    assert!(mtp.pending_ack.is_empty());
    let ack = msgs_ack(vec![1, 5, 9]);
    assert_eq!(&buffer[16..16 + ack.len()], ack.as_slice());
}

#[test]
fn acks_beyond_8192_wait_for_the_next_msgs_ack() {
    // "Max 8192 IDs are allowed per constructor": the oldest 8192 go with the request, the rest
    // wait for the next message.
    let mut mtp = mtp();
    mtp.pending_ack = (0..8195).collect();

    let mut buffer = DequeBuffer::with_capacity(0, 0);
    assert!(mtp.push(&mut buffer, b"Hey!").is_some());
    assert_eq!(mtp.pending_ack, [8192, 8193, 8194]);
    let first = msgs_ack((0..8192).collect());
    assert_eq!(&buffer[16..16 + first.len()], first.as_slice());

    // The next request carries the rest, here in the same packet.
    let next = buffer.len() + 16;
    assert!(mtp.push(&mut buffer, b"Bye!").is_some());
    assert!(mtp.pending_ack.is_empty());
    let rest = msgs_ack(vec![8192, 8193, 8194]);
    assert_eq!(&buffer[next..next + rest.len()], rest.as_slice());
}
