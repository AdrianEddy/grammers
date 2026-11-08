// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Message identifiers from 2038-01-19T03:14:08Z on, whose top bit is set: they are the unsigned
//! numbers they always were, though the `long` they are carried in reads negative.

use grammers_mtproto::mtp::{Deserialization, DeserializeError, Mtp, Plain};

/// A plain message from the server carrying `data`, its identifier `msg_id`.
fn plain_message(msg_id: u64, data: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(0i64.to_le_bytes()); // auth_key_id
    bytes.extend(msg_id.to_le_bytes());
    bytes.extend((data.len() as i32).to_le_bytes());
    bytes.extend(data);
    bytes
}

#[test]
fn plain_answer_from_past_2038_is_read() {
    // Sent at 2038-01-19T03:14:08Z: its identifier is above i64::MAX, and still a response's (1
    // modulo 4).
    let msg_id = (1 << 63) | 1;
    let answer = Plain::new()
        .deserialize(&mut plain_message(msg_id, &[1, 2, 3, 4]))
        .unwrap();
    assert!(matches!(
        answer.as_slice(),
        [Deserialization::RpcResult(result)] if result.body == [1, 2, 3, 4]
    ));

    // One that isn't a response's is refused, whatever its top bit.
    for msg_id in [msg_id + 2, 3, 0] {
        assert_eq!(
            Plain::new()
                .deserialize(&mut plain_message(msg_id, &[1, 2, 3, 4]))
                .unwrap_err(),
            DeserializeError::BadMessageId {
                got: msg_id.cast_signed()
            }
        );
    }
}
