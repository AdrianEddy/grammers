// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A client clock ahead of the server's: after `bad_msg_notification` 17 (`msg_id` too high), the
//! next message identifiers follow the corrected clock, in the same session while the server has
//! taken none of its messages, in a new one once it has; a notice about an identifier from before
//! the last correction changes neither. The identifiers come from the server's clock, which the
//! monotonic clock carries on, never from the machine's wall clock after it starts. Identifiers
//! are compared as the unsigned numbers they are, so they rise past i64::MAX, in 2038.

use super::*;
use crate::mtp::BadMessage;

/// 2020-01-01T00:00:00Z and 2021-01-01T00:00:00Z: server times years from the machine's clock.
const NEW_YEAR_2020: i64 = 1_577_836_800;
const NEW_YEAR_2021: i64 = 1_609_459_200;

fn now() -> i64 {
    ServerClock::wall_clock_since_epoch().as_secs() as i64
}

/// The seconds of a message identifier.
fn seconds(msg_id: i64) -> i64 {
    msg_id >> 32
}

/// Serializes a request in a packet of its own; returns its message identifier.
fn request(mtp: &mut Encrypted) -> i64 {
    let mut buffer = DequeBuffer::with_capacity(0, 0);
    let ping = tl::functions::Ping { ping_id: 1 }.to_bytes();
    let msg_id = mtp.push(&mut buffer, &ping).unwrap().into_inner();
    mtp.finalize(&mut buffer);
    msg_id
}

/// A message the server sends at `seconds` on its clock.
fn message(seconds: i64, body: &[u8]) -> Vec<u8> {
    let mut bytes = ((seconds << 32) | 1).to_le_bytes().to_vec();
    bytes.extend(1i32.to_le_bytes());
    bytes.extend((body.len() as i32).to_le_bytes());
    bytes.extend(body);
    bytes
}

/// Processes a packet carrying `bodies`, in a container if there are several, sent now.
fn receive(mtp: &mut Encrypted, bodies: &[Vec<u8>]) -> Vec<Deserialization> {
    receive_at(mtp, now(), bodies)
}

/// [`receive`], the packet sent at `seconds` on the server's clock.
fn receive_at(mtp: &mut Encrypted, seconds: i64, bodies: &[Vec<u8>]) -> Vec<Deserialization> {
    let message = match bodies {
        [body] => message(seconds, body),
        _ => {
            let mut container = manual_tl::MessageContainer::CONSTRUCTOR_ID
                .to_le_bytes()
                .to_vec();
            container.extend((bodies.len() as i32).to_le_bytes());
            bodies
                .iter()
                .for_each(|body| container.extend(self::message(seconds, body)));
            self::message(seconds, &container)
        }
    };
    let mut packet = 0i64.to_le_bytes().to_vec();
    packet.extend(mtp.session_id.to_le_bytes());
    packet.extend(message);
    packet.extend([0; 12]);
    mtp.deserialize_plaintext(&packet).unwrap()
}

fn too_high(bad_msg_id: i64) -> Vec<u8> {
    tl::enums::BadMsgNotification::Notification(tl::types::BadMsgNotification {
        bad_msg_id,
        bad_msg_seqno: 1,
        error_code: 17,
    })
    .to_bytes()
}

fn too_low(bad_msg_id: i64) -> Vec<u8> {
    tl::enums::BadMsgNotification::Notification(tl::types::BadMsgNotification {
        bad_msg_id,
        bad_msg_seqno: 1,
        error_code: 16,
    })
    .to_bytes()
}

fn bad_salt(bad_msg_id: i64) -> Vec<u8> {
    tl::enums::BadMsgNotification::BadServerSalt(tl::types::BadServerSalt {
        bad_msg_id,
        bad_msg_seqno: 1,
        error_code: 48,
        new_server_salt: 0,
    })
    .to_bytes()
}

fn session_created(first_msg_id: i64) -> Vec<u8> {
    tl::enums::NewSession::Created(tl::types::NewSessionCreated {
        first_msg_id,
        unique_id: 1,
        server_salt: 0,
    })
    .to_bytes()
}

fn rpc_result(req_msg_id: i64) -> Vec<u8> {
    let mut bytes = manual_tl::RpcResult::CONSTRUCTOR_ID.to_le_bytes().to_vec();
    bytes.extend(req_msg_id.to_le_bytes());
    bytes.extend(true.to_bytes());
    bytes
}

fn is_reset(results: &[Deserialization]) -> bool {
    results
        .iter()
        .any(|result| matches!(result, Deserialization::SessionReset))
}

#[test]
fn session_the_server_took_nothing_of_follows_the_corrected_clock() {
    // A clock 1000 s fast and a key from an earlier connection: no handshake gave it the server's
    // time, so the server refuses the first request, for its salt and then for its `msg_id`.
    let mut mtp = Encrypted::build().time_offset(1000).finish([0; 256]);
    let session = mtp.session_id;
    let sent = request(&mut mtp);
    assert!(!is_reset(&receive(&mut mtp, &[bad_salt(sent)])));
    let resent = request(&mut mtp);
    let results = receive(&mut mtp, &[too_high(resent)]);
    assert!(matches!(
        results.as_slice(),
        [Deserialization::BadMessage(BadMessage { code: 17, msg_id })] if *msg_id == MsgId(resent)
    ));

    // Nothing of the session was taken, so the next identifiers follow the server's clock in it.
    let next = request(&mut mtp);
    assert!(next < resent, "{next} is still above {resent}");
    assert!((seconds(next) - now()).abs() <= 5);
    assert_eq!(mtp.session_id, session);
}

#[test]
fn session_the_server_took_a_message_of_is_replaced() {
    let mut mtp = Encrypted::build().finish([0; 256]);
    let session = mtp.session_id;
    let first = request(&mut mtp);
    receive(&mut mtp, &[session_created(first), rpc_result(first)]);

    // The client's clock of the server's time gets 1000 s ahead (the server's own clock stepped
    // back, say): the next request is refused, beside something to acknowledge.
    mtp.clock.anchor += Duration::from_secs(1000);
    let sent = request(&mut mtp);
    let results = receive(
        &mut mtp,
        &[tl::enums::Updates::TooLong.to_bytes(), too_high(sent)],
    );
    assert!(matches!(
        results.as_slice(),
        [
            Deserialization::Update(_),
            Deserialization::BadMessage(BadMessage { code: 17, .. }),
            Deserialization::SessionReset,
        ]
    ));

    // The identifiers can't go back in a session the server took messages of: a new one starts,
    // with nothing of the old one's to acknowledge, its sequence from zero.
    assert_ne!(mtp.session_id, session);
    assert!(mtp.pending_ack.is_empty());
    let next = request(&mut mtp);
    assert!(next < sent);
    assert!((seconds(next) - now()).abs() <= 5);
    assert_eq!(mtp.sequence, 2);

    // A later notice about the new session's messages finds nothing taken in it.
    let again = request(&mut mtp);
    let session = mtp.session_id;
    assert!(!is_reset(&receive(&mut mtp, &[too_high(again)])));
    assert_eq!(mtp.session_id, session);

    // The server takes the new session's first message: its `new_session_created` reaches the
    // caller, a gap in the updates, which went to the old session.
    let taken = request(&mut mtp);
    assert!(matches!(
        receive(&mut mtp, &[session_created(taken)]).as_slice(),
        [Deserialization::NewSession]
    ));
}

#[test]
fn notice_about_an_identifier_before_the_correction_leaves_the_clock() {
    // A clock 1000 s fast, and two requests in flight before the server took anything: the notice
    // about the first corrects the clock, and the request goes again below the second.
    let mut mtp = Encrypted::build().time_offset(1000).finish([0; 256]);
    let session = mtp.session_id;
    let first = request(&mut mtp);
    let second = request(&mut mtp);
    let server_time = now();
    receive_at(&mut mtp, server_time, &[too_high(first)]);
    let resent = request(&mut mtp);
    assert!(resent < second);

    // Then the notice about the second: it names an identifier above every one issued since the
    // correction, so from before it. Correcting the clock from it again could set the clock back
    // (the seconds each correction is taken in may differ by one; 2 here, whatever the clock's
    // phase), and the next identifier below the one just sent, in the same session. It still
    // reaches the caller, which sends that request again, but leaves the clock.
    let results = receive_at(&mut mtp, server_time - 2, &[too_high(second)]);
    assert!(matches!(
        results.as_slice(),
        [Deserialization::BadMessage(BadMessage { code: 17, msg_id })] if *msg_id == MsgId(second)
    ));
    let next = request(&mut mtp);
    assert!(next > resent, "{next} went back below {resent}");
    assert_eq!(mtp.session_id, session);
}

#[test]
fn notice_about_an_identifier_before_the_correction_starts_no_session() {
    // As above, but the notice about the second comes beside the server's taking the request sent
    // again: a fresh notice would start a new session now, and the request taken would go again.
    let mut mtp = Encrypted::build().time_offset(1000).finish([0; 256]);
    let session = mtp.session_id;
    let first = request(&mut mtp);
    let second = request(&mut mtp);
    receive(&mut mtp, &[too_high(first)]);
    let resent = request(&mut mtp);
    let results = receive(&mut mtp, &[session_created(resent), too_high(second)]);
    assert!(matches!(
        results.as_slice(),
        [
            Deserialization::NewSession,
            Deserialization::BadMessage(BadMessage { code: 17, msg_id }),
        ] if *msg_id == MsgId(second)
    ));
    assert_eq!(mtp.session_id, session);
    assert!(request(&mut mtp) > resent);
}

#[test]
fn what_the_server_took_counts_wherever_it_is_in_the_packet() {
    let mut mtp = Encrypted::build().finish([0; 256]);
    let session = mtp.session_id;
    let first = request(&mut mtp);
    let second = request(&mut mtp);

    // The refusal of the second comes before the answer to the first: the server took the first,
    // so the session is replaced, after the answer reaches its request.
    let results = receive(&mut mtp, &[too_high(second), rpc_result(first)]);
    assert!(matches!(
        results.as_slice(),
        [
            Deserialization::BadMessage(BadMessage { code: 17, .. }),
            Deserialization::RpcResult(RpcResult { msg_id, .. }),
            Deserialization::SessionReset,
        ] if *msg_id == MsgId(first)
    ));
    assert_ne!(mtp.session_id, session);
}

#[test]
fn msg_id_too_low_stays_in_the_session() {
    // A clock behind the server's: the identifiers after the correction rise anyway.
    let mut mtp = Encrypted::build().time_offset(-1000).finish([0; 256]);
    let session = mtp.session_id;
    let first = request(&mut mtp);
    receive(&mut mtp, &[session_created(first)]);
    let sent = request(&mut mtp);
    assert!(!is_reset(&receive(&mut mtp, &[too_low(sent)])));
    assert_eq!(mtp.session_id, session);
    let next = request(&mut mtp);
    assert!(next > sent);
    assert!((seconds(next) - now()).abs() <= 5);
}

#[test]
fn identifiers_follow_the_server_clock_not_the_wall_clock() {
    // The server's clock, set 10 s ago at a time years from the machine's: the monotonic clock
    // carried it on by those 10 s, and the machine's wall clock, read only when the MTP was built,
    // plays no part. So a step of the wall clock on an open connection can't move the identifiers.
    let mut mtp = Encrypted::build().finish([0; 256]);
    mtp.clock = ServerClock {
        anchor: Duration::from_secs(NEW_YEAR_2020 as u64),
        at: Instant::now().checked_sub(Duration::from_secs(10)).unwrap(),
    };
    let sent = request(&mut mtp);
    let expected = NEW_YEAR_2020 + 10;
    assert!(
        (seconds(sent) - expected).abs() <= 1,
        "{} is not the server's {expected}",
        seconds(sent)
    );

    // A correction sets the clock from the server's own time, and it goes on from there.
    receive_at(&mut mtp, NEW_YEAR_2021, &[too_low(sent)]);
    let next = request(&mut mtp);
    assert!(
        (seconds(next) - NEW_YEAR_2021).abs() <= 1,
        "{} is not the server's {NEW_YEAR_2021}",
        seconds(next)
    );
}

/// 2038-01-19T03:14:07Z: the last second whose message identifiers are below i64::MAX, the most
/// the `long` they are sent in holds as a positive number.
const LAST_SECOND_BELOW_I64_MAX: u32 = (1 << 31) - 1;

#[test]
fn identifiers_past_i64_max_still_rise() {
    let mut mtp = Encrypted::build().finish([0; 256]);
    mtp.clock = ServerClock::at_server_time(LAST_SECOND_BELOW_I64_MAX);
    let before = request(&mut mtp);
    assert!(before > 0);

    // A second later the identifiers' top bit is set: the next one is the server's time all the
    // same, above the last as an unsigned number, rather than held just past it as if it were
    // below.
    mtp.clock.anchor += Duration::from_secs(1);
    let after = request(&mut mtp);
    assert_eq!(
        after.cast_unsigned() >> 32,
        u64::from(LAST_SECOND_BELOW_I64_MAX) + 1
    );
    assert!(MsgId(after) > MsgId(before));
}

#[test]
fn notice_about_an_identifier_issued_before_one_past_i64_max_corrects_the_clock() {
    // A clock 1000 s fast, at the last second below i64::MAX and then the first past it: two
    // requests in flight before the server took anything.
    let mut mtp = Encrypted::build().finish([0; 256]);
    let session = mtp.session_id;
    mtp.clock = ServerClock::at_server_time(LAST_SECOND_BELOW_I64_MAX);
    let first = request(&mut mtp);
    mtp.clock.anchor += Duration::from_secs(1);
    let second = request(&mut mtp);

    // The notice about the first names an identifier issued since the last correction, below the
    // second as unsigned numbers: it corrects the clock, in the same session.
    let server_time = i64::from(LAST_SECOND_BELOW_I64_MAX) - 1000;
    let results = receive_at(&mut mtp, server_time, &[too_high(first)]);
    assert!(matches!(
        results.as_slice(),
        [Deserialization::BadMessage(BadMessage { code: 17, msg_id })] if *msg_id == MsgId(first)
    ));
    let next = request(&mut mtp);
    assert!((seconds(next) - server_time).abs() <= 1);
    assert!(MsgId(next) < MsgId(first) && MsgId(first) < MsgId(second));
    assert_eq!(mtp.session_id, session);
}
