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
//! are compared as the unsigned numbers they are, so they rise past i64::MAX, in 2038. The salts'
//! validity is read on the same clock, modulo 2^32 as it holds seconds, past 2038 too. The
//! server's time in `future_salts` sets a clock still from the wall clock, back too, and after that
//! only moves it forward.

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
        at: Instant::now().checked_sub(Duration::from_secs(10)).unwrap(),
        ..ServerClock::at_server_time(NEW_YEAR_2020 as u32)
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

/// The server's answer to `get_future_salts` (its message `req_msg_id`), sent at `now` on its
/// clock: each salt with the time it is valid from. The times go as the protocol's `int`, the
/// seconds modulo 2^32.
fn future_salts(req_msg_id: i64, now: i64, salts: &[(i64, i64)]) -> Vec<u8> {
    let salts = salts
        .iter()
        .map(|&(valid_since, salt)| tl::types::FutureSalt {
            valid_since: valid_since as i32,
            valid_until: (valid_since + 3600) as i32,
            salt,
        })
        .collect();
    tl::enums::FutureSalts::Salts(tl::types::FutureSalts {
        req_msg_id,
        now: now as i32,
        salts: tl::RawVec(salts),
    })
    .to_bytes()
}

/// An MTP whose clock is at 2020-01-01, with three salts from the server then: the one in use, an
/// hour old, and the next two, valid from 500 s and from 5000 s on.
fn mtp_with_salts() -> Encrypted {
    let mut mtp = Encrypted::build().finish([0; 256]);
    mtp.clock = ServerClock::at_server_time(NEW_YEAR_2020 as u32);
    mtp.salt_request_msg_id = Some(MsgId(7));
    let salts = [
        (NEW_YEAR_2020 - 3600, 1),
        (NEW_YEAR_2020 + 500, 2),
        (NEW_YEAR_2020 + 5000, 3),
    ];
    receive_at(
        &mut mtp,
        NEW_YEAR_2020,
        &[future_salts(7, NEW_YEAR_2020, &salts)],
    );
    mtp
}

#[test]
fn salt_and_identifiers_follow_one_clock_after_a_correction() {
    let mut mtp = mtp_with_salts();
    let sent = request(&mut mtp);
    assert_eq!(mtp.get_current_salt(), 1);

    // The server's 16 says its clock is 1000 s ahead of the client's: on it, the next salt is
    // valid by now, so the next request takes both its identifier and its salt from the corrected
    // clock.
    receive_at(&mut mtp, NEW_YEAR_2020 + 1000, &[too_low(sent)]);
    let next = request(&mut mtp);
    assert!((seconds(next) - (NEW_YEAR_2020 + 1000)).abs() <= 1);
    assert_eq!(mtp.get_current_salt(), 2);
}

#[test]
fn a_correction_past_several_salts_drops_them_all() {
    let mut mtp = mtp_with_salts();
    let sent = request(&mut mtp);

    // The server's 16 says its clock is 6000 s ahead of the client's: on it, both later salts are
    // valid, so the next request skips the one between and takes the last, as TDLib's
    // `AuthData::update_salt` would. That leaves one salt, so it waits behind a request for more.
    receive_at(&mut mtp, NEW_YEAR_2020 + 6000, &[too_low(sent)]);
    let mut buffer = DequeBuffer::with_capacity(0, 0);
    let ping = tl::functions::Ping { ping_id: 1 }.to_bytes();
    let pushed = mtp.push(&mut buffer, &ping);
    assert_eq!(mtp.get_current_salt(), 3);
    assert!(pushed.is_none() && mtp.salt_request_msg_id.is_some());
}

#[test]
fn salts_across_2038_are_used_in_order() {
    // 2038-01-19T03:14:08Z, from which the protocol's `int` times read negative: an answer half an
    // hour before it, with the salt in use and the next two, both valid from after that second.
    let wrap = i64::from(LAST_SECOND_BELOW_I64_MAX) + 1;
    let mut mtp = Encrypted::build().finish([0; 256]);
    mtp.clock = ServerClock::at_server_time((wrap - 1800) as u32);
    mtp.salt_request_msg_id = Some(MsgId(7));
    let salts = [(wrap - 2800, 1), (wrap + 800, 2), (wrap + 4400, 3)];
    receive_at(
        &mut mtp,
        wrap - 1800,
        &[future_salts(7, wrap - 1800, &salts)],
    );
    let sent = request(&mut mtp);
    assert_eq!(mtp.get_current_salt(), 1);

    // On a clock moved past the second one's `valid_since` by more than `SALT_USE_DELAY` (by a
    // 16), it is the one in use, the third still to come.
    receive_at(&mut mtp, wrap + 900, &[too_low(sent)]);
    request(&mut mtp);
    assert_eq!(mtp.get_current_salt(), 2);
}

#[test]
fn first_future_salts_now_sets_a_clock_from_the_wall_clock_back_too() {
    // A key from an earlier connection and a wall clock 20 s fast: less than the 30 s the server
    // answers with a 17, so nothing else corrects the clock it starts from.
    let mut mtp = Encrypted::build().time_offset(20).finish([0; 256]);
    mtp.salt_request_msg_id = Some(MsgId(7));

    // The server's first time sets it, back as well: the next identifier, and the salt in use,
    // follow the server's time. On the fast clock the next salt, valid from 45 s ago, would be
    // past `SALT_USE_DELAY` already.
    let server_time = now();
    let salts = [
        (server_time - 3600, 1),
        (server_time - 45, 2),
        (server_time + 3600, 3),
    ];
    receive_at(
        &mut mtp,
        server_time,
        &[future_salts(7, server_time, &salts)],
    );
    let next = request(&mut mtp);
    assert!(
        (seconds(next) - server_time).abs() <= 1,
        "{} is not the server's {server_time}",
        seconds(next)
    );
    assert_eq!(mtp.get_current_salt(), 1);

    // After that the server's times only move it forward: an answer from 10 s before (one that
    // took long to come) leaves it.
    mtp.salt_request_msg_id = Some(MsgId(9));
    let before = server_time - 10;
    receive_at(&mut mtp, before, &[future_salts(9, before, &salts)]);
    let (clock, _) = mtp.clock.now();
    assert!((i64::from(clock) - server_time).abs() <= 1);
}

#[test]
fn future_salts_now_moves_a_server_set_clock_forward_never_back() {
    // On a clock the server's time set, it answers at a time 1000 s ahead: the server's time is at
    // least that, so the clock moves up to it, and the next identifier and the salt in use follow.
    let mut mtp = mtp_with_salts();
    mtp.salt_request_msg_id = Some(MsgId(9));
    let salts = [
        (NEW_YEAR_2020 - 3600, 1),
        (NEW_YEAR_2020 + 500, 2),
        (NEW_YEAR_2020 + 5000, 3),
    ];
    let ahead = NEW_YEAR_2020 + 1000;
    receive_at(&mut mtp, ahead, &[future_salts(9, ahead, &salts)]);
    let next = request(&mut mtp);
    assert!((seconds(next) - ahead).abs() <= 1);
    assert_eq!(mtp.get_current_salt(), 2);

    // An answer from a time behind the clock leaves it: the server's time is at least the latest
    // it said. The identifiers would rise anyway, a session's never fall; the salt shows the
    // clock: of the same three salts again, the next one is valid on it, so it is the one in use.
    mtp.salt_request_msg_id = Some(MsgId(11));
    receive_at(&mut mtp, ahead, &[future_salts(11, NEW_YEAR_2020, &salts)]);
    let later = request(&mut mtp);
    assert!((seconds(later) - ahead).abs() <= 1);
    assert_eq!(mtp.get_current_salt(), 2);
}
