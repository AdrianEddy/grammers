// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! An update with a wrong `qts` is dropped and schedules the account's difference after the
//! backoff's next step, as TDLib's `add_pending_qts_update` does; a wrong `pts` is only dropped.

use super::*;

/// `updates` with the common box's next update and one whose `qts` is wrong (1, with a count of
/// 1: the secondary box would have stood at 0).
fn with_wrong_qts(date: i32, pts: i32) -> UpdatesLike {
    UpdatesLike::Updates(tl::enums::Updates::Updates(tl::types::Updates {
        updates: vec![update(pts), qts_update(1)],
        users: Vec::new(),
        chats: Vec::new(),
        date,
        seq: NO_SEQ,
    }))
}

#[test]
fn wrong_qts_schedules_the_accounts_difference() {
    let mut message_boxes = settled(T0);
    let (updates, _, _) = message_boxes
        .process_updates(with_wrong_qts(11, 101), T0)
        .unwrap();
    // The update that follows on is handed out; the wrong one is dropped.
    assert_eq!(updates.len(), 1);
    assert_eq!(
        updates[0].1.message_box,
        Some(MessageBox::Common { pts: 101 })
    );
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    assert_eq!(
        message_boxes.take_delta(),
        common_delta(101, 11, 5),
        "the secondary box didn't move"
    );

    // The difference comes after the backoff's first step, not at once.
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        T0 + Duration::from_secs(1)
    );
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(11, 101, 50))
    );
}

#[test]
fn wrong_qts_while_the_account_gets_its_difference_schedules_nothing_more() {
    let mut message_boxes = settled(T0);
    assert_eq!(
        message_boxes.process_updates(UpdatesLike::ConnectionClosed, T0),
        Err(Gap)
    );
    message_boxes.process_updates(qts_updates(1), T0).unwrap();
    // The running difference covers it: once answered, the next is the idle one.
    message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 11, seq: 5 }.into(),
        T0,
    );
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        next_updates_deadline(T0)
    );
}

#[test]
fn wrong_qts_while_a_retry_is_armed_changes_nothing() {
    let mut message_boxes = settled(T0);
    assert_eq!(
        message_boxes.process_updates(UpdatesLike::ConnectionClosed, T0),
        Err(Gap)
    );
    assert!(message_boxes.end_account_difference(T0).is_empty());
    // The retry armed for T0 + 1 s stays, and its step isn't doubled.
    message_boxes
        .process_updates(qts_updates(1), T0 + POSSIBLE_GAP_TIMEOUT)
        .unwrap();
    let due = time_of_next_difference(&mut message_boxes, T0 + POSSIBLE_GAP_TIMEOUT);
    assert_eq!(due, T0 + Duration::from_secs(1));
    assert!(message_boxes.end_account_difference(due).is_empty());
    assert_eq!(
        time_of_next_difference(&mut message_boxes, due),
        due + Duration::from_secs(2)
    );
}

#[test]
fn wrong_qts_steps_the_backoff_like_a_failed_difference() {
    let mut message_boxes = settled(T0);
    message_boxes.process_updates(qts_updates(1), T0).unwrap();
    let due = time_of_next_difference(&mut message_boxes, T0);
    assert_eq!(due, T0 + Duration::from_secs(1));
    // The scheduled difference fails too: the next wait is the backoff's second step.
    assert!(message_boxes.end_account_difference(due).is_empty());
    assert_eq!(
        time_of_next_difference(&mut message_boxes, due),
        due + Duration::from_secs(2)
    );
}

#[test]
fn wrong_pts_schedules_nothing() {
    let mut message_boxes = settled(T0);
    // A count not below its `pts`: in the common box, and in a channel's.
    for wrong in [
        updates(11, NO_SEQ, 1),
        channel_updates(CHANNEL, 11, NO_SEQ, 1),
    ] {
        let (updates, _, _) = message_boxes.process_updates(wrong, T0).unwrap();
        assert!(updates.is_empty());
    }
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        next_updates_deadline(T0)
    );
}
