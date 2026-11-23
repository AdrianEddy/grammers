// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The account's deadlines fall due while a channel gets its difference
//! ([`MessageBoxes::check_deadlines`]): its possible gaps' and its idle deadline begin its
//! difference beside the channel's, and while its retry is armed a possible gap of the account's
//! is still dropped after its half second.

use super::*;

/// [`settled`] boxes whose [`CHANNEL`] gets its difference, unanswered yet.
fn channel_pending(now: Now) -> MessageBoxes {
    let mut message_boxes = settled(now);
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    message_boxes
}

fn both_pending() -> DiffPending {
    DiffPending {
        account: true,
        channels: 1,
    }
}

fn channel_only() -> DiffPending {
    DiffPending {
        account: false,
        channels: 1,
    }
}

#[test]
fn the_accounts_possible_gap_falls_due_while_a_channel_gets_its_difference() {
    let mut message_boxes = channel_pending(T0);
    // 102 waits for 101 in the common box's possible gap.
    let (held, _, _) = message_boxes
        .process_updates(updates(11, NO_SEQ, 102), T0)
        .unwrap();
    assert!(held.is_empty());

    // Not yet: the gap may fill for half a second.
    let early = T0 + POSSIBLE_GAP_TIMEOUT / 2;
    assert_eq!(message_boxes.check_deadlines(early), early);
    assert_eq!(message_boxes.difference_pending(), channel_only());

    // Its half second over, the account's difference begins beside the channel's.
    let half = T0 + POSSIBLE_GAP_TIMEOUT;
    assert_eq!(message_boxes.check_deadlines(half), half);
    assert_eq!(message_boxes.difference_pending(), both_pending());
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 100, 50))
    );
}

#[test]
fn the_accounts_idle_deadline_falls_due_while_a_channel_gets_its_difference() {
    let mut message_boxes = channel_pending(T0);
    let idle = next_updates_deadline(T0);
    let before = idle - Duration::from_secs(1);
    assert_eq!(message_boxes.check_deadlines(before), before);
    assert_eq!(message_boxes.difference_pending(), channel_only());

    assert_eq!(message_boxes.check_deadlines(idle), idle);
    assert_eq!(message_boxes.difference_pending(), both_pending());
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 100, 50))
    );

    // Answered, the account's next idle deadline runs from then.
    message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 11, seq: 5 }.into(),
        idle,
    );
    message_boxes.apply_channel_difference(CHANNEL, channel_difference_empty(20), idle);
    assert_eq!(
        time_of_next_difference(&mut message_boxes, idle),
        next_updates_deadline(idle)
    );
}

#[test]
fn the_accounts_possible_gap_is_dropped_while_a_channel_gets_its_difference_and_the_retry_waits() {
    let mut message_boxes = settled(T0);
    assert!(message_boxes.begin_account_difference());
    assert!(message_boxes.end_account_difference(T0).is_empty());
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    // 102 waits for 101 in the common box's possible gap, while the retry waits for T0 + 1 s.
    let (held, _, _) = message_boxes
        .process_updates(updates(11, NO_SEQ, 102), T0)
        .unwrap();
    assert!(held.is_empty());

    // Its half second over, the gap is dropped: the retry is the account's one deadline.
    let half = T0 + POSSIBLE_GAP_TIMEOUT;
    assert_eq!(message_boxes.check_deadlines(half), half);
    assert_eq!(message_boxes.difference_pending(), channel_only());
    // A late 101 is handed out alone: 102 is the retry's difference's to return.
    let (updates, _, _) = message_boxes
        .process_updates(updates(11, NO_SEQ, 101), half)
        .unwrap();
    assert_eq!(pts_of(&updates), [101]);

    // The retry still falls due at its time, beside the channel's difference.
    let due = T0 + Duration::from_secs(1);
    assert_eq!(message_boxes.check_deadlines(due), due);
    assert_eq!(message_boxes.difference_pending(), both_pending());
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 101, 50))
    );
}
