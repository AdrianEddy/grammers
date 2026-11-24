// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A channel's deadlines fall due while another channel gets its difference
//! ([`MessageBoxes::check_deadlines`]): its possible gap's and its retry's begin its difference at
//! their time, behind the channel waiting already, not once that one's difference ends.

use super::*;

/// The second channel, which [`settled`] boxes hold at [`OTHER_PTS`].
const OTHER: i64 = 2000;
const OTHER_PTS: i32 = 40;

/// [`settled`] boxes that also hold [`OTHER`], and whose [`CHANNEL`] gets its difference,
/// unanswered yet: a long catch-up.
fn catching_up(now: Now) -> MessageBoxes {
    let mut message_boxes = settled(now);
    message_boxes.try_set_channel_state(OTHER, OTHER_PTS);
    message_boxes.take_delta();
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    message_boxes
}

fn pending(channels: usize) -> DiffPending {
    DiffPending {
        account: false,
        channels,
    }
}

#[test]
fn a_channels_possible_gap_falls_due_while_another_channel_gets_its_difference() {
    let mut message_boxes = catching_up(T0);
    // OTHER's 42 waits for 41 in its possible gap.
    let (held, _, _) = message_boxes
        .process_updates(channel_updates(OTHER, NO_DATE, NO_SEQ, OTHER_PTS + 2), T0)
        .unwrap();
    assert!(held.is_empty());

    // Not yet: the gap may fill for half a second.
    let early = T0 + POSSIBLE_GAP_TIMEOUT / 2;
    assert_eq!(message_boxes.check_deadlines(early), early);
    assert_eq!(message_boxes.difference_pending(), pending(1));

    // Its half second over, OTHER's difference begins, behind CHANNEL's.
    let half = T0 + POSSIBLE_GAP_TIMEOUT;
    assert_eq!(message_boxes.check_deadlines(half), half);
    assert_eq!(message_boxes.difference_pending(), pending(2));
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(CHANNEL, 20))
    );
    // A 41 that comes now waits for that difference, which returns it.
    let (postponed, _, _) = message_boxes
        .process_updates(channel_updates(OTHER, NO_DATE, NO_SEQ, OTHER_PTS + 1), half)
        .unwrap();
    assert!(postponed.is_empty());

    // CHANNEL's difference answered, OTHER's is asked from its own `pts`.
    message_boxes.apply_channel_difference(CHANNEL, channel_difference_empty(20), half);
    assert_eq!(message_boxes.difference_pending(), pending(1));
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(OTHER, OTHER_PTS))
    );
}

#[test]
fn a_channels_retry_falls_due_while_another_channel_gets_its_difference() {
    let mut message_boxes = catching_up(T0);
    // OTHER's difference failed for a temporary reason first: its retry waits a second.
    assert!(message_boxes.begin_channel_difference(OTHER));
    assert!(
        message_boxes
            .end_channel_difference(OTHER, PrematureEndReason::TemporaryServerIssues, T0)
            .is_empty()
    );
    assert_eq!(message_boxes.difference_pending(), pending(1));

    let early = T0 + Duration::from_millis(500);
    assert_eq!(message_boxes.check_deadlines(early), early);
    assert_eq!(message_boxes.difference_pending(), pending(1));

    // At its time, beside CHANNEL's catch-up.
    let due = T0 + Duration::from_secs(1);
    assert_eq!(message_boxes.check_deadlines(due), due);
    assert_eq!(message_boxes.difference_pending(), pending(2));
    message_boxes.apply_channel_difference(CHANNEL, channel_difference_empty(20), due);
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(OTHER, OTHER_PTS))
    );
}

#[test]
fn deadlines_checked_while_differences_are_pending_wake_no_earlier_than_due() {
    let mut message_boxes = catching_up(T0);
    message_boxes
        .process_updates(channel_updates(OTHER, NO_DATE, NO_SEQ, OTHER_PTS + 2), T0)
        .unwrap();
    // A check while CHANNEL's difference is pending notes OTHER's gap, then nothing is due until
    // it: once CHANNEL's difference ends, the next deadline is the gap's.
    assert_eq!(message_boxes.check_deadlines(T0), T0);
    message_boxes.apply_channel_difference(CHANNEL, channel_difference_empty(20), T0);
    assert_eq!(message_boxes.check_deadlines(T0), T0 + POSSIBLE_GAP_TIMEOUT);
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        T0 + POSSIBLE_GAP_TIMEOUT
    );
    assert_eq!(message_boxes.difference_pending(), pending(1));
}
