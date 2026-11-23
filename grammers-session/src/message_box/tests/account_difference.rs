// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The account's difference ended without an answer ([`MessageBoxes::end_account_difference`]):
//! retried after a backoff that only the caller cuts short
//! ([`MessageBoxes::begin_account_difference`]), due also while a channel gets its difference,
//! with the channels' deadlines checked meanwhile, the postponed updates that follow on handed
//! out, the containers' `seq` held for the retry and the account's possible gaps dropped after
//! their half second; and the account's `pts` restored from the server's after it refused the
//! account's state ([`MessageBoxes::restore_pts`]), the rest of that state kept.

use super::*;

/// The box and place of each update handed out, in order.
fn boxes_of(updates: &[(tl::enums::Update, State)]) -> Vec<MessageBox> {
    updates
        .iter()
        .map(|(_, state)| state.message_box.unwrap())
        .collect()
}

/// [`settled`] boxes whose account's difference was begun at `now` by a closed connection.
fn getting_account_difference(now: Now) -> MessageBoxes {
    let mut message_boxes = settled(now);
    assert_eq!(
        message_boxes.process_updates(UpdatesLike::ConnectionClosed, now),
        Err(Gap)
    );
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 100, 50))
    );
    message_boxes
}

fn account_pending() -> DiffPending {
    DiffPending {
        account: true,
        channels: 0,
    }
}

#[test]
fn unanswered_account_difference_is_retried_after_a_backoff() {
    let mut message_boxes = getting_account_difference(T0);
    // Seeded: the waits at the cap are 64 to 80 s.
    message_boxes.seed_retry_jitter(SEED);

    let mut now = T0;
    for step in [1, 2, 4, 8, 16, 32, 64, 64] {
        assert!(message_boxes.end_account_difference(now).is_empty());
        assert_eq!(message_boxes.difference_pending(), DiffPending::default());
        let due = time_of_next_difference(&mut message_boxes, now);
        assert_account_wait(due - now, step);
        assert_eq!(
            message_boxes.get_difference(),
            Some(get_difference(10, 100, 50))
        );
        now = due;
    }
    // The state never moved.
    assert_eq!(message_boxes.take_delta(), StateDelta::default());

    // An answer starts the backoff over.
    message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 11, seq: 5 }.into(),
        now,
    );
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    assert_eq!(
        message_boxes.process_updates(UpdatesLike::ConnectionClosed, now),
        Err(Gap)
    );
    assert!(message_boxes.end_account_difference(now).is_empty());
    assert_eq!(
        time_of_next_difference(&mut message_boxes, now),
        now + Duration::from_secs(1)
    );
}

#[test]
fn ending_an_account_difference_not_being_fetched_changes_nothing() {
    let mut message_boxes = settled(T0);
    assert!(message_boxes.end_account_difference(T0).is_empty());
    // No retry was armed: the next difference is the idle one.
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        next_updates_deadline(T0)
    );
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
}

#[test]
fn channel_deadlines_fire_while_the_account_waits_for_its_retry() {
    let mut message_boxes = getting_account_difference(T0);
    assert!(message_boxes.end_account_difference(T0).is_empty());

    // A channel's gap, while the account's retry is armed for T0 + 1 s: its difference falls due
    // after the half second a gap may wait, before the account's.
    let (updates, _, _) = message_boxes
        .process_updates(channel_updates(CHANNEL, 11, NO_SEQ, 22), T0)
        .unwrap();
    assert!(updates.is_empty(), "held in the channel's possible gap");
    let due = time_of_next_difference(&mut message_boxes, T0);
    assert_eq!(due, T0 + POSSIBLE_GAP_TIMEOUT);
    assert_eq!(
        message_boxes.difference_pending(),
        DiffPending {
            account: false,
            channels: 1
        }
    );
    let request = message_boxes.get_channel_difference().unwrap();
    assert_eq!(channel_of(&request), CHANNEL);
    message_boxes.apply_channel_difference(CHANNEL, channel_difference_empty(22), due);

    // The account's retry still comes at its time.
    assert_eq!(
        time_of_next_difference(&mut message_boxes, due),
        T0 + Duration::from_secs(1)
    );
    assert_eq!(message_boxes.difference_pending(), account_pending());
}

#[test]
fn gaps_wait_for_the_accounts_retry() {
    for (gap, handed_out) in [
        // A `pts` gap: held in the common box's possible gap, whose half second doesn't begin the
        // difference.
        (updates(11, NO_SEQ, 102), Ok(Vec::new())),
        // A `seq` gap: its `seq` held for the retry's difference, its update, which follows on,
        // handed out.
        (
            updates(11, 7, 101),
            Ok(vec![MessageBox::Common { pts: 101 }]),
        ),
        (UpdatesLike::Updates(tl::enums::Updates::TooLong), Err(Gap)),
        (UpdatesLike::ConnectionClosed, Err(Gap)),
        (UpdatesLike::Lagged, Err(Gap)),
        (UpdatesLike::MalformedUpdates, Err(Gap)),
        (UpdatesLike::NewSession, Err(Gap)),
    ] {
        let mut message_boxes = getting_account_difference(T0);
        assert!(message_boxes.end_account_difference(T0).is_empty());
        let result = message_boxes
            .process_updates(gap, T0)
            .map(|(updates, _, _)| boxes_of(&updates));
        assert_eq!(result, handed_out);
        assert_eq!(message_boxes.difference_pending(), DiffPending::default());
        assert_eq!(
            time_of_next_difference(&mut message_boxes, T0),
            T0 + Duration::from_secs(1)
        );
    }
}

#[test]
fn the_accounts_retry_falls_due_while_a_channel_gets_its_difference() {
    let mut message_boxes = getting_account_difference(T0);
    assert!(message_boxes.end_account_difference(T0).is_empty());
    // A channel's catch-up the server answers in parts: still pending when the retry is due.
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    let half = T0 + POSSIBLE_GAP_TIMEOUT;
    message_boxes.apply_channel_difference(CHANNEL, channel_difference(21, 21, false), half);
    assert_eq!(message_boxes.check_deadlines(half), half);
    assert_eq!(
        message_boxes.difference_pending(),
        DiffPending {
            account: false,
            channels: 1
        },
        "not due yet"
    );

    let due = T0 + Duration::from_secs(1);
    message_boxes.apply_channel_difference(CHANNEL, channel_difference(22, 22, false), due);
    assert_eq!(message_boxes.check_deadlines(due), due);
    assert_eq!(
        message_boxes.difference_pending(),
        DiffPending {
            account: true,
            channels: 1
        }
    );
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 100, 50))
    );
}

#[test]
fn updates_that_follow_on_are_handed_out_after_an_unanswered_end() {
    let mut message_boxes = getting_account_difference(T0);
    // While the difference runs, the socket brings the common box's next update, one after a gap,
    // and the secondary box's next one; a container's `seq` is held too.
    for postponed in [
        updates(11, NO_SEQ, 101),
        updates(11, NO_SEQ, 104),
        qts_updates(51),
        updates(12, 6, 102),
    ] {
        let (updates, _, _) = message_boxes.process_updates(postponed, T0).unwrap();
        assert!(updates.is_empty(), "postponed");
    }

    // The ones that follow on from the kept state are handed out: 101 and 102, and qts 51; 104,
    // after the gap, is dropped for the retry to return. The `seq` the container carried isn't
    // taken.
    let ended = message_boxes.end_account_difference(T0);
    let boxes: Vec<_> = ended
        .iter()
        .map(|(_, state)| state.message_box.unwrap())
        .collect();
    assert_eq!(
        boxes,
        [
            MessageBox::Secondary { qts: 51 },
            MessageBox::Common { pts: 101 },
            MessageBox::Common { pts: 102 },
        ]
    );
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            common: Some(CommonState {
                pts: 102,
                date: 10,
                seq: 5
            }),
            qts: Some(51),
            ..StateDelta::default()
        }
    );
    assert_eq!(message_boxes.get_difference(), None);

    // The next container is past the local `seq`, which stayed 5. Its `seq` is held for the
    // retry's difference, and its update, which follows on, is handed out.
    let (updates, _, _) = message_boxes
        .process_updates(updates(13, 7, 103), T0)
        .unwrap();
    assert_eq!(boxes_of(&updates), [MessageBox::Common { pts: 103 }]);
    assert_eq!(message_boxes.take_delta(), common_delta(103, 10, 5));

    // The retry's difference starts from there.
    let due = time_of_next_difference(&mut message_boxes, T0);
    assert_eq!(due, T0 + Duration::from_secs(1));
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 103, 51))
    );
}

#[test]
fn containers_past_the_seq_are_handed_out_while_the_retry_waits() {
    let mut message_boxes = getting_account_difference(T0);
    assert!(message_boxes.end_account_difference(T0).is_empty());

    // Two containers past the local `seq` (5): the common box's next update, and a channel's.
    let (updates, _, _) = message_boxes
        .process_updates(updates(11, 7, 101), T0)
        .unwrap();
    assert_eq!(boxes_of(&updates), [MessageBox::Common { pts: 101 }]);
    let (updates, _, _) = message_boxes
        .process_updates(channel_updates(CHANNEL, 11, 8, 21), T0)
        .unwrap();
    assert_eq!(
        boxes_of(&updates),
        [MessageBox::Channel {
            channel_id: CHANNEL,
            pts: 21
        }]
    );
    // Their `date` and `seq` are the retry's difference's to set.
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            common: Some(CommonState {
                pts: 101,
                date: 10,
                seq: 5
            }),
            channels: vec![ChannelState {
                id: CHANNEL,
                pts: 21
            }],
            ..StateDelta::default()
        }
    );

    // The retry still falls due at its time. Its answer sets `seq`, and covers both containers:
    // no other difference begins.
    let due = time_of_next_difference(&mut message_boxes, T0);
    assert_eq!(due, T0 + Duration::from_secs(1));
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 101, 50))
    );
    message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 12, seq: 8 }.into(),
        due,
    );
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    assert_eq!(message_boxes.take_delta(), common_delta(101, 12, 8));
    assert_eq!(
        time_of_next_difference(&mut message_boxes, due),
        next_updates_deadline(due)
    );
}

#[test]
fn a_possible_gap_of_the_accounts_is_dropped_after_its_half_second_while_the_retry_waits() {
    let mut message_boxes = getting_account_difference(T0);
    assert!(message_boxes.end_account_difference(T0).is_empty());
    // 102 waits for 101 in the common box's possible gap.
    let (held, _, _) = message_boxes
        .process_updates(updates(11, NO_SEQ, 102), T0)
        .unwrap();
    assert!(held.is_empty());

    // Its half second over, the gap is dropped, and the retry is still the only difference due,
    // at its time.
    let half = T0 + POSSIBLE_GAP_TIMEOUT;
    assert_eq!(
        message_boxes.check_deadlines(half),
        T0 + Duration::from_secs(1)
    );
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    // A late 101 is handed out alone: 102 is the retry's difference's to return.
    let (updates, _, _) = message_boxes
        .process_updates(updates(11, NO_SEQ, 101), half)
        .unwrap();
    assert_eq!(boxes_of(&updates), [MessageBox::Common { pts: 101 }]);
    assert_eq!(
        time_of_next_difference(&mut message_boxes, half),
        T0 + Duration::from_secs(1)
    );
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 101, 50))
    );
}

#[test]
fn begin_account_difference_cuts_the_retry_short() {
    let mut message_boxes = getting_account_difference(T0);
    assert!(message_boxes.end_account_difference(T0).is_empty());

    // A new connection, before the retry's time: the difference is begun at once.
    assert!(message_boxes.begin_account_difference());
    assert_eq!(message_boxes.difference_pending(), account_pending());
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 100, 50))
    );
    // Begun again while pending: nothing more.
    assert!(message_boxes.begin_account_difference());
    assert_eq!(message_boxes.difference_pending(), account_pending());

    // Answered: the retry it replaced doesn't fire; the next difference is the idle one.
    let half = T0 + POSSIBLE_GAP_TIMEOUT;
    message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 11, seq: 5 }.into(),
        half,
    );
    assert_eq!(
        time_of_next_difference(&mut message_boxes, half),
        next_updates_deadline(half)
    );
}

#[test]
fn begin_account_difference_keeps_the_backoffs_step() {
    let mut message_boxes = getting_account_difference(T0);
    assert!(message_boxes.end_account_difference(T0).is_empty());
    assert!(message_boxes.begin_account_difference());

    // Unanswered again: the retry waits the backoff's second step.
    let half = T0 + POSSIBLE_GAP_TIMEOUT;
    assert!(message_boxes.end_account_difference(half).is_empty());
    assert_eq!(
        time_of_next_difference(&mut message_boxes, half),
        half + Duration::from_secs(2)
    );
}

#[test]
fn begin_account_difference_needs_the_common_state() {
    let mut message_boxes = MessageBoxes::new();
    assert!(!message_boxes.begin_account_difference());
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    assert_eq!(message_boxes.get_difference(), None);
}

#[test]
fn a_box_drops_its_own_postponed_updates_after_its_gap() {
    // A secondary box ahead of the common one, so that the common box's gap sorts first.
    let state = UpdatesState {
        pts: 10,
        qts: 50,
        date: 10,
        seq: 5,
        channels: Vec::new(),
    };
    let mut message_boxes = MessageBoxes::load(state, T0);
    assert_eq!(message_boxes.difference_pending(), account_pending());
    // The common box's next update is missing; the secondary box's follow on.
    for postponed in [updates(11, NO_SEQ, 12), qts_updates(51), qts_updates(52)] {
        message_boxes.process_updates(postponed, T0).unwrap();
    }
    let ended = message_boxes.end_account_difference(T0);
    let boxes: Vec<_> = ended
        .iter()
        .map(|(_, state)| state.message_box.unwrap())
        .collect();
    assert_eq!(
        boxes,
        [
            MessageBox::Secondary { qts: 51 },
            MessageBox::Secondary { qts: 52 },
        ]
    );
}

#[test]
fn restore_pts_moves_only_the_common_box() {
    let mut message_boxes = getting_account_difference(T0);
    // Postponed while the difference runs: the common box's next update.
    message_boxes
        .process_updates(updates(11, NO_SEQ, 101), T0)
        .unwrap();

    // The server refused the difference's state, and `updates.getState` answered pts 500.
    assert!(
        message_boxes.restore_pts(500, T0).is_empty(),
        "101 is the jump's"
    );
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    // Only the common box moved: the qts (50), the date (10) and the seq (5) are kept.
    assert_eq!(message_boxes.take_delta(), common_delta(500, 10, 5));
    // The channels keep theirs.
    assert_eq!(
        message_boxes.session_state().channels,
        [ChannelState {
            id: CHANNEL,
            pts: 20
        }]
    );
    // The next update follows on from the new pts.
    let (updates, _, _) = message_boxes
        .process_updates(updates(NO_DATE, NO_SEQ, 501), T0)
        .unwrap();
    assert_eq!(boxes_of(&updates), [MessageBox::Common { pts: 501 }]);
    // The idle deadline starts over, and the next difference asks from the new pts with the kept
    // date and qts.
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        next_updates_deadline(T0)
    );
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 501, 50))
    );
}

#[test]
fn restore_pts_hands_out_what_follows_on_from_the_kept_state() {
    let mut message_boxes = getting_account_difference(T0);
    // Postponed while the difference runs: the secondary box's next update, and a container that
    // follows on from the local `seq` (5).
    for postponed in [qts_updates(51), updates(12, 6, 101)] {
        let (updates, _, _) = message_boxes.process_updates(postponed, T0).unwrap();
        assert!(updates.is_empty(), "postponed");
    }

    // The server's pts covers the common box's update; the secondary box's follows on from the
    // kept qts, and the container's `seq` from the kept one (the date stays).
    let restored = message_boxes.restore_pts(500, T0);
    assert_eq!(boxes_of(&restored), [MessageBox::Secondary { qts: 51 }]);
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            common: Some(CommonState {
                pts: 500,
                date: 10,
                seq: 6
            }),
            qts: Some(51),
            ..StateDelta::default()
        }
    );
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
}

#[test]
fn restore_pts_begins_the_difference_for_a_container_past_the_kept_seq() {
    let mut message_boxes = getting_account_difference(T0);
    // Postponed while the difference runs: a container past the local `seq` (5).
    let (updates, _, _) = message_boxes
        .process_updates(updates(12, 7, 101), T0)
        .unwrap();
    assert!(updates.is_empty(), "postponed");

    // The container before it is missing: the account's difference begins again, from the new pts
    // with the kept date and qts.
    assert!(message_boxes.restore_pts(500, T0).is_empty());
    assert_eq!(message_boxes.difference_pending(), account_pending());
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 500, 50))
    );
}

#[test]
fn restore_pts_cancels_the_accounts_retry() {
    let mut message_boxes = getting_account_difference(T0);
    assert!(message_boxes.end_account_difference(T0).is_empty());

    assert!(message_boxes.restore_pts(500, T0).is_empty());
    assert_eq!(message_boxes.take_delta(), common_delta(500, 10, 5));
    // No retry is armed, so a gap begins the account's difference at once.
    assert_eq!(
        message_boxes.process_updates(UpdatesLike::ConnectionClosed, T0),
        Err(Gap)
    );
    assert_eq!(message_boxes.difference_pending(), account_pending());
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 500, 50))
    );
    // And its backoff starts over.
    assert!(message_boxes.end_account_difference(T0).is_empty());
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        T0 + Duration::from_secs(1)
    );
}
