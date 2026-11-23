// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The account's `seq` orders the updates no box orders, as TDLib's `UpdatesManager` applies them
//! ([`MessageBoxes::process_updates`]): a container's box updates go to their boxes whatever its
//! `seq`, before its other updates; those wait for the containers before it for half a second
//! from the first one held, then for the account's difference (also while a channel gets its),
//! which hands them out if they follow on from its `seq` and drops them if it covered them (an
//! unanswered end drops those still past it, and an empty difference those held before it began);
//! while the difference runs, every container's but an `updateShort`'s wait for its end; a few
//! apply as they arrive, and `updateChannelTooLong` begins its channel's difference first. The
//! `date` moves only forward, and a container of no `seq` doesn't move it while one waits past the
//! `seq`.

use super::*;

/// An update no box orders, and that waits for its container's `seq`.
fn ordered() -> tl::enums::Update {
    tl::enums::Update::SavedReactionTags
}

/// An update no box orders that applies as it arrives: a sent message's id.
fn at_once() -> tl::enums::Update {
    tl::enums::Update::MessageId(tl::types::UpdateMessageId {
        id: 7,
        random_id: 1,
    })
}

/// A container of `updates` at `date` and `seq` (no `seq` if [`NO_SEQ`]).
fn container(date: i32, seq: i32, updates: Vec<tl::enums::Update>) -> UpdatesLike {
    UpdatesLike::Updates(tl::enums::Updates::Updates(tl::types::Updates {
        updates,
        users: Vec::new(),
        chats: Vec::new(),
        date,
        seq,
    }))
}

/// For each update handed out, its box's `pts`, or the `seq` it was handed out at if no box
/// orders it (negated, to tell the two apart).
fn order_of(updates: &[(tl::enums::Update, State)]) -> Vec<i32> {
    updates
        .iter()
        .map(|(_, state)| match state.message_box {
            Some(message_box) => message_box.pts(),
            None => -state.seq,
        })
        .collect()
}

fn handed_out(message_boxes: &mut MessageBoxes, updates: UpdatesLike, now: Now) -> Vec<i32> {
    let (updates, _, _) = message_boxes.process_updates(updates, now).unwrap();
    order_of(&updates)
}

fn account_pending() -> DiffPending {
    DiffPending {
        account: true,
        channels: 0,
    }
}

/// A difference that returns nothing at `state`'s `seq` with the common box at `pts`.
fn difference_at(date: i32, seq: i32, pts: i32) -> tl::enums::updates::Difference {
    tl::types::updates::Difference {
        new_messages: Vec::new(),
        new_encrypted_messages: Vec::new(),
        other_updates: Vec::new(),
        chats: Vec::new(),
        users: Vec::new(),
        state: state(date, seq, pts, 50).into(),
    }
    .into()
}

#[test]
fn a_container_past_the_seq_waits_for_the_one_before_it() {
    let mut message_boxes = settled(T0);
    // seq 7 before seq 6 (the local one is 5), each with the common box's update and one that
    // waits for the `seq`; 102 waits for 101 in its box, and its container for seq 6.
    assert!(
        handed_out(
            &mut message_boxes,
            container(12, 7, vec![update(102), ordered()]),
            T0
        )
        .is_empty()
    );
    assert_eq!(message_boxes.take_delta(), StateDelta::default());

    // seq 6 fills both gaps: the box's updates in order, then each container's other update.
    let later = T0 + POSSIBLE_GAP_TIMEOUT / 2;
    assert_eq!(
        handed_out(
            &mut message_boxes,
            container(11, 6, vec![update(101), ordered()]),
            later
        ),
        [101, 102, -6, -7]
    );
    assert_eq!(message_boxes.take_delta(), common_delta(102, 12, 7));
    // No gap is left: the next difference is the idle one.
    assert_eq!(
        time_of_next_difference(&mut message_boxes, later),
        next_updates_deadline(later)
    );
}

#[test]
fn a_seq_gap_keeps_its_deadline_while_any_container_is_held() {
    let mut message_boxes = settled(T0);
    // seq 7 opens the gap: seq 6 is missing.
    assert!(handed_out(&mut message_boxes, container(12, 7, vec![ordered()]), T0).is_empty());
    // A quarter second on, seq 9 joins it, and seq 6 fills it in part: seq 8 is still missing.
    let later = T0 + POSSIBLE_GAP_TIMEOUT / 2;
    assert!(handed_out(&mut message_boxes, container(14, 9, vec![ordered()]), later).is_empty());
    assert_eq!(
        handed_out(&mut message_boxes, container(11, 6, vec![ordered()]), later),
        [-6, -7]
    );
    // seq 9 waits out the gap's own half second, not a new one.
    assert_eq!(
        time_of_next_difference(&mut message_boxes, later),
        T0 + POSSIBLE_GAP_TIMEOUT
    );
}

#[test]
fn a_pts_gap_doesnt_hold_the_seq_back() {
    let mut message_boxes = settled(T0);
    // seq 6 brings 102, whose box waits for 101; the container's `seq` is taken all the same, so
    // seq 7, which brings 101, follows on, and nothing is missing.
    assert!(handed_out(&mut message_boxes, container(11, 6, vec![update(102)]), T0).is_empty());
    assert_eq!(
        handed_out(&mut message_boxes, container(12, 7, vec![update(101)]), T0),
        [101, 102]
    );
    assert_eq!(message_boxes.take_delta(), common_delta(102, 12, 7));
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        next_updates_deadline(T0)
    );
}

#[test]
fn a_container_past_the_seq_gets_the_difference_after_half_a_second() {
    for (difference_seq, handed) in [
        // The difference covers the container: its update is dropped.
        (7, Vec::new()),
        // The container follows on from the difference's `seq`: handed out at the `date` it left.
        (6, vec![-7]),
    ] {
        let mut message_boxes = settled(T0);
        // Its box update follows on: handed out at once.
        assert_eq!(
            handed_out(
                &mut message_boxes,
                container(12, 7, vec![update(101), ordered()]),
                T0
            ),
            [101]
        );
        assert_eq!(message_boxes.get_difference(), None);
        assert_eq!(
            time_of_next_difference(&mut message_boxes, T0),
            T0 + POSSIBLE_GAP_TIMEOUT
        );
        assert_eq!(
            message_boxes.get_difference(),
            Some(get_difference(10, 101, 50))
        );
        let due = T0 + POSSIBLE_GAP_TIMEOUT;
        let (updates, _, _) =
            message_boxes.apply_difference(difference_at(11, difference_seq, 101), due);
        assert_eq!(order_of(&updates), handed);
        assert_eq!(message_boxes.take_delta(), common_delta(101, 11, 7));
        assert_eq!(
            time_of_next_difference(&mut message_boxes, due),
            next_updates_deadline(due)
        );
    }
}

#[test]
fn a_seq_gap_falls_due_while_a_channel_gets_its_difference() {
    let mut message_boxes = settled(T0);
    // A driver that checked its deadlines already: the gap's own must bring the next one forward.
    assert_eq!(message_boxes.check_deadlines(T0), next_updates_deadline(T0));
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    let channel_only = DiffPending {
        account: false,
        channels: 1,
    };
    // seq 7 past the local 5 while the channel catches up: its box update follows on and is
    // handed out at once, its other update waits for seq 6.
    assert_eq!(
        handed_out(
            &mut message_boxes,
            container(12, 7, vec![update(101), ordered()]),
            T0
        ),
        [101]
    );

    // Not yet: seq 6 may come for half a second.
    let early = T0 + POSSIBLE_GAP_TIMEOUT / 2;
    assert_eq!(message_boxes.check_deadlines(early), early);
    assert_eq!(message_boxes.difference_pending(), channel_only);

    // Its half second over, the account's difference begins beside the channel's.
    let half = T0 + POSSIBLE_GAP_TIMEOUT;
    assert_eq!(message_boxes.check_deadlines(half), half);
    assert_eq!(
        message_boxes.difference_pending(),
        DiffPending {
            account: true,
            channels: 1
        }
    );
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 101, 50))
    );

    // It ends at seq 6 while the channel still catches up: seq 7 follows on, handed out at the
    // `date` it left.
    let (updates, _, _) = message_boxes.apply_difference(difference_at(11, 6, 101), half);
    assert_eq!(order_of(&updates), [-7]);
    assert_eq!(message_boxes.take_delta(), common_delta(101, 11, 7));
    assert_eq!(message_boxes.difference_pending(), channel_only);

    // Once the channel's ends, no gap is left: the next difference is the idle one.
    message_boxes.apply_channel_difference(CHANNEL, channel_difference_empty(20), half);
    assert_eq!(
        time_of_next_difference(&mut message_boxes, half),
        next_updates_deadline(half)
    );
}

#[test]
fn a_difference_holds_the_containers_still_past_its_seq() {
    let mut message_boxes = settled(T0);
    assert!(handed_out(&mut message_boxes, container(12, 9, vec![ordered()]), T0).is_empty());
    let due = time_of_next_difference(&mut message_boxes, T0);
    assert_eq!(due, T0 + POSSIBLE_GAP_TIMEOUT);

    // The difference ends at seq 7: seq 8 is still missing. The container waits for it another
    // half second, from the difference's end, then the account's difference begins again.
    let (updates, _, _) = message_boxes.apply_difference(difference_at(11, 7, 100), due);
    assert!(updates.is_empty());
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    assert_eq!(
        time_of_next_difference(&mut message_boxes, due),
        due + POSSIBLE_GAP_TIMEOUT
    );
    assert_eq!(message_boxes.difference_pending(), account_pending());

    // Its answer covers it.
    let again = due + POSSIBLE_GAP_TIMEOUT;
    let (updates, _, _) = message_boxes.apply_difference(difference_at(12, 9, 100), again);
    assert!(updates.is_empty());
    assert_eq!(
        time_of_next_difference(&mut message_boxes, again),
        next_updates_deadline(again)
    );
}

#[test]
fn an_empty_difference_drops_the_containers_held_before_it_still_past_its_seq() {
    let mut message_boxes = settled(T0);
    assert!(handed_out(&mut message_boxes, container(12, 9, vec![ordered()]), T0).is_empty());
    let due = time_of_next_difference(&mut message_boxes, T0);
    // Empty at seq 7, the server's latest: the container held past the `seq` before it began, and
    // still past it, is dropped, as TDLib drops its pending ones then, and no other difference
    // begins.
    let (updates, _, _) = message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 11, seq: 7 }.into(),
        due,
    );
    assert!(updates.is_empty());
    assert_eq!(message_boxes.take_delta(), common_delta(100, 11, 7));
    // seq 8 follows on.
    assert_eq!(
        handed_out(&mut message_boxes, container(13, 8, vec![ordered()]), due),
        [-8]
    );
    assert_eq!(
        time_of_next_difference(&mut message_boxes, due),
        next_updates_deadline(due)
    );
}

#[test]
fn an_empty_difference_keeps_the_containers_that_arrived_during_it() {
    let mut message_boxes = settled(T0);
    assert!(message_boxes.begin_account_difference());
    // seq 7 arrives while the difference runs, past the local `seq` (5).
    assert!(handed_out(&mut message_boxes, container(13, 7, vec![ordered()]), T0).is_empty());
    // Empty at seq 5: seq 7 waits for seq 6, as TDLib checks its postponed containers again, for
    // half a second from the difference's end.
    let (updates, _, _) = message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 11, seq: 5 }.into(),
        T0,
    );
    assert!(updates.is_empty());
    assert_eq!(message_boxes.take_delta(), common_delta(100, 11, 5));
    assert_eq!(message_boxes.check_deadlines(T0), T0 + POSSIBLE_GAP_TIMEOUT);
    // seq 6 fills the gap: both handed out.
    let later = T0 + POSSIBLE_GAP_TIMEOUT / 2;
    assert_eq!(
        handed_out(&mut message_boxes, container(12, 6, vec![ordered()]), later),
        [-6, -7]
    );
    assert_eq!(message_boxes.take_delta(), common_delta(100, 13, 7));
    assert_eq!(
        time_of_next_difference(&mut message_boxes, later),
        next_updates_deadline(T0)
    );
}

#[test]
fn a_container_kept_past_an_empty_difference_is_dropped_by_the_next_empty_one() {
    let empty = || -> tl::enums::updates::Difference {
        tl::types::updates::DifferenceEmpty { date: 11, seq: 5 }.into()
    };
    let mut message_boxes = settled(T0);
    assert!(message_boxes.begin_account_difference());
    assert!(handed_out(&mut message_boxes, container(13, 7, vec![ordered()]), T0).is_empty());
    let (updates, _, _) = message_boxes.apply_difference(empty(), T0);
    assert!(updates.is_empty());
    // seq 6 doesn't come: half a second on, the account's difference begins again, and its empty
    // answer drops seq 7, held from before it now. No other difference begins, so a server whose
    // `seq` lags costs one more difference, not a loop of them.
    let due = time_of_next_difference(&mut message_boxes, T0);
    assert_eq!(due, T0 + POSSIBLE_GAP_TIMEOUT);
    let (updates, _, _) = message_boxes.apply_difference(empty(), due);
    assert!(updates.is_empty());
    // A late seq 6 is handed out alone, and no gap is left: the next difference is the idle one.
    assert_eq!(
        handed_out(&mut message_boxes, container(12, 6, vec![ordered()]), due),
        [-6]
    );
    assert_eq!(
        time_of_next_difference(&mut message_boxes, due),
        next_updates_deadline(due)
    );
}

#[test]
fn an_update_short_applies_while_the_difference_runs() {
    let mut message_boxes = settled(T0);
    assert!(message_boxes.begin_account_difference());
    // TDLib hands an `updateShort` to its handler as it arrives; a container of no `seq` waits.
    let short = UpdatesLike::Updates(
        tl::types::UpdateShort {
            update: ordered(),
            date: 12,
        }
        .into(),
    );
    assert_eq!(handed_out(&mut message_boxes, short, T0), [-5]);
    assert!(
        handed_out(
            &mut message_boxes,
            container(12, NO_SEQ, vec![ordered()]),
            T0
        )
        .is_empty()
    );
    // Neither moves the state while the difference runs; its end hands out the container's.
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
    let (updates, _, _) = message_boxes.apply_difference(difference_at(11, 5, 100), T0);
    assert_eq!(order_of(&updates), [-5]);
    assert_eq!(message_boxes.take_delta(), common_delta(100, 11, 5));
}

#[test]
fn a_container_of_no_seq_keeps_the_date_while_the_seq_has_a_gap() {
    let mut message_boxes = settled(T0);
    // seq 7 waits for the missing seq 6.
    assert!(handed_out(&mut message_boxes, container(12, 7, vec![ordered()]), T0).is_empty());
    // A later container of no `seq` is handed out at once, but the `date` stays before the gap,
    // where the difference that fills it asks from.
    assert_eq!(
        handed_out(
            &mut message_boxes,
            container(13, NO_SEQ, vec![at_once()]),
            T0
        ),
        [-5]
    );
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        T0 + POSSIBLE_GAP_TIMEOUT
    );
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 100, 50))
    );
}

#[test]
fn a_container_held_across_a_difference_doesnt_take_the_date_back() {
    let mut message_boxes = settled(T0);
    assert!(handed_out(&mut message_boxes, container(12, 7, vec![ordered()]), T0).is_empty());
    let due = time_of_next_difference(&mut message_boxes, T0);
    // The difference ends at seq 5 with a later date: seq 7 waits for seq 6 again.
    let (updates, _, _) = message_boxes.apply_difference(difference_at(20, 5, 100), due);
    assert!(updates.is_empty());
    assert_eq!(message_boxes.take_delta(), common_delta(100, 20, 5));
    // seq 6 fills the gap: both handed out, the `seq` moves, the `date` stays the difference's.
    assert_eq!(
        handed_out(&mut message_boxes, container(11, 6, vec![ordered()]), due),
        [-6, -7]
    );
    assert_eq!(message_boxes.take_delta(), common_delta(100, 20, 7));
}

#[test]
fn a_container_overtaken_by_a_later_date_doesnt_take_it_back() {
    let mut message_boxes = settled(T0);
    // A container of no `seq` dated 13 arrives first (none waits past the `seq`: its `date` is
    // taken).
    assert_eq!(
        handed_out(
            &mut message_boxes,
            container(13, NO_SEQ, vec![at_once()]),
            T0
        ),
        [-5]
    );
    assert_eq!(message_boxes.take_delta(), common_delta(100, 13, 5));
    // Then seq 7 and seq 6, dated before it: the `seq` moves, the `date` stays 13.
    assert!(handed_out(&mut message_boxes, container(12, 7, vec![ordered()]), T0).is_empty());
    assert_eq!(
        handed_out(&mut message_boxes, container(11, 6, vec![ordered()]), T0),
        [-6, -7]
    );
    assert_eq!(message_boxes.take_delta(), common_delta(100, 13, 7));
}

#[test]
fn while_the_difference_runs_every_containers_updates_wait_for_it() {
    let mut message_boxes = settled(T0);
    assert!(message_boxes.begin_account_difference());
    // A container of no `seq` and one past the local `seq`, arriving in that order.
    for updates in [
        container(12, 7, vec![ordered()]),
        container(NO_DATE, NO_SEQ, vec![ordered()]),
    ] {
        assert!(handed_out(&mut message_boxes, updates, T0).is_empty());
    }
    // The difference ends at seq 6: the container of no `seq` first, then seq 7, which follows on.
    let (updates, _, _) = message_boxes.apply_difference(difference_at(11, 6, 100), T0);
    assert_eq!(order_of(&updates), [-6, -7]);
    assert_eq!(message_boxes.take_delta(), common_delta(100, 11, 7));
}

#[test]
fn some_updates_apply_as_they_arrive() {
    let mut message_boxes = settled(T0);
    // A sent message's id and its message, past the `seq`: both handed out at once, so they pair.
    let (updates, _, _) = message_boxes
        .process_updates(container(12, 7, vec![update(101), at_once()]), T0)
        .unwrap();
    assert_eq!(order_of(&updates), [-7, 101]);
    assert!(matches!(updates[0].0, tl::enums::Update::MessageId(_)));
    // And while the account gets its difference.
    let due = time_of_next_difference(&mut message_boxes, T0);
    assert_eq!(message_boxes.difference_pending(), account_pending());
    let (updates, _, _) = message_boxes
        .process_updates(container(12, 8, vec![at_once()]), due)
        .unwrap();
    assert_eq!(order_of(&updates), [-8]);
}

#[test]
fn channel_too_long_goes_before_the_rest_of_its_container() {
    let mut message_boxes = settled(T0);
    // seq 6 follows on, with an update that waits for it, the channel's next update, and, last,
    // `updateChannelTooLong` with no `pts`. That begins the channel's difference before the others
    // apply, so the channel's update waits for it; the `seq` one goes out.
    let too_long: tl::enums::Update = tl::types::UpdateChannelTooLong {
        channel_id: CHANNEL,
        pts: None,
    }
    .into();
    let updates = vec![ordered(), channel_update(CHANNEL, 21), too_long];
    assert_eq!(
        handed_out(&mut message_boxes, container(11, 6, updates), T0),
        [-6]
    );
    assert_eq!(
        message_boxes.difference_pending(),
        DiffPending {
            account: false,
            channels: 1
        }
    );
    // The difference finds nothing new: the channel's update follows on from it.
    let (updates, _, _) =
        message_boxes.apply_channel_difference(CHANNEL, channel_difference_empty(20), T0);
    assert_eq!(order_of(&updates), [21]);
}

#[test]
fn a_seq_gap_is_dropped_after_its_half_second_while_the_retry_waits() {
    let mut message_boxes = settled(T0);
    assert!(message_boxes.begin_account_difference());
    assert!(message_boxes.end_account_difference(T0).is_empty());
    assert!(handed_out(&mut message_boxes, container(12, 7, vec![ordered()]), T0).is_empty());

    // Its half second over, the container is dropped, and the retry is the only difference due.
    let half = T0 + POSSIBLE_GAP_TIMEOUT;
    assert_eq!(
        message_boxes.check_deadlines(half),
        T0 + Duration::from_secs(1)
    );
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    // A late seq 6 is handed out alone: 7's update is gone, for the retry's difference to set the
    // `seq`.
    assert_eq!(
        handed_out(&mut message_boxes, container(11, 6, vec![ordered()]), half),
        [-6]
    );
    assert_eq!(
        time_of_next_difference(&mut message_boxes, half),
        T0 + Duration::from_secs(1)
    );
}

#[test]
fn a_seq_gap_due_with_the_retry_is_kept_for_its_difference() {
    let retry = T0 + Duration::from_secs(1);
    // seq 7 arrives half a second before the retry, its gap due with it; or later, its gap due
    // after the retry, checked late, once both have passed.
    for arrival in [
        retry - POSSIBLE_GAP_TIMEOUT,
        retry - POSSIBLE_GAP_TIMEOUT / 2,
    ] {
        let mut message_boxes = settled(T0);
        assert!(message_boxes.begin_account_difference());
        assert!(message_boxes.end_account_difference(T0).is_empty());
        assert!(
            handed_out(
                &mut message_boxes,
                container(12, 7, vec![ordered()]),
                arrival
            )
            .is_empty()
        );
        let checked = arrival + POSSIBLE_GAP_TIMEOUT;
        assert_eq!(message_boxes.check_deadlines(checked), checked);
        assert_eq!(message_boxes.difference_pending(), account_pending());
        // The retry's difference ends at seq 6: seq 7 follows on, at the date it left.
        let (updates, _, _) = message_boxes.apply_difference(difference_at(11, 6, 100), checked);
        assert_eq!(order_of(&updates), [-7]);
        assert_eq!(message_boxes.take_delta(), common_delta(100, 11, 7));
    }
}

#[test]
fn an_unanswered_end_hands_out_the_held_containers_that_follow_on() {
    let mut message_boxes = settled(T0);
    assert!(message_boxes.begin_account_difference());
    for updates in [
        container(12, 8, vec![ordered()]),
        container(11, 6, vec![ordered()]),
    ] {
        assert!(handed_out(&mut message_boxes, updates, T0).is_empty());
    }
    // seq 6 follows on from the local `seq`; seq 8 is past it, and dropped for the retry.
    let ended = message_boxes.end_account_difference(T0);
    assert_eq!(order_of(&ended), [-6]);
    assert_eq!(message_boxes.take_delta(), common_delta(100, 10, 6));
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        T0 + Duration::from_secs(1)
    );
}

#[test]
fn without_the_accounts_state_a_container_past_the_seq_isnt_held() {
    // Nothing to order by and no difference to fill the gap with: its update is dropped, not held
    // for good.
    let mut message_boxes = MessageBoxes::new();
    assert!(handed_out(&mut message_boxes, container(12, 7, vec![ordered()]), T0).is_empty());
    assert!(message_boxes.held_seq.is_empty());
    assert_eq!(message_boxes.seq_gap, None);
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
}
