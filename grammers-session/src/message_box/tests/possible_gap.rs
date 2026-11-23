// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A box's possible gap holds its updates in the order they apply, as they arrive: once the gap
//! fills, they come out in order, a copy of one it holds already is dropped, and an update whose
//! `pts_count` is 0 applies before one of the same start that moves the `pts`, in a batch and
//! after a difference too.

use super::*;

/// An update of the common box at `pts` that moves it by `count`.
fn counted(pts: i32, count: i32) -> tl::enums::Update {
    tl::enums::Update::DeleteMessages(tl::types::UpdateDeleteMessages {
        messages: Vec::new(),
        pts,
        pts_count: count,
    })
}

/// `updates` in a container of no `seq`.
fn container(updates: Vec<tl::enums::Update>) -> UpdatesLike {
    UpdatesLike::Updates(tl::enums::Updates::Updates(tl::types::Updates {
        updates,
        users: Vec::new(),
        chats: Vec::new(),
        date: NO_DATE,
        seq: NO_SEQ,
    }))
}

/// The `pts` and `pts_count` of each update handed out, in order.
fn applied(updates: &[(tl::enums::Update, State)]) -> Vec<(i32, i32)> {
    updates
        .iter()
        .map(|(update, _)| {
            let info = PtsInfo::from_update(update).unwrap().unwrap();
            (info.pts, info.count)
        })
        .collect()
}

#[test]
fn a_possible_gap_hands_its_updates_out_in_order() {
    let mut message_boxes = settled(T0);
    // Behind the missing 101, out of order, one of them twice.
    for pts in [105, 103, 104, 103, 102] {
        let (held, _, _) = message_boxes
            .process_updates(updates(NO_DATE, NO_SEQ, pts), T0)
            .unwrap();
        assert!(held.is_empty(), "{pts} held");
    }
    assert_eq!(message_boxes.take_delta(), StateDelta::default());

    let (filled, _, _) = message_boxes
        .process_updates(updates(NO_DATE, NO_SEQ, 101), T0)
        .unwrap();
    assert_eq!(pts_of(&filled), [101, 102, 103, 104, 105]);
    // Nothing is held any more: the next update follows on.
    let (next, _, _) = message_boxes
        .process_updates(updates(NO_DATE, NO_SEQ, 106), T0)
        .unwrap();
    assert_eq!(pts_of(&next), [106]);
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        next_updates_deadline(T0)
    );
}

#[test]
fn a_gap_keeps_its_deadline_as_it_fills_in_part() {
    let mut message_boxes = settled(T0);
    for pts in [103, 105] {
        message_boxes
            .process_updates(updates(NO_DATE, NO_SEQ, pts), T0)
            .unwrap();
    }
    // 101 and 102 fill the gap up to 103; 104 is still missing, and the gap's deadline is the one
    // it opened with.
    let later = T0 + POSSIBLE_GAP_TIMEOUT / 2;
    for (pts, out) in [(101, vec![101]), (102, vec![102, 103])] {
        let (updates, _, _) = message_boxes
            .process_updates(updates(NO_DATE, NO_SEQ, pts), later)
            .unwrap();
        assert_eq!(pts_of(&updates), out);
    }
    assert_eq!(
        time_of_next_difference(&mut message_boxes, later),
        T0 + POSSIBLE_GAP_TIMEOUT
    );
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 103, 50))
    );
}

#[test]
fn a_possible_gap_applies_a_pts_count_0_update_before_one_that_moves_the_pts() {
    // Both start at 101, behind the missing 101, arriving in either order.
    for arrivals in [
        [counted(101, 0), counted(102, 1)],
        [counted(102, 1), counted(101, 0)],
    ] {
        let mut message_boxes = settled(T0);
        for update in arrivals {
            let (held, _, _) = message_boxes
                .process_updates(container(vec![update]), T0)
                .unwrap();
            assert!(held.is_empty());
        }
        let (updates, _, _) = message_boxes
            .process_updates(updates(NO_DATE, NO_SEQ, 101), T0)
            .unwrap();
        assert_eq!(applied(&updates), [(101, 1), (101, 0), (102, 1)]);
    }
}

#[test]
fn a_batch_applies_a_pts_count_0_update_before_one_that_moves_the_pts() {
    let mut message_boxes = settled(T0);
    // In one container, the one that moves the `pts` first.
    let (updates, _, _) = message_boxes
        .process_updates(container(vec![counted(101, 1), counted(100, 0)]), T0)
        .unwrap();
    assert_eq!(applied(&updates), [(100, 0), (101, 1)]);
}

#[test]
fn a_replay_applies_a_pts_count_0_update_before_one_that_moves_the_pts() {
    let mut message_boxes = settled(T0);
    assert!(message_boxes.begin_account_difference());
    // Postponed while the account gets its difference, the one that moves the `pts` first.
    for update in [counted(101, 1), counted(100, 0)] {
        let (postponed, _, _) = message_boxes
            .process_updates(container(vec![update]), T0)
            .unwrap();
        assert!(postponed.is_empty());
    }
    let (updates, _, _) = message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 10, seq: 5 }.into(),
        T0,
    );
    assert_eq!(applied(&updates), [(100, 0), (101, 1)]);
}
