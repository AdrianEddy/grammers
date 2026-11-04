// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use grammers_tl_types as tl;
use proptest::prelude::*;

use super::defs::{
    CommonState, DiffPending, Gap, MessageBox, MessageBoxes, NO_DATE, NO_PTS, NO_SEQ,
    NO_UPDATES_TIMEOUT, Now, PtsInfo, State, StateDelta, UpdateAndPeers, UpdatesLike,
};
use super::{Key, POSSIBLE_GAP_TIMEOUT, PrematureEndReason, next_updates_deadline};
use crate::types::{ChannelState, UpdatesState};

/// The caller's time origin. Far from zero, so nothing passes by reading a clock of its own.
const T0: Now = Duration::from_secs(1000 * 24 * 60 * 60);

const CHANNEL: i64 = 1000;

fn state(date: i32, seq: i32, pts: i32, qts: i32) -> tl::types::updates::State {
    tl::types::updates::State {
        pts,
        qts,
        date,
        seq,
        unread_count: 0,
    }
}

fn update(pts: i32) -> tl::enums::Update {
    tl::enums::Update::DeleteMessages(tl::types::UpdateDeleteMessages {
        messages: Vec::new(),
        pts,
        pts_count: 1,
    })
}

fn updates(date: i32, seq: i32, pts: i32) -> UpdatesLike {
    UpdatesLike::Updates(tl::enums::Updates::Updates(tl::types::Updates {
        updates: vec![update(pts)],
        users: Vec::new(),
        chats: Vec::new(),
        date,
        seq,
    }))
}

fn qts_update(qts: i32) -> tl::enums::Update {
    tl::enums::Update::BotStopped(tl::types::UpdateBotStopped {
        user_id: 1,
        date: NO_DATE,
        stopped: true,
        qts,
    })
}

fn channel_update(channel_id: i64, pts: i32) -> tl::enums::Update {
    tl::enums::Update::DeleteChannelMessages(tl::types::UpdateDeleteChannelMessages {
        channel_id,
        messages: Vec::new(),
        pts,
        pts_count: 1,
    })
}

fn channel_updates(channel_id: i64, date: i32, seq: i32, pts: i32) -> UpdatesLike {
    UpdatesLike::Updates(tl::enums::Updates::Updates(tl::types::Updates {
        updates: vec![channel_update(channel_id, pts)],
        users: Vec::new(),
        chats: Vec::new(),
        date,
        seq,
    }))
}

fn updates_ok(date: i32, seq: i32, pts: i32) -> Result<UpdateAndPeers, Gap> {
    Ok((
        vec![(
            update(pts),
            State {
                date,
                seq,
                message_box: Some(MessageBox::Common { pts }),
            },
        )],
        Vec::new(),
        Vec::new(),
    ))
}

fn merge_updates(updates: Vec<Result<UpdateAndPeers, Gap>>) -> Result<UpdateAndPeers, Gap> {
    updates
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map(|updates| {
            updates
                .into_iter()
                .fold((Vec::new(), Vec::new(), Vec::new()), |mut result, tuple| {
                    result.0.extend(tuple.0);
                    result.1.extend(tuple.1);
                    result.2.extend(tuple.2);
                    result
                })
        })
}

fn get_difference(date: i32, pts: i32, qts: i32) -> tl::functions::updates::GetDifference {
    tl::functions::updates::GetDifference {
        pts,
        pts_limit: None,
        pts_total_limit: None,
        date,
        qts,
        qts_limit: None,
    }
}

fn get_channel_difference(id: i64, pts: i32) -> tl::functions::updates::GetChannelDifference {
    tl::functions::updates::GetChannelDifference {
        force: false,
        channel: tl::enums::InputChannel::Channel(tl::types::InputChannel {
            channel_id: id,
            access_hash: 0,
        }),
        filter: tl::enums::ChannelMessagesFilter::Empty,
        pts,
        limit: 0,
    }
}

fn channel_difference_empty(pts: i32) -> tl::enums::updates::ChannelDifference {
    tl::types::updates::ChannelDifferenceEmpty {
        r#final: true,
        pts,
        timeout: None,
    }
    .into()
}

/// The state [`settled`] starts from: date 10, seq 5, pts 100, qts 50, and [`CHANNEL`] at 20.
fn loaded_state() -> UpdatesState {
    UpdatesState {
        pts: 100,
        qts: 50,
        date: 10,
        seq: 5,
        channels: vec![ChannelState {
            id: CHANNEL,
            pts: 20,
        }],
    }
}

/// [`loaded_state`] after the start-up differences found nothing new, with the delta taken.
fn settled(now: Now) -> MessageBoxes {
    let state = loaded_state();
    let mut message_boxes = MessageBoxes::load(state.clone(), now);
    message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty {
            date: state.date,
            seq: state.seq,
        }
        .into(),
        now,
    );
    while let Some(request) = message_boxes.get_channel_difference() {
        message_boxes.apply_channel_difference(channel_difference_empty(request.pts), now);
    }
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    message_boxes
}

/// Replay a delta onto stored state, the way a caller persisting deltas does.
fn apply_delta(state: &mut UpdatesState, delta: StateDelta) {
    if let Some(common) = delta.common {
        state.pts = common.pts;
        state.date = common.date;
        state.seq = common.seq;
    }
    if let Some(qts) = delta.qts {
        state.qts = qts;
    }
    for channel in delta.channels {
        match state.channels.binary_search_by_key(&channel.id, |c| c.id) {
            Ok(i) => state.channels[i].pts = channel.pts,
            Err(i) => state.channels.insert(i, channel),
        }
    }
    for id in delta.removed_channels {
        if let Ok(i) = state.channels.binary_search_by_key(&id, |c| c.id) {
            state.channels.remove(i);
        }
    }
}

/// Step the clock from deadline to deadline, as a driver sleeping on them does, until a
/// difference falls due, and return when it did.
fn time_of_next_difference(message_boxes: &mut MessageBoxes, mut now: Now) -> Now {
    for _ in 0..1000 {
        let deadline = message_boxes.check_deadlines(now);
        if message_boxes.difference_pending() != DiffPending::default() {
            return now;
        }
        assert!(deadline > now, "a deadline in the past spins");
        now = deadline;
    }
    panic!("no difference fell due");
}

fn common_delta(pts: i32, date: i32, seq: i32) -> StateDelta {
    StateDelta {
        common: Some(CommonState { pts, date, seq }),
        ..StateDelta::default()
    }
}

#[test]
fn test_connect_flow_empty() {
    let state = UpdatesState {
        pts: NO_PTS,
        qts: NO_PTS,
        date: NO_DATE,
        seq: NO_SEQ,
        channels: Vec::new(),
    };
    let message_boxes = MessageBoxes::load(state.clone(), T0);

    assert!(message_boxes.is_empty());
    assert_eq!(message_boxes.get_difference(), None);
    assert_eq!(message_boxes.get_channel_difference(), None);
    assert_eq!(message_boxes.session_state(), state)
}

#[test]
fn test_connect_flow_with_data() {
    let state = UpdatesState {
        pts: 12,
        qts: 34,
        date: 56,
        seq: 78,
        channels: vec![ChannelState { id: 43, pts: 21 }],
    };
    let message_boxes = MessageBoxes::load(state.clone(), T0);

    assert!(!message_boxes.is_empty());
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(56, 12, 34))
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(43, 21))
    );
    assert_eq!(message_boxes.session_state(), state)
}

#[test]
fn test_complete_login_flow() {
    let mut message_boxes = MessageBoxes::new();
    assert!(message_boxes.is_empty());

    message_boxes.set_state(state(56, 78, 12, 34), T0);
    assert!(!message_boxes.is_empty());
    assert_eq!(message_boxes.get_difference(), None);
    assert_eq!(message_boxes.get_channel_difference(), None);
    assert_eq!(
        message_boxes.session_state(),
        UpdatesState {
            pts: 12,
            qts: 34,
            date: 56,
            seq: 78,
            channels: Vec::new()
        }
    )
}

#[test]
fn test_iter_dialogs_flow() {
    let mut message_boxes = MessageBoxes::new();

    message_boxes.try_set_channel_state(98, 76, T0);
    message_boxes.try_set_channel_state(54, 32, T0);
    message_boxes.try_set_channel_state(98, 10, T0);

    assert_eq!(
        message_boxes.session_state(),
        UpdatesState {
            channels: vec![
                // Notably: sorted, and only the first set is kept.
                ChannelState { id: 54, pts: 32 },
                ChannelState { id: 98, pts: 76 },
            ],
            ..message_boxes.session_state()
        }
    )
}

#[test]
fn test_next_raw_update_flow_empty() {
    let mut now = T0;
    let mut message_boxes = MessageBoxes::new();

    let deadline = next_updates_deadline(now);
    assert_eq!(message_boxes.check_deadlines(now), deadline);
    assert_eq!(message_boxes.get_difference(), None);
    assert_eq!(message_boxes.get_channel_difference(), None);

    now += NO_UPDATES_TIMEOUT / 2; // assert unchanged since previous deadline was not met yet
    assert_eq!(message_boxes.check_deadlines(now), deadline);
    assert_eq!(message_boxes.get_difference(), None);
    assert_eq!(message_boxes.get_channel_difference(), None);

    now += NO_UPDATES_TIMEOUT; // assert changed to avoid sleeping with no delay
    assert_eq!(
        message_boxes.check_deadlines(now),
        next_updates_deadline(now)
    );
    assert_eq!(message_boxes.get_difference(), None);
    assert_eq!(message_boxes.get_channel_difference(), None);

    now += NO_UPDATES_TIMEOUT + Duration::from_secs(1); // assert change based on now, not last deadline
    assert_eq!(
        message_boxes.check_deadlines(now),
        next_updates_deadline(now)
    );
    assert_eq!(message_boxes.get_difference(), None);
    assert_eq!(message_boxes.get_channel_difference(), None);
}

#[test]
fn test_next_raw_update_flow_common_timeout() {
    let mut now = T0;
    let mut message_boxes = MessageBoxes::new();
    message_boxes.set_state(state(56, 78, 12, 34), now);

    now += NO_UPDATES_TIMEOUT;

    assert_eq!(message_boxes.get_difference(), None);
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(56, 12, 34))
    );

    message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 90, seq: 91 }.into(),
        now,
    );

    assert_eq!(message_boxes.get_difference(), None);
    assert_eq!(
        message_boxes.check_deadlines(now),
        next_updates_deadline(now)
    );
    assert_eq!(
        message_boxes.session_state(),
        UpdatesState {
            pts: 12,
            qts: 34,
            date: 90,
            seq: 91,
            channels: Vec::new()
        }
    );
}

#[test]
fn test_next_raw_update_flow_channel_timeout() {
    let mut now = T0;
    let mut message_boxes = MessageBoxes::new();
    message_boxes.try_set_channel_state(12, 34, now);

    now += NO_UPDATES_TIMEOUT;

    assert_eq!(message_boxes.get_channel_difference(), None);
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(12, 34))
    );

    message_boxes.apply_channel_difference(channel_difference_empty(56), now);

    assert_eq!(message_boxes.get_difference(), None);
    assert_eq!(
        message_boxes.check_deadlines(now),
        next_updates_deadline(now)
    );
    assert_eq!(
        message_boxes.session_state(),
        UpdatesState {
            channels: vec![ChannelState { id: 12, pts: 56 }],
            ..message_boxes.session_state()
        }
    );
}

#[test]
fn test_next_raw_update_flow_channel_issues() {
    let mut now = T0;
    let mut message_boxes = MessageBoxes::new();
    message_boxes.try_set_channel_state(12, 34, now);

    now += NO_UPDATES_TIMEOUT;
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert!(message_boxes.get_channel_difference().is_some());

    assert!(
        message_boxes
            .end_channel_difference(PrematureEndReason::TemporaryServerIssues, now)
            .is_empty()
    );
    assert!(message_boxes.get_channel_difference().is_none());
    assert_eq!(
        message_boxes.session_state(),
        UpdatesState {
            channels: vec![ChannelState { id: 12, pts: 34 }],
            ..message_boxes.session_state()
        }
    );
}

#[test]
fn test_next_raw_update_flow_channel_ban() {
    let mut now = T0;
    let mut message_boxes = MessageBoxes::new();
    message_boxes.try_set_channel_state(12, 34, now);

    now += NO_UPDATES_TIMEOUT;
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert!(message_boxes.get_channel_difference().is_some());

    assert!(
        message_boxes
            .end_channel_difference(PrematureEndReason::Banned, now)
            .is_empty()
    );
    assert!(message_boxes.get_channel_difference().is_none());
    assert_eq!(
        message_boxes.session_state(),
        UpdatesState {
            channels: vec![],
            ..message_boxes.session_state()
        }
    );
}

#[test]
fn test_next_raw_update_flow_no_new_get_diff_if_already_fetching() {
    let mut now = T0;
    let mut message_boxes = MessageBoxes::new();

    // t(0) = first channel registers timeout at t(3)
    message_boxes.try_set_channel_state(11, 12, now);

    // t(2) = second and third channel register timeout at t(5)
    now += 2 * (NO_UPDATES_TIMEOUT / 3);
    message_boxes.try_set_channel_state(21, 22, now);
    message_boxes.try_set_channel_state(31, 32, now);

    // t(4) = first channel times out
    now += 2 * (NO_UPDATES_TIMEOUT / 3);
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(11, 12))
    );

    // t(6) = second and third channel should have timed out by now, but didn't yet
    now += 2 * (NO_UPDATES_TIMEOUT / 3);
    assert!(
        message_boxes
            .end_channel_difference(PrematureEndReason::TemporaryServerIssues, now)
            .is_empty()
    );
    assert_eq!(message_boxes.get_channel_difference(), None);

    // t(6) = checking deadlines now triggers difference for second and third
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(21, 22))
    );
    assert!(
        message_boxes
            .end_channel_difference(PrematureEndReason::TemporaryServerIssues, now)
            .is_empty()
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(31, 32))
    );
    assert!(
        message_boxes
            .end_channel_difference(PrematureEndReason::TemporaryServerIssues, now)
            .is_empty()
    );
    assert_eq!(message_boxes.get_channel_difference(), None);
}

#[test]
fn test_process_socket_updates_flow_already_processed() {
    let mut message_boxes = MessageBoxes::new();

    message_boxes.set_state(state(12, 34, 56, 78), T0);

    for (seq, pts) in [
        (33, 57),     // seq already applied (<)
        (34, 57),     // seq already applied (=)
        (35, 55),     // seq ok (>), pts already applied (<)
        (35, 56),     // seq ok (>), pts already applied (=)
        (NO_PTS, 55), // seq ok (0), pts already applied (<)
        (NO_PTS, 56), // seq ok (0), pts already applied (=)
    ] {
        assert_eq!(
            message_boxes.process_updates(updates(13, seq, pts), T0), // date doesn't matter
            Ok((Vec::new(), Vec::new(), Vec::new()))
        );
    }
}

#[test]
fn test_process_socket_updates_flow_common_already_applied() {
    let mut message_boxes = MessageBoxes::new();

    message_boxes.set_state(state(12, 34, 56, 78), T0);

    for (seq, pts) in [
        (33, 57),     // seq already applied (<)
        (34, 57),     // seq already applied (=)
        (35, 55),     // seq ok (>), pts already applied (<)
        (35, 56),     // seq ok (>), pts already applied (=)
        (NO_PTS, 55), // seq ok (0), pts already applied (<)
        (NO_PTS, 56), // seq ok (0), pts already applied (=)
    ] {
        assert_eq!(
            message_boxes.process_updates(updates(13, seq, pts), T0), // date doesn't matter
            Ok((Vec::new(), Vec::new(), Vec::new()))
        );
    }
}

#[test]
fn test_process_socket_updates_flow_common_difference_ok() {
    let mut message_boxes = MessageBoxes::new();

    message_boxes.set_state(state(12, 34, 56, 78), T0);
    message_boxes.try_begin_get_diff(Key::Common);

    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(12, 56, 78))
    );
    // Postponed while the difference runs, and returned once it ends, if it didn't return it.
    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 57), T0),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    assert_eq!(
        Ok(message_boxes.apply_difference(
            tl::types::updates::DifferenceEmpty { date: 12, seq: 34 }.into(),
            T0,
        )),
        updates_ok(12, 34, 57)
    );
}

#[test]
fn test_process_socket_updates_flow_common_ok() {
    let mut message_boxes = MessageBoxes::new();

    message_boxes.set_state(state(12, 34, 56, 78), T0);

    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 57), T0),
        updates_ok(12, 34, 57)
    );
    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, 35, 58), T0),
        updates_ok(12, 35, 58)
    );
    assert_eq!(
        message_boxes.process_updates(updates(13, 36, 59), T0),
        updates_ok(13, 36, 59)
    );
    assert_eq!(
        message_boxes.process_updates(updates(14, NO_SEQ, 60), T0),
        updates_ok(14, 36, 60)
    );
}

#[test]
fn test_process_socket_updates_flow_common_seq_gap() {
    let mut message_boxes = MessageBoxes::new();

    message_boxes.set_state(state(12, 34, 56, 78), T0);

    assert_eq!(
        message_boxes.process_updates(updates(13, 36, 57), T0),
        Err(Gap)
    );
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(12, 56, 78))
    );
}

#[test]
fn test_process_socket_updates_flow_common_pts_possible_gap_resolves() {
    let mut now = T0;
    let mut message_boxes = MessageBoxes::new();

    message_boxes.set_state(state(12, 34, 56, 78), now);

    // No additional updates before gap resolves.
    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 58), now),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    now += POSSIBLE_GAP_TIMEOUT / 2;
    message_boxes.check_deadlines(now);
    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 57), now),
        merge_updates(vec![updates_ok(12, 34, 57), updates_ok(12, 34, 58)])
    );

    // One additional update before gap resolves.
    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 61), now),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    now += POSSIBLE_GAP_TIMEOUT / 4;
    message_boxes.check_deadlines(now);
    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 60), now),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    now += POSSIBLE_GAP_TIMEOUT / 4;
    message_boxes.check_deadlines(now);
    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 59), now),
        merge_updates(vec![
            updates_ok(12, 34, 59),
            updates_ok(12, 34, 60),
            updates_ok(12, 34, 61),
        ])
    );
}

#[test]
fn test_process_socket_updates_flow_common_pts_gap() {
    let mut now = T0;
    let mut message_boxes = MessageBoxes::new();

    message_boxes.set_state(state(12, 34, 56, 78), now);

    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 58), now),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    now += 3 * (POSSIBLE_GAP_TIMEOUT / 2);
    assert_eq!(message_boxes.get_difference(), None);
    message_boxes.check_deadlines(now);
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(12, 56, 78))
    );
}

#[test]
fn test_process_socket_updates_trickle_causes_gap() {
    let mut now = T0;
    let gap_deadline = now + POSSIBLE_GAP_TIMEOUT;
    let mut message_boxes = MessageBoxes::new();
    message_boxes.set_state(state(12, 34, 56, 78), now);

    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 58), now),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    now += 2 * (POSSIBLE_GAP_TIMEOUT / 5);
    assert_eq!(message_boxes.check_deadlines(now), gap_deadline);
    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 59), now),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    now += 2 * (POSSIBLE_GAP_TIMEOUT / 5);
    assert_eq!(message_boxes.check_deadlines(now), gap_deadline);
    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 60), now),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    now += 2 * (POSSIBLE_GAP_TIMEOUT / 5);
    // The gap's deadline passed: the difference is due now.
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert!(message_boxes.get_difference().is_some());
}

#[test]
fn test_process_socket_update_for_new_channel_dispatches_first_update() {
    let mut message_boxes = MessageBoxes::new();
    let (channel_id, date, seq, pts) = (12, 0, 0, 78);
    let ret = message_boxes.process_updates(channel_updates(channel_id, date, seq, pts), T0);
    assert_eq!(
        ret,
        Ok((
            vec![(
                channel_update(channel_id, pts),
                State {
                    date,
                    seq,
                    message_box: Some(MessageBox::Channel { channel_id, pts })
                }
            )],
            vec![],
            vec![]
        ))
    );
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            channels: vec![ChannelState {
                id: channel_id,
                pts
            }],
            ..StateDelta::default()
        }
    );
}

#[test]
fn test_common_and_channel_difference_do_not_duplicate_message() {
    let state = UpdatesState {
        pts: 12,
        qts: 34,
        date: 56,
        seq: 78,
        channels: vec![ChannelState { id: 43, pts: 21 }],
    };
    let mut message_boxes = MessageBoxes::load(state.clone(), T0);
    let message = tl::enums::Message::Empty(tl::types::MessageEmpty {
        id: 42,
        peer_id: Some(tl::enums::Peer::Channel(tl::types::PeerChannel {
            channel_id: 43,
        })),
    });
    assert!(message_boxes.getting_diff_for.contains(&Key::Channel(43)));
    let (updates, _, _) = message_boxes.apply_difference(
        tl::enums::updates::Difference::Difference(tl::types::updates::Difference {
            new_messages: vec![],
            new_encrypted_messages: vec![],
            other_updates: vec![tl::enums::Update::NewChannelMessage(
                tl::types::UpdateNewChannelMessage {
                    message: message.clone(),
                    pts: 23,
                    pts_count: 1,
                },
            )],
            chats: vec![],
            users: vec![],
            state: tl::enums::updates::State::State(tl::types::updates::State {
                pts: 14,
                qts: 0,
                date: NO_DATE,
                seq: NO_SEQ,
                unread_count: 0,
            }),
        }),
        T0,
    );
    assert_eq!(updates, vec![]);

    let (updates, _, _) = message_boxes.apply_channel_difference(
        tl::enums::updates::ChannelDifference::Difference(tl::types::updates::ChannelDifference {
            r#final: true,
            pts: 23,
            timeout: None,
            new_messages: vec![message.clone()],
            other_updates: vec![],
            chats: vec![],
            users: vec![],
        }),
        T0,
    );

    assert_eq!(updates.len(), 1);
}

// TF1: the delta of each call.

#[test]
fn held_gap_updates_do_not_advance_delta() {
    let mut message_boxes = settled(T0);

    // 101 is missing, so 102 waits for it; the same for the channel's 21.
    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 102), T0),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    assert_eq!(
        message_boxes.process_updates(channel_updates(CHANNEL, NO_DATE, NO_SEQ, 22), T0),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    assert_eq!(message_boxes.take_delta(), StateDelta::default());

    // Even once the gap's deadline makes the difference due.
    let now = T0 + POSSIBLE_GAP_TIMEOUT;
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert_eq!(
        message_boxes.difference_pending(),
        DiffPending {
            account: true,
            channels: 1
        }
    );
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
    assert_eq!(message_boxes.session_state(), loaded_state());
}

#[test]
fn gap_fill_returns_held_and_advances() {
    let mut message_boxes = settled(T0);

    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 102), T0),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    assert_eq!(message_boxes.take_delta(), StateDelta::default());

    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 101), T0),
        merge_updates(vec![updates_ok(10, 5, 101), updates_ok(10, 5, 102)])
    );
    assert_eq!(message_boxes.take_delta(), common_delta(102, 10, 5));
    // The delta is taken: the next one starts empty.
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
}

#[test]
fn delta_spans_the_calls_since_the_last_take() {
    let mut message_boxes = settled(T0);

    message_boxes
        .process_updates(updates(11, 6, 101), T0)
        .unwrap();
    message_boxes
        .process_updates(channel_updates(CHANNEL, NO_DATE, NO_SEQ, 21), T0)
        .unwrap();
    message_boxes
        .process_updates(updates(NO_DATE, NO_SEQ, 102), T0)
        .unwrap();
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            common: Some(CommonState {
                pts: 102,
                date: 11,
                seq: 6
            }),
            channels: vec![ChannelState {
                id: CHANNEL,
                pts: 21
            }],
            ..StateDelta::default()
        }
    );
}

#[test]
fn difference_empty_advances_date_seq() {
    let mut message_boxes = settled(T0);
    let now = T0 + NO_UPDATES_TIMEOUT;
    message_boxes.check_deadlines(now);
    assert!(message_boxes.difference_pending().account);

    let (updates, _, _) = message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 20, seq: 9 }.into(),
        now,
    );
    assert!(updates.is_empty());
    assert_eq!(message_boxes.take_delta(), common_delta(100, 20, 9));
}

#[test]
fn channel_difference_empty_advances_pts() {
    let mut message_boxes = MessageBoxes::load(loaded_state(), T0);
    message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 10, seq: 5 }.into(),
        T0,
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(CHANNEL, 20))
    );

    let (updates, _, _) = message_boxes.apply_channel_difference(channel_difference_empty(25), T0);
    assert!(updates.is_empty());
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            channels: vec![ChannelState {
                id: CHANNEL,
                pts: 25
            }],
            ..StateDelta::default()
        }
    );
}

#[test]
fn difference_slice_delta_is_intermediate_state() {
    let mut message_boxes = MessageBoxes::load(loaded_state(), T0);

    let (updates, _, _) = message_boxes.apply_difference(
        tl::types::updates::DifferenceSlice {
            new_messages: Vec::new(),
            new_encrypted_messages: Vec::new(),
            other_updates: vec![update(101), qts_update(51)],
            chats: Vec::new(),
            users: Vec::new(),
            intermediate_state: state(15, 7, 101, 51).into(),
        }
        .into(),
        T0,
    );
    assert_eq!(updates.len(), 2);
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            common: Some(CommonState {
                pts: 101,
                date: 15,
                seq: 7
            }),
            qts: Some(51),
            ..StateDelta::default()
        }
    );
    // The slice isn't the end: the next request starts from its state.
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(15, 101, 51))
    );
}

#[test]
fn difference_too_long_delta_is_new_pts() {
    let mut message_boxes = MessageBoxes::load(loaded_state(), T0);

    let (updates, _, _) = message_boxes.apply_difference(
        tl::types::updates::DifferenceTooLong { pts: 5000 }.into(),
        T0,
    );
    assert!(updates.is_empty());
    assert_eq!(message_boxes.take_delta(), common_delta(5000, 10, 5));
    assert_eq!(message_boxes.get_difference(), None);
}

#[test]
fn banned_channel_is_removed_in_delta() {
    let mut message_boxes = MessageBoxes::load(loaded_state(), T0);
    message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 10, seq: 5 }.into(),
        T0,
    );
    assert!(message_boxes.get_channel_difference().is_some());

    assert!(
        message_boxes
            .end_channel_difference(PrematureEndReason::Banned, T0)
            .is_empty()
    );
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            removed_channels: vec![CHANNEL],
            ..StateDelta::default()
        }
    );
    assert!(message_boxes.session_state().channels.is_empty());
}

#[test]
fn deadlines_follow_supplied_time() {
    let mut message_boxes = MessageBoxes::new();
    message_boxes.set_state(state(10, 5, 100, 50), T0);
    assert_eq!(message_boxes.check_deadlines(T0), T0 + NO_UPDATES_TIMEOUT);

    // A possible gap waits exactly 0.5 s of the supplied time, and no less.
    let held = T0 + Duration::from_secs(1);
    message_boxes
        .process_updates(updates(NO_DATE, NO_SEQ, 102), held)
        .unwrap();
    let gap_deadline = held + POSSIBLE_GAP_TIMEOUT;
    assert_eq!(message_boxes.check_deadlines(held), gap_deadline);
    let just_before = gap_deadline - Duration::from_nanos(1);
    assert_eq!(message_boxes.check_deadlines(just_before), gap_deadline);
    assert_eq!(message_boxes.get_difference(), None);
    assert_eq!(message_boxes.check_deadlines(gap_deadline), gap_deadline);
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 100, 50))
    );

    // The idle deadline restarts from the time the difference ended.
    let ended = gap_deadline + Duration::from_secs(3);
    message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 10, seq: 5 }.into(),
        ended,
    );
    assert_eq!(
        time_of_next_difference(&mut message_boxes, ended),
        ended + NO_UPDATES_TIMEOUT
    );
}

#[test]
fn final_channel_difference_returns_its_updates() {
    let mut message_boxes = MessageBoxes::load(loaded_state(), T0);
    message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 10, seq: 5 }.into(),
        T0,
    );
    message_boxes.take_delta();

    let (updates, _, _) = message_boxes.apply_channel_difference(
        tl::types::updates::ChannelDifference {
            r#final: true,
            pts: 22,
            timeout: None,
            new_messages: Vec::new(),
            other_updates: vec![channel_update(CHANNEL, 21), channel_update(CHANNEL, 22)],
            chats: Vec::new(),
            users: Vec::new(),
        }
        .into(),
        T0,
    );
    assert_eq!(
        updates.into_iter().map(|(u, _)| u).collect::<Vec<_>>(),
        vec![channel_update(CHANNEL, 21), channel_update(CHANNEL, 22)]
    );
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            channels: vec![ChannelState {
                id: CHANNEL,
                pts: 22
            }],
            ..StateDelta::default()
        }
    );
    assert_eq!(message_boxes.get_channel_difference(), None);
}

#[test]
fn account_difference_returns_both_boxes_updates() {
    let mut message_boxes = settled(T0);

    // The common box hears from the server; the secondary box doesn't, so its idle deadline
    // passes first, and the difference that follows must still return the common box's updates.
    let now = T0 + NO_UPDATES_TIMEOUT / 2;
    assert_eq!(
        message_boxes.process_updates(updates(NO_DATE, NO_SEQ, 101), now),
        updates_ok(10, 5, 101)
    );
    message_boxes.take_delta();
    let now = T0 + NO_UPDATES_TIMEOUT;
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert!(message_boxes.difference_pending().account);

    let (updates, _, _) = message_boxes.apply_difference(
        tl::types::updates::Difference {
            new_messages: Vec::new(),
            new_encrypted_messages: Vec::new(),
            other_updates: vec![update(102), qts_update(51)],
            chats: Vec::new(),
            users: Vec::new(),
            state: state(11, 6, 102, 51).into(),
        }
        .into(),
        now,
    );
    // Returned in `pts - count` order.
    assert_eq!(
        updates.into_iter().map(|(u, _)| u).collect::<Vec<_>>(),
        vec![qts_update(51), update(102)]
    );
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            common: Some(CommonState {
                pts: 102,
                date: 11,
                seq: 6
            }),
            qts: Some(51),
            ..StateDelta::default()
        }
    );
}

// Socket updates for a box getting its difference wait for it to end.

/// The `pts` of each returned update that has one, in the order returned.
fn pts_of(updates: &[(tl::enums::Update, State)]) -> Vec<i32> {
    updates
        .iter()
        .filter_map(|(update, _)| PtsInfo::from_update(update))
        .map(|info| info.pts)
        .collect()
}

/// `updateChannelTooLong` alone on the socket.
fn channel_too_long(channel_id: i64, pts: Option<i32>) -> UpdatesLike {
    UpdatesLike::Updates(
        tl::types::UpdateShort {
            update: tl::types::UpdateChannelTooLong { channel_id, pts }.into(),
            date: 11,
        }
        .into(),
    )
}

/// A difference of [`CHANNEL`] returning its updates `from..=to`.
fn channel_difference(from: i32, to: i32, r#final: bool) -> tl::enums::updates::ChannelDifference {
    tl::types::updates::ChannelDifference {
        r#final,
        pts: to,
        timeout: None,
        new_messages: Vec::new(),
        other_updates: (from..=to)
            .map(|pts| channel_update(CHANNEL, pts))
            .collect(),
        chats: Vec::new(),
        users: Vec::new(),
    }
    .into()
}

fn channel_delta(pts: i32) -> StateDelta {
    StateDelta {
        channels: vec![ChannelState { id: CHANNEL, pts }],
        ..StateDelta::default()
    }
}

#[test]
fn socket_update_postponed_while_box_diffs() {
    // The channel is at 20 and the server has too much to send. Meanwhile 181 arrives on the
    // socket: it waits for the difference, and moves nothing.
    let diffing = || {
        let mut message_boxes = settled(T0);
        message_boxes
            .process_updates(channel_too_long(CHANNEL, None), T0)
            .unwrap();
        assert_eq!(
            message_boxes.process_updates(channel_updates(CHANNEL, NO_DATE, NO_SEQ, 181), T0),
            Ok((Vec::new(), Vec::new(), Vec::new()))
        );
        assert_eq!(message_boxes.take_delta(), StateDelta::default());
        message_boxes
    };

    // The difference returns it: it comes once, in its place, also across a partial answer.
    let mut message_boxes = diffing();
    let (returned, _, _) =
        message_boxes.apply_channel_difference(channel_difference(21, 100, false), T0);
    assert_eq!(pts_of(&returned), (21..=100).collect::<Vec<_>>());
    let (returned, _, _) =
        message_boxes.apply_channel_difference(channel_difference(101, 181, true), T0);
    assert_eq!(pts_of(&returned), (101..=181).collect::<Vec<_>>());
    assert_eq!(message_boxes.take_delta(), channel_delta(181));

    // The difference ends right before it: it follows the difference's updates.
    let mut message_boxes = diffing();
    let (returned, _, _) =
        message_boxes.apply_channel_difference(channel_difference(21, 180, true), T0);
    assert_eq!(pts_of(&returned), (21..=181).collect::<Vec<_>>());
    assert_eq!(message_boxes.take_delta(), channel_delta(181));
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());

    // The difference ends further back: it waits on the gap, and once the wait passes, the
    // channel's difference is due again.
    let mut message_boxes = diffing();
    let (returned, _, _) =
        message_boxes.apply_channel_difference(channel_difference(21, 179, true), T0);
    assert_eq!(pts_of(&returned), (21..=179).collect::<Vec<_>>());
    assert_eq!(message_boxes.take_delta(), channel_delta(179));
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        T0 + POSSIBLE_GAP_TIMEOUT
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(CHANNEL, 179))
    );

    // A sequenced container whose updates all wait still counts for `date` and `seq`, so the
    // next container follows on instead of finding a gap.
    let mut message_boxes = diffing();
    assert_eq!(
        message_boxes.process_updates(channel_updates(CHANNEL, 11, 6, 182), T0),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    assert_eq!(message_boxes.take_delta(), common_delta(100, 11, 6));
    assert_eq!(
        message_boxes.process_updates(updates(12, 7, 101), T0),
        updates_ok(12, 7, 101)
    );
}

#[test]
fn postponed_updates_after_premature_end() {
    // The channel at 20 gets its difference, and 24, 21 and 22 arrive meanwhile.
    let diffing = || {
        let mut message_boxes = settled(T0);
        message_boxes
            .process_updates(channel_too_long(CHANNEL, None), T0)
            .unwrap();
        for pts in [24, 21, 22] {
            assert_eq!(
                message_boxes.process_updates(channel_updates(CHANNEL, NO_DATE, NO_SEQ, pts), T0),
                Ok((Vec::new(), Vec::new(), Vec::new()))
            );
        }
        message_boxes
    };

    // A temporary failure returns those that follow on, and 24 waits on the gap as if it arrived
    // now: once the wait passes, the channel's difference is due again.
    let mut message_boxes = diffing();
    let returned =
        message_boxes.end_channel_difference(PrematureEndReason::TemporaryServerIssues, T0);
    assert_eq!(pts_of(&returned), vec![21, 22]);
    assert_eq!(message_boxes.take_delta(), channel_delta(22));
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        T0 + POSSIBLE_GAP_TIMEOUT
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(CHANNEL, 22))
    );

    // A banned channel's go with its state.
    let mut message_boxes = diffing();
    assert!(
        message_boxes
            .end_channel_difference(PrematureEndReason::Banned, T0)
            .is_empty()
    );
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            removed_channels: vec![CHANNEL],
            ..StateDelta::default()
        }
    );
}

#[test]
fn socket_update_postponed_while_account_diffs() {
    // The account (pts 100, seq 5) gets its difference, as the server has too much to send.
    // Meanwhile a message arrives (pts 103), then a sequenced container (pts 104, seq 8): their
    // updates wait for the difference, and so does the container's `seq`. A container the
    // account already had (seq 5) is skipped, as when it gets no difference.
    let diffing = || {
        let mut message_boxes = settled(T0);
        assert_eq!(
            message_boxes.process_updates(UpdatesLike::Updates(tl::enums::Updates::TooLong), T0),
            Err(Gap)
        );
        assert!(message_boxes.difference_pending().account);
        let short_message = tl::types::UpdateShortMessage {
            out: false,
            mentioned: false,
            media_unread: false,
            silent: false,
            id: 7,
            user_id: 1,
            message: String::new(),
            pts: 103,
            pts_count: 1,
            date: 12,
            fwd_from: None,
            via_bot_id: None,
            reply_to: None,
            entities: None,
            ttl_period: None,
        };
        let old = UpdatesLike::Updates(
            tl::types::Updates {
                updates: vec![tl::enums::Update::Config],
                users: Vec::new(),
                chats: Vec::new(),
                date: 10,
                seq: 5,
            }
            .into(),
        );
        for container in [
            UpdatesLike::Updates(short_message.into()),
            updates(12, 8, 104),
            old,
        ] {
            assert_eq!(
                message_boxes.process_updates(container, T0),
                Ok((Vec::new(), Vec::new(), Vec::new()))
            );
        }
        assert_eq!(message_boxes.take_delta(), StateDelta::default());
        message_boxes
    };
    let difference = |pts, seq| -> tl::enums::updates::Difference {
        tl::types::updates::Difference {
            new_messages: Vec::new(),
            new_encrypted_messages: Vec::new(),
            other_updates: (101..=pts).map(update).collect(),
            chats: Vec::new(),
            users: Vec::new(),
            state: state(11, seq, pts, 50).into(),
        }
        .into()
    };

    // The difference returns both: each comes once.
    let mut message_boxes = diffing();
    let (returned, _, _) = message_boxes.apply_difference(difference(104, 8), T0);
    assert_eq!(pts_of(&returned), vec![101, 102, 103, 104]);
    assert_eq!(message_boxes.take_delta(), common_delta(104, 11, 8));
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());

    // The difference ends before them, at seq 7: they follow its updates, and the container's
    // `seq` follows on from the difference's.
    let mut message_boxes = diffing();
    let (returned, _, _) = message_boxes.apply_difference(difference(102, 7), T0);
    assert_eq!(pts_of(&returned), vec![101, 102, 103, 104]);
    assert_eq!(message_boxes.take_delta(), common_delta(104, 11, 8));
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());

    // The difference ends at seq 6: a container is missing before theirs, so the account's
    // difference is due again.
    let mut message_boxes = diffing();
    let (returned, _, _) = message_boxes.apply_difference(difference(102, 6), T0);
    assert_eq!(pts_of(&returned), vec![101, 102, 103, 104]);
    assert_eq!(message_boxes.take_delta(), common_delta(104, 11, 6));
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(11, 104, 50))
    );
}

// The property test: a simulated server, a lossy and reordering socket, and the client's
// message boxes, with every delta replayed the way a caller stores it.

const CHANNEL_A: i64 = 7001;
const CHANNEL_B: i64 = 7002;

/// One event of the simulated server.
#[derive(Clone, Debug)]
struct Event {
    /// Unique, carried as the message id (or the user id of a secondary box update).
    id: i32,
    key: Key,
    /// The box's `pts` (or `qts`) after the event.
    pts: i32,
    count: i32,
    /// A new message rather than a deletion. Differences return new messages separately.
    message: bool,
    date: i32,
    seq: i32,
    /// The account's state right after the event, for `differenceSlice`.
    after: tl::types::updates::State,
}

impl Event {
    fn peer(&self) -> tl::enums::Peer {
        match self.key {
            Key::Channel(channel_id) => tl::types::PeerChannel { channel_id }.into(),
            _ => tl::types::PeerUser { user_id: 1 }.into(),
        }
    }

    fn message(&self) -> tl::enums::Message {
        tl::types::MessageEmpty {
            id: self.id,
            peer_id: Some(self.peer()),
        }
        .into()
    }

    fn update(&self) -> tl::enums::Update {
        match (self.key, self.message) {
            (Key::Common, true) => tl::types::UpdateNewMessage {
                message: self.message(),
                pts: self.pts,
                pts_count: self.count,
            }
            .into(),
            (Key::Common, false) => tl::types::UpdateDeleteMessages {
                messages: vec![self.id],
                pts: self.pts,
                pts_count: self.count,
            }
            .into(),
            (Key::Secondary, _) => tl::types::UpdateBotStopped {
                user_id: self.id.into(),
                date: self.date,
                stopped: true,
                qts: self.pts,
            }
            .into(),
            (Key::Channel(_), true) => tl::types::UpdateNewChannelMessage {
                message: self.message(),
                pts: self.pts,
                pts_count: self.count,
            }
            .into(),
            (Key::Channel(channel_id), false) => tl::types::UpdateDeleteChannelMessages {
                channel_id,
                messages: vec![self.id],
                pts: self.pts,
                pts_count: self.count,
            }
            .into(),
        }
    }

    /// The event alone on the socket.
    fn container(&self) -> UpdatesLike {
        UpdatesLike::Updates(
            tl::types::Updates {
                updates: vec![self.update()],
                users: Vec::new(),
                chats: Vec::new(),
                date: self.date,
                seq: self.seq,
            }
            .into(),
        )
    }
}

/// The id of the event an update the message boxes returned stands for.
fn event_id(update: &tl::enums::Update) -> Option<i32> {
    let message_id = |message: &tl::enums::Message| match message {
        tl::enums::Message::Empty(message) => Some(message.id),
        _ => None,
    };
    match update {
        tl::enums::Update::NewMessage(u) => message_id(&u.message),
        tl::enums::Update::DeleteMessages(u) => u.messages.first().copied(),
        tl::enums::Update::BotStopped(u) => i32::try_from(u.user_id).ok(),
        tl::enums::Update::NewChannelMessage(u) => message_id(&u.message),
        tl::enums::Update::DeleteChannelMessages(u) => u.messages.first().copied(),
        _ => None,
    }
}

struct Server {
    state: UpdatesState,
    events: Vec<Event>,
    /// Events sent on the socket and not delivered yet, as indices into `events`.
    wire: Vec<usize>,
}

impl Server {
    fn account_state(&self) -> tl::types::updates::State {
        state(
            self.state.date,
            self.state.seq,
            self.state.pts,
            self.state.qts,
        )
    }

    fn pts(&self, key: Key) -> i32 {
        match key {
            Key::Common => self.state.pts,
            Key::Secondary => self.state.qts,
            Key::Channel(id) => self
                .state
                .channels
                .iter()
                .find(|channel| channel.id == id)
                .map_or(NO_PTS, |channel| channel.pts),
        }
    }

    fn generate(&mut self, key: Key, count: i32, message: bool, sequenced: bool) {
        // The secondary box counts every update as one.
        let count = if key == Key::Secondary { 1 } else { count };
        self.state.date += 1;
        let pts = match key {
            Key::Common => &mut self.state.pts,
            Key::Secondary => &mut self.state.qts,
            Key::Channel(id) => {
                &mut self
                    .state
                    .channels
                    .iter_mut()
                    .find(|channel| channel.id == id)
                    .expect("the model's channels exist")
                    .pts
            }
        };
        *pts += count;
        let pts = *pts;
        // Channel updates don't take part in the account's `seq`.
        let seq = if sequenced && !matches!(key, Key::Channel(_)) {
            self.state.seq += 1;
            self.state.seq
        } else {
            NO_SEQ
        };
        let id = i32::try_from(self.events.len()).expect("few events") + 1;
        self.events.push(Event {
            id,
            key,
            pts,
            count,
            message,
            date: self.state.date,
            seq,
            after: self.account_state(),
        });
        self.wire.push(self.events.len() - 1);
    }

    /// Answer `updates.getDifference` from the state it asks from.
    fn difference(
        &self,
        request: &tl::functions::updates::GetDifference,
        slice: bool,
        channel_updates: bool,
    ) -> tl::enums::updates::Difference {
        let pending = self
            .events
            .iter()
            .filter(|event| match event.key {
                Key::Common => event.pts > request.pts,
                Key::Secondary => event.pts > request.qts,
                Key::Channel(_) => false,
            })
            .collect::<Vec<_>>();
        let Some((first, last)) = pending.first().zip(pending.last()) else {
            return tl::types::updates::DifferenceEmpty {
                date: self.state.date,
                seq: self.state.seq,
            }
            .into();
        };
        let sliced = slice && pending.len() > 1;
        let included = if sliced {
            &pending[..pending.len() / 2]
        } else {
            &pending[..]
        };
        let last: &Event = if sliced {
            included[included.len() - 1]
        } else {
            last
        };

        let mut new_messages = Vec::new();
        let mut other_updates = Vec::new();
        for event in included {
            if event.key == Key::Common && event.message {
                new_messages.push(event.message());
            } else {
                other_updates.push(event.update());
            }
        }
        // The server may add channel updates from the same stretch of time.
        if channel_updates {
            other_updates.extend(
                self.events
                    .iter()
                    .filter(|event| {
                        matches!(event.key, Key::Channel(_))
                            && (first.id..=last.id).contains(&event.id)
                    })
                    .map(Event::update),
            );
        }
        if sliced {
            // The state after the slice's last event, but never behind the request: the client
            // already has what the other box did since (`last.after` may be older there).
            let mut intermediate_state = last.after.clone();
            intermediate_state.pts = intermediate_state.pts.max(request.pts);
            intermediate_state.qts = intermediate_state.qts.max(request.qts);
            tl::types::updates::DifferenceSlice {
                new_messages,
                new_encrypted_messages: Vec::new(),
                other_updates,
                chats: Vec::new(),
                users: Vec::new(),
                intermediate_state: intermediate_state.into(),
            }
            .into()
        } else {
            tl::types::updates::Difference {
                new_messages,
                new_encrypted_messages: Vec::new(),
                other_updates,
                chats: Vec::new(),
                users: Vec::new(),
                state: self.account_state().into(),
            }
            .into()
        }
    }

    /// Answer `updates.getChannelDifference` from the `pts` it asks from, `limit` events at most.
    fn channel_difference(
        &self,
        request: &tl::functions::updates::GetChannelDifference,
        limit: usize,
    ) -> tl::enums::updates::ChannelDifference {
        let tl::enums::InputChannel::Channel(channel) = &request.channel else {
            panic!("the message boxes ask for a channel");
        };
        let key = Key::Channel(channel.channel_id);
        let pending = self
            .events
            .iter()
            .filter(|event| event.key == key && event.pts > request.pts)
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return channel_difference_empty(self.pts(key));
        }
        let taken = &pending[..limit.min(pending.len())];
        let r#final = taken.len() == pending.len();
        tl::types::updates::ChannelDifference {
            r#final,
            pts: if r#final {
                self.pts(key)
            } else {
                taken[taken.len() - 1].pts
            },
            timeout: None,
            new_messages: taken
                .iter()
                .filter(|event| event.message)
                .map(|event| event.message())
                .collect(),
            other_updates: taken
                .iter()
                .filter(|event| !event.message)
                .map(|event| event.update())
                .collect(),
            chats: Vec::new(),
            users: Vec::new(),
        }
        .into()
    }
}

/// The client: its message boxes, its clock, the state it stored from the deltas and the
/// events it was handed.
struct Client {
    message_boxes: MessageBoxes,
    now: Now,
    stored: UpdatesState,
    /// The ids of the events handed out.
    handed_out: BTreeSet<i32>,
    /// For each box, the highest `pts` of the events handed out so far.
    handed_out_up_to: BTreeMap<Key, i32>,
}

impl Client {
    /// Record the events one call handed out. None was handed out before, and each comes after
    /// every event of its box that an earlier call handed out: a box's events come in order, call
    /// by call. (Within one call they needn't: a difference returns its new messages last.)
    fn handed_out(&mut self, server: &Server, updates: &[(tl::enums::Update, State)]) {
        let events = updates
            .iter()
            .filter_map(|(update, _)| event_id(update))
            .map(|id| &server.events[usize::try_from(id - 1).expect("ids start at 1")])
            .collect::<Vec<_>>();
        for event in &events {
            assert!(
                self.handed_out.insert(event.id),
                "{event:?} handed out twice"
            );
            if let Some(&up_to) = self.handed_out_up_to.get(&event.key) {
                assert!(
                    event.pts > up_to,
                    "{event:?} handed out after an event of its box at {up_to}"
                );
            }
        }
        for event in events {
            let up_to = self.handed_out_up_to.entry(event.key).or_insert(event.pts);
            *up_to = (*up_to).max(event.pts);
        }
    }

    /// Store the delta, then check the stored state against the live one and the server's.
    fn store(&mut self, server: &Server, base: &UpdatesState) {
        apply_delta(&mut self.stored, self.message_boxes.take_delta());
        assert_eq!(self.stored, self.message_boxes.session_state());

        for key in [
            Key::Common,
            Key::Secondary,
            Key::Channel(CHANNEL_A),
            Key::Channel(CHANNEL_B),
        ] {
            let stored = stored_pts(&self.stored, key);
            // The stored state sits where an event (or the start) left the box, never inside
            // an event that counts more than one.
            assert!(
                stored == stored_pts(base, key)
                    || server
                        .events
                        .iter()
                        .any(|event| event.key == key && event.pts == stored),
                "{key:?} stored at {stored}, which no event ends at"
            );
            // Nothing the stored state covers was skipped.
            for event in server.events.iter().filter(|event| event.key == key) {
                assert!(
                    event.pts > stored || self.handed_out.contains(&event.id),
                    "{key:?} stored at {stored}, past {event:?}, which was never returned"
                );
            }
        }
    }

    fn check_pending(&self) {
        let pending = self.message_boxes.difference_pending();
        assert_eq!(
            pending.account,
            self.message_boxes.get_difference().is_some()
        );
        assert_eq!(
            pending.channels > 0,
            self.message_boxes.get_channel_difference().is_some()
        );
    }

    fn serve_difference(&mut self, server: &Server, slice: bool, channel_updates: bool) -> bool {
        let Some(request) = self.message_boxes.get_difference() else {
            return false;
        };
        let difference = server.difference(&request, slice, channel_updates);
        let (returned, _, _) = self.message_boxes.apply_difference(difference, self.now);
        self.handed_out(server, &returned);
        true
    }

    fn serve_channel_difference(&mut self, server: &Server, limit: usize) -> bool {
        let Some(request) = self.message_boxes.get_channel_difference() else {
            return false;
        };
        let difference = server.channel_difference(&request, limit);
        let (returned, _, _) = self
            .message_boxes
            .apply_channel_difference(difference, self.now);
        self.handed_out(server, &returned);
        true
    }
}

fn stored_pts(state: &UpdatesState, key: Key) -> i32 {
    match key {
        Key::Common => state.pts,
        Key::Secondary => state.qts,
        Key::Channel(id) => state
            .channels
            .iter()
            .find(|channel| channel.id == id)
            .map_or(NO_PTS, |channel| channel.pts),
    }
}

#[derive(Clone, Debug)]
enum Op {
    /// The server generates an event and sends it.
    Generate {
        key: Key,
        count: i32,
        message: bool,
        sequenced: bool,
    },
    /// The socket delivers a sent event, out of order; a duplicate stays on the wire.
    Deliver { index: usize, duplicate: bool },
    /// The socket loses a sent event.
    Lose { index: usize },
    /// Time passes and the client checks its deadlines.
    Wait { millis: u64 },
    /// The client fetches the account difference, if one is due.
    ServeDifference { slice: bool, channel_updates: bool },
    /// The client fetches a channel difference, if one is due, or gives up on it for now.
    ServeChannelDifference { limit: usize, fail: bool },
}

fn op() -> impl Strategy<Value = Op> {
    let key = prop_oneof![
        Just(Key::Common),
        Just(Key::Secondary),
        Just(Key::Channel(CHANNEL_A)),
        Just(Key::Channel(CHANNEL_B)),
    ];
    prop_oneof![
        4 => (key, 1..=2i32, any::<bool>(), any::<bool>()).prop_map(
            |(key, count, message, sequenced)| Op::Generate {
                key,
                count,
                message,
                sequenced,
            }
        ),
        5 => (any::<usize>(), prop::bool::weighted(0.2))
            .prop_map(|(index, duplicate)| Op::Deliver { index, duplicate }),
        1 => any::<usize>().prop_map(|index| Op::Lose { index }),
        2 => prop_oneof![0..400u64, 400..2_000u64, Just(16 * 60 * 1_000u64)]
            .prop_map(|millis| Op::Wait { millis }),
        2 => (any::<bool>(), any::<bool>()).prop_map(|(slice, channel_updates)| {
            Op::ServeDifference {
                slice,
                channel_updates,
            }
        }),
        2 => (1..=3usize, prop::bool::weighted(0.1))
            .prop_map(|(limit, fail)| Op::ServeChannelDifference { limit, fail }),
    ]
}

fn run(ops: Vec<(Op, bool)>) {
    let base = UpdatesState {
        pts: 100,
        qts: 50,
        date: 1000,
        seq: 10,
        channels: vec![
            ChannelState {
                id: CHANNEL_A,
                pts: 200,
            },
            ChannelState {
                id: CHANNEL_B,
                pts: 300,
            },
        ],
    };
    let mut server = Server {
        state: base.clone(),
        events: Vec::new(),
        wire: Vec::new(),
    };
    let mut client = Client {
        message_boxes: MessageBoxes::load(base.clone(), T0),
        now: T0,
        stored: base.clone(),
        handed_out: BTreeSet::new(),
        handed_out_up_to: BTreeMap::new(),
    };

    for (op, store) in ops {
        match op {
            Op::Generate {
                key,
                count,
                message,
                sequenced,
            } => server.generate(key, count, message, sequenced),
            Op::Deliver { index, duplicate } => {
                if !server.wire.is_empty() {
                    let index = index % server.wire.len();
                    let event = if duplicate {
                        server.wire[index]
                    } else {
                        server.wire.remove(index)
                    };
                    let container = server.events[event].container();
                    if let Ok((returned, _, _)) =
                        client.message_boxes.process_updates(container, client.now)
                    {
                        client.handed_out(&server, &returned);
                    }
                }
            }
            Op::Lose { index } => {
                if !server.wire.is_empty() {
                    let index = index % server.wire.len();
                    server.wire.remove(index);
                }
            }
            Op::Wait { millis } => {
                client.now += Duration::from_millis(millis);
                let deadline = client.message_boxes.check_deadlines(client.now);
                assert!(deadline >= client.now, "a deadline in the past spins");
            }
            Op::ServeDifference {
                slice,
                channel_updates,
            } => {
                client.serve_difference(&server, slice, channel_updates);
            }
            Op::ServeChannelDifference { limit, fail } => {
                if fail {
                    if client.message_boxes.get_channel_difference().is_some() {
                        let returned = client.message_boxes.end_channel_difference(
                            PrematureEndReason::TemporaryServerIssues,
                            client.now,
                        );
                        client.handed_out(&server, &returned);
                    }
                } else {
                    client.serve_channel_difference(&server, limit);
                }
            }
        }
        client.check_pending();
        if store {
            client.store(&server, &base);
        }
    }
    client.store(&server, &base);

    // Drain: the socket delivers what is left, and the client fetches every difference that
    // falls due until it holds the server's state and was handed every event.
    while let Some(event) = server.wire.pop() {
        let container = server.events[event].container();
        if let Ok((returned, _, _)) = client.message_boxes.process_updates(container, client.now) {
            client.handed_out(&server, &returned);
        }
    }
    for _ in 0..3 {
        client.now += NO_UPDATES_TIMEOUT + POSSIBLE_GAP_TIMEOUT;
        client.message_boxes.check_deadlines(client.now);
        while client.serve_difference(&server, false, false)
            || client.serve_channel_difference(&server, usize::MAX)
        {}
    }
    client.store(&server, &base);
    assert_eq!(client.stored, server.state);
    assert_eq!(client.handed_out.len(), server.events.len());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// After every call, the stored deltas equal the live state, and the stored state covers only
    /// events the client was handed: never one held back or dropped. Every event is handed out
    /// once, and after the earlier events of its box.
    #[test]
    fn delta_matches_returned_updates(
        ops in prop::collection::vec((op(), prop::bool::weighted(0.7)), 1..120)
    ) {
        run(ops);
    }
}
