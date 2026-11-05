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

fn qts_updates(qts: i32) -> UpdatesLike {
    UpdatesLike::Updates(tl::enums::Updates::Updates(tl::types::Updates {
        updates: vec![qts_update(qts)],
        users: Vec::new(),
        chats: Vec::new(),
        date: NO_DATE,
        seq: NO_SEQ,
    }))
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

/// The channel an `updates.getChannelDifference` request asks about.
fn channel_of(request: &tl::functions::updates::GetChannelDifference) -> i64 {
    match &request.channel {
        tl::enums::InputChannel::Channel(channel) => channel.channel_id,
        _ => panic!("the message boxes ask for a channel"),
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
        message_boxes.apply_channel_difference(
            channel_of(&request),
            channel_difference_empty(request.pts),
            now,
        );
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
    // On startup, only the account's difference (TF7).
    assert_eq!(message_boxes.get_channel_difference(), None);
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

    message_boxes.try_set_channel_state(98, 76);
    message_boxes.try_set_channel_state(54, 32);
    message_boxes.try_set_channel_state(98, 10);

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
    message_boxes.try_set_channel_state(12, 34);

    now += NO_UPDATES_TIMEOUT;

    // A quiet channel doesn't time out (TF7); the caller asks for its difference.
    assert_eq!(
        message_boxes.check_deadlines(now),
        next_updates_deadline(now)
    );
    assert_eq!(message_boxes.get_channel_difference(), None);
    assert!(message_boxes.begin_channel_difference(12));
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(12, 34))
    );

    message_boxes.apply_channel_difference(12, channel_difference_empty(56), now);

    assert_eq!(message_boxes.get_difference(), None);
    assert_eq!(message_boxes.get_channel_difference(), None);
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
    let mut message_boxes = MessageBoxes::new();
    message_boxes.try_set_channel_state(12, 34);

    assert!(message_boxes.begin_channel_difference(12));
    assert!(message_boxes.get_channel_difference().is_some());

    assert!(
        message_boxes
            .end_channel_difference(12, PrematureEndReason::TemporaryServerIssues, T0)
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
    let mut message_boxes = MessageBoxes::new();
    message_boxes.try_set_channel_state(12, 34);

    assert!(message_boxes.begin_channel_difference(12));
    assert!(message_boxes.get_channel_difference().is_some());

    assert!(
        message_boxes
            .end_channel_difference(12, PrematureEndReason::Banned, T0)
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
    message_boxes.try_set_channel_state(11, 12);
    message_boxes.try_set_channel_state(21, 22);
    message_boxes.try_set_channel_state(31, 32);

    // t(0) = first channel has a gap, which waits until t(5)
    message_boxes
        .process_updates(channel_updates(11, NO_DATE, NO_SEQ, 14), now)
        .unwrap();

    // t(3) = second and third channel have gaps, which wait until t(8)
    now += 3 * (POSSIBLE_GAP_TIMEOUT / 5);
    message_boxes
        .process_updates(channel_updates(21, NO_DATE, NO_SEQ, 24), now)
        .unwrap();
    message_boxes
        .process_updates(channel_updates(31, NO_DATE, NO_SEQ, 34), now)
        .unwrap();

    // t(5) = first channel's gap times out
    now += 2 * (POSSIBLE_GAP_TIMEOUT / 5);
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(11, 12))
    );

    // t(10) = second and third channel's gaps should have timed out by now, but didn't yet
    now += POSSIBLE_GAP_TIMEOUT;
    assert!(
        message_boxes
            .end_channel_difference(11, PrematureEndReason::TemporaryServerIssues, now)
            .is_empty()
    );
    assert_eq!(message_boxes.get_channel_difference(), None);

    // t(10) = checking deadlines now triggers difference for second and third
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(21, 22))
    );
    assert!(
        message_boxes
            .end_channel_difference(21, PrematureEndReason::TemporaryServerIssues, now)
            .is_empty()
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(31, 32))
    );
    assert!(
        message_boxes
            .end_channel_difference(31, PrematureEndReason::TemporaryServerIssues, now)
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
    assert!(message_boxes.begin_channel_difference(43));
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
        43,
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
    let mut message_boxes = settled(T0);
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(CHANNEL, 20))
    );

    let (updates, _, _) =
        message_boxes.apply_channel_difference(CHANNEL, channel_difference_empty(25), T0);
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
    let mut message_boxes = settled(T0);
    assert!(message_boxes.begin_channel_difference(CHANNEL));

    assert!(
        message_boxes
            .end_channel_difference(CHANNEL, PrematureEndReason::Banned, T0)
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
    let mut message_boxes = settled(T0);
    assert!(message_boxes.begin_channel_difference(CHANNEL));

    let (updates, _, _) = message_boxes.apply_channel_difference(
        CHANNEL,
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

    // Only the secondary box finds a gap. Once its wait passes, the difference that follows must
    // still return the common box's updates.
    assert_eq!(
        message_boxes.process_updates(qts_updates(52), T0),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    let now = T0 + POSSIBLE_GAP_TIMEOUT;
    assert_eq!(message_boxes.check_deadlines(now), now);
    assert_eq!(
        message_boxes.difference_pending(),
        DiffPending {
            account: true,
            channels: 0
        }
    );

    let (updates, _, _) = message_boxes.apply_difference(
        tl::types::updates::Difference {
            new_messages: Vec::new(),
            new_encrypted_messages: Vec::new(),
            other_updates: vec![update(101), qts_update(51), qts_update(52)],
            chats: Vec::new(),
            users: Vec::new(),
            state: state(11, 6, 101, 52).into(),
        }
        .into(),
        now,
    );
    // Returned in `pts - count` order.
    assert_eq!(
        updates.into_iter().map(|(u, _)| u).collect::<Vec<_>>(),
        vec![qts_update(51), qts_update(52), update(101)]
    );
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            common: Some(CommonState {
                pts: 101,
                date: 11,
                seq: 6
            }),
            qts: Some(52),
            ..StateDelta::default()
        }
    );
}

// Socket updates for a box getting its difference wait for it to end.

/// The `pts` of each returned update that has one, in the order returned.
fn pts_of(updates: &[(tl::enums::Update, State)]) -> Vec<i32> {
    updates
        .iter()
        .filter_map(|(update, _)| PtsInfo::from_update(update).ok().flatten())
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
            .process_updates(channel_too_long(CHANNEL, Some(180)), T0)
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
        message_boxes.apply_channel_difference(CHANNEL, channel_difference(21, 100, false), T0);
    assert_eq!(pts_of(&returned), (21..=100).collect::<Vec<_>>());
    let (returned, _, _) =
        message_boxes.apply_channel_difference(CHANNEL, channel_difference(101, 181, true), T0);
    assert_eq!(pts_of(&returned), (101..=181).collect::<Vec<_>>());
    assert_eq!(message_boxes.take_delta(), channel_delta(181));

    // The difference ends right before it: it follows the difference's updates.
    let mut message_boxes = diffing();
    let (returned, _, _) =
        message_boxes.apply_channel_difference(CHANNEL, channel_difference(21, 180, true), T0);
    assert_eq!(pts_of(&returned), (21..=181).collect::<Vec<_>>());
    assert_eq!(message_boxes.take_delta(), channel_delta(181));
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());

    // The difference ends further back: it waits on the gap, and once the wait passes, the
    // channel's difference is due again.
    let mut message_boxes = diffing();
    let (returned, _, _) =
        message_boxes.apply_channel_difference(CHANNEL, channel_difference(21, 179, true), T0);
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
        assert!(message_boxes.begin_channel_difference(CHANNEL));
        for pts in [24, 21, 22] {
            assert_eq!(
                message_boxes.process_updates(channel_updates(CHANNEL, NO_DATE, NO_SEQ, pts), T0),
                Ok((Vec::new(), Vec::new(), Vec::new()))
            );
        }
        message_boxes
    };

    // A temporary failure returns those that follow on, and drops 24: no gap gets the
    // difference ahead of the backoff, and the retry returns 24 with 23.
    let mut message_boxes = diffing();
    let returned = message_boxes.end_channel_difference(
        CHANNEL,
        PrematureEndReason::TemporaryServerIssues,
        T0,
    );
    assert_eq!(pts_of(&returned), vec![21, 22]);
    assert_eq!(message_boxes.take_delta(), channel_delta(22));
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        T0 + Duration::from_secs(1)
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(CHANNEL, 22))
    );

    // A banned channel's go with its state.
    let mut message_boxes = diffing();
    assert!(
        message_boxes
            .end_channel_difference(CHANNEL, PrematureEndReason::Banned, T0)
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

// TF7: channel differences only on a gap, a server's request or the caller's.

#[test]
fn load_diffs_account_boxes_only() {
    let mut state = loaded_state();
    state.channels = (1..=300).map(|id| ChannelState { id, pts: 10 }).collect();
    let mut message_boxes = MessageBoxes::load(state, T0);
    assert_eq!(
        message_boxes.difference_pending(),
        DiffPending {
            account: true,
            channels: 0
        }
    );
    message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 10, seq: 5 }.into(),
        T0,
    );
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    assert_eq!(message_boxes.get_channel_difference(), None);
}

#[test]
fn quiet_channel_never_diffs() {
    let mut state = loaded_state();
    state.channels = (1..=300).map(|id| ChannelState { id, pts: 10 }).collect();
    let mut message_boxes = MessageBoxes::load(state.clone(), T0);

    // A day of a driver sleeping from deadline to deadline, with the account quiet too: only
    // the account's idle difference ever falls due.
    let mut now = T0;
    let mut account_differences = Vec::new();
    while now < T0 + Duration::from_secs(24 * 60 * 60) {
        if message_boxes.get_difference().is_some() {
            account_differences.push(now);
            message_boxes.apply_difference(
                tl::types::updates::DifferenceEmpty {
                    date: state.date,
                    seq: state.seq,
                }
                .into(),
                now,
            );
        }
        assert_eq!(message_boxes.difference_pending().channels, 0);
        assert_eq!(message_boxes.get_channel_difference(), None);
        let deadline = message_boxes.check_deadlines(now);
        assert!(deadline > now || message_boxes.difference_pending().account);
        now = deadline;
    }
    // The start-up difference, then one every 15 minutes.
    assert_eq!(
        account_differences,
        (0..24 * 4)
            .map(|i| T0 + i * NO_UPDATES_TIMEOUT)
            .collect::<Vec<_>>()
    );
    assert_eq!(message_boxes.session_state(), state);
}

#[test]
fn account_idle_deadline_fires() {
    let mut message_boxes = settled(T0);
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        T0 + NO_UPDATES_TIMEOUT
    );
    assert_eq!(
        message_boxes.difference_pending(),
        DiffPending {
            account: true,
            channels: 0
        }
    );
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 100, 50))
    );
}

#[test]
fn active_account_runs_no_idle_difference() {
    let mut message_boxes = settled(T0);

    // A day of a driver sleeping from deadline to deadline, with a common box update every 10
    // minutes and none on the secondary box: the account never goes 15 minutes without updates.
    let every = Duration::from_secs(10 * 60);
    let mut now = T0;
    let mut next_update = T0 + every;
    let mut pts = 100;
    while now < T0 + Duration::from_secs(24 * 60 * 60) {
        let deadline = message_boxes.check_deadlines(now);
        assert_eq!(
            message_boxes.difference_pending(),
            DiffPending::default(),
            "at {:?}",
            now - T0
        );
        if next_update <= deadline {
            now = next_update;
            pts += 1;
            assert_eq!(
                message_boxes.process_updates(updates(NO_DATE, NO_SEQ, pts), now),
                updates_ok(10, 5, pts)
            );
            next_update += every;
        } else {
            now = deadline;
        }
    }

    // Once the updates stop, the difference is due 15 minutes after the last one.
    let last_update = next_update - every;
    assert_eq!(
        time_of_next_difference(&mut message_boxes, now),
        last_update + NO_UPDATES_TIMEOUT
    );
}

#[test]
fn qts_update_restarts_account_deadline() {
    let mut message_boxes = settled(T0);

    // Only the secondary box hears from the server, and that restarts the account's deadline.
    let heard = T0 + Duration::from_secs(10 * 60);
    assert_eq!(
        message_boxes.check_deadlines(heard),
        T0 + NO_UPDATES_TIMEOUT
    );
    let (updates, _, _) = message_boxes
        .process_updates(qts_updates(51), heard)
        .unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(
        time_of_next_difference(&mut message_boxes, heard),
        heard + NO_UPDATES_TIMEOUT
    );
    assert_eq!(
        message_boxes.get_difference(),
        Some(get_difference(10, 100, 51))
    );
}

#[test]
fn channel_too_long_triggers_channel_diff() {
    let mut message_boxes = settled(T0);

    let too_long = UpdatesLike::Updates(
        tl::types::UpdateShort {
            update: tl::types::UpdateChannelTooLong {
                channel_id: CHANNEL,
                pts: None,
            }
            .into(),
            date: 11,
        }
        .into(),
    );
    assert_eq!(
        message_boxes.process_updates(too_long, T0),
        Ok((Vec::new(), Vec::new(), Vec::new()))
    );
    assert_eq!(
        message_boxes.difference_pending(),
        DiffPending {
            account: false,
            channels: 1
        }
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(CHANNEL, 20))
    );
}

#[test]
fn channel_too_long_keeps_local_pts() {
    let mut message_boxes = settled(T0);

    // The channel is at 20; the server says it has too much to send, and is at 180.
    let too_long = UpdatesLike::Updates(
        tl::types::Updates {
            updates: vec![
                tl::types::UpdateChannelTooLong {
                    channel_id: CHANNEL,
                    pts: Some(180),
                }
                .into(),
            ],
            users: Vec::new(),
            chats: Vec::new(),
            date: NO_DATE,
            seq: NO_SEQ,
        }
        .into(),
    );
    message_boxes.process_updates(too_long, T0).unwrap();
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
    // The difference starts from the channel's own state.
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(CHANNEL, 20))
    );

    // Updates up to 180 come back first, and only then the state moves past them.
    let (updates, _, _) = message_boxes.apply_channel_difference(
        CHANNEL,
        tl::types::updates::ChannelDifference {
            r#final: false,
            pts: 100,
            timeout: None,
            new_messages: Vec::new(),
            other_updates: (21..=100).map(|pts| channel_update(CHANNEL, pts)).collect(),
            chats: Vec::new(),
            users: Vec::new(),
        }
        .into(),
        T0,
    );
    assert_eq!(updates.len(), 80);
    assert_eq!(
        message_boxes.take_delta().channels,
        vec![ChannelState {
            id: CHANNEL,
            pts: 100
        }]
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(CHANNEL, 100))
    );
    let (updates, _, _) = message_boxes.apply_channel_difference(
        CHANNEL,
        tl::types::updates::ChannelDifference {
            r#final: true,
            pts: 180,
            timeout: None,
            new_messages: Vec::new(),
            other_updates: (101..=180)
                .map(|pts| channel_update(CHANNEL, pts))
                .collect(),
            chats: Vec::new(),
            users: Vec::new(),
        }
        .into(),
        T0,
    );
    assert_eq!(updates.len(), 80);
    assert_eq!(
        message_boxes.take_delta().channels,
        vec![ChannelState {
            id: CHANNEL,
            pts: 180
        }]
    );
    assert_eq!(message_boxes.get_channel_difference(), None);
}

#[test]
fn channel_too_long_creates_unknown_entry() {
    let mut message_boxes = settled(T0);
    let unknown = CHANNEL + 1;

    // Without a `pts`, there is nothing to get the difference from.
    let too_long = |pts| {
        UpdatesLike::Updates(
            tl::types::UpdateShort {
                update: tl::types::UpdateChannelTooLong {
                    channel_id: unknown,
                    pts,
                }
                .into(),
                date: 11,
            }
            .into(),
        )
    };
    message_boxes.process_updates(too_long(None), T0).unwrap();
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    assert_eq!(message_boxes.take_delta(), StateDelta::default());

    // With one, the channel starts there.
    message_boxes
        .process_updates(too_long(Some(500)), T0)
        .unwrap();
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            channels: vec![ChannelState {
                id: unknown,
                pts: 500
            }],
            ..StateDelta::default()
        }
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(unknown, 500))
    );
}

#[test]
fn no_site_arms_a_channel_idle_deadline() {
    // Channels have no idle deadline: only the account has one. Besides a possible gap's wait,
    // the only deadline a channel has of its own is the retry after a temporary failure, and no
    // other site arms it.
    fn channel_retries(message_boxes: &MessageBoxes) -> Vec<(i64, Now)> {
        message_boxes
            .entries
            .iter()
            .filter_map(|entry| match entry.key {
                Key::Channel(id) => entry.retry_at.map(|at| (id, at)),
                _ => None,
            })
            .collect()
    }
    fn assert_no_channel_deadline(message_boxes: &MessageBoxes) {
        assert_eq!(channel_retries(message_boxes), Vec::new());
    }

    // Every way a channel entry is made or touched: loaded, set from dialogs, created by its
    // first update, created and diffed by `updateChannelTooLong`, updated in and out of order,
    // returned by the account's and its own difference, final or not, and ended early.
    let mut now = T0;
    let mut message_boxes = MessageBoxes::load(loaded_state(), now);
    message_boxes.try_set_channel_state(2000, 1);
    message_boxes
        .process_updates(channel_updates(3000, NO_DATE, NO_SEQ, 5), now)
        .unwrap();
    message_boxes
        .process_updates(
            UpdatesLike::Updates(
                tl::types::UpdateShort {
                    update: tl::types::UpdateChannelTooLong {
                        channel_id: 4000,
                        pts: Some(9),
                    }
                    .into(),
                    date: 11,
                }
                .into(),
            ),
            now,
        )
        .unwrap();
    assert_no_channel_deadline(&message_boxes);

    message_boxes.apply_difference(
        tl::types::updates::Difference {
            new_messages: Vec::new(),
            new_encrypted_messages: Vec::new(),
            other_updates: vec![channel_update(CHANNEL, 21), channel_update(2000, 3)],
            chats: Vec::new(),
            users: Vec::new(),
            state: state(10, 5, 100, 50).into(),
        }
        .into(),
        now,
    );
    assert_no_channel_deadline(&message_boxes);

    let with_timeout = |r#final, pts| {
        tl::types::updates::ChannelDifference {
            r#final,
            pts,
            timeout: Some(30),
            new_messages: Vec::new(),
            other_updates: vec![channel_update(4000, pts)],
            chats: Vec::new(),
            users: Vec::new(),
        }
        .into()
    };
    message_boxes.apply_channel_difference(4000, with_timeout(false, 10), now);
    message_boxes.apply_channel_difference(4000, with_timeout(true, 11), now);
    assert_no_channel_deadline(&message_boxes);

    now += POSSIBLE_GAP_TIMEOUT;
    message_boxes.check_deadlines(now);
    while let Some(request) = message_boxes.get_channel_difference() {
        message_boxes.apply_channel_difference(
            channel_of(&request),
            channel_difference_empty(request.pts + 1),
            now,
        );
    }
    assert_no_channel_deadline(&message_boxes);
    // Nothing is armed but the account's idle deadline, from the end of its difference.
    assert_eq!(message_boxes.check_deadlines(now), T0 + NO_UPDATES_TIMEOUT);

    // Ended early: the retry, and nothing else.
    assert!(message_boxes.begin_channel_difference(3000));
    assert!(
        message_boxes
            .end_channel_difference(3000, PrematureEndReason::TemporaryServerIssues, now)
            .is_empty()
    );
    message_boxes
        .process_updates(channel_updates(CHANNEL, NO_DATE, NO_SEQ, 22), now)
        .unwrap();
    let retry = now + Duration::from_secs(1);
    assert_eq!(channel_retries(&message_boxes), vec![(3000, retry)]);
    assert_eq!(message_boxes.check_deadlines(now), retry);
    assert_eq!(message_boxes.entries.len(), 2 + 4);
}

#[test]
fn explicit_channel_diff_runs_once() {
    let mut message_boxes = settled(T0);
    assert!(!message_boxes.begin_channel_difference(CHANNEL + 1));
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    // Asking again while it is pending changes nothing.
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    assert_eq!(message_boxes.difference_pending().channels, 1);

    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(CHANNEL, 20))
    );
    message_boxes.apply_channel_difference(
        CHANNEL,
        tl::types::updates::ChannelDifferenceEmpty {
            r#final: true,
            pts: 20,
            timeout: Some(5),
        }
        .into(),
        T0,
    );
    assert_eq!(message_boxes.get_channel_difference(), None);

    // The server's `timeout` is the caller's to act on: the channel isn't diffed again by itself.
    let mut now = T0;
    while now < T0 + Duration::from_secs(60 * 60) {
        now = message_boxes.check_deadlines(now);
        assert_eq!(message_boxes.difference_pending().channels, 0);
        if message_boxes.get_difference().is_some() {
            message_boxes.apply_difference(
                tl::types::updates::DifferenceEmpty { date: 10, seq: 5 }.into(),
                now,
            );
        }
    }
}

#[test]
fn explicit_channel_diff_goes_first() {
    let (a, b, c) = (CHANNEL + 1, CHANNEL + 2, CHANNEL + 3);
    let mut message_boxes = settled(T0);
    message_boxes.try_set_channel_state(a, 10);
    message_boxes.try_set_channel_state(b, 20);
    message_boxes.try_set_channel_state(c, 30);
    message_boxes.take_delta();

    // The server has too much to send for `a` and `b`: they wait in that order.
    let too_long = |channel_id| {
        tl::types::UpdateChannelTooLong {
            channel_id,
            pts: None,
        }
        .into()
    };
    message_boxes
        .process_updates(
            UpdatesLike::Updates(
                tl::types::Updates {
                    updates: vec![too_long(a), too_long(b)],
                    users: Vec::new(),
                    chats: Vec::new(),
                    date: NO_DATE,
                    seq: NO_SEQ,
                }
                .into(),
            ),
            T0,
        )
        .unwrap();
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(a, 10))
    );

    // The user opens a channel: it goes first, also if it was already waiting.
    assert!(message_boxes.begin_channel_difference(c));
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(c, 30))
    );
    assert!(message_boxes.begin_channel_difference(b));
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(b, 20))
    );
    assert_eq!(message_boxes.difference_pending().channels, 3);

    // `c` goes first while `b`'s request is in flight: the answer still goes to `b` alone.
    let request = message_boxes.get_channel_difference().unwrap();
    assert!(message_boxes.begin_channel_difference(c));
    message_boxes.apply_channel_difference(channel_of(&request), channel_difference_empty(25), T0);
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            channels: vec![ChannelState { id: b, pts: 25 }],
            ..StateDelta::default()
        }
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(c, 30))
    );

    // The same for a request that fails.
    let request = message_boxes.get_channel_difference().unwrap();
    assert!(message_boxes.begin_channel_difference(a));
    assert!(
        message_boxes
            .end_channel_difference(
                channel_of(&request),
                PrematureEndReason::TemporaryServerIssues,
                T0,
            )
            .is_empty()
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(a, 10))
    );
    assert_eq!(message_boxes.difference_pending().channels, 1);
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
}

#[test]
fn temporary_channel_failure_retries_with_backoff() {
    let mut message_boxes = settled(T0);
    let fail = |message_boxes: &mut MessageBoxes, now| {
        let request = message_boxes.get_channel_difference().unwrap();
        assert_eq!(channel_of(&request), CHANNEL);
        assert!(
            message_boxes
                .end_channel_difference(CHANNEL, PrematureEndReason::TemporaryServerIssues, now,)
                .is_empty()
        );
        assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    };
    let pending = DiffPending {
        account: false,
        channels: 1,
    };
    let secs = Duration::from_secs;

    // Each failure in a row waits twice as long as the one before, up to 64 s.
    let mut now = T0;
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    fail(&mut message_boxes, now);
    for wait in [1, 2, 4, 8, 16, 32, 64, 64] {
        let due = time_of_next_difference(&mut message_boxes, now);
        assert_eq!(due, now + secs(wait));
        assert_eq!(message_boxes.difference_pending(), pending);
        now = due;
        fail(&mut message_boxes, now);
    }

    // The server answers: the next failure waits 1 s again.
    now = time_of_next_difference(&mut message_boxes, now);
    message_boxes.apply_channel_difference(CHANNEL, channel_difference_empty(20), now);
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    fail(&mut message_boxes, now);
    assert_eq!(
        time_of_next_difference(&mut message_boxes, now),
        now + secs(1)
    );
    now += secs(1);
    fail(&mut message_boxes, now);

    // An answer that isn't final counts as well: the server works again.
    now = time_of_next_difference(&mut message_boxes, now);
    message_boxes.apply_channel_difference(
        CHANNEL,
        tl::types::updates::ChannelDifference {
            r#final: false,
            pts: 21,
            timeout: None,
            new_messages: Vec::new(),
            other_updates: vec![channel_update(CHANNEL, 21)],
            chats: Vec::new(),
            users: Vec::new(),
        }
        .into(),
        now,
    );
    assert_eq!(message_boxes.difference_pending(), pending);
    fail(&mut message_boxes, now);
    assert_eq!(message_boxes.check_deadlines(now), now + secs(1));

    // The next difference to fall due is the account's idle one, at `account_due`: no channel
    // retry is left. It ends empty, which restarts the account's deadline.
    let next_is_account = |message_boxes: &mut MessageBoxes, now, account_due| {
        assert_eq!(time_of_next_difference(message_boxes, now), account_due);
        assert_eq!(
            message_boxes.difference_pending(),
            DiffPending {
                account: true,
                channels: 0
            }
        );
        message_boxes.apply_difference(
            tl::types::updates::DifferenceEmpty { date: 10, seq: 5 }.into(),
            account_due,
        );
        account_due
    };

    // A gap begins the difference before the retry is due: the retry is spent, and doesn't fire.
    let failed = now;
    message_boxes
        .process_updates(channel_updates(CHANNEL, NO_DATE, NO_SEQ, 23), now)
        .unwrap();
    now = time_of_next_difference(&mut message_boxes, now);
    assert_eq!(now, failed + POSSIBLE_GAP_TIMEOUT);
    message_boxes.apply_channel_difference(CHANNEL, channel_difference_empty(23), now);
    now = next_is_account(&mut message_boxes, now, T0 + NO_UPDATES_TIMEOUT);

    // The caller asks for the difference before the retry is due: the same.
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    fail(&mut message_boxes, now);
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    message_boxes.apply_channel_difference(CHANNEL, channel_difference_empty(23), now);
    now = next_is_account(&mut message_boxes, now, now + NO_UPDATES_TIMEOUT);

    // A spent retry doesn't reset the backoff: if the difference that spent it fails too, that
    // is the next failure in a row.
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    fail(&mut message_boxes, now);
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    fail(&mut message_boxes, now);
    assert_eq!(
        time_of_next_difference(&mut message_boxes, now),
        now + secs(2)
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

    /// `updateChannelTooLong`, with the channel's current `pts`.
    fn too_long(&self, channel_id: i64) -> UpdatesLike {
        UpdatesLike::Updates(
            tl::types::UpdateShort {
                update: self.channel_too_long(channel_id),
                date: self.state.date,
            }
            .into(),
        )
    }

    fn channel_too_long(&self, channel_id: i64) -> tl::enums::Update {
        tl::types::UpdateChannelTooLong {
            channel_id,
            pts: Some(self.pts(Key::Channel(channel_id))),
        }
        .into()
    }

    /// Answer `updates.getDifference` from the state it asks from.
    fn difference(
        &self,
        request: &tl::functions::updates::GetDifference,
        slice: bool,
        channels: Channels,
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
        // The server may add something about the channels from the same stretch of time.
        let channel_events = self.events.iter().filter(|event| {
            matches!(event.key, Key::Channel(_)) && (first.id..=last.id).contains(&event.id)
        });
        match channels {
            Channels::Nothing => {}
            Channels::Updates => other_updates.extend(channel_events.map(Event::update)),
            Channels::TooLong => {
                let mut too_long = channel_events
                    .filter_map(|event| match event.key {
                        Key::Channel(channel_id) => Some(channel_id),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                too_long.sort_unstable();
                too_long.dedup();
                other_updates.extend(
                    too_long
                        .into_iter()
                        .map(|channel_id| self.channel_too_long(channel_id)),
                );
            }
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
        let key = Key::Channel(channel_of(request));
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

    fn serve_difference(&mut self, server: &Server, slice: bool, channels: Channels) -> bool {
        let Some(request) = self.message_boxes.get_difference() else {
            return false;
        };
        let difference = server.difference(&request, slice, channels);
        let (returned, _, _) = self.message_boxes.apply_difference(difference, self.now);
        self.handed_out(server, &returned);
        true
    }

    /// Fetch a channel difference, if one is due, at most `limit` events, or give up on it for
    /// now (`fail`). With `open`, the user opens that channel while the request is in flight.
    fn serve_channel_difference(
        &mut self,
        server: &Server,
        limit: usize,
        fail: bool,
        open: Option<i64>,
    ) -> bool {
        let Some(request) = self.message_boxes.get_channel_difference() else {
            return false;
        };
        if let Some(channel) = open {
            assert!(self.message_boxes.begin_channel_difference(channel));
        }
        let channel_id = channel_of(&request);
        if fail {
            let returned = self.message_boxes.end_channel_difference(
                channel_id,
                PrematureEndReason::TemporaryServerIssues,
                self.now,
            );
            self.handed_out(server, &returned);
        } else {
            let difference = server.channel_difference(&request, limit);
            let (returned, _, _) = self
                .message_boxes
                .apply_channel_difference(channel_id, difference, self.now);
            self.handed_out(server, &returned);
        }
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
    ServeDifference { slice: bool, channels: Channels },
    /// The client fetches a channel difference, if one is due, or gives up on it for now; the
    /// user may open a channel meanwhile.
    ServeChannelDifference {
        limit: usize,
        fail: bool,
        open: Option<i64>,
    },
    /// The server says a channel has too many updates to send.
    TooLong { channel: i64 },
    /// The user opens a channel.
    Open { channel: i64 },
}

/// What the server adds to an account difference about the channels.
#[derive(Clone, Copy, Debug)]
enum Channels {
    Nothing,
    /// Channel updates from the same stretch of time.
    Updates,
    /// `updateChannelTooLong` for the channels with updates in that stretch of time.
    TooLong,
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
        2 => (
            any::<bool>(),
            prop_oneof![
                Just(Channels::Nothing),
                Just(Channels::Updates),
                Just(Channels::TooLong)
            ]
        )
            .prop_map(|(slice, channels)| Op::ServeDifference { slice, channels }),
        2 => (
            1..=3usize,
            prop::bool::weighted(0.1),
            prop::option::weighted(0.2, prop_oneof![Just(CHANNEL_A), Just(CHANNEL_B)])
        )
            .prop_map(|(limit, fail, open)| Op::ServeChannelDifference { limit, fail, open }),
        1 => prop_oneof![Just(CHANNEL_A), Just(CHANNEL_B)].prop_map(|channel| Op::TooLong { channel }),
        1 => prop_oneof![Just(CHANNEL_A), Just(CHANNEL_B)].prop_map(|channel| Op::Open { channel }),
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
            Op::ServeDifference { slice, channels } => {
                client.serve_difference(&server, slice, channels);
            }
            Op::ServeChannelDifference { limit, fail, open } => {
                client.serve_channel_difference(&server, limit, fail, open);
            }
            Op::TooLong { channel } => {
                let too_long = server.too_long(channel);
                let (returned, _, _) = client
                    .message_boxes
                    .process_updates(too_long, client.now)
                    .expect("a lone updateChannelTooLong is no gap");
                client.handed_out(&server, &returned);
            }
            Op::Open { channel } => {
                assert!(client.message_boxes.begin_channel_difference(channel));
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
        // A quiet channel is never diffed by itself (TF7): the user opens it.
        for channel in [CHANNEL_A, CHANNEL_B] {
            assert!(client.message_boxes.begin_channel_difference(channel));
        }
        while client.serve_difference(&server, false, Channels::Nothing)
            || client.serve_channel_difference(&server, usize::MAX, false, None)
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

// Server data that breaks the protocol is handled, never a panic (TF3).

/// `updates.channelDifferenceTooLong` for [`CHANNEL`] with the given `dialog`.
fn channel_difference_too_long(dialog: tl::enums::Dialog) -> tl::enums::updates::ChannelDifference {
    tl::types::updates::ChannelDifferenceTooLong {
        r#final: true,
        timeout: None,
        dialog,
        messages: Vec::new(),
        chats: Vec::new(),
        users: Vec::new(),
    }
    .into()
}

fn notify_settings() -> tl::enums::PeerNotifySettings {
    tl::types::PeerNotifySettings {
        show_previews: None,
        silent: None,
        mute_until: None,
        ios_sound: None,
        android_sound: None,
        other_sound: None,
        stories_muted: None,
        stories_hide_sender: None,
        stories_ios_sound: None,
        stories_android_sound: None,
        stories_other_sound: None,
    }
    .into()
}

/// [`CHANNEL`]'s dialog, with the given `pts`.
fn channel_dialog(pts: Option<i32>) -> tl::enums::Dialog {
    tl::types::Dialog {
        pinned: false,
        unread_mark: false,
        view_forum_as_messages: false,
        peer: tl::types::PeerChannel {
            channel_id: CHANNEL,
        }
        .into(),
        top_message: 1,
        read_inbox_max_id: 0,
        read_outbox_max_id: 0,
        unread_count: 0,
        unread_mentions_count: 0,
        unread_reactions_count: 0,
        unread_poll_votes_count: 0,
        notify_settings: notify_settings(),
        pts,
        draft: None,
        folder_id: None,
        ttl_period: None,
    }
    .into()
}

fn folder_dialog() -> tl::enums::Dialog {
    tl::types::DialogFolder {
        pinned: false,
        folder: tl::types::Folder {
            autofill_new_broadcasts: false,
            autofill_public_groups: false,
            autofill_new_correspondents: false,
            id: 1,
            title: "Archive".into(),
            photo: None,
        }
        .into(),
        peer: tl::types::PeerChannel {
            channel_id: CHANNEL,
        }
        .into(),
        top_message: 1,
        unread_muted_peers_count: 0,
        unread_unmuted_peers_count: 0,
        unread_muted_messages_count: 0,
        unread_unmuted_messages_count: 0,
    }
    .into()
}

fn community_dialog() -> tl::enums::Dialog {
    tl::types::DialogCommunity {
        pinned: false,
        community_id: 7,
        notify_settings: notify_settings(),
    }
    .into()
}

/// `update` alone on the socket, in an `updateShort`.
fn short(update: tl::enums::Update) -> UpdatesLike {
    UpdatesLike::Updates(
        tl::types::UpdateShort {
            update,
            date: NO_DATE,
        }
        .into(),
    )
}

const NOTHING: Result<UpdateAndPeers, Gap> = Ok((Vec::new(), Vec::new(), Vec::new()));

#[test]
fn folder_dialog_in_channel_too_long_is_premature_end() {
    // The channel at 20 gets its difference, and 21 and 24 arrive meanwhile.
    let diffing = || {
        let mut message_boxes = settled(T0);
        assert!(message_boxes.begin_channel_difference(CHANNEL));
        for pts in [24, 21] {
            assert_eq!(
                message_boxes.process_updates(channel_updates(CHANNEL, NO_DATE, NO_SEQ, pts), T0),
                NOTHING
            );
        }
        message_boxes
    };

    // A dialog with no `pts` to move the channel to ends the difference without moving it, as
    // TDLib does: what follows on comes back, the rest is dropped, and no retry is armed.
    for dialog in [folder_dialog(), community_dialog(), channel_dialog(None)] {
        let mut message_boxes = diffing();
        let (returned, users, chats) = message_boxes.apply_channel_difference(
            CHANNEL,
            channel_difference_too_long(dialog),
            T0,
        );
        assert_eq!(pts_of(&returned), vec![21]);
        assert!(users.is_empty() && chats.is_empty());
        assert_eq!(message_boxes.take_delta(), channel_delta(21));
        assert_eq!(message_boxes.difference_pending(), DiffPending::default());
        assert_eq!(message_boxes.get_channel_difference(), None);
        // The account's idle deadline is the next one: the channel waits for its next gap.
        assert_eq!(
            time_of_next_difference(&mut message_boxes, T0),
            T0 + NO_UPDATES_TIMEOUT
        );
        assert!(message_boxes.difference_pending().account);
        assert_eq!(message_boxes.difference_pending().channels, 0);
    }

    // Its next gap gets the difference again, from where it stopped.
    let mut message_boxes = diffing();
    let _ = message_boxes.apply_channel_difference(
        CHANNEL,
        channel_difference_too_long(folder_dialog()),
        T0,
    );
    let _ = message_boxes.take_delta();
    assert_eq!(
        message_boxes.process_updates(channel_updates(CHANNEL, NO_DATE, NO_SEQ, 30), T0),
        NOTHING
    );
    assert_eq!(
        time_of_next_difference(&mut message_boxes, T0),
        T0 + POSSIBLE_GAP_TIMEOUT
    );
    assert_eq!(
        message_boxes.get_channel_difference(),
        Some(get_channel_difference(CHANNEL, 21))
    );

    // A dialog with its `pts` still moves the channel there.
    let mut message_boxes = diffing();
    let (returned, _, _) = message_boxes.apply_channel_difference(
        CHANNEL,
        channel_difference_too_long(channel_dialog(Some(23))),
        T0,
    );
    assert_eq!(pts_of(&returned), vec![24]);
    assert_eq!(message_boxes.take_delta(), channel_delta(24));
}

#[test]
fn wrong_pts_updates_are_dropped() {
    let mut message_boxes = settled(T0);
    let in_channel = tl::enums::Message::Empty(tl::types::MessageEmpty {
        id: 1,
        peer_id: Some(
            tl::types::PeerChannel {
                channel_id: CHANNEL,
            }
            .into(),
        ),
    });
    let wrong: [tl::enums::Update; 7] = [
        // Common-box updates about a channel, which has its own sequence.
        tl::types::UpdateNewMessage {
            message: in_channel.clone(),
            pts: 101,
            pts_count: 1,
        }
        .into(),
        tl::types::UpdateEditMessage {
            message: in_channel,
            pts: 101,
            pts_count: 1,
        }
        .into(),
        tl::types::UpdatePinnedMessages {
            pinned: true,
            peer: tl::types::PeerChannel {
                channel_id: CHANNEL,
            }
            .into(),
            messages: vec![1],
            pts: 101,
            pts_count: 1,
        }
        .into(),
        tl::types::UpdateReadHistoryOutbox {
            peer: tl::types::PeerChannel {
                channel_id: CHANNEL,
            }
            .into(),
            max_id: 1,
            pts: 101,
            pts_count: 1,
        }
        .into(),
        // A negative count, and a `pts` that isn't above its count.
        tl::types::UpdateDeleteMessages {
            messages: Vec::new(),
            pts: 101,
            pts_count: -1,
        }
        .into(),
        channel_update(CHANNEL, 1),
        qts_update(1),
    ];
    for update in wrong {
        assert_eq!(message_boxes.process_updates(short(update), T0), NOTHING);
        assert_eq!(message_boxes.take_delta(), StateDelta::default());
    }
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());

    // Each box's next update still follows on.
    let (returned, _, _) = message_boxes
        .process_updates(short(update(101)), T0)
        .unwrap();
    assert_eq!(pts_of(&returned), vec![101]);
    let (returned, _, _) = message_boxes
        .process_updates(channel_updates(CHANNEL, NO_DATE, NO_SEQ, 21), T0)
        .unwrap();
    assert_eq!(pts_of(&returned), vec![21]);
}

#[test]
fn state_near_i32_max_does_not_overflow() {
    let mut message_boxes = MessageBoxes::new();
    message_boxes.set_state(state(10, i32::MAX, i32::MAX - 1, i32::MAX), T0);
    message_boxes.try_set_channel_state(CHANNEL, i32::MAX);
    let _ = message_boxes.take_delta();

    // A `pts` and `count` whose sum would overflow: an update the box has already.
    let far = tl::types::UpdateDeleteMessages {
        messages: Vec::new(),
        pts: i32::MAX,
        pts_count: 5,
    };
    assert_eq!(
        message_boxes.process_updates(short(far.into()), T0),
        NOTHING
    );
    assert_eq!(
        message_boxes.process_updates(channel_updates(CHANNEL, NO_DATE, NO_SEQ, i32::MAX), T0),
        NOTHING
    );
    assert_eq!(
        message_boxes.process_updates(qts_updates(i32::MAX), T0),
        NOTHING
    );
    // A sequenced container after `seq` `i32::MAX`: handled already.
    assert_eq!(
        message_boxes.process_updates(updates(11, i32::MIN, i32::MAX), T0),
        NOTHING
    );
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
}

#[test]
fn answers_for_a_box_not_getting_its_difference_are_dropped() {
    let mut message_boxes = settled(T0);

    // Nothing asked for the account's difference or the channel's: the answers change nothing.
    let (returned, users, chats) = message_boxes.apply_difference(
        tl::types::updates::DifferenceTooLong { pts: 500 }.into(),
        T0,
    );
    assert!(returned.is_empty() && users.is_empty() && chats.is_empty());
    let (returned, users, chats) =
        message_boxes.apply_channel_difference(CHANNEL, channel_difference(21, 30, true), T0);
    assert!(returned.is_empty() && users.is_empty() && chats.is_empty());
    for reason in [
        PrematureEndReason::TemporaryServerIssues,
        PrematureEndReason::Banned,
    ] {
        assert!(
            message_boxes
                .end_channel_difference(CHANNEL, reason, T0)
                .is_empty()
        );
    }
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    assert_eq!(
        message_boxes.session_state().channels,
        vec![ChannelState {
            id: CHANNEL,
            pts: 20
        }]
    );

    // A second answer to a channel request repeated while the first was in flight.
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    let (returned, _, _) =
        message_boxes.apply_channel_difference(CHANNEL, channel_difference(21, 30, true), T0);
    assert_eq!(pts_of(&returned), (21..=30).collect::<Vec<_>>());
    let (returned, _, _) =
        message_boxes.apply_channel_difference(CHANNEL, channel_difference(21, 25, true), T0);
    assert!(returned.is_empty());
    assert_eq!(message_boxes.take_delta(), channel_delta(30));
}

#[test]
fn lagged_updates_are_a_gap() {
    // Updates dropped on a full channel: the account's difference begins at once, the state
    // stays where it was.
    let mut message_boxes = settled(T0);
    assert_eq!(
        message_boxes.process_updates(UpdatesLike::Lagged, T0),
        Err(Gap)
    );
    assert_eq!(
        message_boxes.difference_pending(),
        DiffPending {
            account: true,
            channels: 0
        }
    );
    assert_eq!(message_boxes.check_deadlines(T0), T0);
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
    assert_eq!(message_boxes.session_state(), loaded_state());
}
