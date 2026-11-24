// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The account's update state is its common box's `pts` ([`MessageBoxes::has_account_state`]):
//! a state loaded with channels or a `qts` but no `pts` holds none, and
//! [`MessageBoxes::set_state`] then sets it, keeping the channels' boxes.

use super::*;

#[test]
fn channels_without_a_pts_hold_no_account_state() {
    let loaded = UpdatesState {
        pts: NO_PTS,
        qts: 7,
        date: 10,
        seq: 5,
        channels: vec![ChannelState {
            id: CHANNEL,
            pts: 20,
        }],
    };
    let mut message_boxes = MessageBoxes::load(loaded, T0);
    assert!(!message_boxes.has_account_state());
    assert!(!message_boxes.is_empty());
    // No difference of the account's to ask for.
    assert_eq!(message_boxes.get_difference(), None);
    assert_eq!(message_boxes.take_delta(), StateDelta::default());

    message_boxes.set_state(state(11, 6, 100, 50), T0);
    assert!(message_boxes.has_account_state());
    assert_eq!(message_boxes.channel_pts(CHANNEL), Some(20));
    assert_eq!(
        message_boxes.take_delta(),
        StateDelta {
            qts: Some(50),
            ..common_delta(100, 11, 6)
        }
    );
    assert_eq!(
        message_boxes.session_state(),
        UpdatesState {
            pts: 100,
            qts: 50,
            date: 11,
            seq: 6,
            channels: vec![ChannelState {
                id: CHANNEL,
                pts: 20,
            }],
        }
    );
}

#[test]
fn boxes_without_any_state_get_the_accounts_from_set_state() {
    let mut message_boxes = MessageBoxes::new();
    assert!(!message_boxes.has_account_state() && message_boxes.is_empty());
    message_boxes.set_state(state(10, 5, 100, 50), T0);
    assert!(message_boxes.has_account_state() && !message_boxes.is_empty());
}
