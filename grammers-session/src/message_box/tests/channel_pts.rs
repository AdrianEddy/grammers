// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A channel's `pts` as the message boxes hold it ([`MessageBoxes::channel_pts`]): a loaded
//! channel's, one that moved, one learnt from an update, none for an unknown channel or a banned
//! one.

use super::*;

/// A channel the boxes don't know.
const UNKNOWN: i64 = 2000;

#[test]
fn channel_pts_follows_the_boxes_state() {
    let mut message_boxes = settled(T0);
    assert_eq!(message_boxes.channel_pts(CHANNEL), Some(20));
    assert_eq!(message_boxes.channel_pts(UNKNOWN), None);

    // It moves as the channel's updates apply, and a channel an update brings has its own.
    message_boxes
        .process_updates(channel_updates(CHANNEL, 11, NO_SEQ, 21), T0)
        .unwrap();
    assert_eq!(message_boxes.channel_pts(CHANNEL), Some(21));
    message_boxes
        .process_updates(channel_updates(UNKNOWN, 11, NO_SEQ, 7), T0)
        .unwrap();
    assert_eq!(message_boxes.channel_pts(UNKNOWN), Some(7));

    // What `session_state` lists.
    for channel in message_boxes.session_state().channels {
        assert_eq!(message_boxes.channel_pts(channel.id), Some(channel.pts));
    }

    // A banned channel's state is dropped.
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    let request = message_boxes.get_channel_difference().unwrap();
    assert_eq!(channel_of(&request), CHANNEL);
    let ended = message_boxes.end_channel_difference(CHANNEL, PrematureEndReason::Banned, T0);
    assert!(ended.is_empty());
    assert_eq!(message_boxes.channel_pts(CHANNEL), None);
    assert_eq!(message_boxes.channel_pts(UNKNOWN), Some(7));
}

#[test]
fn channel_pts_holds_while_the_channel_gets_its_difference() {
    let mut message_boxes = settled(T0);
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    // An update postponed meanwhile doesn't move it.
    message_boxes
        .process_updates(channel_updates(CHANNEL, 11, NO_SEQ, 21), T0)
        .unwrap();
    assert_eq!(message_boxes.channel_pts(CHANNEL), Some(20));
    // The difference does.
    message_boxes.apply_channel_difference(CHANNEL, channel_difference_empty(25), T0);
    assert_eq!(message_boxes.channel_pts(CHANNEL), Some(25));
}
