// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A connection's open ([`UpdatesLike::ConnectionOpened`]) is no gap, unlike its close: it hands
//! nothing out and moves no state, also while the account gets its difference.

use super::*;

#[test]
fn an_open_is_no_gap_and_moves_nothing() {
    let mut message_boxes = settled(T0);
    let (out, users, chats) = message_boxes
        .process_updates(UpdatesLike::ConnectionOpened, T0)
        .unwrap();
    assert!(out.is_empty() && users.is_empty() && chats.is_empty());
    assert_eq!(message_boxes.take_delta(), StateDelta::default());
    assert_eq!(message_boxes.difference_pending(), DiffPending::default());
    assert_eq!(message_boxes.session_state(), loaded_state());

    // The close is a gap: the account's difference begins.
    assert!(
        message_boxes
            .process_updates(UpdatesLike::ConnectionClosed, T0)
            .is_err()
    );
    assert!(message_boxes.difference_pending().account);
    // An open meanwhile holds nothing for its end, which hands nothing more out.
    message_boxes
        .process_updates(UpdatesLike::ConnectionOpened, T0)
        .unwrap();
    let (out, _, _) = message_boxes.apply_difference(
        tl::types::updates::DifferenceEmpty { date: 10, seq: 5 }.into(),
        T0,
    );
    assert!(out.is_empty());
    assert_eq!(message_boxes.session_state(), loaded_state());
}
