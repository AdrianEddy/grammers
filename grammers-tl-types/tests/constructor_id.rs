// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A boxed enum's `constructor_id` is its value's constructor, the four bytes its serialization
//! starts with, for a variant without parameters, an ordinary one and a recursive boxed one, and a
//! constant can name it.
#![cfg(feature = "tl-api")]

use grammers_tl_types::{Identifiable, Serializable, enums, types};

/// The constructor identifier `value` serializes first.
fn serialized_id(value: &impl Serializable) -> u32 {
    u32::from_le_bytes(value.to_bytes()[..4].try_into().unwrap())
}

#[test]
fn constructor_id_is_the_constructor_serialized() {
    // The accessor is a `const fn`.
    const EMPTY: u32 = enums::MessageAction::Empty.constructor_id();
    let empty = enums::MessageAction::Empty;
    assert_eq!(empty.constructor_id(), 0xb6aef7b0);
    assert_eq!(EMPTY, 0xb6aef7b0);
    assert_eq!(
        empty.constructor_id(),
        types::MessageActionEmpty::CONSTRUCTOR_ID
    );
    assert_eq!(empty.constructor_id(), serialized_id(&empty));

    let created = enums::MessageAction::ChatCreate(types::MessageActionChatCreate {
        title: "a group".into(),
        users: vec![1, 2],
    });
    assert_eq!(created.constructor_id(), 0xbd47cbad);
    assert_eq!(created.constructor_id(), serialized_id(&created));

    let status = enums::Update::UserStatus(types::UpdateUserStatus {
        user_id: 7,
        status: enums::UserStatus::Empty,
    });
    assert_eq!(status.constructor_id(), 0xe5bdf8de);
    assert_eq!(
        status.constructor_id(),
        types::UpdateUserStatus::CONSTRUCTOR_ID
    );
    assert_eq!(status.constructor_id(), serialized_id(&status));

    let bold = enums::RichText::TextBold(Box::new(types::TextBold {
        text: enums::RichText::TextEmpty,
    }));
    assert_eq!(bold.constructor_id(), 0x6724abc4);
    assert_eq!(bold.constructor_id(), serialized_id(&bold));
}
