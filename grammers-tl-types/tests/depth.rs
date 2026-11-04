// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Objects nested past `MAX_DEPTH` are `Error::TooDeep`. Without the limit, deserialization
//! recursed until the stack overflowed, which aborts the process: on a 2 MiB thread, tokio's
//! default for its workers, after about 300 nested `textBold` (4 bytes each) in a debug build and
//! 1,450 in a release build.
#![cfg(feature = "tl-api")]

use std::thread;

use grammers_tl_types::deserialize::{Error, MAX_DEPTH};
use grammers_tl_types::{Deserializable, Identifiable, enums, types};

const MAX: usize = MAX_DEPTH as usize;

/// Far deeper than any stack holds without the limit.
const VERY_DEEP: usize = 100_000;

/// `vector#1cb5c415`, the boxed `Vector`'s constructor.
const VECTOR: u32 = 0x1cb5c415;

fn push(bytes: &mut Vec<u8>, word: u32) {
    bytes.extend_from_slice(&word.to_le_bytes());
}

/// `bolds` nested `textBold` around a `textEmpty`: `bolds + 1` nested `RichText` objects.
fn push_bold_text(bytes: &mut Vec<u8>, bolds: usize) {
    for _ in 0..bolds {
        push(bytes, types::TextBold::CONSTRUCTOR_ID);
    }
    push(bytes, types::TextEmpty::CONSTRUCTOR_ID);
}

fn bold_text(bolds: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    push_bold_text(&mut bytes, bolds);
    bytes
}

/// A pushed update as the sender reads it (`enums::Updates::from_bytes`), five boxed objects
/// around `bolds` nested `textBold`: `updateShort { updateWebPage { webPage { cached_page: page {
/// blocks: [pageBlockParagraph { text }] } } pts: 7 pts_count: 1 } }`.
fn update_with_bold_text(bolds: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    push(&mut bytes, types::UpdateShort::CONSTRUCTOR_ID);
    push(&mut bytes, types::UpdateWebPage::CONSTRUCTOR_ID);
    push(&mut bytes, types::WebPage::CONSTRUCTOR_ID);
    push(&mut bytes, 1 << 10); // flags: cached_page
    bytes.extend_from_slice(&1i64.to_le_bytes()); // id
    push(&mut bytes, 0); // url: ""
    push(&mut bytes, 0); // display_url: ""
    push(&mut bytes, 0); // hash
    push(&mut bytes, types::Page::CONSTRUCTOR_ID);
    push(&mut bytes, 0); // flags
    push(&mut bytes, 0); // url: ""
    push(&mut bytes, VECTOR); // blocks
    push(&mut bytes, 1);
    push(&mut bytes, types::PageBlockParagraph::CONSTRUCTOR_ID);
    push_bold_text(&mut bytes, bolds);
    push(&mut bytes, VECTOR); // photos
    push(&mut bytes, 0);
    push(&mut bytes, VECTOR); // documents
    push(&mut bytes, 0);
    push(&mut bytes, 7); // pts
    push(&mut bytes, 1); // pts_count
    push(&mut bytes, 0); // date
    bytes
}

/// `arrays` nested one-item `jsonArray` around a `jsonNull`: `arrays + 1` nested `JSONValue`
/// objects, a cycle through `Vector<JSONValue>` that needs no `Box`.
fn json_array(arrays: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for _ in 0..arrays {
        push(&mut bytes, types::JsonArray::CONSTRUCTOR_ID);
        push(&mut bytes, VECTOR);
        push(&mut bytes, 1);
    }
    push(&mut bytes, types::JsonNull::CONSTRUCTOR_ID);
    bytes
}

/// The `textBold` around the innermost text.
fn bolds(mut text: &enums::RichText) -> usize {
    let mut bolds = 0;
    while let enums::RichText::TextBold(bold) = text {
        bolds += 1;
        text = &bold.text;
    }
    bolds
}

/// The `textBold` in an update built by `update_with_bold_text`, and its `pts`.
fn update_bolds_and_pts(update: &enums::Updates) -> Option<(usize, i32)> {
    let enums::Updates::UpdateShort(short) = update else {
        return None;
    };
    let enums::Update::WebPage(update) = &short.update else {
        return None;
    };
    let enums::WebPage::Page(web_page) = &update.webpage else {
        return None;
    };
    let Some(enums::Page::Page(page)) = &web_page.cached_page else {
        return None;
    };
    let [enums::PageBlock::Paragraph(paragraph)] = page.blocks.as_slice() else {
        return None;
    };
    Some((bolds(&paragraph.text), update.pts))
}

/// Runs `f` on a thread with tokio's default worker stack, 2 MiB.
fn on_worker_stack<T: Send>(f: impl FnOnce() -> T + Send) -> T {
    thread::scope(|scope| {
        thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn_scoped(scope, f)
            .expect("spawn a thread")
            .join()
            .expect("no panic")
    })
}

#[test]
fn rich_text_past_depth_limit_is_error() {
    // One `RichText` past the limit.
    assert_eq!(
        enums::RichText::from_bytes(&bold_text(MAX)).err(),
        Some(Error::TooDeep)
    );

    // Far past it, in a pushed update, on a worker's stack: an error, not an abort.
    let update = update_with_bold_text(VERY_DEEP);
    assert_eq!(
        on_worker_stack(|| enums::Updates::from_bytes(&update).err()),
        Some(Error::TooDeep)
    );
}

#[test]
fn rich_text_within_depth_limit_deserializes() {
    // `MAX_DEPTH` nested `RichText`: the limit itself is accepted.
    let text = enums::RichText::from_bytes(&bold_text(MAX - 1)).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(bolds(&text), MAX - 1);

    // An object's level is released when it ends: each text of a `textConcat` reaches the limit.
    let mut concat = Vec::new();
    push(&mut concat, types::TextConcat::CONSTRUCTOR_ID);
    push(&mut concat, VECTOR);
    push(&mut concat, 2);
    push_bold_text(&mut concat, MAX - 2);
    push_bold_text(&mut concat, MAX - 2);
    let text = enums::RichText::from_bytes(&concat).unwrap_or_else(|e| panic!("{e}"));
    let enums::RichText::TextConcat(concat) = text else {
        panic!("not a textConcat");
    };
    let texts: Vec<usize> = concat.texts.iter().map(bolds).collect();
    assert_eq!(texts, [MAX - 2, MAX - 2]);

    // A pushed update with formatted text, on a worker's stack, read to its end.
    let update = update_with_bold_text(10);
    let read = on_worker_stack(|| {
        enums::Updates::from_bytes(&update).map(|update| update_bolds_and_pts(&update))
    });
    assert_eq!(read, Ok(Some((10, 7))));
}

#[test]
fn json_array_past_depth_limit_is_error() {
    // The limit itself is accepted; one `JSONValue` past it is not.
    assert!(enums::Jsonvalue::from_bytes(&json_array(MAX - 1)).is_ok());
    assert_eq!(
        enums::Jsonvalue::from_bytes(&json_array(MAX)).err(),
        Some(Error::TooDeep)
    );

    // Far past it, on a worker's stack: an error, not an abort.
    let deep = json_array(VERY_DEEP);
    assert_eq!(
        on_worker_stack(|| enums::Jsonvalue::from_bytes(&deep).err()),
        Some(Error::TooDeep)
    );
}
