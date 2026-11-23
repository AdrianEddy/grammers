// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! An RPC error's value is a word of its name made of digits alone, which leaves the name; a digit
//! within a word stays in the name, and is no value.

use grammers_mtsender::RpcError;
use grammers_tl_types as tl;

/// The name and the value `message` reads as.
fn parse(message: &str) -> (String, Option<u32>) {
    let error = RpcError::from(tl::types::RpcError {
        error_code: 420,
        error_message: message.into(),
    });
    assert_eq!(error.code, 420);
    assert_eq!(error.caused_by, None);
    (error.name, error.value)
}

#[test]
fn the_value_is_a_word_of_digits() {
    // Last, as most errors carry it.
    assert_eq!(parse("FLOOD_WAIT_31"), ("FLOOD_WAIT".into(), Some(31)));
    // Within the name.
    assert_eq!(
        parse("INTERDC_2_CALL_ERROR"),
        ("INTERDC_CALL_ERROR".into(), Some(2))
    );
    // First.
    assert_eq!(parse("5_SECONDS_LEFT"), ("SECONDS_LEFT".into(), Some(5)));
    // Alone.
    assert_eq!(parse("42"), (String::new(), Some(42)));
}

#[test]
fn digits_within_a_word_are_the_names() {
    // The wait is the last word; `2FA` is part of the name.
    assert_eq!(
        parse("2FA_CONFIRM_WAIT_604800"),
        ("2FA_CONFIRM_WAIT".into(), Some(604_800))
    );
    // No word of digits: no value, and the name whole.
    assert_eq!(parse("P0NY_FLOODWAIT"), ("P0NY_FLOODWAIT".into(), None));
    assert_eq!(parse("FLOOD_WAIT_5MIN"), ("FLOOD_WAIT_5MIN".into(), None));
    assert_eq!(parse("CHAT_INVALID"), ("CHAT_INVALID".into(), None));
}

#[test]
fn only_the_first_word_of_digits_is_the_value() {
    assert_eq!(parse("A_1_B_2"), ("A_B_2".into(), Some(1)));
}

#[test]
fn a_word_of_digits_that_is_no_u32_stays_in_the_name() {
    // Past `u32::MAX`, then a sign, which `u32`'s parse would take.
    assert_eq!(
        parse("UNTIL_99999999999_WAIT_3"),
        ("UNTIL_99999999999_WAIT".into(), Some(3))
    );
    assert_eq!(parse("WAIT_+3"), ("WAIT_+3".into(), None));
}

#[test]
fn empty_words_are_no_value() {
    assert_eq!(parse("FLOOD__WAIT_"), ("FLOOD__WAIT_".into(), None));
    assert_eq!(parse(""), (String::new(), None));
}
