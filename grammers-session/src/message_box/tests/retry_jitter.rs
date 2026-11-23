// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The jitter of the retries after a box's difference ended unanswered
//! ([`MessageBoxes::seed_retry_jitter`]): unseeded, every wait is its backoff step; seeded, the
//! account's waits are 64 to 80 s at the cap and exact below it, and a channel's within 20 % of
//! its step below the cap and 64 to 80 s at it, as TDLib's; the same seed draws the same waits.

use super::*;

/// The steps of nine failures in a row, in seconds.
const STEPS: [u64; 9] = [1, 2, 4, 8, 16, 32, 64, 64, 64];

fn seeded(seed: Option<u64>) -> MessageBoxes {
    let mut message_boxes = settled(T0);
    if let Some(seed) = seed {
        message_boxes.seed_retry_jitter(seed);
    }
    message_boxes
}

/// The account's waits after `n` unanswered ends of its difference in a row.
fn account_waits(seed: Option<u64>, n: usize) -> Vec<Duration> {
    let mut message_boxes = seeded(seed);
    assert!(message_boxes.begin_account_difference());
    let mut now = T0;
    (0..n)
        .map(|_| {
            assert!(message_boxes.end_account_difference(now).is_empty());
            let due = time_of_next_difference(&mut message_boxes, now);
            assert!(message_boxes.get_difference().is_some());
            let wait = due - now;
            now = due;
            wait
        })
        .collect()
}

/// [`CHANNEL`]'s waits after `n` temporary failures of its difference in a row.
fn channel_waits(seed: Option<u64>, n: usize) -> Vec<Duration> {
    let mut message_boxes = seeded(seed);
    assert!(message_boxes.begin_channel_difference(CHANNEL));
    let mut now = T0;
    (0..n)
        .map(|_| {
            let ended = message_boxes.end_channel_difference(
                CHANNEL,
                PrematureEndReason::TemporaryServerIssues,
                now,
            );
            assert!(ended.is_empty());
            let due = time_of_next_difference(&mut message_boxes, now);
            let request = message_boxes.get_channel_difference().unwrap();
            assert_eq!(channel_of(&request), CHANNEL);
            let wait = due - now;
            now = due;
            wait
        })
        .collect()
}

#[test]
fn unseeded_waits_are_the_steps() {
    let steps: Vec<_> = STEPS.into_iter().map(Duration::from_secs).collect();
    assert_eq!(account_waits(None, STEPS.len()), steps);
    assert_eq!(channel_waits(None, STEPS.len()), steps);
}

#[test]
fn seeded_waits_spread_as_tdlibs() {
    for (wait, step) in account_waits(Some(SEED), STEPS.len())
        .into_iter()
        .zip(STEPS)
    {
        assert_account_wait(wait, step);
    }
    for (wait, step) in channel_waits(Some(SEED), STEPS.len())
        .into_iter()
        .zip(STEPS)
    {
        assert_channel_wait(wait, step);
    }
}

#[test]
fn a_seed_draws_the_same_waits() {
    assert_eq!(
        account_waits(Some(SEED), STEPS.len()),
        account_waits(Some(SEED), STEPS.len())
    );
    assert_eq!(
        channel_waits(Some(SEED), STEPS.len()),
        channel_waits(Some(SEED), STEPS.len())
    );
}

#[test]
fn different_seeds_draw_different_waits() {
    let (one, two) = (account_waits(Some(1), 7), account_waits(Some(2), 7));
    assert_eq!(one[..6], two[..6], "exact below the cap");
    assert_ne!(one[6], two[6], "drawn at the cap");
    assert_ne!(channel_waits(Some(1), 1), channel_waits(Some(2), 1));
}

#[test]
fn the_waits_cover_their_ranges() {
    // Over 64 seeds, the account's first wait at the cap and a channel's wait at the 8 s step reach
    // near both ends of their ranges.
    let caps: Vec<_> = (0..64)
        .map(|seed| account_waits(Some(seed), 7)[6])
        .collect();
    assert!(caps.iter().any(|&wait| wait < Duration::from_secs(65)));
    assert!(caps.iter().any(|&wait| wait > Duration::from_secs(79)));
    let eights: Vec<_> = (0..64)
        .map(|seed| channel_waits(Some(seed), 4)[3])
        .collect();
    assert!(eights.iter().any(|&wait| wait < Duration::from_secs(7)));
    assert!(eights.iter().any(|&wait| wait > Duration::from_secs(9)));
}
