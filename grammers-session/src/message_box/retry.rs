// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The backoff of a box whose difference ended unanswered ([`RETRY_MIN`]), and its jitter.
//!
//! The message boxes read no clock and no randomness of their own: the caller seeds the jitter
//! ([`MessageBoxes::seed_retry_jitter`]), from the operating system's randomness or from a
//! simulation's seed, so the waits are reproducible from it. Unseeded, every wait is its step.

use std::time::Duration;

use super::defs::{MessageBoxes, RETRY_CAP_SPREAD, RETRY_MAX, RETRY_MIN};

/// How a box's retry spreads its waits once the jitter is seeded, as TDLib spreads them.
#[derive(Clone, Copy, Debug)]
pub(super) enum Spread {
    /// The account's: each step exact below the cap (`schedule_get_difference`).
    Account,
    /// A channel's: each step ±20 % below the cap (`retry_get_channel_difference_later`).
    Channel,
}

/// SplitMix64's next output from `state`.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A duration drawn from `0..=upto`, to the nanosecond, by Lemire's multiply-shift (no division;
/// the bias is at most `upto`'s nanoseconds over 2⁶⁴).
fn uniform(state: &mut u64, upto: Duration) -> Duration {
    let bound = u64::try_from(upto.as_nanos()).unwrap_or(u64::MAX);
    let draw = (u128::from(splitmix64(state)) * (u128::from(bound) + 1)) >> 64;
    // Below `bound + 1`, so it fits.
    Duration::from_nanos(u64::try_from(draw).unwrap_or(bound))
}

impl MessageBoxes {
    /// Seed the jitter of the retries after a box's difference ended unanswered
    /// ([`MessageBoxes::end_account_difference`],
    /// [`super::PrematureEndReason::TemporaryServerIssues`]), so that clients the server failed at
    /// once don't retry in step: at the backoff's cap, a wait of 64 to 80 s instead of 64 s (TDLib
    /// waits 60 to 80 s at its cap), and below it, a channel's wait within 20 % of its step either
    /// way, as TDLib's are; the account's waits below the cap stay exact, as TDLib's do. The waits
    /// follow from the seed alone: seed from the operating system's randomness, or from a
    /// simulation's seed to reproduce a run. Unseeded, every wait is its step (tdesktop's), and the
    /// boxes stay deterministic. Seeding again starts the draws over.
    pub fn seed_retry_jitter(&mut self, seed: u64) {
        self.retry_rng = Some(seed);
    }

    /// The step after `step`, a box's last one (zero before its first failure in a row), and the
    /// wait it gives, as `spread` jitters it once seeded.
    pub(super) fn next_retry(&mut self, step: Duration, spread: Spread) -> (Duration, Duration) {
        let step = (step * 2).clamp(RETRY_MIN, RETRY_MAX);
        let Some(state) = &mut self.retry_rng else {
            return (step, step);
        };
        let wait = if step == RETRY_MAX {
            RETRY_MAX + uniform(state, RETRY_CAP_SPREAD)
        } else {
            match spread {
                Spread::Account => step,
                Spread::Channel => step * 4 / 5 + uniform(state, step * 2 / 5),
            }
        };
        (step, wait)
    }
}
