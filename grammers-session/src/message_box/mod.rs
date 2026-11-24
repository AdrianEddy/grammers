// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This module deals with correct handling of updates,
//! including gaps,
//! and knowing when the code should "get difference"
//! (the set of updates that the client should know by now
//! minus the set of updates that it actually knows).
//!
//! Each chat has its own [`LiveEntry`] in the [`MessageBoxes`]
//! (this `struct` is the "entry point").
//! At any given time,
//! the message box may be either getting difference for them
//! (entry is in [`MessageBoxes::getting_diff_for`])
//! or not.
//! If not getting difference,
//! a possible gap may be found for the updates
//! (the entry's [`LiveEntry::possible_gap`] is set).
//! Otherwise, the entry is on its happy path.
//!
//! Gaps are cleared when they are either
//! resolved on their own
//! (by waiting for a short time)
//! or because we got the difference for the corresponding entry.
//!
//! The updates no entry orders go in the order of the account's `seq`, as TDLib applies them
//! ([`MessageBoxes::process_updates`]): a container past the local `seq` waits for the ones
//! before it the same short time ([`MessageBoxes::seq_gap`]), and then for the account's
//! difference, as an entry's gap does.
//!
//! While there are entries for which their difference must be fetched,
//! [`MessageBoxes::check_deadlines`] will always return the current time,
//! since "now" is the time to get the difference.
//!
//! A difference the server doesn't answer with one ends with
//! [`MessageBoxes::end_channel_difference`] or [`MessageBoxes::end_account_difference`], and is
//! due again after a backoff. The account's, when the server refuses its state, ends with the
//! server's `pts` instead ([`MessageBoxes::restore_pts`]).
//!
//! The message boxes never read a clock: the caller passes the current time ([`Now`])
//! to every method that arms or checks a deadline.
//!
//! After each call, [`MessageBoxes::take_delta`] returns the state that call advanced
//! ([`StateDelta`]), which covers exactly the updates the call returned. A caller that stores
//! each delta together with the updates it returned never stores state ahead of its updates,
//! except for the server's own skips that [`StateDelta`] lists.
//!
//! Updates and differences come from the server: an update whose `pts` breaks the protocol is
//! dropped (one whose `qts` does also schedules the account's difference), an unusable difference
//! ends the box's difference, and an answer for a box that isn't getting its difference changes
//! nothing. None of them panics.

// Here and in the submodules, unexpected server data is handled, never a panic.
#![cfg_attr(
    not(test),
    deny(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing
    )
)]

mod adaptor;
mod defs;
mod retry;
#[cfg(test)]
mod tests;

use std::cmp::{Ordering, Reverse};
use std::collections::VecDeque;
use std::mem;
use std::time::Duration;

pub use defs::{
    CommonState, DiffPending, Gap, MessageBox, MessageBoxes, Now, State, StateDelta,
    UpdateAndPeers, UpdatesLike,
};
use defs::{DirtySet, HeldSeq, Key};
use defs::{
    LiveEntry, NO_DATE, NO_PTS, NO_SEQ, POSSIBLE_GAP_TIMEOUT, PossibleGap, PtsInfo, WrongPts,
};
use grammers_tl_types as tl;
use retry::Spread;
use tracing::{debug, info, trace, warn};

use crate::types::{ChannelState, UpdatesState};

fn next_updates_deadline(now: Now) -> Now {
    now + defs::NO_UPDATES_TIMEOUT
}

/// Where an update goes when a batch is sorted: by where it applies in its box's sequence
/// ([`PtsInfo::order`]), an update in no box's first.
fn update_sort_key(info: Option<&PtsInfo>) -> (i32, i32) {
    info.map_or((NO_PTS, NO_PTS), PtsInfo::order)
}

/// Whether a container whose `seq_start` is the one given follows on from the local `seq`
/// (`Equal`), was handled already (`Greater`), or comes after a gap (`Less`). The `seq` values are
/// the server's, so they are compared without overflow.
fn next_seq_cmp(local_seq: i32, seq_start: i32) -> Ordering {
    (i64::from(local_seq) + 1).cmp(&i64::from(seq_start))
}

impl MessageBox {
    pub fn pts(&self) -> i32 {
        match self {
            MessageBox::Common { pts } => *pts,
            MessageBox::Secondary { qts } => *qts,
            MessageBox::Channel { pts, .. } => *pts,
        }
    }
}

impl From<PtsInfo> for MessageBox {
    fn from(value: PtsInfo) -> Self {
        match value.key {
            Key::Common => Self::Common { pts: value.pts },
            Key::Secondary => Self::Secondary { qts: value.pts },
            Key::Channel(channel_id) => Self::Channel {
                channel_id,
                pts: value.pts,
            },
        }
    }
}

impl LiveEntry {
    fn new(key: Key, pts: i32) -> Self {
        Self {
            key,
            pts,
            possible_gap: None,
            retry_at: None,
            retry_delay: Duration::ZERO,
            postponed: Vec::new(),
        }
    }

    /// When the entry's difference falls due by itself: its possible gap's wait ends, or its
    /// retry after a temporary failure.
    fn deadline(&self) -> Option<Now> {
        let gap = self.possible_gap.as_ref().map(|gap| gap.deadline);
        gap.into_iter().chain(self.retry_at).min()
    }

    /// The entry starts getting its difference, which covers what it was waiting for: the
    /// updates held by a possible gap, and a retry.
    fn begin_difference(&mut self) {
        debug_assert!(
            self.postponed.is_empty(),
            "every end of a difference takes its postponed updates"
        );
        self.possible_gap = None;
        self.retry_at = None;
    }
}

#[allow(clippy::new_without_default)]
/// Creation, querying, and setting base state.
impl MessageBoxes {
    /// Create a new, empty [`MessageBoxes`].
    pub fn new() -> Self {
        trace!("created new message box with no previous state");
        Self {
            entries: Vec::new(),
            date: NO_DATE,
            seq: NO_SEQ,
            getting_diff_for: Vec::new(),
            account_deadline: Duration::MAX,
            account_retry_at: None,
            account_retry_delay: Duration::ZERO,
            retry_rng: None,
            account_postponed: Vec::new(),
            held_seq: VecDeque::new(),
            seq_gap: None,
            // No deadline is armed: the first `check_deadlines` computes the next one.
            next_deadline: Duration::ZERO,
            dirty: DirtySet::default(),
        }
    }

    /// Create a [`MessageBoxes`] from a previously known update state.
    ///
    /// Only the account's difference is due:
    ///
    /// > On startup, only `updates.getDifference` should be called […].
    /// > `updates.getChannelDifference` does not have to be manually called for all channels on
    /// > startup. Instead, `updates.getChannelDifference` will be automatically triggered (only
    /// > for channels that need catching up) by a set of `updateChannelTooLong` updates that will
    /// > be returned by the `updates.getDifference` call.
    ///
    /// The loaded state is where the first [`MessageBoxes::take_delta`] starts from, so it
    /// isn't part of any delta.
    pub fn load(state: UpdatesState, now: Now) -> Self {
        trace!(?state, "created new message box with state");
        let mut entries = Vec::with_capacity(2 + state.channels.len());

        if state.pts != NO_PTS {
            entries.push(LiveEntry::new(Key::Common, state.pts));
        }
        if state.qts != NO_PTS {
            entries.push(LiveEntry::new(Key::Secondary, state.qts));
        }
        let account_deadline = if entries.is_empty() {
            Duration::MAX
        } else {
            next_updates_deadline(now)
        };
        entries.extend(
            state
                .channels
                .iter()
                .map(|c| LiveEntry::new(Key::Channel(c.id), c.pts)),
        );
        entries.sort_by_key(|entry| entry.key);

        let mut message_boxes = Self {
            entries,
            date: state.date,
            seq: state.seq,
            getting_diff_for: Vec::new(),
            account_deadline,
            account_retry_at: None,
            account_retry_delay: Duration::ZERO,
            retry_rng: None,
            account_postponed: Vec::new(),
            held_seq: VecDeque::new(),
            seq_gap: None,
            next_deadline: Duration::ZERO,
            dirty: DirtySet::default(),
        };
        message_boxes.begin_account_difference();
        message_boxes
    }

    fn entry(&self, key: Key) -> Option<&LiveEntry> {
        self.entries
            .binary_search_by_key(&key, |entry| entry.key)
            .ok()
            .and_then(|i| self.entries.get(i))
    }

    /// The entry with `key`, or where to insert one.
    fn find_entry(&mut self, key: Key) -> Result<&mut LiveEntry, usize> {
        let i = self.entries.binary_search_by_key(&key, |entry| entry.key)?;
        // The search found it there.
        self.entries.get_mut(i).ok_or(i)
    }

    fn update_entry(&mut self, key: Key, updater: impl FnOnce(&mut LiveEntry)) -> bool {
        match self.find_entry(key) {
            Ok(entry) => {
                updater(entry);
                true
            }
            Err(_) => false,
        }
    }

    /// Apply `updater` to the entry with `entry`'s key, inserting `entry` first if there is none.
    fn force_update_entry(&mut self, mut entry: LiveEntry, updater: impl FnOnce(&mut LiveEntry)) {
        let key = entry.key;
        match self.find_entry(key) {
            Ok(existing) => {
                let pts = existing.pts;
                updater(existing);
                if existing.pts != pts {
                    self.dirty.mark(key);
                }
            }
            Err(i) => {
                updater(&mut entry);
                self.dirty.mark(key);
                self.entries.insert(i, entry);
            }
        }
    }

    /// Insert `entry`, replacing any entry with its key.
    fn set_entry(&mut self, entry: LiveEntry) {
        let key = entry.key;
        match self.find_entry(key) {
            Ok(existing) => {
                let changed = existing.pts != entry.pts;
                *existing = entry;
                if changed {
                    self.dirty.mark(key);
                }
            }
            Err(i) => {
                self.dirty.mark(key);
                self.entries.insert(i, entry);
            }
        }
    }

    fn set_pts(&mut self, key: Key, pts: i32) {
        match self.find_entry(key) {
            Ok(entry) => {
                if entry.pts != pts {
                    entry.pts = pts;
                    self.dirty.mark(key);
                }
            }
            Err(i) => {
                self.dirty.mark(key);
                self.entries.insert(i, LiveEntry::new(key, pts));
            }
        }
    }

    /// Keep `next_deadline` no later than a deadline just set.
    fn arm(&mut self, deadline: Now) {
        self.next_deadline = self.next_deadline.min(deadline);
    }

    /// Restart the account's idle deadline, as one of its boxes heard from the server at `now`.
    fn heard_from_account(&mut self, now: Now) {
        self.account_deadline = next_updates_deadline(now);
        self.arm(self.account_deadline);
    }

    /// Set the `date` and the `seq` to a state the server gave as it is: a difference's, or the
    /// account's own ([`MessageBoxes::set_state`]). A container moves them with
    /// [`MessageBoxes::advance_date_seq`].
    fn set_date_seq(&mut self, date: i32, seq: i32) {
        if self.date != date || self.seq != seq {
            self.date = date;
            self.seq = seq;
            self.dirty.common = true;
        }
    }

    /// Move the `date` and the `seq` to a container's applied from the socket: the `date` only
    /// forward, as TDLib's `set_date` and tdesktop's `Updates::setState` move it, so a container
    /// dated before one already applied (overtaken, or held across a difference) doesn't take back
    /// the cursor `updates.getDifference` asks from.
    fn advance_date_seq(&mut self, date: i32, seq: i32) {
        self.set_date_seq(date.max(self.date), seq);
    }

    fn pop_entry(&mut self, key: Key) -> Option<LiveEntry> {
        match self.entries.binary_search_by_key(&key, |entry| entry.key) {
            Ok(i) => {
                self.dirty.mark(key);
                Some(self.entries.remove(i))
            }
            Err(_) => None,
        }
    }

    fn entry_pts(&self, key: Key) -> i32 {
        self.entry(key).map_or(NO_PTS, |entry| entry.pts)
    }

    /// Return the current state in a format that sessions understand.
    ///
    /// This should be used for persisting the state.
    pub fn session_state(&self) -> UpdatesState {
        UpdatesState {
            pts: self.entry_pts(Key::Common),
            qts: self.entry_pts(Key::Secondary),
            date: self.date,
            seq: self.seq,
            channels: self
                .entries
                .iter()
                .filter_map(|entry| match entry.key {
                    Key::Channel(id) => Some(ChannelState { id, pts: entry.pts }),
                    _ => None,
                })
                .collect(),
        }
    }

    /// The `pts` of channel `channel_id`'s box, if the message boxes hold its state: the one
    /// [`MessageBoxes::session_state`] would list for it, which the deltas
    /// ([`MessageBoxes::take_delta`]) report as it moves. `None` for a channel they never had, and
    /// for one whose state they dropped ([`PrematureEndReason::Banned`]).
    pub fn channel_pts(&self, channel_id: i64) -> Option<i32> {
        self.entry(Key::Channel(channel_id)).map(|entry| entry.pts)
    }

    /// Return true if the message boxes hold no box at all: neither the account's state
    /// ([`MessageBoxes::has_account_state`]) nor the secondary box's or any channel's.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether the message boxes hold the account's update state, its common box's `pts`, which
    /// the account's difference is asked from: set by [`MessageBoxes::set_state`], or loaded with
    /// it ([`MessageBoxes::load`]). A state loaded without a `pts`, the secondary box's `qts` or
    /// channels' boxes alone, holds none, and [`MessageBoxes::set_state`] then sets it, keeping
    /// the channels' boxes.
    pub fn has_account_state(&self) -> bool {
        self.entry(Key::Common).is_some()
    }

    /// Return what the message boxes advanced since the previous call, or since they were
    /// created or loaded, and start over.
    ///
    /// Taken right after a call, the delta covers exactly the updates that call returned (see
    /// [`StateDelta`]). Taken after several calls, it covers the updates all of them returned.
    pub fn take_delta(&mut self) -> StateDelta {
        let common = mem::take(&mut self.dirty.common).then(|| CommonState {
            pts: self.entry_pts(Key::Common),
            date: self.date,
            seq: self.seq,
        });
        let qts = mem::take(&mut self.dirty.qts).then(|| self.entry_pts(Key::Secondary));
        let mut channels = Vec::new();
        let mut removed_channels = Vec::new();
        for &id in &self.dirty.channels {
            match self.entry(Key::Channel(id)) {
                Some(entry) => channels.push(ChannelState { id, pts: entry.pts }),
                None => removed_channels.push(id),
            }
        }
        self.dirty.channels.clear();
        StateDelta {
            common,
            qts,
            channels,
            removed_channels,
        }
    }

    /// Return which differences are waiting to be fetched.
    pub fn difference_pending(&self) -> DiffPending {
        let channels = self
            .getting_diff_for
            .iter()
            .filter(|key| matches!(key, Key::Channel(_)))
            .count();
        DiffPending {
            account: self.getting_diff_for.len() > channels,
            channels,
        }
    }

    /// Return the next deadline when receiving updates should timeout: the latest time to call
    /// this method again. It can come before any deadline actually passes, as a deadline that
    /// moved later is only noticed then.
    ///
    /// If a deadline expired, the corresponding entries will be marked as needing to get its difference.
    /// While there are entries pending of getting their difference, this method returns `now`.
    /// Every box's timers are its own, as tdesktop's and TDLib's are, so none waits for another
    /// box's difference: the account's retry after an unanswered end
    /// ([`MessageBoxes::end_account_difference`]), its boxes' possible gaps, its `seq` gap (the
    /// containers held past the local `seq`, [`MessageBoxes::process_updates`]) and its idle
    /// deadline, and each channel's possible gap and its retry after a temporary failure
    /// ([`MessageBoxes::end_channel_difference`]), fall due while other boxes get their
    /// differences: a channel that falls due waits behind the channels already waiting for theirs
    /// ([`MessageBoxes::get_channel_difference`]). While the account's retry is armed, it is the
    /// account's one deadline to begin its difference: a possible gap of the account's, or its
    /// `seq` gap, whose half second is over while the retry waits is dropped instead, for the
    /// retry's difference to return its updates and set the `seq`; one due with the retry is kept
    /// for that difference.
    pub fn check_deadlines(&mut self, now: Now) -> Now {
        if now >= self.next_deadline {
            // To avoid hogging up all CPU by sleeping for no duration,
            // if no deadline is armed, the next one is a full timeout away.
            let mut next_deadline =
                next_updates_deadline(now).min(self.check_account_deadlines(now));
            for entry in &mut self.entries {
                let Key::Channel(_) = entry.key else {
                    continue;
                };
                // A channel getting its difference has none: beginning it cleared both.
                let Some(deadline) = entry.deadline() else {
                    continue;
                };
                if now < deadline {
                    next_deadline = next_deadline.min(deadline);
                    continue;
                }
                debug!(?entry, "deadline for forcibly fetching updates met");
                // The moral equivalent of `try_begin_get_diff`, behind those waiting already.
                entry.begin_difference();
                debug_assert!(
                    !self.getting_diff_for.contains(&entry.key),
                    "a channel getting its difference has no deadline: beginning it cleared both"
                );
                self.getting_diff_for.push(entry.key);
            }
            self.next_deadline = next_deadline;
        }

        if self.getting_diff_for.is_empty() {
            self.next_deadline
        } else {
            now
        }
    }

    /// Check the account's deadlines at `now`, and begin its difference if one is due: its retry
    /// while one is armed ([`MessageBoxes::end_account_difference`]), which is then the account's
    /// one deadline to begin it, else its idle deadline, and its boxes' possible gaps and its `seq`
    /// gap ([`MessageBoxes::seq_gap`]). Return its next deadline, `Duration::MAX` while its
    /// difference is pending.
    ///
    /// While the retry waits, a possible gap of the account's, or its `seq` gap, whose half second
    /// is over is dropped instead, for the retry's difference to return its updates and set the
    /// `seq`. One due with the retry is kept for the difference that begins then.
    fn check_account_deadlines(&mut self, now: Now) -> Now {
        // Both account boxes always get their difference together (`begin_account_difference`).
        if self.getting_diff_for.contains(&Key::Common) {
            return Duration::MAX;
        }
        debug_assert_eq!(
            self.seq_gap.is_some(),
            !self.held_seq.is_empty(),
            "the seq gap is open exactly while containers are held (`settle_seq_gap`)"
        );
        let retrying = self.account_retry_at.is_some();
        let (mut due, mut next) = match self.account_retry_at {
            Some(retry_at) => (now >= retry_at, retry_at),
            None => (now >= self.account_deadline, self.account_deadline),
        };
        if due && !retrying {
            debug!("no updates for the account in a while; getting its difference");
            // Restarted here, not only when the difference ends: if it can't begin (no common
            // state), the deadline mustn't stay in the past.
            self.account_deadline = next_updates_deadline(now);
            next = self.account_deadline;
        }
        // A passed gap of the account's is dropped only while the retry waits: one due with the
        // retry is left to the difference that begins below.
        let retry_waits = retrying && !due;
        for key in [Key::Common, Key::Secondary] {
            let Ok(entry) = self.find_entry(key) else {
                continue;
            };
            // An account box's deadline is its possible gap's alone: the retry is the boxes'.
            let Some(deadline) = entry.possible_gap.as_ref().map(|gap| gap.deadline) else {
                continue;
            };
            if now < deadline {
                next = next.min(deadline);
            } else if retry_waits {
                // A possible gap of the account's doesn't begin its difference while the retry
                // waits. It holds its updates for its half second, in case the socket fills it,
                // and is then dropped, for the retry's difference to return them, as beginning the
                // difference would drop them; held until the retry, it would only grow.
                debug!(
                    ?key,
                    "dropping a possible gap's updates; the account's retry returns them"
                );
                entry.possible_gap = None;
            } else {
                debug!(?entry, "deadline for forcibly fetching updates met");
                due = true;
            }
        }
        // The containers held past the local `seq`, the same way: due with the retry, they are
        // kept for its difference, as TDLib keeps its `pending_seq_updates_` when its seq gap's
        // timeout begins `getDifference` (`fill_seq_gap`, `before_get_difference`).
        if let Some(deadline) = self.seq_gap {
            if now < deadline {
                next = next.min(deadline);
            } else if retry_waits {
                debug!(
                    count = self.held_seq.len(),
                    "dropping the containers held past the seq; the account's retry sets it"
                );
                self.held_seq.clear();
                self.settle_seq_gap(now);
            } else {
                debug!("the account's seq gap wasn't filled in time; getting its difference");
                due = true;
            }
        }
        if due && self.begin_account_difference() {
            return Duration::MAX;
        }
        next
    }

    /// Sets the update state.
    ///
    /// Should be called right after login if [`MessageBoxes::new`] was used, otherwise undesirable
    /// updates will be fetched.
    ///
    /// Should only be called without the account's state ([`MessageBoxes::has_account_state`]).
    /// The channels' boxes the message boxes hold already are kept, each at its own `pts`; the
    /// secondary box takes the state's `qts`.
    pub fn set_state(&mut self, state: tl::types::updates::State, now: Now) {
        trace!(?state, "setting state");
        debug_assert!(!self.has_account_state());
        self.set_entry(LiveEntry::new(Key::Common, state.pts));
        self.set_entry(LiveEntry::new(Key::Secondary, state.qts));
        self.set_date_seq(state.date, state.seq);
        self.heard_from_account(now);
    }

    /// Like [`MessageBoxes::set_state`], but for channels. Useful when getting dialogs.
    ///
    /// The update state will only be updated if no entry was known previously.
    pub fn try_set_channel_state(&mut self, id: i64, pts: i32) {
        trace!(channel_id = id, pts, "trying to set channel state");
        if self.entry(Key::Channel(id)).is_none() {
            self.set_entry(LiveEntry::new(Key::Channel(id), pts));
        }
    }

    /// Begin getting the difference of a channel the caller wants caught up, such as one the
    /// user has open:
    ///
    /// > clients (user accounts only) should also additionally invoke
    /// > `updates.getChannelDifference` periodically for channels and supergroups the user is
    /// > currently viewing […].
    /// > If the returned difference is final, and the user is still viewing the messages of the
    /// > supergroup/channel […], `updates.getChannelDifference` should be re-invoked after
    /// > `timeout` seconds (if the flag is specified, otherwise after 1 second).
    ///
    /// That schedule, and the limit of 10 such channels, are the caller's: this only asks for one
    /// difference, which [`MessageBoxes::get_channel_difference`] then returns.
    ///
    /// The channel goes before the channels already waiting for theirs (`updateChannelTooLong`,
    /// gaps, retries), also if it is one of them: the caller asks for what the user is looking at.
    /// A request already in flight still gets its answer, as
    /// [`MessageBoxes::apply_channel_difference`] and [`MessageBoxes::end_channel_difference`]
    /// name the channel they answer for.
    ///
    /// Returns `true` if the channel's difference is pending, also if it already was. Returns
    /// `false`, and does nothing, if the channel has no known state to get the difference from
    /// (see [`MessageBoxes::try_set_channel_state`]).
    pub fn begin_channel_difference(&mut self, channel_id: i64) -> bool {
        let key = Key::Channel(channel_id);
        if let Some(i) = self.getting_diff_for.iter().position(|&k| k == key) {
            if let Some(waiting) = self.getting_diff_for.get_mut(..=i) {
                waiting.rotate_right(1);
            }
        } else if self.update_entry(key, LiveEntry::begin_difference) {
            self.getting_diff_for.insert(0, key);
        } else {
            info!(
                channel_id,
                "tried to begin getting difference but do not have entry for the channel"
            );
            return false;
        }
        true
    }

    /// Try to begin getting difference for the given entry.
    /// Fails if the entry does not have a previously-known state that can be used to get its difference.
    /// The account's waits for its armed retry, if one is ([`MessageBoxes::account_retry_at`]).
    ///
    /// Clears any previous gaps.
    fn try_begin_get_diff(&mut self, key: Key) {
        if let Key::Common | Key::Secondary = key {
            if self.account_retry_at.is_some() {
                debug!("the account's difference waits for its retry");
            } else {
                self.begin_account_difference();
            }
        } else if !self.getting_diff_for.contains(&key) {
            if self.update_entry(key, LiveEntry::begin_difference) {
                self.getting_diff_for.push(key);
            } else {
                info!(
                    ?key,
                    "tried to begin getting difference but do not have entry for the key"
                );
            }
        }
    }

    /// Begin getting the account's difference now, also while a retry is armed for it after an
    /// unanswered end ([`MessageBoxes::end_account_difference`]), which this cancels: for a reason
    /// of the caller's that the retry doesn't cover, such as a new connection. That is a new
    /// session, whose difference TDLib asks for at once whatever its retry timer, as the updates
    /// the server pushed to the earlier one may be lost. The caller paces such reasons, a
    /// reconnect's backoff for instance. The backoff keeps its step: if this difference ends
    /// unanswered too, the retry waits the next one.
    ///
    /// `updates.getDifference` returns the updates of the common and the secondary box at once,
    /// so both boxes always get difference together. Otherwise the updates of the box not
    /// getting difference would be checked against the state the difference had just moved to
    /// its end, and dropped as already handled.
    ///
    /// Returns `true` if the account's difference is pending, also if it already was. Returns
    /// `false`, and does nothing, if the common box has no state to get it from (see
    /// [`MessageBoxes::set_state`]). Clears any previous gaps of both boxes; the containers held
    /// for the account's `seq` wait for the difference's end ([`MessageBoxes::process_updates`]).
    pub fn begin_account_difference(&mut self) -> bool {
        if self.entry(Key::Common).is_none() {
            info!("tried to begin getting difference but do not have the common state");
            return false;
        }
        self.account_retry_at = None;
        // The `seq` gap closes while the difference runs; its end settles it again
        // (`settle_seq_gap`).
        self.seq_gap = None;
        for key in [Key::Common, Key::Secondary] {
            if !self.getting_diff_for.contains(&key) {
                self.update_entry(key, LiveEntry::begin_difference);
                self.getting_diff_for.push(key);
            }
        }
        true
    }

    /// Finish getting the account's difference, which the server answered (`end`), restart its
    /// idle deadline, and add to `result` the updates postponed for the account meanwhile, and
    /// those held for its `seq`, that follow on from the state it left. A container still past the
    /// `seq` waits for the ones before it, unless the answer was `updates.differenceEmpty` and it
    /// was held before the difference began ([`MessageBoxes::replay_held_seq`]).
    fn finish_account_difference(
        &mut self,
        now: Now,
        end: DifferenceEnd,
        result: &mut Vec<(tl::enums::Update, State)>,
    ) {
        for key in [Key::Common, Key::Secondary] {
            self.try_end_get_diff(key);
        }
        self.heard_from_account(now);
        let postponed = mem::take(&mut self.account_postponed);
        self.replay_postponed(postponed, false, now, result);
        self.replay_held_seq(now, end, result);
    }

    /// Finish getting difference for the given entry.
    /// Returns `false`, and does nothing, if no difference was being fetched for the given key.
    fn try_end_get_diff(&mut self, key: Key) -> bool {
        let i = match self.getting_diff_for.iter().position(|&k| k == key) {
            Some(i) => i,
            None => return false,
        };
        self.getting_diff_for.remove(i);

        debug_assert!(
            self.entry(key)
                .is_none_or(|entry| entry.possible_gap.is_none()),
            "gaps shouldn't be created while getting difference"
        );
        true
    }
}

/// What [`MessageBoxes::apply_updates`] made of a batch.
struct Applied {
    /// The updates to hand out now, each with its state; from the socket, those that wait for
    /// their container's `seq` last, if it lets them out now.
    out: Vec<(tl::enums::Update, State)>,
    /// From the socket, the updates that wait for their container's `seq` if it doesn't let them
    /// out now ([`Source::Socket`]), for [`MessageBoxes::process_updates`] to hold or drop; empty
    /// otherwise.
    by_seq: Vec<tl::enums::Update>,
    /// Whether the batch counts as applied for the `date` of a container of no `seq`: it handed
    /// out or postponed an update, and no box it touched waits on a gap.
    counts_as_applied: bool,
}

/// How the account's difference ended, for the containers held for its `seq`
/// ([`MessageBoxes::replay_held_seq`]).
#[derive(Clone, Copy, Debug)]
enum DifferenceEnd {
    /// The server answered with its state: a difference, `updates.differenceTooLong`, or the
    /// `pts` [`MessageBoxes::restore_pts`] restores.
    Answered,
    /// `updates.differenceEmpty`, whose `seq` is the server's latest.
    Empty,
    /// No answer to apply ([`MessageBoxes::end_account_difference`]).
    Unanswered,
}

/// Where the updates [`MessageBoxes::apply_updates`] processes come from.
#[derive(Clone, Copy, Debug)]
enum Source {
    /// The socket, or a request's result. `seq_now`: whether the container's updates that wait
    /// for its `seq` go out now, after its box updates, or into [`Applied::by_seq`]
    /// ([`MessageBoxes::process_updates`] decides it from the `seq`).
    Socket { seq_now: bool },
    /// The `other_updates` of the account's difference.
    AccountDifference,
    /// The `other_updates` of a channel's difference.
    ChannelDifference(i64),
}

impl Source {
    /// Whether `update`, which no box orders, waits for its container's `seq`: from the socket,
    /// unless it applies as it arrives ([`adaptor::applies_at_once`]). Never
    /// `updateChannelTooLong`, which begins its channel's difference before the channel's other
    /// updates in the batch apply. A difference hands out every update.
    fn waits_for_seq(self, update: &tl::enums::Update) -> bool {
        matches!(self, Source::Socket { .. })
            && !matches!(update, tl::enums::Update::ChannelTooLong(_))
            && !adaptor::applies_at_once(update)
    }

    /// Whether this is the difference of `key`'s box, which returns that box's updates in order
    /// and sets its state itself.
    fn is_difference_of(self, key: Key) -> bool {
        match (self, key) {
            (Source::AccountDifference, Key::Common | Key::Secondary) => true,
            (Source::ChannelDifference(id), Key::Channel(channel_id)) => id == channel_id,
            _ => false,
        }
    }
}

// "Normal" updates flow (processing and detection of gaps).
impl MessageBoxes {
    /// Process updates received through the socket, or in a request's result, and return those
    /// to hand out.
    ///
    /// A container's updates that a box orders (by `pts`, `qts`, or a channel's `pts`) go to
    /// their boxes at once, whatever the container's `seq`, as TDLib's `on_pending_updates` sends
    /// them, each box checking its own. Updates for a box that is getting its difference are
    /// postponed, as the [updates' documentation] asks:
    ///
    /// > Implementations \[have\] to postpone updates received via the socket while
    /// > filling gaps in the event and `Update` sequences, as well as avoid filling
    /// > gaps in the same sequence.
    ///
    /// The call that ends that difference ([`MessageBoxes::apply_difference`],
    /// [`MessageBoxes::apply_channel_difference`], [`MessageBoxes::end_channel_difference`],
    /// [`MessageBoxes::end_account_difference`]) returns them after the difference's own updates,
    /// checked against the state it left the way updates arriving then are: dropped if the
    /// difference returned them, held if updates before them are still missing (after a temporary
    /// failure, dropped too, for the retry to return). Their users and chats are returned now, with
    /// this call's.
    ///
    /// The container's other updates, which no box orders, go in the order of the account's
    /// `seq`, as TDLib applies them, but for the few TDLib applies as they arrive (typing, a sent
    /// message's id, and the like) and an `updateShort`'s, which TDLib applies at once even while
    /// the account gets its difference. A container of no `seq` hands them out at once, and moves
    /// the `date` as the documentation's rule says, but never back, nor while a container waits
    /// past the local `seq`: the difference that fills that gap asks from the `date` before it. A
    /// container that follows on from the local `seq` hands them out and moves the `seq`, and the
    /// `date` forward, and so do the containers held after it that then follow on; one the local
    /// `seq` covers is dropped. One past it is a gap: it is held for the containers before it for
    /// half a second ([`MessageBoxes::check_deadlines`]), as a box's gap is, and then the
    /// account's difference begins (while the account's retry waits, it is dropped instead, for
    /// the retry's difference to set the `seq`). While the account gets its difference, every
    /// container's are held, checked once the difference ends against the `seq` it left: handed
    /// out if they follow on, dropped if it covered them, held for the containers before them
    /// otherwise. An unanswered end drops those, and an empty difference those held past the `seq`
    /// before it began, as TDLib drops its pending ones then; those that arrived during it wait, as
    /// TDLib checks its postponed ones again. Boxes without the account's state
    /// ([`MessageBoxes::set_state`]) have no difference to wait for, and drop them. A held
    /// container's users and chats, too, are returned now, with this call's, not later with its
    /// updates.
    ///
    /// [updates' documentation]: https://core.telegram.org/api/updates
    pub fn process_updates(
        &mut self,
        updates: UpdatesLike,
        now: Now,
    ) -> Result<UpdateAndPeers, Gap> {
        trace!(?updates, "processing updates");

        // An `updateShort` applies as it arrives, also while the account gets its difference:
        // TDLib hands it to its handler at once (`on_get_updates_impl`), where `updates` and
        // `updatesCombined` go through `on_pending_updates`, which postpones them meanwhile.
        let short = matches!(
            updates,
            UpdatesLike::Updates(tl::enums::Updates::UpdateShort(_))
        );

        // Top level, when handling received `updates` and `updatesCombined`.
        // `updatesCombined` groups all the fields we care about, which is why we use it.
        //
        // This assumes all access hashes are already known to the client (socket updates are
        // expected to use `ensure_known_peer_hashes`, and the result from getting difference
        // has to deal with the peers in a different way).
        let tl::types::UpdatesCombined {
            date,
            seq_start,
            seq,
            updates,
            users,
            chats,
        } = match adaptor::adapt(updates) {
            Ok(combined) => combined,
            Err(Gap) => {
                self.try_begin_get_diff(Key::Common);
                return Err(Gap);
            }
        };

        let new_date = if date == NO_DATE { self.date } else { date };
        let new_seq = if seq == NO_SEQ { self.seq } else { seq };
        // While the account's retry is armed, its difference sets the `date` of a container of no
        // `seq` (as it arrived: a wrong `qts` in this one arms the retry for what follows).
        let retrying = self.account_retry_at.is_some();
        // Both account boxes always get their difference together (`begin_account_difference`).
        // Read before the batch applies, as is where the container goes in the `seq` sequence
        // (`None` for a container of no `seq`): applying it moves neither.
        let in_difference = self.getting_diff_for.contains(&Key::Common);
        let seq_order = (seq_start != NO_SEQ).then(|| next_seq_cmp(self.seq, seq_start));
        // Whether its updates that wait for the `seq` go out now, after its box updates: an
        // `updateShort`'s, and outside the account's difference those of a container of no `seq`
        // and of one that follows on. Otherwise `by_seq` has them, to hold or drop.
        let seq_now = if in_difference {
            short
        } else {
            matches!(seq_order, None | Some(Ordering::Equal))
        };
        let Applied {
            mut out,
            by_seq,
            counts_as_applied,
        } = self.apply_updates(
            updates,
            (new_date, new_seq),
            now,
            Source::Socket { seq_now },
        );

        if in_difference {
            // The difference sets `date` and `seq`: the container's are checked once it ends (one
            // of no `seq` with nothing to hold has nothing to check), an `updateShort`'s went out.
            if !short && (seq_order.is_some() || !by_seq.is_empty()) {
                self.hold_seq(
                    HeldSeq {
                        seq_start,
                        seq,
                        date,
                        updates: by_seq,
                        during_difference: true,
                    },
                    now,
                );
            }
        } else {
            match seq_order {
                // > For all the other [not `updates` or `updatesCombined`] `Updates` type
                // > constructors there is no need to check `seq` or change a local state.
                None => {
                    // The `date` is the cursor of the `seq` sequence's events in
                    // `updates.getDifference`: while a container waits past the local `seq`, the
                    // difference that fills the gap asks from before it, as TDLib's `fill_seq_gap`
                    // asks from the `date` its containers applied in `seq` order left (TDLib takes
                    // none from a container of no `seq`).
                    if counts_as_applied && !retrying && self.held_seq.is_empty() {
                        // > If the updates were applied, local *Updates* state must be updated
                        // > with `seq` (unless it's 0) and `date` from the constructor.
                        self.advance_date_seq(new_date, new_seq);
                    }
                }
                // Ignore
                Some(Ordering::Greater) => {
                    debug!(
                        "skipping updates that were already handled at seq = {}",
                        self.seq
                    );
                }
                // Apply
                Some(Ordering::Equal) => {
                    self.advance_date_seq(new_date, new_seq);
                    self.apply_held_seq(now, &mut out);
                }
                // Without the account's state there is no difference to fill the gap with, and
                // nothing to order by: dropped, as before the state is set
                // (`MessageBoxes::set_state`), rather than held for good.
                Some(Ordering::Less) if self.entry(Key::Common).is_none() => {
                    debug!("dropping updates past the seq; the account has no state yet");
                }
                Some(Ordering::Less) => {
                    debug!(
                        "gap detected (local seq {}, remote seq {})",
                        self.seq, seq_start
                    );
                    self.hold_seq(
                        HeldSeq {
                            seq_start,
                            seq,
                            date,
                            updates: by_seq,
                            during_difference: false,
                        },
                        now,
                    );
                }
            }
        }

        Ok((out, users, chats))
    }

    /// Hold a container for the account's `seq`, in the order the held containers apply
    /// ([`MessageBoxes::held_seq`]): an in-order one lands last. Outside the account's difference
    /// it is past the local `seq`, and opens the `seq` gap at `now` if none is open
    /// ([`MessageBoxes::settle_seq_gap`]).
    fn hold_seq(&mut self, held: HeldSeq, now: Now) {
        let order = (held.seq_start, Reverse(held.seq));
        let at = self
            .held_seq
            .partition_point(|other| (other.seq_start, Reverse(other.seq)) <= order);
        self.held_seq.insert(at, held);
        self.settle_seq_gap(now);
    }

    /// Keep the account's `seq` gap ([`MessageBoxes::seq_gap`]) armed exactly while containers
    /// are held outside its difference; every change to the held containers ends with this.
    /// Closed while none is held, and while the difference runs, whose end checks them; opened at
    /// `now`'s half second if one is held and none is open; an open one keeps its deadline, as a
    /// box's possible gap does.
    fn settle_seq_gap(&mut self, now: Now) {
        if self.held_seq.is_empty() || self.getting_diff_for.contains(&Key::Common) {
            self.seq_gap = None;
        } else if self.seq_gap.is_none() {
            let deadline = now + POSSIBLE_GAP_TIMEOUT;
            self.seq_gap = Some(deadline);
            self.arm(deadline);
        }
    }

    /// Outside the account's difference, once the local `seq` moved: hand out the containers held
    /// past it that now follow on, in order, each moving the `seq`, and the `date` forward, and
    /// drop those it covers, until one is still missing; then settle the `seq` gap
    /// ([`MessageBoxes::settle_seq_gap`]: closed once none is held, its deadline kept while any
    /// is).
    fn apply_held_seq(&mut self, now: Now, result: &mut Vec<(tl::enums::Update, State)>) {
        while let Some(seq_start) = self.held_seq.front().map(|held| held.seq_start) {
            let order = next_seq_cmp(self.seq, seq_start);
            if order == Ordering::Less {
                break;
            }
            let Some(held) = self.held_seq.pop_front() else {
                break;
            };
            if order == Ordering::Greater {
                debug!(
                    "skipping held updates that were already handled at seq = {}",
                    self.seq
                );
                continue;
            }
            let date = if held.date == NO_DATE {
                self.date
            } else {
                held.date
            };
            self.hand_out_held(held, date, result);
        }
        self.settle_seq_gap(now);
    }

    /// Once the account's difference has ended (`end`), check the containers held for its `seq`
    /// meanwhile against the `seq` it left, in order, and add to `result` the updates of those
    /// that follow on: one of no `seq`, and one whose `seq_start` follows on, which moves the
    /// `seq` (the `date` stays the difference's: TDLib ignores the `date` of a container postponed
    /// while it ran, which may be the older, and moves its own only forward). One the `seq` covers
    /// is dropped. The first one still past it, and every later one, wait for the containers
    /// before them, the `seq` gap opening at `now`, as on arrival
    /// ([`MessageBoxes::settle_seq_gap`]). An unanswered end drops them instead, for the retry's
    /// difference to set the `seq`. An empty difference, whose `seq` is the server's latest, drops
    /// those held past the `seq` before it began, as TDLib drops its `pending_seq_updates_` then
    /// (`on_get_difference`), while those that arrived during it wait, as TDLib checks its
    /// `postponed_updates_` again (`after_get_difference`). One that waits counts as held before
    /// the next difference, so a second empty one drops it: a server whose `seq` lags costs one
    /// more difference, not a loop of them.
    fn replay_held_seq(
        &mut self,
        now: Now,
        end: DifferenceEnd,
        result: &mut Vec<(tl::enums::Update, State)>,
    ) {
        while let Some(seq_start) = self.held_seq.front().map(|held| held.seq_start) {
            let order = if seq_start == NO_SEQ {
                Ordering::Equal
            } else {
                next_seq_cmp(self.seq, seq_start)
            };
            if order == Ordering::Less {
                break;
            }
            let Some(held) = self.held_seq.pop_front() else {
                break;
            };
            if order == Ordering::Greater {
                debug!(
                    "skipping held updates the difference covered (local seq {}, remote seq {})",
                    self.seq, held.seq_start
                );
                continue;
            }
            self.hand_out_held(held, self.date, result);
        }
        let before = self.held_seq.len();
        match end {
            DifferenceEnd::Answered => {}
            DifferenceEnd::Empty => self.held_seq.retain(|held| held.during_difference),
            DifferenceEnd::Unanswered => self.held_seq.clear(),
        }
        if self.held_seq.len() < before {
            debug!(
                count = before - self.held_seq.len(),
                "dropping the containers held past the seq; the next difference sets it"
            );
        }
        if let Some(first) = self.held_seq.front() {
            debug!(
                "gap detected after the difference (local seq {}, remote seq {})",
                self.seq, first.seq_start
            );
        }
        for held in &mut self.held_seq {
            held.during_difference = false;
        }
        self.settle_seq_gap(now);
    }

    /// Add `held`'s updates to `result` with `date` and its `seq`, which become the local ones, the
    /// `date` only forward ([`MessageBoxes::advance_date_seq`]; a container of no `seq` keeps the
    /// local `seq`).
    fn hand_out_held(
        &mut self,
        held: HeldSeq,
        date: i32,
        result: &mut Vec<(tl::enums::Update, State)>,
    ) {
        let seq = if held.seq == NO_SEQ {
            self.seq
        } else {
            held.seq
        };
        result.extend(held.updates.into_iter().map(|update| {
            (
                update,
                State {
                    date,
                    seq,
                    message_box: None,
                },
            )
        }));
        self.advance_date_seq(date, seq);
    }

    /// Process `updates`, a container's or a difference's `other_updates`: return those to hand
    /// out, with the container's `date` and `seq`, and, from the socket, those that wait for the
    /// container's `seq`, last if it lets them out now and apart if it doesn't ([`Applied`]).
    fn apply_updates(
        &mut self,
        updates: Vec<tl::enums::Update>,
        (date, seq): (i32, i32),
        now: Now,
        source: Source,
    ) -> Applied {
        // Each update with its place in its box's sequence, read once, and whether it waits for
        // its container's `seq` (`Source::waits_for_seq`). One whose `pts` breaks the protocol is
        // dropped, as TDLib drops it; a later update of its box then finds the gap. A wrong `qts`
        // also schedules the account's difference, as TDLib schedules it
        // (`add_pending_qts_update`), where a wrong `pts` doesn't.
        let mut wrong_qts = false;
        let mut updates: Vec<(Option<PtsInfo>, bool, tl::enums::Update)> = updates
            .into_iter()
            .filter_map(|update| match PtsInfo::from_update(&update) {
                Ok(info) => {
                    let waits = info.is_none() && source.waits_for_seq(&update);
                    Some((info, waits, update))
                }
                // Logged without the update, which holds the user's messages; every caller has
                // traced its whole input already.
                Err(WrongPts::CommonInChannel) => {
                    warn!(
                        "dropping a common-box update about a channel; its pts breaks the protocol"
                    );
                    None
                }
                Err(WrongPts::Count(info)) => {
                    warn!(?info, "dropping an update whose pts breaks the protocol");
                    wrong_qts |= info.key == Key::Secondary;
                    None
                }
            })
            .collect();
        if wrong_qts {
            self.schedule_account_difference(now);
        }

        // Telegram can send updates out of order (e.g. `ReadChannelInbox` first
        // and then `NewChannelMessage`, both with the same `pts`, but the `count`
        // is `0` and `1` respectively), so we sort them first. Those that wait for the
        // container's `seq` go last, as TDLib's `on_pending_updates` sends a container's updates
        // to their boxes before it applies the rest in `seq` order (`process_seq_updates`).
        updates.sort_by_key(|(info, waits, _)| (*waits, update_sort_key(info.as_ref())));

        let seq_now = matches!(source, Source::Socket { seq_now: true });
        let mut result = Vec::with_capacity(updates.len());
        let mut by_seq = Vec::new();
        let mut postponed = false;
        let mut have_unresolved_gaps = false;
        for (info, waits, update) in updates {
            if let tl::enums::Update::ChannelTooLong(u) = update {
                // Can skip this one early since no processing can be done for the entry.
                //
                // A known channel's difference starts from its own `pts`. The update only says
                // that the server has too many updates to send:
                //
                // > the updatesTooLong/updateChannelTooLong updates […] simply indicate that the
                // > number of queued updates for a message box is too large to be delivered
                // > passively through the socket, and updates.getDifference/
                // > updates.getChannelDifference can be invoked normally to fetch the difference.
                //
                // Taking its `pts` instead would skip every update between the two. It is only
                // the starting point of a channel without one:
                //
                // > the initial channel state can be obtained […] or it can be received as an
                // > updateChannelTooLong update.
                let key = Key::Channel(u.channel_id);
                if self.entry(key).is_none()
                    && let Some(info) = info
                {
                    self.set_entry(LiveEntry::new(key, info.pts));
                }
                self.try_begin_get_diff(key);
                continue;
            }

            if waits && !seq_now {
                // Its container is held or dropped (`MessageBoxes::process_updates`).
                by_seq.push(update);
                continue;
            }
            let Some(info) = info else {
                // No box orders it: it applies as it arrives, a difference returned it, or its
                // container's `seq` lets it out now.
                result.push((
                    update,
                    State {
                        date,
                        seq,
                        message_box: None,
                    },
                ));
                continue;
            };

            if source.is_difference_of(info.key) {
                // Order does not need to be checked: the difference returns its box's updates in
                // order, and sets the box's state itself.
                result.push((
                    update,
                    State {
                        date,
                        seq,
                        message_box: Some(info.into()),
                    },
                ));
            } else if self.getting_diff_for.contains(&info.key) {
                debug!(
                    key = ?info.key,
                    count = ?info.count,
                    remote = ?info.pts,
                    "postponing update (getting difference)",
                );
                self.postpone(info, update);
                postponed = true;
            } else {
                have_unresolved_gaps |=
                    self.apply_in_order(update, info, (date, seq), now, &mut result);
            }
        }

        let counts_as_applied =
            (postponed || !result.is_empty() || !by_seq.is_empty()) && !have_unresolved_gaps;
        Applied {
            out: result,
            by_seq,
            counts_as_applied,
        }
    }

    /// Hold `update` until the difference of its box, `info.key`, ends.
    fn postpone(&mut self, info: PtsInfo, update: tl::enums::Update) {
        match info.key {
            Key::Common | Key::Secondary => self.account_postponed.push((info, update)),
            Key::Channel(_) => {
                let found = self.update_entry(info.key, |entry| {
                    entry.postponed.push((info, update));
                });
                debug_assert!(found, "a channel getting its difference has an entry");
            }
        }
    }

    /// Apply `update` to its box (`info`) if it is the box's next one, along with the updates the
    /// box's possible gap holds that follow it; hold it in that gap if updates before it are
    /// missing; drop it if the box has it already. Applied updates go to `result`, with the
    /// `date` and `seq` given.
    ///
    /// Returns whether the box's possible gap still holds updates.
    fn apply_in_order(
        &mut self,
        update: tl::enums::Update,
        info: PtsInfo,
        (date, seq): (i32, i32),
        now: Now,
        result: &mut Vec<(tl::enums::Update, State)>,
    ) -> bool {
        let key = info.key;
        let mk_state = |message_box| State {
            date,
            seq,
            message_box: Some(message_box),
        };
        let mut applied = false;
        let mut held = false;
        let mut opened = None;
        self.force_update_entry(LiveEntry::new(key, info.start()), |entry| {
            match entry.pts.cmp(&info.start()) {
                // Apply
                Ordering::Equal => {
                    entry.pts = info.pts;
                    applied = true;
                    result.push((update, mk_state(info.into())));
                }
                // Ignore
                Ordering::Greater => {
                    debug!(
                        key = ?info.key,
                        local = ?entry.pts,
                        count = ?info.count,
                        remote = ?info.pts,
                        "skipping update",
                    );
                }
                Ordering::Less => {
                    // TODO store chats too?
                    info!(
                        key = ?info.key,
                        local = ?entry.pts,
                        count = ?info.count,
                        remote = ?info.pts,
                        "gap on update",
                    );

                    match &mut entry.possible_gap {
                        // In the order the updates apply, found by a binary search.
                        Some(gap) => {
                            let order = info.order();
                            let at = gap
                                .updates
                                .partition_point(|(held, _)| held.order() <= order);
                            gap.updates.insert(at, (info, update));
                        }
                        // The gap's deadline is armed once, when it opens.
                        None => {
                            let deadline = now + POSSIBLE_GAP_TIMEOUT;
                            opened = Some(deadline);
                            entry.possible_gap = Some(PossibleGap {
                                deadline,
                                updates: VecDeque::from([(info, update)]),
                            });
                        }
                    }
                }
            };

            if let Some(gap) = &mut entry.possible_gap {
                while let Some(start) = gap.updates.front().map(|(gap_info, _)| gap_info.start()) {
                    let order = entry.pts.cmp(&start);
                    if order == Ordering::Less {
                        break;
                    }
                    // The box's next update, or one it has already.
                    if let Some((gap_info, gap_update)) = gap.updates.pop_front()
                        && order == Ordering::Equal
                    {
                        entry.pts = gap_info.pts;
                        // possible gap can only be resolve if new update closed
                        // the gap - this has already updated its deadline if so.
                        result.push((gap_update, mk_state(gap_info.into())));
                    }
                }
                held = !gap.updates.is_empty();
            }
            if !held {
                entry.possible_gap = None;
            }
        });

        // An update applied to either of the account's boxes restarts its idle deadline.
        if applied && !matches!(key, Key::Channel(_)) {
            self.heard_from_account(now);
        }
        if let Some(deadline) = opened {
            self.arm(deadline);
        }
        held
    }

    /// Check the updates postponed while their box got its difference against the state the
    /// difference left, in `pts` order and the way updates arriving now are checked, and add those
    /// that follow on to `result`. One the difference returned is dropped. One that doesn't follow
    /// on yet waits in its box's possible gap, unless `drop_after_gap`: the difference ended
    /// without an answer to apply, so it and every later one of its box are dropped, for the box's
    /// next difference to return (a retry after a temporary failure, or its next gap), rather than
    /// held in a gap that would get the difference again at once. The account's two boxes are
    /// postponed together, and each drops its own.
    fn replay_postponed(
        &mut self,
        mut postponed: Vec<(PtsInfo, tl::enums::Update)>,
        drop_after_gap: bool,
        now: Now,
        result: &mut Vec<(tl::enums::Update, State)>,
    ) {
        postponed.sort_by_key(|(info, _)| info.order());
        for (info, update) in postponed {
            // This check alone drops every later update of a box once one doesn't follow on: a
            // box's `pts` moves only through its own updates, which come in `start` order, so
            // once one is dropped, the box's `pts` stays below every later one's `start`.
            if drop_after_gap
                && self
                    .entry(info.key)
                    .is_some_and(|entry| entry.pts < info.start())
            {
                debug!(
                    key = ?info.key,
                    remote = ?info.pts,
                    "dropping a postponed update after a gap; the next difference returns it",
                );
                continue;
            }
            self.apply_in_order(update, info, (self.date, self.seq), now, result);
        }
    }
}

/// Getting and applying account difference.
impl MessageBoxes {
    /// Return the request that needs to be made to get the difference, if any.
    pub fn get_difference(&self) -> Option<tl::functions::updates::GetDifference> {
        // Both account boxes always get their difference together (`begin_account_difference`),
        // which only begins with a common entry.
        if !self.getting_diff_for.contains(&Key::Common) {
            return None;
        }
        let pts = self.entry(Key::Common)?.pts;

        let gd = tl::functions::updates::GetDifference {
            pts,
            pts_limit: None,
            pts_total_limit: None,
            date: self.date.max(1), // non-zero or the request will fail
            qts: self
                .entry(Key::Secondary)
                .map(|entry| entry.pts)
                .unwrap_or(NO_PTS),
            qts_limit: None,
        };
        trace!(?gd, "requesting");
        Some(gd)
    }

    /// Similar to [`MessageBoxes::process_updates`], but using the result from getting difference.
    ///
    /// A difference that isn't a slice ends the account's. The updates postponed for the account
    /// while it ran then follow the difference's own (see [`MessageBoxes::process_updates`]).
    ///
    /// A difference while the account isn't getting one, such as a second answer to a request
    /// repeated while the first was in flight, is dropped: it changes nothing and returns nothing.
    pub fn apply_difference(
        &mut self,
        difference: tl::enums::updates::Difference,
        now: Now,
    ) -> UpdateAndPeers {
        trace!(?difference, "applying account difference");
        // Both account boxes always get their difference together (`begin_account_difference`).
        if !self.getting_diff_for.contains(&Key::Common) {
            warn!("dropping a difference for the account, which isn't getting one");
            return (Vec::new(), Vec::new(), Vec::new());
        }
        // The server answered, so the next temporary failure waits the shortest time again.
        self.account_retry_delay = Duration::ZERO;
        let finish: bool;
        let end = if matches!(difference, tl::enums::updates::Difference::Empty(_)) {
            DifferenceEnd::Empty
        } else {
            DifferenceEnd::Answered
        };
        let mut result = match difference {
            tl::enums::updates::Difference::Empty(diff) => {
                debug!(
                    "handling empty difference (date = {}, seq = {}); no longer getting diff",
                    diff.date, diff.seq
                );
                finish = true;
                self.set_date_seq(diff.date, diff.seq);
                (Vec::new(), Vec::new(), Vec::new())
            }
            tl::enums::updates::Difference::Difference(diff) => {
                debug!(
                    state = ?diff.state,
                    "handling full difference; no longer getting diff",
                );
                finish = true;
                self.apply_difference_type(diff, now)
            }
            tl::enums::updates::Difference::Slice(tl::types::updates::DifferenceSlice {
                new_messages,
                new_encrypted_messages,
                other_updates,
                chats,
                users,
                intermediate_state: state,
            }) => {
                debug!(?state, "handling partial difference");
                finish = false;
                self.apply_difference_type(
                    tl::types::updates::Difference {
                        new_messages,
                        new_encrypted_messages,
                        other_updates,
                        chats,
                        users,
                        state,
                    },
                    now,
                )
            }
            tl::enums::updates::Difference::TooLong(diff) => {
                debug!(
                    "handling too-long difference (pts = {}); no longer getting diff",
                    diff.pts
                );
                finish = true;
                // the deadline will be reset once the diff ends
                self.set_pts(Key::Common, diff.pts);
                (Vec::new(), Vec::new(), Vec::new())
            }
        };

        if finish {
            self.finish_account_difference(now, end, &mut result.0);
        }

        result
    }

    fn apply_difference_type(
        &mut self,
        tl::types::updates::Difference {
            new_messages,
            new_encrypted_messages,
            other_updates,
            chats,
            users,
            state: tl::enums::updates::State::State(state),
        }: tl::types::updates::Difference,
        now: Now,
    ) -> UpdateAndPeers {
        self.set_date_seq(state.date, state.seq);
        self.set_pts(Key::Common, state.pts);
        self.set_pts(Key::Secondary, state.qts);
        let mk_state = |message_box| State {
            date: state.date,
            seq: state.seq,
            message_box: Some(message_box),
        };

        // The account's updates are returned as they are, not checked against the state just set
        // (`Source::AccountDifference`). `other_updates` can also contain channel updates, such as
        // `updateChannelTooLong` and `updateNewChannelMessage`. Those are checked like socket
        // updates: dropped if already handled, and postponed if their channel is getting its
        // difference.
        //
        // It is possible that the result from `GetDifference` includes users with `min = true`.
        // TODO in that case, we will have to resort to getUsers.
        let mut result_updates = self
            .apply_updates(
                other_updates,
                (state.date, state.seq),
                now,
                Source::AccountDifference,
            )
            .out;

        result_updates.extend(
            new_messages
                .into_iter()
                .map(|message| {
                    (
                        tl::types::UpdateNewMessage {
                            message,
                            pts: NO_PTS,
                            pts_count: 0,
                        }
                        .into(),
                        mk_state(MessageBox::Common { pts: state.pts }),
                    )
                })
                .chain(new_encrypted_messages.into_iter().map(|message| {
                    (
                        tl::types::UpdateNewEncryptedMessage {
                            message,
                            qts: NO_PTS,
                        }
                        .into(),
                        mk_state(MessageBox::Secondary { qts: state.qts }),
                    )
                })),
        );

        (result_updates, users, chats)
    }

    /// End the account's difference when the server didn't answer with one: it failed for a
    /// temporary reason, such as an internal server error, or its answer can't be read, the same
    /// on every try. TDLib schedules the account's difference again after any failure of it
    /// (`on_failed_get_difference`), as tdesktop does (`differenceFail`).
    ///
    /// The account's state is kept, and its difference is due again after a backoff: 1 second
    /// after the first unanswered end in a row, doubling with each one up to 64 seconds (64 to 80
    /// with the caller's jitter, [`MessageBoxes::seed_retry_jitter`]), until the
    /// server answers one ([`MessageBoxes::check_deadlines`]), also while channels get their
    /// differences. Meanwhile nothing but the caller's [`MessageBoxes::begin_account_difference`]
    /// begins it, for a reason the retry doesn't cover, such as a new connection: the account's
    /// gaps wait for the retry, a possible gap's updates, and the containers held past the `seq`,
    /// dropped once their half second is over before the retry falls due, for the retry's
    /// difference to return them and set the `seq` (one due with the retry is kept for its
    /// difference). The account's updates that follow on are applied, in their boxes and in `seq`
    /// order (see [`MessageBoxes::process_updates`]), and the channels' deadlines are checked
    /// again: a channel's difference doesn't wait for the account's.
    ///
    /// Returns the updates postponed for the account while its difference ran that follow on from
    /// its state, and those of the containers held for its `seq` meanwhile that follow on from the
    /// local `seq` (see [`MessageBoxes::process_updates`]); their users and chats were returned
    /// when they arrived. Each of the account's boxes drops the rest from its first one that
    /// doesn't follow on, as after a channel's temporary failure, and the retry's difference
    /// returns them; the containers from the first one past the `seq` are dropped the same way,
    /// the retry's difference setting the `seq`.
    ///
    /// A server that refuses the account's state itself (`PERSISTENT_TIMESTAMP_INVALID`) is
    /// answered with its `pts` instead ([`MessageBoxes::restore_pts`]).
    ///
    /// Ending the account's difference while it isn't getting one, such as for a second answer to
    /// a request repeated while the first was in flight, changes nothing and returns nothing.
    #[must_use = "the returned updates have moved the account's state"]
    pub fn end_account_difference(&mut self, now: Now) -> Vec<(tl::enums::Update, State)> {
        trace!("ending the account's difference");
        // Both account boxes always get their difference together (`begin_account_difference`).
        if !self.getting_diff_for.contains(&Key::Common) {
            warn!("ignoring the end of the account's difference, which isn't getting one");
            return Vec::new();
        }
        for key in [Key::Common, Key::Secondary] {
            self.try_end_get_diff(key);
        }
        self.arm_account_retry(now);
        let mut result = Vec::new();
        let postponed = mem::take(&mut self.account_postponed);
        self.replay_postponed(postponed, true, now, &mut result);
        self.replay_held_seq(now, DifferenceEnd::Unanswered, &mut result);
        result
    }

    /// Restore the account's `pts` from `updates.getState` after the server refused the state of
    /// its difference with `PERSISTENT_TIMESTAMP_INVALID`, as TDLib does (`on_get_updates_state`'s
    /// "Restore PTS"): only the common box moves, to `pts`; the secondary box's `qts`, the `date`
    /// and the `seq` stay. So the account's next difference returns the secondary box's updates
    /// after the kept `qts`, which the server's would skip for good (a difference's `qts` confirms
    /// receipt to the server), and a container past the kept `seq` begins it once its half second
    /// is over, as after any lost push.
    ///
    /// The account's difference ends as an answered one does, the way `updates.differenceTooLong`,
    /// which moves only the common box too, ends it: a retry armed for it
    /// ([`MessageBoxes::end_account_difference`]) is cancelled and its backoff starts over, the
    /// idle deadline restarts, and the updates postponed for the account and the containers held
    /// for its `seq` meanwhile are checked against the state it leaves (see
    /// [`MessageBoxes::process_updates`]). The common box's up to `pts` are skipped with the jump,
    /// which the caller tells the user of, as for `updates.differenceTooLong`; the updates that
    /// follow on, the secondary box's from the kept `qts` among them, are returned (their users
    /// and chats were returned when they arrived); a container past the kept `seq` waits for the
    /// ones before it for half a second, then begins the account's difference.
    ///
    /// Boxes without the account's state get it from [`MessageBoxes::set_state`], after login.
    #[must_use = "the returned updates have moved the account's state"]
    pub fn restore_pts(&mut self, pts: i32, now: Now) -> Vec<(tl::enums::Update, State)> {
        trace!(pts, "restoring the account's pts");
        // The server answered: no retry is due, and the next temporary failure waits the shortest
        // time again.
        self.account_retry_at = None;
        self.account_retry_delay = Duration::ZERO;
        // A new entry: a possible gap the common box held while a retry waited is the jump's.
        self.set_entry(LiveEntry::new(Key::Common, pts));
        let mut result = Vec::new();
        self.finish_account_difference(now, DifferenceEnd::Answered, &mut result);
        result
    }

    /// Schedule the account's difference after an update with a wrong `qts`, as TDLib does
    /// (`add_pending_qts_update`, which calls `schedule_get_difference("wrong QTS")`): it is due
    /// after the next backoff step ([`MessageBoxes::account_retry_at`]), the step a failed
    /// difference waits, rather than at once. Nothing changes if the account is getting its
    /// difference, or a retry is armed for it: either covers this one. Nor without the common
    /// box's state, which the account's difference starts from.
    fn schedule_account_difference(&mut self, now: Now) {
        if self.account_retry_at.is_some() || self.getting_diff_for.contains(&Key::Common) {
            return;
        }
        if self.entry(Key::Common).is_none() {
            info!("a wrong qts asks for the account's difference, but there is no common state");
            return;
        }
        debug!("a wrong qts schedules the account's difference");
        self.arm_account_retry(now);
    }

    /// Arm the account's retry ([`MessageBoxes::account_retry_at`]) at the next backoff step from
    /// `now`.
    fn arm_account_retry(&mut self, now: Now) {
        let (step, wait) = self.next_retry(self.account_retry_delay, Spread::Account);
        self.account_retry_delay = step;
        let retry_at = now + wait;
        self.account_retry_at = Some(retry_at);
        self.arm(retry_at);
    }
}

/// Getting and applying channel difference.
impl MessageBoxes {
    /// Return the request that needs to be made to get a channel's difference, if any: the
    /// channel [`MessageBoxes::begin_channel_difference`] asked for last, if it is still waiting,
    /// otherwise the one waiting longest. Its answer goes to
    /// [`MessageBoxes::apply_channel_difference`] or [`MessageBoxes::end_channel_difference`],
    /// with the channel the request names.
    ///
    /// Note that this only returns the skeleton of a request.
    /// Both the channel hash and limit will be their default value.
    pub fn get_channel_difference(&self) -> Option<tl::functions::updates::GetChannelDifference> {
        // A channel only begins getting its difference with an entry, and keeps it until it ends.
        let (channel_id, pts) = self.getting_diff_for.iter().find_map(|&key| match key {
            Key::Channel(id) => self.entry(key).map(|entry| (id, entry.pts)),
            _ => None,
        })?;

        let gd = tl::functions::updates::GetChannelDifference {
            force: false,
            channel: tl::types::InputChannel {
                channel_id,
                access_hash: 0,
            }
            .into(),
            filter: tl::enums::ChannelMessagesFilter::Empty,
            pts,
            limit: 0,
        };
        trace!(?gd, "requesting");
        Some(gd)
    }

    /// Similar to [`MessageBoxes::process_updates`], but using the result from getting the
    /// difference of the channel `channel_id`, the one the request named.
    ///
    /// A final difference ends it. The updates postponed for the channel while it ran then follow
    /// the difference's own (see [`MessageBoxes::process_updates`]). Its `timeout` is the
    /// caller's, to schedule the next one for a channel the user has open
    /// ([`MessageBoxes::begin_channel_difference`]): channels get no difference by themselves.
    ///
    /// `updates.channelDifferenceTooLong` moves the channel's `pts` to its `dialog`'s, and its
    /// `dialog` and `messages` are dropped: the caller reads them from the response first. One
    /// whose `dialog` has no `pts`, or is a folder or community, ends the channel's difference
    /// without moving its `pts`, as TDLib does: like [`MessageBoxes::end_channel_difference`] after
    /// a temporary failure, but with no retry; its next gap gets the difference again.
    ///
    /// A difference for a channel that isn't getting one, such as a second answer to a request
    /// repeated while the first was in flight, is dropped: it changes nothing and returns nothing.
    pub fn apply_channel_difference(
        &mut self,
        channel_id: i64,
        difference: tl::enums::updates::ChannelDifference,
        now: Now,
    ) -> UpdateAndPeers {
        let key = Key::Channel(channel_id);
        if !self.getting_diff_for.contains(&key) {
            warn!(
                channel_id,
                "dropping the difference of a channel that isn't getting one"
            );
            return (Vec::new(), Vec::new(), Vec::new());
        }

        trace!(channel_id, ?difference, "applying channel difference");

        // The server answered, so the next temporary failure waits the shortest time again.
        self.update_entry(key, |entry| {
            entry.possible_gap = None;
            entry.retry_delay = Duration::ZERO;
        });

        let Some(tl::types::updates::ChannelDifference {
            r#final,
            pts,
            timeout: _,
            new_messages,
            other_updates,
            chats,
            users,
        }) = adaptor::adapt_channel_difference(difference)
        else {
            // No `pts` to move to: the difference ends unanswered, as in TDLib
            // (`after_get_channel_difference`), and no retry is armed; the next gap gets it again.
            self.try_end_get_diff(key);
            let mut result = Vec::new();
            self.replay_channel_postponed(key, true, now, &mut result);
            return (result, Vec::new(), Vec::new());
        };

        // The channel's updates are returned as they are, not checked against the `pts` this
        // difference ends at, which would drop every one of them as already handled
        // (`Source::ChannelDifference`).
        let mut result_updates = self
            .apply_updates(
                other_updates,
                (self.date, self.seq),
                now,
                Source::ChannelDifference(channel_id),
            )
            .out;
        self.set_pts(key, pts);

        result_updates.extend(new_messages.into_iter().map(|message| {
            (
                tl::types::UpdateNewChannelMessage {
                    message,
                    pts: NO_PTS,
                    pts_count: 0,
                }
                .into(),
                State {
                    date: self.date,
                    seq: self.seq,
                    message_box: Some(MessageBox::Channel { channel_id, pts }),
                },
            )
        }));

        if r#final {
            debug!(
                "handling channel {} difference; no longer getting diff",
                channel_id
            );
            self.try_end_get_diff(key);
            self.replay_channel_postponed(key, false, now, &mut result_updates);
        } else {
            debug!("handling channel {} difference", channel_id);
        }

        (result_updates, users, chats)
    }

    /// End the difference of the channel `channel_id`, the one the request named, when the
    /// server didn't answer with one ([`PrematureEndReason`]).
    ///
    /// Returns the updates postponed for the channel while its difference ran that follow on from
    /// its state (see [`MessageBoxes::process_updates`]); their users and chats were returned when
    /// they arrived. After a temporary failure the others are dropped: the difference is due again
    /// after a backoff, and returns them. A banned channel's are dropped with its state.
    ///
    /// Ending the difference of a channel that isn't getting one, such as for a second answer to a
    /// request repeated while the first was in flight, changes nothing and returns nothing.
    #[must_use = "the returned updates have moved the channel's state"]
    pub fn end_channel_difference(
        &mut self,
        channel_id: i64,
        reason: PrematureEndReason,
        now: Now,
    ) -> Vec<(tl::enums::Update, State)> {
        let key = Key::Channel(channel_id);
        trace!(channel_id, ?reason, "ending channel difference");
        if !self.try_end_get_diff(key) {
            warn!(
                channel_id,
                "ignoring the end of the difference of a channel that isn't getting one"
            );
            return Vec::new();
        }
        let mut result = Vec::new();
        match reason {
            PrematureEndReason::TemporaryServerIssues => {
                if let Some(entry) = self.entry(key) {
                    let (step, wait) = self.next_retry(entry.retry_delay, Spread::Channel);
                    let retry_at = now + wait;
                    self.update_entry(key, |entry| {
                        entry.retry_delay = step;
                        entry.retry_at = Some(retry_at);
                    });
                    self.arm(retry_at);
                }
                // The others aren't held in a possible gap, whose 0.5 s would get the difference
                // again ahead of the backoff. Like TDLib (`after_get_channel_difference`), this
                // drops them, and the retry returns them.
                self.replay_channel_postponed(key, true, now, &mut result);
            }
            PrematureEndReason::Banned => {
                self.pop_entry(key);
            }
        }
        result
    }

    /// Once the difference of the channel `key` has ended, add to `result` the updates postponed
    /// for it meanwhile that follow on from its state ([`MessageBoxes::replay_postponed`]).
    /// `drop_after_gap` when it ended without an answer to apply, its `pts` unchanged: the others
    /// are dropped, as TDLib's `after_get_channel_difference` drops them after an unsuccessful
    /// difference.
    fn replay_channel_postponed(
        &mut self,
        key: Key,
        drop_after_gap: bool,
        now: Now,
        result: &mut Vec<(tl::enums::Update, State)>,
    ) {
        let postponed = self
            .find_entry(key)
            .map(|entry| mem::take(&mut entry.postponed))
            .unwrap_or_default();
        self.replay_postponed(postponed, drop_after_gap, now, result);
    }
}

/// Reason for calling [`MessageBoxes::end_channel_difference`].
#[derive(Debug)]
pub enum PrematureEndReason {
    /// The channel difference failed to be fetched for a temporary reason,
    /// such as `PERSISTENT_TIMESTAMP_OUTDATED` or another internal server error.
    ///
    /// The channel state is kept, and its difference is due again after a backoff:
    /// 1 second after the first failure in a row, doubling with each one up to 64 seconds
    /// (each within 20 % either way, and 64 to 80 at the cap, with the caller's jitter,
    /// [`MessageBoxes::seed_retry_jitter`]), until the server answers
    /// ([`MessageBoxes::check_deadlines`]).
    /// It is due sooner on the channel's next gap or `updateChannelTooLong`,
    /// or when the caller asks for it ([`MessageBoxes::begin_channel_difference`]).
    TemporaryServerIssues,
    /// The logged-in account has been banned from the channel and won't
    /// be able to fetch the difference in the future.
    ///
    /// The channel state will be removed,
    /// and getting its difference should not be retried in the future.
    Banned,
}
