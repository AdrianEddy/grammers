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
//! (entry is in [`MessageBoxes::possible_gaps`]).
//! Otherwise, the entry is on its happy path.
//!
//! Gaps are cleared when they are either
//! resolved on their own
//! (by waiting for a short time)
//! or because we got the difference for the corresponding entry.
//!
//! While there are entries for which their difference must be fetched,
//! [`MessageBoxes::check_deadlines`] will always return the current time,
//! since "now" is the time to get the difference.
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
//! dropped, an unusable difference ends the box's difference, and an answer for a box that isn't
//! getting its difference changes nothing. None of them panics.

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
#[cfg(test)]
mod tests;

use std::cmp::{Ordering, Reverse};
use std::mem;
use std::time::Duration;

use defs::{CHANNEL_RETRY_MAX, CHANNEL_RETRY_MIN, DirtySet, Key};
pub use defs::{
    CommonState, DiffPending, Gap, MessageBox, MessageBoxes, Now, State, StateDelta,
    UpdateAndPeers, UpdatesLike,
};
use defs::{
    LiveEntry, NO_DATE, NO_PTS, NO_SEQ, POSSIBLE_GAP_TIMEOUT, PossibleGap, PtsInfo, WrongPts,
};
use grammers_tl_types as tl;
use tracing::{debug, info, trace, warn};

use crate::types::{ChannelState, UpdatesState};

fn next_updates_deadline(now: Now) -> Now {
    now + defs::NO_UPDATES_TIMEOUT
}

/// Where an update goes when a batch is sorted: by where it applies in its box's sequence.
fn update_sort_key(info: Option<&PtsInfo>) -> i32 {
    info.map_or(NO_PTS, PtsInfo::start)
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
            account_postponed: Vec::new(),
            postponed_seq: Vec::new(),
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
            account_postponed: Vec::new(),
            postponed_seq: Vec::new(),
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

    fn set_date_seq(&mut self, date: i32, seq: i32) {
        if self.date != date || self.seq != seq {
            self.date = date;
            self.seq = seq;
            self.dirty.common = true;
        }
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

    /// Return true if the message box is empty and has no state yet.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
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
    pub fn check_deadlines(&mut self, now: Now) -> Now {
        if !self.getting_diff_for.is_empty() {
            return now;
        }
        if now < self.next_deadline {
            return self.next_deadline;
        }

        // To avoid hogging up all CPU by sleeping for no duration,
        // if no deadline is armed, the next one is a full timeout away.
        let mut next_deadline = next_updates_deadline(now);
        let mut account_due = now >= self.account_deadline;
        if account_due {
            debug!("no updates for the account in a while; getting its difference");
            // Restarted here, not only when the difference ends: if it can't begin (no common
            // state), the deadline mustn't stay in the past.
            self.account_deadline = next_deadline;
        }
        next_deadline = next_deadline.min(self.account_deadline);
        for entry in &mut self.entries {
            let Some(deadline) = entry.deadline() else {
                continue;
            };
            if now < deadline {
                next_deadline = next_deadline.min(deadline);
                continue;
            }
            debug!(?entry, "deadline for forcibly fetching updates met");
            match entry.key {
                Key::Common | Key::Secondary => account_due = true,
                // The moral equivalent of `try_begin_get_diff`.
                Key::Channel(_) => {
                    entry.begin_difference();
                    self.getting_diff_for.push(entry.key);
                }
            }
        }
        if account_due {
            self.begin_account_difference();
        }
        self.next_deadline = next_deadline;

        if self.getting_diff_for.is_empty() {
            next_deadline
        } else {
            now
        }
    }

    /// Sets the update state.
    ///
    /// Should be called right after login if [`MessageBoxes::new`] was used, otherwise undesirable
    /// updates will be fetched.
    ///
    /// Should only be called while [`MessageBoxes::is_empty`].
    pub fn set_state(&mut self, state: tl::types::updates::State, now: Now) {
        trace!(?state, "setting state");
        debug_assert!(self.is_empty());
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
    ///
    /// Clears any previous gaps.
    fn try_begin_get_diff(&mut self, key: Key) {
        if let Key::Common | Key::Secondary = key {
            self.begin_account_difference();
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

    /// Begin getting the account's difference.
    /// Fails if the common box has no state to get it from.
    ///
    /// `updates.getDifference` returns the updates of the common and the secondary box at once,
    /// so both boxes always get difference together. Otherwise the updates of the box not
    /// getting difference would be checked against the state the difference had just moved to
    /// its end, and dropped as already handled.
    ///
    /// Clears any previous gaps of both.
    fn begin_account_difference(&mut self) {
        if self.entry(Key::Common).is_none() {
            info!("tried to begin getting difference but do not have the common state");
            return;
        }
        for key in [Key::Common, Key::Secondary] {
            if !self.getting_diff_for.contains(&key) {
                self.update_entry(key, LiveEntry::begin_difference);
                self.getting_diff_for.push(key);
            }
        }
    }

    /// Finish getting the account's difference, restart its idle deadline, and add to `result`
    /// the updates postponed for the account meanwhile that follow on from the state it left.
    fn end_account_difference(&mut self, now: Now, result: &mut Vec<(tl::enums::Update, State)>) {
        for key in [Key::Common, Key::Secondary] {
            self.try_end_get_diff(key);
        }
        self.heard_from_account(now);
        let postponed = mem::take(&mut self.account_postponed);
        self.replay_postponed(postponed, false, now, result);
        // Last: a missing container begins the account's difference again.
        self.replay_postponed_seq();
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

/// Where the updates [`MessageBoxes::apply_updates`] processes come from.
#[derive(Clone, Copy, Debug)]
enum Source {
    /// The socket, or a request's result.
    Socket,
    /// The `other_updates` of the account's difference.
    AccountDifference,
    /// The `other_updates` of a channel's difference.
    ChannelDifference(i64),
}

impl Source {
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
    /// Updates for a box that is getting its difference are postponed, as the
    /// [updates' documentation] asks:
    ///
    /// > Implementations \[have\] to postpone updates received via the socket while
    /// > filling gaps in the event and `Update` sequences, as well as avoid filling
    /// > gaps in the same sequence.
    ///
    /// The call that ends that difference ([`MessageBoxes::apply_difference`],
    /// [`MessageBoxes::apply_channel_difference`], [`MessageBoxes::end_channel_difference`])
    /// returns them after the difference's own updates, checked against the state it left the way
    /// updates arriving then are: dropped if the difference returned them, held if updates before
    /// them are still missing (after a temporary failure, dropped too, for the retry to return).
    /// Their users and chats are returned now, with this call's. In the same way, while the
    /// account gets its difference, a container's `seq` is checked once the difference ends,
    /// against the `seq` it ended at.
    ///
    /// [updates' documentation]: https://core.telegram.org/api/updates
    pub fn process_updates(
        &mut self,
        updates: UpdatesLike,
        now: Now,
    ) -> Result<UpdateAndPeers, Gap> {
        trace!(?updates, "processing updates");

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

        // Both account boxes always get their difference together (`begin_account_difference`).
        let account_in_diff = self.getting_diff_for.contains(&Key::Common);

        // > For all the other [not `updates` or `updatesCombined`] `Updates` type constructors
        // > there is no need to check `seq` or change a local state.
        if seq_start != NO_SEQ {
            match next_seq_cmp(self.seq, seq_start) {
                // Ignore
                Ordering::Greater => {
                    debug!(
                        "skipping updates that were already handled at seq = {}",
                        self.seq
                    );
                    return Ok((Vec::new(), users, chats));
                }
                // The difference replaces the local `seq`: a newer container is checked against
                // the difference's once it ends.
                _ if account_in_diff => self.postponed_seq.push((seq_start, seq)),
                // Apply
                Ordering::Equal => {}
                Ordering::Less => {
                    debug!(
                        "gap detected (local seq {}, remote seq {})",
                        self.seq, seq_start
                    );
                    self.try_begin_get_diff(Key::Common);
                    return Err(Gap);
                }
            }
        }

        let new_date = if date == NO_DATE { self.date } else { date };
        let new_seq = if seq == NO_SEQ { self.seq } else { seq };
        let (result, applied) =
            self.apply_updates(updates, (new_date, new_seq), now, Source::Socket);

        // While the account gets its difference, the difference sets them.
        if applied && !account_in_diff {
            // > If the updates were applied, local *Updates* state must be updated
            // > with `seq` (unless it's 0) and `date` from the constructor.
            self.set_date_seq(new_date, new_seq);
        }

        Ok((result, users, chats))
    }

    /// Process `updates`, a container's or a difference's `other_updates`, and return those to
    /// hand out, with the container's `date` and `seq`. Also return whether the container counts
    /// as applied for its `date` and `seq`: it handed out or postponed an update, and no box it
    /// touched waits on a gap.
    fn apply_updates(
        &mut self,
        updates: Vec<tl::enums::Update>,
        (date, seq): (i32, i32),
        now: Now,
        source: Source,
    ) -> (Vec<(tl::enums::Update, State)>, bool) {
        // Each update with its place in its box's sequence, read once. One whose `pts` breaks the
        // protocol is dropped, as TDLib drops it; a later update of its box then finds the gap.
        let mut updates: Vec<(Option<PtsInfo>, tl::enums::Update)> = updates
            .into_iter()
            .filter_map(|update| match PtsInfo::from_update(&update) {
                Ok(info) => Some((info, update)),
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
                    None
                }
            })
            .collect();

        // Telegram can send updates out of order (e.g. `ReadChannelInbox` first
        // and then `NewChannelMessage`, both with the same `pts`, but the `count`
        // is `0` and `1` respectively), so we sort them first.
        updates.sort_by_key(|(info, _)| update_sort_key(info.as_ref()));

        let mut result = Vec::with_capacity(updates.len());
        let mut postponed = false;
        let mut have_unresolved_gaps = false;
        for (info, update) in updates {
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

            let Some(info) = info else {
                // No pts means that the update can be applied in any order.
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

        let applied = (postponed || !result.is_empty()) && !have_unresolved_gaps;
        (result, applied)
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
        let mut gap_deadline = None;
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

                    entry
                        .possible_gap
                        .get_or_insert_with(|| PossibleGap {
                            deadline: now + POSSIBLE_GAP_TIMEOUT,
                            updates: Vec::new(),
                        })
                        .updates
                        .push((info, update));
                }
            };

            if let Some(mut gap) = entry.possible_gap.take() {
                gap.updates
                    .sort_by_key(|(gap_info, _)| Reverse(gap_info.start()));
                while let Some((gap_info, gap_update)) = gap.updates.pop() {
                    match entry.pts.cmp(&gap_info.start()) {
                        Ordering::Equal => {
                            entry.pts = gap_info.pts;
                            // possible gap can only be resolve if new update closed
                            // the gap - this has already updated its deadline if so.
                            result.push((gap_update, mk_state(gap_info.into())));
                        }
                        Ordering::Greater => {}
                        Ordering::Less => {
                            gap.updates.push((gap_info, gap_update));
                            break;
                        }
                    };
                }
                if !gap.updates.is_empty() {
                    gap_deadline = Some(gap.deadline);
                    entry.possible_gap = Some(gap);
                }
            }
        });

        // An update applied to either of the account's boxes restarts its idle deadline.
        if applied && !matches!(key, Key::Channel(_)) {
            self.heard_from_account(now);
        }
        match gap_deadline {
            Some(deadline) => {
                self.arm(deadline);
                true
            }
            None => false,
        }
    }

    /// Check the updates postponed while their box got its difference against the state the
    /// difference left, in `pts` order and the way updates arriving now are checked, and add those
    /// that follow on to `result`. One the difference returned is dropped. One that doesn't follow
    /// on yet waits in its box's possible gap, unless `drop_after_gap`: the difference ended
    /// without an answer to apply, so it and every later one are dropped, for the box's next
    /// difference to return (a retry after a temporary failure, or its next gap), rather than held
    /// in a gap that would get the difference again at once.
    fn replay_postponed(
        &mut self,
        mut postponed: Vec<(PtsInfo, tl::enums::Update)>,
        drop_after_gap: bool,
        now: Now,
        result: &mut Vec<(tl::enums::Update, State)>,
    ) {
        postponed.sort_by_key(|(info, _)| info.start());
        for (info, update) in postponed {
            if drop_after_gap
                && self
                    .entry(info.key)
                    .is_some_and(|entry| entry.pts < info.start())
            {
                debug!(
                    key = ?info.key,
                    remote = ?info.pts,
                    "dropping postponed updates after a gap; the next difference returns them",
                );
                break;
            }
            self.apply_in_order(update, info, (self.date, self.seq), now, result);
        }
    }

    /// Check the `seq` of the containers that arrived while the account got its difference
    /// against the `seq` the difference ended at, in order. A container the difference covered
    /// is skipped. One that follows on advances `seq`, and `date` stays the difference's, which
    /// may be the newer one (TDLib does the same). One after a container that is still missing
    /// begins the account's difference again, which covers it and every later one.
    fn replay_postponed_seq(&mut self) {
        let mut postponed = mem::take(&mut self.postponed_seq);
        postponed.sort_unstable();
        for (seq_start, seq) in postponed {
            match next_seq_cmp(self.seq, seq_start) {
                Ordering::Equal => self.set_date_seq(self.date, seq),
                Ordering::Greater => {}
                Ordering::Less => {
                    debug!(
                        "gap detected after the difference (local seq {}, remote seq {})",
                        self.seq, seq_start
                    );
                    self.begin_account_difference();
                    break;
                }
            }
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
        let finish: bool;
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
            self.end_account_difference(now, &mut result.0);
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
        let (mut result_updates, _) = self.apply_updates(
            other_updates,
            (state.date, state.seq),
            now,
            Source::AccountDifference,
        );

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
        let (mut result_updates, _) = self.apply_updates(
            other_updates,
            (self.date, self.seq),
            now,
            Source::ChannelDifference(channel_id),
        );
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
                let mut retry_at = None;
                self.update_entry(key, |entry| {
                    entry.retry_delay =
                        (entry.retry_delay * 2).clamp(CHANNEL_RETRY_MIN, CHANNEL_RETRY_MAX);
                    entry.retry_at = Some(now + entry.retry_delay);
                    retry_at = entry.retry_at;
                });
                if let Some(retry_at) = retry_at {
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
    /// 1 second after the first failure in a row, doubling with each one up to 64 seconds,
    /// until the server answers ([`MessageBoxes::check_deadlines`]).
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
