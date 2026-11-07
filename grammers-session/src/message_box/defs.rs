// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::time::Duration;

use grammers_tl_types as tl;

use crate::types::ChannelState;

/// Monotonic time chosen by the caller: how long it has been since an origin of its choice,
/// such as the client's start.
///
/// [`MessageBoxes`] never reads a clock. Every method that arms or checks a deadline takes the
/// current time instead, so tests can drive the 0.5 s gap wait and the 15 minute idle deadline
/// with a paused or simulated clock. It must never decrease between calls on the same
/// [`MessageBoxes`].
pub type Now = Duration;

/// Telegram sends `seq` equal to `0` when "it doesn't matter", so we use that value too.
pub(super) const NO_SEQ: i32 = 0;

/// It has been observed that Telegram may send updates with `qts` equal to `0` (for
/// example with `ChannelParticipant`), interleaved with non-zero `qts` values. This
/// presumably means that the ordering should be "ignored" in that case.
///
/// One can speculate this is done because the field is not optional in the TL definition.
///
/// Not ignoring the `pts` information in those updates can lead to failures resolving gaps.
pub(super) const NO_PTS: i32 = 0;

/// Non-update types like `messages.affectedMessages` can contain `pts` that should still be
/// processed. Because there's no `date`, a value of `0` is used as the sentinel value for
/// the `date` when constructing the dummy `Updates` (in order to handle them uniformly).
pub(super) const NO_DATE: i32 = 0;

// > It may be useful to wait up to 0.5 seconds
pub(super) const POSSIBLE_GAP_TIMEOUT: Duration = Duration::from_millis(500);

/// After how long without updates the client will "timeout".
///
/// When this timeout occurs,
/// the client will attempt to fetch updates by itself,
/// ignoring all the updates that arrive in the meantime.
/// After all updates are fetched when this happens,
/// the client will resume normal operation,
/// and the timeout will reset.
///
/// Documentation recommends 15 minutes without updates
/// (<https://core.telegram.org/api/updates>).
///
/// Only the account has this deadline ([`MessageBoxes::account_deadline`]). A quiet channel isn't
/// a sign of missed updates: the server asks for its difference with `updateChannelTooLong` when
/// it has too many to send, and the documentation says not to get a channel's difference again
/// after a final one "unless the user has opened the channel/supergroup", which is the caller's to
/// decide ([`MessageBoxes::begin_channel_difference`]).
pub(super) const NO_UPDATES_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// How long a channel waits to get its difference again after it failed for a temporary reason
/// ([`super::PrematureEndReason::TemporaryServerIssues`]). The wait doubles with each failure in a
/// row, up to [`CHANNEL_RETRY_MAX`], and starts over once the server answers. These are
/// tdesktop's (`failDifferenceStartTimerFor`); TDLib's are close (1 s doubling to about 60 s,
/// with jitter, `retry_get_channel_difference_later`). There is no jitter here: the message boxes
/// stay deterministic, and one client's channels don't fail in step with other clients'.
pub(super) const CHANNEL_RETRY_MIN: Duration = Duration::from_secs(1);
pub(super) const CHANNEL_RETRY_MAX: Duration = Duration::from_secs(64);

/// A sortable [`MessageBox`] entry key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Key {
    Common,
    Secondary,
    Channel(i64),
}

/// A live [`MessageBox`] entry.
#[derive(Debug)]
pub(super) struct LiveEntry {
    /// The variant of the [`MessageBox`] that this entry represents.
    pub(super) key: Key,

    /// The local persistent timestamp value that this [`MessageBox`] has.
    pub(super) pts: i32,

    /// If the entry has a gap and may soon trigger the need to get difference.
    pub(super) possible_gap: Option<PossibleGap>,

    /// When a channel whose difference failed for a temporary reason gets it again. It is cleared
    /// once the channel's difference begins, by this deadline or any other way, so it fires at
    /// most once per failure. Never set for the account's boxes.
    pub(super) retry_at: Option<Now>,

    /// The channel's last wait before getting its difference again ([`CHANNEL_RETRY_MIN`]):
    /// zero until its difference fails, and again once the server answers.
    pub(super) retry_delay: Duration,

    /// Updates for the channel that arrived while it got its difference, held until the difference
    /// ends ([`MessageBoxes::process_updates`]), each with its place in the channel's sequence.
    /// Empty while it gets none. The account's boxes hold theirs in
    /// [`MessageBoxes::account_postponed`].
    pub(super) postponed: Vec<(PtsInfo, tl::enums::Update)>,
}

/// Contains all live message boxes and is able to process incoming updates for each of them.
///
/// See <https://core.telegram.org/api/updates#message-related-event-sequences>.
#[derive(Debug)]
pub struct MessageBoxes {
    /// Live entries, sorted by key.
    pub(super) entries: Vec<LiveEntry>,

    /// Common [`State`] fields.
    pub(super) date: i32,
    pub(super) seq: i32,

    /// Optimization field to quickly query all entries that are currently being fetched.
    pub(super) getting_diff_for: Vec<Key>,

    /// When the account gets its difference if neither of its boxes hears from the server before
    /// then ([`NO_UPDATES_TIMEOUT`]). Both boxes always get their difference together, so they
    /// share one deadline: a box that is quiet while the other one isn't, like the secondary box
    /// of an account without secret chats, doesn't make the account poll. `Duration::MAX` (never)
    /// while the account has no state.
    pub(super) account_deadline: Now,

    /// Updates for the account's boxes that arrived while the account got its difference, held
    /// until the difference ends ([`MessageBoxes::process_updates`]). Both boxes always get it
    /// together, so they share this, also while the secondary box has no entry yet.
    pub(super) account_postponed: Vec<(PtsInfo, tl::enums::Update)>,

    /// The `seq_start` and `seq` of each sequenced container that arrived while the account got
    /// its difference. The difference replaces the local `seq`, so they are checked against the
    /// difference's once it ends.
    pub(super) postponed_seq: Vec<(i32, i32)>,

    /// Optimization field: no deadline is earlier than this one. It can be earlier than the
    /// closest deadline, because a deadline that moves later doesn't move it;
    /// [`MessageBoxes::check_deadlines`] recomputes it once it passes.
    pub(super) next_deadline: Now,

    /// What changed since the last [`MessageBoxes::take_delta`].
    pub(super) dirty: DirtySet,
}

/// The parts of the state that changed since the last [`MessageBoxes::take_delta`].
#[derive(Debug, Default)]
pub(super) struct DirtySet {
    /// The common box's `pts`, the `date` or the `seq` changed.
    pub(super) common: bool,
    /// The secondary box's `qts` changed.
    pub(super) qts: bool,
    /// Channels whose entry was inserted, changed or removed. Sorted and without duplicates, so
    /// it never names more channels than were ever seen, even if nobody takes the delta.
    pub(super) channels: Vec<i64>,
}

impl DirtySet {
    pub(super) fn mark(&mut self, key: Key) {
        match key {
            Key::Common => self.common = true,
            Key::Secondary => self.qts = true,
            Key::Channel(id) => {
                if let Err(i) = self.channels.binary_search(&id) {
                    self.channels.insert(i, id);
                }
            }
        }
    }
}

/// Represents the information needed to correctly handle a specific `tl::enums::Update`.
///
/// [`PtsInfo::from_update`] only returns one whose `pts` is above its `count`, and whose `count`
/// isn't negative: the box's `pts` before the update ([`PtsInfo::start`]) is then positive.
#[derive(Clone, Copy, Debug)]
pub(super) struct PtsInfo {
    pub(super) key: Key,
    pub(super) pts: i32,
    pub(super) count: i32,
}

/// Why an update's `pts` breaks the protocol, TDLib's "wrong PTS update". The update is dropped,
/// as TDLib drops it (`check_pts_update`, `add_pending_pts_update`); the box's state doesn't move.
/// It holds what was wrong, never the update, so it can be logged: the update carries the user's
/// messages and contacts.
#[derive(Debug)]
pub(super) enum WrongPts {
    /// An update of the common box about a channel, whose own `pts` sequence has it.
    CommonInChannel,
    /// An update whose `count` is negative or not below its `pts`.
    Count(PtsInfo),
}

// > ### Recovering gaps
// > […] Manually obtaining updates is also required in the following situations:
// > • Loss of sync: a gap was found in `seq` / `pts` / `qts` (as described above).
// >   It may be useful to wait up to 0.5 seconds in this situation and abort the sync in case a new update
// >   arrives, that fills the gap.
//
// This is really easy to trigger by spamming messages in a channel (with as little as 3 members works), because
// the updates produced by the RPC request take a while to arrive (whereas the read update comes faster alone).
#[derive(Debug)]
pub(super) struct PossibleGap {
    pub(super) deadline: Now,
    /// Pending updates (those with a larger PTS, producing the gap which may later be filled),
    /// each with its place in the box's sequence.
    pub(super) updates: Vec<(PtsInfo, tl::enums::Update)>,
}

/// Error when a gap has been detected during update processing.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Gap;

/// Alias for the commonly-referenced three-tuple of update and related peers.
pub type UpdateAndPeers = (
    Vec<(tl::enums::Update, State)>,
    Vec<tl::enums::User>,
    Vec<tl::enums::Chat>,
);

/// Anything that should be treated like an update.
///
/// Telegram unfortunately also includes update state in types
/// that are not part of the usual [`tl::enums::Updates`].
#[derive(Debug)]
pub enum UpdatesLike {
    /// The usual variant, received passively from Telegram.
    Updates(tl::enums::Updates),
    /// Special-case for short-sent messages,
    /// where the request is also needed to construct a complete update.
    ShortSentMessage {
        /// The request that triggered the short update.
        request: tl::functions::messages::SendMessage,
        /// The incomplete update caused by the request.
        update: tl::types::UpdateShortSentMessage,
    },
    /// Special-case for requests that affect some messages.
    AffectedMessages(tl::types::messages::AffectedMessages),
    /// Special-case for channel-specific requests that affect messages (e.g.
    /// `channels.deleteMessages`). The `channel_id` is needed so the `pts` can
    /// be applied to the correct `Key::Channel` instead of `Key::Common`.
    AffectedChannelMessages {
        affected: tl::types::messages::AffectedMessages,
        channel_id: i64,
        message_ids: Vec<i32>,
    },
    /// Special-case for requests that lead to users being invited.
    InvitedUsers(tl::types::messages::InvitedUsers),
    /// Special-case for join requests that contain updates inside a `ChatInviteJoinResultOk`.
    ChatInviteJoinResult(tl::types::messages::ChatInviteJoinResultOk),
    /// Indicates that the connection was closed and had to be recreated.
    /// This may mean that an update gap now exists and should be resolved.
    ConnectionClosed,
    /// Indicates that passively-received updates were malformed.
    /// Either the constructor identifier was unexpected (possibly a stale channel
    /// update) or the updates were cut short during deserialization (very unlikely).
    /// This should be treated as a gap.
    MalformedUpdates,
    /// Indicates that received updates were dropped because the bounded channel
    /// carrying them was full. Sent once there is room again, before any later
    /// update from the same connection. This should be treated as a gap.
    Lagged,
    /// Indicates that the server started a new session on the connection that receives
    /// updates (`new_session_created`): updates it sent to an earlier session may be lost.
    /// This should be treated as a gap.
    NewSession,
}

// Public interface around the more tightly-packed internal state.

/// Update state, up to and including the update it is a part of.
///
/// When using `catch_up` with a client, all updates with a
/// state containing a [`MessageBox`] higher than this one will be fetched.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct State {
    /// Last date state assigned by Telegram, which changes somewhat arbitrarily.
    pub date: i32,
    /// Last sequence state assigned by Telegram, which changes somewhat arbitrarily.
    pub seq: i32,
    /// The particular message box change if the update pertains to a message-related event sequence.
    pub message_box: Option<MessageBox>,
}

/// The message box and pts value that uniquely identifies the message-related update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessageBox {
    /// Account-wide persistent timestamp.
    ///
    /// This includes private conversations (one-to-one) and small group chats.
    Common { pts: i32 },
    /// Account-wide secondary persistent timestamp.
    ///
    /// This includes only certain bot updates and secret one-to-one chats.
    Secondary { qts: i32 },
    /// Channel-specific persistent timestamp.
    ///
    /// This includes "megagroup", "broadcast" and "supergroup" channels.
    Channel { channel_id: i64, pts: i32 },
}

/// What the message boxes advanced since the previous [`MessageBoxes::take_delta`]: the current
/// value of each part of the state that changed, and nothing else.
///
/// Taken right after a call, it covers exactly the updates that call returned. Updates held back
/// don't move the state: those waiting while a gap may still fill, and those postponed while their
/// box gets its difference ([`MessageBoxes::process_updates`]). No update the boxes drop moves it
/// either. So storing the delta together with the returned updates never stores state ahead of
/// the updates it stands for, except where the server itself skips events. The caller reads those
/// from the raw response before passing it in:
///
/// - `updates.differenceTooLong` moves the common `pts` to the server's and returns no update.
/// - `updates.channelDifferenceTooLong` moves the channel's `pts` to its `dialog`'s, and its
///   `dialog` and `messages` are dropped.
/// - `updateChannelTooLong` for a channel without an entry starts one at the update's `pts`.
///
/// A call can also advance the state without returning an update or skipping an event:
/// `updates.differenceEmpty` moves `date` and `seq`, and `updates.channelDifferenceEmpty` moves a
/// channel's `pts`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StateDelta {
    /// The common box's state, if its `pts`, the `date` or the `seq` changed.
    pub common: Option<CommonState>,
    /// The secondary box's `qts`, if it changed.
    pub qts: Option<i32>,
    /// The channels whose state was set or changed, sorted by id.
    pub channels: Vec<ChannelState>,
    /// The channels whose state was dropped, sorted by id.
    pub removed_channels: Vec<i64>,
}

/// The common box's state, with the `date` and `seq` that go with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommonState {
    /// Common box `pts`.
    pub pts: i32,
    /// Last `date` Telegram assigned.
    pub date: i32,
    /// Last `seq` Telegram assigned.
    pub seq: i32,
}

/// The differences waiting to be fetched.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiffPending {
    /// `updates.getDifference` is due ([`MessageBoxes::get_difference`]).
    pub account: bool,
    /// How many channels wait for `updates.getChannelDifference`
    /// ([`MessageBoxes::get_channel_difference`]).
    pub channels: usize,
}
