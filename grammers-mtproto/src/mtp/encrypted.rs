// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::cmp::Reverse;
use std::mem;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use grammers_crypto::{AuthKey, decrypt_data_v2, encrypt_data_v2, get_padding_data_v2};
use grammers_tl_types::{self as tl, Deserializable, Identifiable, Serializable};
use tracing::{info, warn};

use super::{
    Deserialization, DeserializationFailure, DeserializeError, Mtp, RpcResult, RpcResultError,
};
use crate::utils::StackBuffer;
use crate::{DequeBuffer, MsgId, manual_tl};

#[cfg(test)]
mod clock_tests;
#[cfg(test)]
mod server_data_tests;

/// How many future salts to fetch or have stored at a given time.
///
/// Not an `usize` because the API expects a signed `i32`. Should be more than two because the
/// code expects to fetch more salts when going from two to one.
const NUM_FUTURE_SALTS: i32 = 64;

/// When switching to a different salt, how many seconds must have passed since its `valid_since`.
///
/// Used to prevent small fluctuations in the system clock.
const SALT_USE_DELAY: i32 = 60;

/// How many message identifiers one `msgs_ack` carries at most: "Max 8192 IDs are allowed per
/// constructor" ([Acknowledgment of Receipt]), as TDLib (`SessionConnection::flush_packet`,
/// `cut_tail(to_ack_message_ids_, 8192, "ack")`). The rest wait for the next message.
///
/// [Acknowledgment of Receipt]: https://core.telegram.org/mtproto/service_messages_about_messages#acknowledgment-of-receipt
const MAX_ACKS_PER_MESSAGE: usize = 8192;

// The constructor, the vector's constructor and length, then the identifiers: such an
// acknowledgment always fits in a message on its own, so it can't hold up the packets after it.
const _: () = assert!(4 + 4 + 4 + 8 * MAX_ACKS_PER_MESSAGE <= manual_tl::Message::MAX_BODY_SIZE);

static UPDATE_IDS: [u32; 9] = [
    tl::types::UpdateShortMessage::CONSTRUCTOR_ID,
    tl::types::UpdateShortChatMessage::CONSTRUCTOR_ID,
    tl::types::UpdateShort::CONSTRUCTOR_ID,
    tl::types::UpdatesCombined::CONSTRUCTOR_ID,
    tl::types::Updates::CONSTRUCTOR_ID,
    tl::types::UpdateShortSentMessage::CONSTRUCTOR_ID,
    tl::types::messages::AffectedMessages::CONSTRUCTOR_ID,
    tl::types::messages::InvitedUsers::CONSTRUCTOR_ID,
    tl::types::messages::ChatInviteJoinResultOk::CONSTRUCTOR_ID,
];

/// A builder to configure [`Mtp`] instances.
///
/// Use the [`Encrypted::build`] method to create builder instances.
pub struct Builder {
    time_offset: i32,
    first_salt: i64,
    compression_threshold: Option<usize>,
}

/// An implementation of the [Mobile Transport Protocol] for ciphertext
/// (encrypted) messages.
///
/// [Mobile Transport Protocol]: https://core.telegram.org/mtproto
pub struct Encrypted {
    /// The Authorization Key to use to encrypt payload.
    auth_key: AuthKey,

    /// The time offset from the server's time, in seconds.
    time_offset: i32,

    /// Salts that may be used when encrypting payload, sorted by valid date descending (so the last one is the one
    /// that can be used now).
    salts: Vec<tl::types::FutureSalt>,

    /// The `now` received from future salts along with the instant when it occured.
    ///
    /// Used to accurately determine when salts become valid.
    start_salt_time: Option<(i32, Instant)>,

    /// Internal request for salts which should not be propagated.
    salt_request_msg_id: Option<MsgId>,

    /// The secure, random identifier for this instance.
    session_id: i64,

    /// The current message sequence number.
    sequence: i32,

    /// The ID of the last message.
    last_msg_id: i64,

    /// Whether the server took a message of this session: it sent one that isn't a notice
    /// refusing one of ours. The first it takes brings `new_session_created`, ahead of any notice
    /// about a later message.
    accepted: bool,

    /// A message of this packet refused one of ours as `msg_id` too high: once the packet is
    /// processed, the next identifiers follow the clock corrected since
    /// ([`Encrypted::follow_corrected_clock`]).
    msg_id_too_high: bool,

    /// Identifiers that need to be acknowledged to the server.
    ///
    /// A [Content-related Message] is "a message requiring an explicit
    /// acknowledgment. These include all the user and many service messages,
    /// virtually all with the exception of containers and acknowledgments."
    ///
    /// [Content-related Message]: https://core.telegram.org/mtproto/description#content-related-message
    pending_ack: Vec<i64>,

    /// If present, the threshold in bytes at which a message will be
    /// considered large enough to attempt compressing it. Otherwise,
    /// outgoing messages will never be compressed.
    compression_threshold: Option<usize>,

    /// Temporary deserialization results.
    deserialization: Vec<Deserialization>,

    /// How many messages are there in the buffer.
    msg_count: usize,

    /// How many more bytes the `gzip_packed` objects of the packet being processed may inflate to
    /// ([`manual_tl::MAX_UNPACKED_PER_PACKET`]).
    unpacked_budget: usize,
}

/// What contains a message being processed, inside the packet it came in.
#[derive(Clone, Copy, Debug, Default)]
struct Nesting {
    /// It is in a `msg_container`, which "may not carry other simple containers".
    in_container: bool,
    /// How many `gzip_packed` objects it was packed in ([`manual_tl::MAX_GZIP_NESTING`]).
    gzip_depth: u32,
}

impl Builder {
    /// Configures the time offset to Telegram servers.
    pub fn time_offset(mut self, offset: i32) -> Self {
        self.time_offset = offset;
        self
    }

    pub fn first_salt(mut self, first_salt: i64) -> Self {
        self.first_salt = first_salt;
        self
    }

    /// Configures the compression threshold for outgoing messages.
    pub fn compression_threshold(mut self, threshold: Option<usize>) -> Self {
        self.compression_threshold = threshold;
        self
    }

    /// Finishes the builder and returns the `MTProto` instance with all
    /// the configuration changes applied.
    pub fn finish(self, auth_key: [u8; 256]) -> Encrypted {
        Encrypted {
            auth_key: AuthKey::from_bytes(auth_key),
            time_offset: self.time_offset,
            salts: vec![tl::types::FutureSalt {
                valid_since: 0,
                valid_until: i32::MAX,
                salt: self.first_salt,
            }],
            start_salt_time: None,
            salt_request_msg_id: None,
            session_id: random_session_id(),
            sequence: 0,
            last_msg_id: 0,
            accepted: false,
            msg_id_too_high: false,
            pending_ack: vec![],
            compression_threshold: self.compression_threshold,
            deserialization: Vec::new(),
            msg_count: 0,
            unpacked_budget: manual_tl::MAX_UNPACKED_PER_PACKET,
        }
    }
}

/// A new session's identifier: secure and random.
#[expect(
    clippy::expect_used,
    reason = "the operating system's random number generator, which nothing here works without; not server data"
)]
fn random_session_id() -> i64 {
    let mut buffer = [0u8; 8];
    getrandom::fill(&mut buffer).expect("failed to generate a secure session_id");
    i64::from_le_bytes(buffer)
}

impl Encrypted {
    /// Start building a new encrypted MTP.
    pub fn build() -> Builder {
        Builder {
            time_offset: 0,
            compression_threshold: crate::DEFAULT_COMPRESSION_THRESHOLD,
            first_salt: 0,
        }
    }

    /// The Authorization Key used for encryption and decryption.
    pub fn auth_key(&self) -> [u8; 256] {
        self.auth_key.to_bytes()
    }

    /// The local time since the epoch. A clock set before the epoch reads as the epoch: the time
    /// offset, corrected from the server's message IDs, makes up for it like for any wrong clock.
    fn now_since_epoch() -> std::time::Duration {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
    }

    /// Correct our time offset based on a known valid message ID.
    ///
    /// The message ID's time comes from the server, and the seconds of a message ID are taken
    /// modulo 2^32 anyway ([`Self::get_new_msg_id`]), so the arithmetic wraps.
    fn correct_time_offset(&mut self, msg_id: i64) {
        let now = Self::now_since_epoch().as_secs() as i32;

        let correct = (msg_id >> 32) as i32;
        self.time_offset = correct.wrapping_sub(now);
    }

    /// Generates a new unique message ID based on the current
    /// time (in ms) since epoch, applying a known time offset.
    fn get_new_msg_id(&mut self) -> i64 {
        let now = Self::now_since_epoch();

        let seconds = (now.as_secs() as i32).wrapping_add(self.time_offset) as u64;
        let nanoseconds = now.subsec_nanos() as u64;
        let mut new_msg_id = ((seconds << 32) | (nanoseconds << 2)) as i64;

        if self.last_msg_id >= new_msg_id {
            new_msg_id = self.last_msg_id.wrapping_add(4);
        }

        self.last_msg_id = new_msg_id;
        new_msg_id
    }

    /// After a `msg_id` too high, lets the next message identifiers follow the corrected clock,
    /// behind the ones sent. A session's identifiers must rise, but those of messages the server
    /// refused leave no trace in it: while it has taken none of this session's, they go on from the
    /// corrected clock in this session, as tdesktop's do. Once it has, they go on in a new session,
    /// whose requests start again ([`Deserialization::SessionReset`]), as TDLib's do and as
    /// [Time Synchronization] says: "the client will have to generate a new session to assure the
    /// monotonicity of message identifiers".
    ///
    /// [Time Synchronization]: https://core.telegram.org/mtproto#time-synchronization
    fn follow_corrected_clock(&mut self) {
        self.last_msg_id = 0;
        if !self.accepted {
            return;
        }
        info!("msg_id too high in a session the server took messages of; starting a new session");
        self.session_id = random_session_id();
        self.sequence = 0;
        self.accepted = false;
        // The old session's messages to acknowledge, and its request for salts, which the server
        // answers there.
        self.pending_ack.clear();
        self.salt_request_msg_id = None;
        self.deserialization.push(Deserialization::SessionReset);
    }

    /// Generates the next sequence number depending on whether
    /// it should be for a content-related query or not.
    fn get_seq_no(&mut self, content_related: bool) -> i32 {
        if content_related {
            self.sequence = self.sequence.wrapping_add(2);
            self.sequence.wrapping_sub(1)
        } else {
            self.sequence
        }
    }

    fn try_serialize_msg(
        &mut self,
        buffer: &mut DequeBuffer<u8>,
        body: &[u8],
        content_related: bool,
    ) -> Option<MsgId> {
        // Make sure it's within bounds both in message count and buffer size.
        if self.msg_count == manual_tl::MessageContainer::MAXIMUM_LENGTH {
            return None;
        }
        let container_overhead = if self.msg_count == 0 {
            0
        } else {
            manual_tl::MessageContainer::SIZE_OVERHEAD
        };
        if buffer.len() + body.len() + manual_tl::Message::SIZE_OVERHEAD + container_overhead
            > manual_tl::MAX_ENCRYPTED_DATA_SIZE
        {
            return None;
        }

        // Within bounds; add it.
        let msg_id = self.get_new_msg_id();

        let sink = &mut |bytes: &[u8]| buffer.extend_back(bytes);
        msg_id.serialize(sink);
        self.get_seq_no(content_related).serialize(sink);
        (body.len() as i32).serialize(sink);
        buffer.extend_back(body);

        self.msg_count += 1;
        Some(MsgId(msg_id))
    }

    fn get_current_salt(&self) -> i64 {
        self.salts.last().map(|s| s.salt).unwrap_or(0)
    }

    fn try_request_salts(&mut self, buffer: &mut DequeBuffer<u8>) {
        if self.salts.len() == 1
            && self.salt_request_msg_id.is_none()
            && self.get_current_salt() != 0
        {
            // If salts are requested in a container leading to bad_msg,
            // the bad_msg_id will refer to the container, not the salts request.
            //
            // We don't keep track of containers and content-related messages they contain for simplicity.
            // This would break, because we couldn't identify the response.
            //
            // So salts are only requested once we have a valid salt to reduce the chances of this happening.
            info!("only one future salt remaining; asking for more salts");
            let body = tl::functions::GetFutureSalts {
                num: NUM_FUTURE_SALTS,
            }
            .to_bytes();
            self.salt_request_msg_id = self.try_serialize_msg(buffer, &body, true);
        }
    }

    /// `finalize`, but without encryption.
    ///
    /// The buffer is *not* cleared, but is instead returned.
    fn finalize_plain(&mut self, buffer: &mut DequeBuffer<u8>) {
        if self.msg_count == 0 {
            return;
        }

        if self.msg_count != 1 {
            // Prepend a container, setting its message ID and sequence number.
            // + 8 because it has to include the constructor ID and length (4 bytes each).
            let len = (buffer.len() + 8) as i32;
            let mut header = StackBuffer::<MESSAGE_CONTAINER_HEADER_LEN>::new();

            // Manually `serialize_msg` because the container body was already written.
            let sink = &mut |bytes: &[u8]| header.extend_from_slice(bytes);
            self.get_new_msg_id().serialize(sink);
            self.get_seq_no(false).serialize(sink);

            len.serialize(sink);

            manual_tl::MessageContainer::CONSTRUCTOR_ID.serialize(sink);
            (self.msg_count as i32).serialize(sink);
            buffer.extend_front(&header.into_inner());
        }

        {
            // Prepend the message header
            let mut header = StackBuffer::<PLAIN_PACKET_HEADER_LEN>::new();
            let sink = &mut |bytes: &[u8]| header.extend_from_slice(bytes);
            self.get_current_salt().serialize(sink); // 8 bytes

            self.session_id.serialize(sink); // 8 bytes
            buffer.extend_front(&header.into_inner());
        }

        self.msg_count = 0;
    }

    /// Processes a decrypted packet from the server, as [`Mtp::deserialize`] does after decrypting.
    fn deserialize_plaintext(
        &mut self,
        plaintext: &[u8],
    ) -> Result<Vec<Deserialization>, DeserializeError> {
        let processed = self.process_plaintext(plaintext);
        // Decided once the whole packet is read, whatever order its messages came in.
        if mem::take(&mut self.msg_id_too_high) {
            self.follow_corrected_clock();
        }

        // For simplicity, and to avoid passing too much stuff around (RPC results, updates),
        // the processing result is stored in self. After processing is done, that temporary
        // state is cleaned and returned with `mem::take`, or dropped if the packet was bad.
        let deserialization = mem::take(&mut self.deserialization);
        processed.map(|()| deserialization)
    }

    /// Processes a decrypted packet from the server: its `salt` and `session_id`, then the message
    /// it carries. The padding after the message is ignored.
    fn process_plaintext(&mut self, plaintext: &[u8]) -> Result<(), DeserializeError> {
        let (_salt, rest) = manual_tl::split_i64(plaintext)?;
        let (session_id, rest) = manual_tl::split_i64(rest)?;
        // > Client is to check that the session_id field in the decrypted message indeed equals
        // > to that of an active session created by the client.
        // (https://core.telegram.org/mtproto/security_guidelines)
        //
        // A packet from another session, such as a replayed one from an earlier session on the
        // same key, is dropped unread: neither processed nor acknowledged.
        if session_id != self.session_id {
            warn!(
                session_id,
                "dropping a packet from another session than this one"
            );
            return Ok(());
        }

        let (message, _padding) = manual_tl::Message::split_from(rest)?;
        self.unpacked_budget = manual_tl::MAX_UNPACKED_PER_PACKET;
        self.process_message(message, Nesting::default())
    }

    fn process_message(
        &mut self,
        message: manual_tl::Message,
        nesting: Nesting,
    ) -> Result<(), DeserializeError> {
        if message.requires_ack() {
            self.pending_ack.push(message.msg_id);
        }

        // Handle all the possible Service Messages:
        // * https://core.telegram.org/mtproto/service_messages
        // * https://core.telegram.org/mtproto/service_messages_about_messages
        //
        let constructor_id = message.constructor_id()?;
        // Anything the server sends in this session, but a notice refusing one of its messages and
        // the wrappers of others, shows that it took one of them.
        if !matches!(
            constructor_id,
            tl::types::BadMsgNotification::CONSTRUCTOR_ID
                | tl::types::BadServerSalt::CONSTRUCTOR_ID
                | manual_tl::MessageContainer::CONSTRUCTOR_ID
                | manual_tl::GzipPacked::CONSTRUCTOR_ID
        ) {
            self.accepted = true;
        }

        // The order of the `match` here is the same as the order in which the
        // items appear in the documentation (to make it easier to review).
        // TODO verify what needs ack and what doesn't
        match constructor_id {
            // Response to an RPC query
            manual_tl::RpcResult::CONSTRUCTOR_ID => self.handle_rpc_result(message),

            // Service Messages about Messages
            // Acknowledgment of Receipt
            tl::types::MsgsAck::CONSTRUCTOR_ID => self.handle_ack(message),
            // Notice of Ignored Error Message
            tl::types::BadMsgNotification::CONSTRUCTOR_ID
            | tl::types::BadServerSalt::CONSTRUCTOR_ID => self.handle_bad_notification(message),
            // Request for Message Status Information
            tl::types::MsgsStateReq::CONSTRUCTOR_ID => self.handle_state_req(message),
            // Informational Message regarding Status of Messages
            tl::types::MsgsStateInfo::CONSTRUCTOR_ID => self.handle_state_info(message),
            // Voluntary Communication of Status of Messages
            tl::types::MsgsAllInfo::CONSTRUCTOR_ID => self.handle_msg_all(message),
            // Extended Voluntary Communication of Status of One Message
            tl::types::MsgDetailedInfo::CONSTRUCTOR_ID
            | tl::types::MsgNewDetailedInfo::CONSTRUCTOR_ID => self.handle_detailed_info(message),
            // Explicit Request to Re-Send Messages & Explicit Request to Re-Send Answers
            tl::types::MsgResendReq::CONSTRUCTOR_ID
            | tl::types::MsgResendAnsReq::CONSTRUCTOR_ID => self.handle_msg_resend(message),

            // Request for several future salts
            tl::types::FutureSalt::CONSTRUCTOR_ID => self.handle_future_salt(message),
            tl::types::FutureSalts::CONSTRUCTOR_ID => self.handle_future_salts(message),
            // Ping Messages (PING/PONG)
            tl::types::Pong::CONSTRUCTOR_ID => self.handle_pong(message),
            // Request to Destroy Session
            tl::types::DestroySessionOk::CONSTRUCTOR_ID
            | tl::types::DestroySessionNone::CONSTRUCTOR_ID => self.handle_destroy_session(message),
            // New Session Creation Notification
            tl::types::NewSessionCreated::CONSTRUCTOR_ID => {
                self.handle_new_session_created(message)
            }
            // Containers (Simple Container)
            manual_tl::MessageContainer::CONSTRUCTOR_ID => self.handle_container(message, nesting),
            // Message Copies
            manual_tl::MessageCopy::CONSTRUCTOR_ID => self.handle_msg_copy(message),
            // Packed Object
            manual_tl::GzipPacked::CONSTRUCTOR_ID => self.handle_gzip_packed(message, nesting),
            // HTTP Wait/Long Poll
            tl::types::HttpWait::CONSTRUCTOR_ID => self.handle_http_wait(message),
            _ => self.handle_update(message),
        }
    }

    /// **[Response to an RPC query]**
    ///
    /// A response to an RPC query is normally wrapped as follows:
    ///
    /// ```tl
    /// rpc_result#f35c6d01 req_msg_id:long result:Object = RpcResult;
    /// ```
    ///
    /// Here `req_msg_id` is the identifier of the message sent by the other
    /// party and containing an RPC query. This way, the recipient knows that
    /// the result is a response to the specific RPC query in question.
    ///
    /// At the same time, this response serves as acknowledgment of the other
    /// party's receipt of the `req_msg_id` message.
    ///
    /// Note that the response to an RPC query must also be acknowledged. Most
    /// frequently, this coincides with the transmission of the next message
    /// (which may have a container attached to it carrying a service message
    /// with the acknowledgment).
    ///
    /// **[RPC Error]**
    ///
    /// The result field returned in response to any RPC query may also
    /// contain an error message in the following format:
    ///
    /// ```tl
    /// rpc_result#f35c6d01 req_msg_id:long result:Object = RpcResult;
    /// ```
    ///
    /// **[Cancellation of an RPC Query]**
    ///
    /// In certain situations, the client does not want to receive a response
    /// to an already transmitted RPC query, for example because the response
    /// turns out to be long and the client has decided to do without it
    /// because of insufficient link capacity. Simply interrupting the TCP
    /// connection will not have any effect because the server would re-send
    /// the missing response at the first opportunity. Therefore, the client
    /// needs a way to cancel receipt of the RPC response message, actually
    /// acknowledging its receipt prior to it being in fact received, which
    /// will settle the server down and prevent it from re-sending the response.
    /// However, the client does not know the RPC response's msg_id prior to
    /// receiving the response; the only thing it knows is the req_msg_id.
    /// i.e. the msg_id of the relevant RPC query. Therefore, a special query
    /// is used:
    ///
    /// ```tl
    /// rpc_drop_answer#58e4a740 req_msg_id:long = RpcDropAnswer;
    /// ```
    ///
    /// The response to this query returns as one of the following messages
    /// wrapped in `rpc_result` and requiring an acknowledgment:
    ///
    /// ```tl
    /// rpc_answer_unknown#5e2ad36e = RpcDropAnswer;
    /// rpc_answer_dropped_running#cd78e586 = RpcDropAnswer;
    /// rpc_answer_dropped#a43ad8b7 msg_id:long seq_no:int bytes:int = RpcDropAnswer;
    /// ```
    ///
    /// The first version of the response is used if the server remembers
    /// nothing of the incoming req_msg_id (if it has already been responded
    /// to, for example). The second version is used if the response was
    /// canceled while the RPC query was being processed (where the RPC query
    /// itself was still fully processed); in this case, the same
    /// `rpc_answer_dropped_running` is also returned in response to the
    /// original query, and both of these responses require an acknowledgment
    /// from the client. The final version means that the RPC response was
    /// removed from the server's outgoing queue, and its `msg_id`, `seq_no`,
    /// and length in `bytes` are transmitted to the client.
    ///
    /// Note that `rpc_answer_dropped_running` and `rpc_answer_dropped` serve
    /// as acknowledgments of the server's receipt of the original query (the
    /// same one, the response to which we wish to forget). In addition, same
    /// as for any RPC queries, any response to `rpc_drop_answer` is an
    /// acknowledgment for `rpc_drop_answer` itself.
    ///
    /// As an alternative to using `rpc_drop_answer`, a new session may be
    /// created after the connection is reset and the old session is removed
    /// through `destroy_session`.
    ///
    /// [Response to an RPC query]: https://core.telegram.org/mtproto/service_messages#response-to-an-rpc-query
    /// [RPC Error]: https://core.telegram.org/mtproto/service_messages#rpc-error
    /// [Cancellation of an RPC Query]: https://core.telegram.org/mtproto/service_messages#cancellation-of-an-rpc-query
    fn handle_rpc_result(&mut self, message: manual_tl::Message) -> Result<(), DeserializeError> {
        let rpc_result = manual_tl::RpcResult::from_bytes(&message.body)?;
        let inner_constructor = rpc_result.inner_constructor();
        let manual_tl::RpcResult { req_msg_id, result } = rpc_result;
        let msg_id = MsgId(req_msg_id);

        // Any error during a RPC result will be given to the user,
        // which means this method itself is doing its job `Ok`.
        let inner_constructor = match inner_constructor {
            Ok(x) => x,
            Err(e) => {
                self.deserialization
                    .push(Deserialization::Failure(DeserializationFailure {
                        msg_id,
                        error: e.into(),
                    }));
                return Ok(());
            }
        };

        match inner_constructor {
            // RPC Error
            tl::types::RpcError::CONSTRUCTOR_ID => match tl::enums::RpcError::from_bytes(&result) {
                Ok(tl::enums::RpcError::Error(error)) => self
                    .deserialization
                    .push(Deserialization::RpcError(RpcResultError { msg_id, error })),
                Err(e) => {
                    self.deserialization
                        .push(Deserialization::Failure(DeserializationFailure {
                            msg_id,
                            error: e.into(),
                        }))
                }
            },

            // Cancellation of an RPC Query
            tl::types::RpcAnswerUnknown::CONSTRUCTOR_ID => {
                // The `msg_id` corresponds to the `rpc_drop_answer` request.
            }
            tl::types::RpcAnswerDroppedRunning::CONSTRUCTOR_ID => {
                // We will receive two `rpc_result`, one with the `msg_id` of
                // `rpc_drop_answer` request and other for the original RPC.
            }
            tl::types::RpcAnswerDropped::CONSTRUCTOR_ID => {
                // "the RPC response was removed from the server's outgoing
                // queue, and its msg_id, seq_no, and length in bytes are
                // transmitted to the client."
            }

            // Response to an RPC query
            // Telegram shouldn't send compressed errors (the overhead
            // would probably outweight the benefits) so we don't check
            // that the decompressed payload is an error or answer drop.
            manual_tl::GzipPacked::CONSTRUCTOR_ID => {
                match manual_tl::GzipPacked::decompress(&result, &mut self.unpacked_budget) {
                    Ok(body) => {
                        self.store_own_updates(msg_id, &body);
                        self.deserialization
                            .push(Deserialization::RpcResult(RpcResult { msg_id, body }));
                    }
                    Err(error) => self.deserialization.push(Deserialization::Failure(
                        DeserializationFailure { msg_id, error },
                    )),
                }
            }
            _ => {
                self.store_own_updates(msg_id, &result);
                self.deserialization
                    .push(Deserialization::RpcResult(RpcResult {
                        msg_id,
                        body: result,
                    }));
            }
        }

        Ok(())
    }

    /// Updates produced by `rpc_result` must be considered as any other updates, since they can
    /// change the `pts`. If this wasn't done, eventually higher levels would find gaps.
    ///
    /// Users may also be interested in handling updates produced by the client as if they were
    /// like any other.
    ///
    /// This MUST be done before storing the [`Deserialization::RpcResult`],
    /// because the stored update might need to reference the request, which
    /// must not have been dropped yet.
    fn store_own_updates(&mut self, msg_id: MsgId, body: &[u8]) {
        match u32::from_bytes(body) {
            Ok(body_id) => {
                if UPDATE_IDS.contains(&body_id) {
                    self.deserialization.push(Deserialization::OwnUpdate {
                        msg_id,
                        update: body.to_vec(),
                    });
                }
            }
            Err(_err) => {
                // Failing is fine, it likely means the update was bad and eventually there will
                // be a gap to fill.
            }
        }
    }

    /// **[Acknowledgment of Receipt]**
    ///
    /// Receipt of virtually all messages (with the exception of some purely
    /// service ones as well as the plain-text messages used in the protocol
    /// for creating an Authorization Key) must be acknowledged.
    ///
    /// This requires the use of the following service message (not requiring
    /// an acknowledgment):
    ///
    /// ```tl
    /// msgs_ack#62d6b459 msg_ids:Vector long = MsgsAck;
    /// ```
    ///
    /// A server usually acknowledges the receipt of a message from a client
    /// (normally, an RPC query) using an RPC response. If a response is a
    /// long time coming, a server may first send a receipt acknowledgment,
    /// and somewhat later, the RPC response itself.
    ///
    /// A client normally acknowledges the receipt of a message from a server
    /// (usually, an RPC response) by adding an acknowledgment to the next RPC
    /// query if it is not transmitted too late (if it is generated, say,
    /// 60-120 seconds following the receipt of a message from the server).
    /// However, if for a long period of time there is no reason to send
    /// messages to the server or if there is a large number of
    /// unacknowledged messages from the server (say, over 16), the client
    /// transmits a stand-alone acknowledgment.
    ///
    /// [Acknowledgment of Receipt]: https://core.telegram.org/mtproto/service_messages_about_messages#acknowledgment-of-receipt
    fn handle_ack(&self, message: manual_tl::Message) -> Result<(), DeserializeError> {
        // TODO notify about this somehow
        let _ack = tl::enums::MsgsAck::from_bytes(&message.body)?;
        Ok(())
    }

    /// **[Notice of Ignored Error Message]**
    ///
    /// In certain cases, a server may notify a client that its incoming
    /// message was ignored for whatever reason. Note that such a notification
    /// cannot be generated unless a message is correctly decoded by the
    /// server.
    ///
    /// ```tl
    /// bad_msg_notification#a7eff811 bad_msg_id:long bad_msg_seqno:int error_code:int = BadMsgNotification;
    /// bad_server_salt#edab447b bad_msg_id:long bad_msg_seqno:int error_code:int new_server_salt:long = BadMsgNotification;
    /// ```
    ///
    /// Here, `error_code` can also take on the following values:
    ///
    /// * 16: `msg_id` too low (most likely, client time is wrong; it would
    ///   be worthwhile to synchronize it using msg_id notifications and re-
    ///   send the original message with the “correct” msg_id or wrap it in a
    ///   container with a new msg_id if the original message had waited too
    ///   long on the client to be transmitted)
    /// * 17: `msg_id` too high (similar to the previous case, the client time
    ///   has to be synchronized, and the message re-sent with the correct
    ///   `msg_id`)
    /// * 18: incorrect two lower order `msg_id` bits (the server expects
    ///   client message `msg_id` to be divisible by 4)
    /// * 19: container `msg_id` is the same as `msg_id` of a previously
    ///   received message (this must never happen)
    /// * 20: message too old, and it cannot be verified whether the server
    ///   has received a message with this `msg_id` or not
    /// * 32: `msg_seqno` too low (the server has already received a message
    ///   with a lower `msg_id` but with either a higher or an equal and odd
    ///   `seqno`)
    /// * 33: `msg_seqno` too high (similarly, there is a message with a
    ///   higher `msg_id` but with either a lower or an equal and odd `seqno`)
    /// * 34: an even `msg_seqno` expected (irrelevant message), but odd
    ///   received
    /// * 35: odd `msg_seqno` expected (relevant message), but even received
    /// * 48: incorrect server salt (in this case, the `bad_server_salt`
    ///   response is received with the correct salt, and the message is to be
    ///   re-sent with it)
    /// * 64: invalid container.
    ///
    /// The intention is that `error_code` values are grouped
    /// (`error_code >> 4`): for example, the codes `0x40 - 0x4f` correspond
    /// to errors in container decomposition.
    ///
    /// Notifications of an ignored message do not require acknowledgment
    /// (i.e., are irrelevant).
    ///
    /// **Important**: if `server_salt` has changed on the server or if client
    /// time is incorrect, any query will result in a notification in the
    /// above format. The client must check that it has, in fact, recently
    /// sent a message with the specified msg_id, and if that is the case,
    /// update its time correction value (the difference between the client's
    /// and the server's clocks) and the server salt based on msg_id and the
    /// `server_salt` notification, so as to use these to (re)send future
    /// messages. In the meantime, the original message (the one that caused
    /// the error message to be returned) must also be re-sent with a better
    /// `msg_id` and/or `server_salt`.
    ///
    /// In addition, the client can update the `server_salt` value used to
    /// send messages to the server, based on the values of RPC responses or
    /// containers carrying an RPC response, provided that this RPC response
    /// is actually a match for the query sent recently. (If there is doubt,
    /// it is best not to update since there is risk of a replay attack).
    ///
    /// [Notice of Ignored Error Message]: https://core.telegram.org/mtproto/service_messages_about_messages#notice-of-ignored-error-message
    fn handle_bad_notification(
        &mut self,
        message: manual_tl::Message,
    ) -> Result<(), DeserializeError> {
        let bad_msg = tl::enums::BadMsgNotification::from_bytes(&message.body)?;

        if self
            .salt_request_msg_id
            .is_some_and(|msg_id| msg_id == MsgId(bad_msg.bad_msg_id()))
        {
            // Response to internal request, do not propagate.
            self.salt_request_msg_id = None;
        } else {
            self.deserialization
                .push(Deserialization::BadMessage(super::BadMessage {
                    msg_id: MsgId(bad_msg.bad_msg_id()),
                    code: bad_msg.error_code(),
                }));
        }

        let bad_msg = match bad_msg {
            tl::enums::BadMsgNotification::Notification(x) => x,
            tl::enums::BadMsgNotification::BadServerSalt(x) => {
                self.salts.clear();
                self.salts.push(tl::types::FutureSalt {
                    valid_since: 0,
                    valid_until: i32::MAX,
                    salt: x.new_server_salt,
                });
                self.salt_request_msg_id = None;

                info!("got bad salt; salts have been reset down to a single one");
                return Ok(());
            }
        };

        match bad_msg.error_code {
            16 => {
                // Sent `msg_id` was too low (our `time_offset` is wrong).
                self.correct_time_offset(message.msg_id);
            }
            // Only about one issued since the last correction (those rise, from below every one
            // refused before): one above the last issued is stale, or replayed, and correcting
            // the clock from it again could take the identifiers back, or start a session for
            // nothing. The caller still sends the message it names again.
            17 if bad_msg.bad_msg_id <= self.last_msg_id => {
                // Sent `msg_id` was too high (our `time_offset` is wrong), and so are the ones
                // after it: the next ones must go below them.
                self.correct_time_offset(message.msg_id);
                self.msg_id_too_high = true;
            }
            32 => {
                // Sent `seq_no` was too low. Bump it by some large-ish value.
                // TODO start with a fresh session rather than guessing
                self.sequence = self.sequence.wrapping_add(64);
            }
            33 => {
                // Sent `seq_no` was too high (this error doesn't seem to occur).
                // TODO start with a fresh session rather than guessing
                self.sequence = self.sequence.wrapping_sub(16);
            }
            _ => {
                // Just notify about it.
            }
        }

        Ok(())
    }

    /// **[Request for Message Status Information]**
    ///
    /// If either party has not received information on the status of its
    /// outgoing messages for a while, it may explicitly request it from the
    /// other party:
    ///
    /// ```tl
    /// msgs_state_req#da69fb52 msg_ids:Vector long = MsgsStateReq;
    /// ```
    ///
    /// [Request for Message Status Information]: https://core.telegram.org/mtproto/service_messages_about_messages#request-for-message-status-information
    fn handle_state_req(&self, _message: manual_tl::Message) -> Result<(), DeserializeError> {
        // TODO implement
        Ok(())
    }

    /// **[Informational Message regarding Status of Messages]**
    ///
    /// ```tl
    /// msgs_state_info#04deb57d req_msg_id:long info:string = MsgsStateInfo;
    /// ```
    ///
    /// Here, `info` is a string that contains exactly one byte of message
    /// status for each message from the incoming `msg_ids` list:
    ///
    /// * 1 = nothing is known about the message (`msg_id` too low, the other
    ///   party may have forgotten it)
    /// * 2 = message not received (`msg_id` falls within the range of stored
    ///   identifiers; however, the other party has certainly not received a
    ///   message like that)
    /// * 3 = message not received (`msg_id` too high; however, the other
    ///   party has certainly not received it yet)
    /// * 4 = message received (note that this response is also at the same
    ///   time a receipt acknowledgment)
    /// * +8 = message already acknowledged
    /// * +16 = message not requiring acknowledgment
    /// * +32 = RPC query contained in message being processed or processing
    ///   already complete
    /// * +64 = content-related response to message already generated
    /// * +128 = other party knows for a fact that message is already received
    ///
    /// This response does not require an acknowledgment. It is an
    /// acknowledgment of the relevant `msgs_state_req`, in and of itself.
    ///
    /// Note that if it turns out suddenly that the other party does not have
    /// a message that looks like it has been sent to it, the message can
    /// simply be re-sent. Even if the other party should receive two copies
    /// of the message at the same time, the duplicate will be ignored.
    /// (If too much time has passed, and the original `msg_id` is not longer
    /// valid, the message is to be wrapped in `msg_copy`).
    ///
    /// [Informational Message regarding Status of Messages]: https://core.telegram.org/mtproto/service_messages_about_messages#informational-message-regarding-status-of-messages
    fn handle_state_info(&mut self, _message: manual_tl::Message) -> Result<(), DeserializeError> {
        // TODO implement
        Ok(())
    }

    /// **[Voluntary Communication of Status of Messages]**
    ///
    /// Either party may voluntarily inform the other party of the status of
    /// the messages transmitted by the other party.
    ///
    /// ```tl
    /// msgs_all_info#8cc0d131 msg_ids:Vector long info:string = MsgsAllInfo;
    /// ```
    ///
    /// All message codes known to this party are enumerated, with the
    /// exception of those for which the +128 and the +16 flags are set.
    /// However, if the +32 flag is set but not +64, then the message status
    /// will still be communicated.
    ///
    /// This message does not require an acknowledgment.
    ///
    /// [Voluntary Communication of Status of Messages]: https://core.telegram.org/mtproto/service_messages_about_messages#voluntary-communication-of-status-of-messages
    fn handle_msg_all(&mut self, _message: manual_tl::Message) -> Result<(), DeserializeError> {
        // TODO implement
        Ok(())
    }

    /// **[Extended Voluntary Communication of Status of One Message]**
    ///
    /// Normally used by the server to respond to the receipt of a duplicate
    /// `msg_id`, especially if a response to the message has already been
    /// generated and the response is large. If the response is small, the
    /// server may re-send the answer itself instead. This message can also
    /// be used as a notification instead of resending a large message.
    ///
    /// ```tl
    /// msg_detailed_info#276d3ec6 msg_id:long answer_msg_id:long bytes:int status:int = MsgDetailedInfo;
    /// msg_new_detailed_info#809db6df answer_msg_id:long bytes:int status:int = MsgDetailedInfo;
    /// ```
    ///
    /// The second version is used to notify of messages that were created on
    /// the server not in response to an RPC query (such as notifications of
    /// new messages) and were transmitted to the client some time ago, but
    /// not acknowledged.
    ///
    /// Currently, `status` is always zero. This may change in future.
    ///
    /// This message does not require an acknowledgment.
    ///
    /// [Extended Voluntary Communication of Status of One Message]: https://core.telegram.org/mtproto/service_messages_about_messages#extended-voluntary-communication-of-status-of-one-message
    fn handle_detailed_info(
        &mut self,
        message: manual_tl::Message,
    ) -> Result<(), DeserializeError> {
        // TODO https://github.com/telegramdesktop/tdesktop/blob/8f82880b938e06b7a2a27685ef9301edb12b4648/Telegram/SourceFiles/mtproto/connection.cpp#L1790-L1820
        // TODO https://github.com/telegramdesktop/tdesktop/blob/8f82880b938e06b7a2a27685ef9301edb12b4648/Telegram/SourceFiles/mtproto/connection.cpp#L1822-L1845
        let msg_detailed = tl::enums::MsgDetailedInfo::from_bytes(&message.body)?;
        match msg_detailed {
            tl::enums::MsgDetailedInfo::Info(x) => {
                self.pending_ack.push(x.answer_msg_id);
            }
            tl::enums::MsgDetailedInfo::MsgNewDetailedInfo(x) => {
                self.pending_ack.push(x.answer_msg_id);
            }
        }
        Ok(())
    }

    /// **[Explicit Request to Re-Send Messages]**
    ///
    /// ```tl
    /// msg_resend_req#7d861a08 msg_ids:Vector long = MsgResendReq;
    /// ```
    ///
    /// The remote party immediately responds by re-sending the requested
    /// messages, normally using the same connection that was used to transmit
    /// the query. If at least one message with requested msg_id does not
    /// exist or has already been forgotten, or has been sent by the
    /// requesting party (known from parity), `MsgsStateInfo` is returned for
    /// all messages requested as if the `MsgResendReq` query had been a
    /// `MsgsStateReq` query as well.
    ///
    /// **[Explicit Request to Re-Send Answers]**
    ///
    /// ```tl
    /// msg_resend_ans_req#8610baeb msg_ids:Vector long = MsgResendReq;
    /// ```
    ///
    /// The remote party immediately responds by re-sending answers to the
    /// requested messages, normally using the same connection that was used
    /// to transmit the query. `MsgsStateInfo` is returned for all messages
    /// requested as if the `MsgResendReq` query had been a MsgsStateReq query
    /// as well.
    ///
    /// [Explicit Request to Re-Send Answers]: https://core.telegram.org/mtproto/service_messages_about_messages#explicit-request-to-re-send-answers
    /// [Explicit Request to Re-Send Messages]: https://core.telegram.org/mtproto/service_messages_about_messages#explicit-request-to-re-send-messages
    fn handle_msg_resend(&self, _message: manual_tl::Message) -> Result<(), DeserializeError> {
        // TODO implement
        // `msg_resend_ans_req` seems to never occur (it was even missing from `mtproto.tl`)
        Ok(())
    }

    /// **[Request for several future salts]**
    ///
    /// The client may at any time request from the server several (between
    /// 1 and 64) future server salts together with their validity periods.
    /// Having stored them in persistent memory, the client may use them to
    /// send messages in the future even if he changes sessions (a server
    /// salt is attached to the Authorization Key rather than being
    /// session-specific).
    ///
    /// ```tl
    /// get_future_salts#b921bd04 num:int = FutureSalts;
    /// future_salt#0949d9dc valid_since:int valid_until:int salt:long = FutureSalt;
    /// future_salts#ae500895 req_msg_id:long now:int salts:vector future_salt = FutureSalts;
    /// ```
    ///
    /// The client must check to see that the response's `req_msg_id` in fact
    /// coincides with `msg_id` of the query for `get_future_salts`. The
    /// server returns a maximum of `num` future server salts (may return
    /// fewer). The response serves as the acknowledgment of the query and
    /// does not require an acknowledgment itself.
    ///
    /// [Request for several future salts]: https://core.telegram.org/mtproto/service_messages#request-for-several-future-salts
    fn handle_future_salts(&mut self, message: manual_tl::Message) -> Result<(), DeserializeError> {
        let tl::enums::FutureSalts::Salts(salts) =
            tl::enums::FutureSalts::from_bytes(&message.body)?;

        if self
            .salt_request_msg_id
            .is_some_and(|msg_id| msg_id == MsgId(salts.req_msg_id))
        {
            // Response to internal request, do not propagate.
            self.salt_request_msg_id = None;
        } else {
            self.deserialization
                .push(Deserialization::RpcResult(RpcResult {
                    msg_id: MsgId(salts.req_msg_id),
                    body: message.body,
                }));
        }

        self.start_salt_time = Some((salts.now, Instant::now()));
        self.salts = salts.salts.0;
        // `Reverse`, not a negation: `valid_since` is the server's, and `-i32::MIN` overflows.
        self.salts.sort_by_key(|salt| Reverse(salt.valid_since));
        info!("got {} future salts", self.salts.len());

        Ok(())
    }

    /// A `future_salt` on its own answers no request (`get_future_salts` returns `future_salts`),
    /// so it is ignored.
    fn handle_future_salt(&mut self, message: manual_tl::Message) -> Result<(), DeserializeError> {
        warn!(
            msg_id = message.msg_id,
            "ignoring a future_salt that answers no request"
        );
        Ok(())
    }

    /// **[Ping Messages (PING/PONG)]**
    ///
    /// ```tl
    /// ping#7abe77ec ping_id:long = Pong;
    /// ```
    ///
    /// A response is usually returned to the same connection:
    ///
    /// ```tl
    /// pong#347773c5 msg_id:long ping_id:long = Pong;
    /// ```
    ///
    /// These messages do not require acknowledgments. A `pong` is
    /// transmitted only in response to a `ping` while a `ping` can be
    /// initiated by either side.
    ///
    /// **[Deferred Connection Closure + PING]**
    ///
    /// ```tl
    /// ping_delay_disconnect#f3427b8c ping_id:long disconnect_delay:int = Pong;
    /// ```
    ///
    /// Works like `ping`. In addition, after this is received, the server
    /// starts a timer which will close the current connection
    /// `disconnect_delay` seconds later unless it receives a new message of
    /// the same type which automatically resets all previous timers. If the
    /// client sends these pings once every 60 seconds, for example, it may
    /// set `disconnect_delay` equal to 75 seconds.
    ///
    /// [Ping Messages (PING/PONG)]: https://core.telegram.org/mtproto/service_messages#ping-messages-ping-pong
    /// [Deferred Connection Closure + PING]: https://core.telegram.org/mtproto/service_messages#deferred-connection-closure-ping
    fn handle_pong(&mut self, message: manual_tl::Message) -> Result<(), DeserializeError> {
        let tl::enums::Pong::Pong(pong) = tl::enums::Pong::from_bytes(&message.body)?;

        self.deserialization
            .push(Deserialization::RpcResult(RpcResult {
                msg_id: MsgId(pong.msg_id),
                body: message.body,
            }));
        Ok(())
    }

    /// **[Request to Destroy Session]**
    ///
    /// Used by the client to notify the server that it may forget the data
    /// from a different session belonging to the same user (i.e. with the
    /// same `auth_key_id`). The result of this being applied to the current
    /// session is undefined.
    ///
    /// ```tl
    /// destroy_session#e7512126 session_id:long = DestroySessionRes;
    /// destroy_session_ok#e22045fc session_id:long = DestroySessionRes;
    /// destroy_session_none#62d350c9 session_id:long = DestroySessionRes;
    /// ```
    ///
    /// [Request to Destroy Session]: https://core.telegram.org/mtproto/service_messages#request-to-destroy-session
    fn handle_destroy_session(&self, _message: manual_tl::Message) -> Result<(), DeserializeError> {
        // TODO implement
        Ok(())
    }

    /// **[New Session Creation Notification]**
    ///
    /// The server notifies the client that a new session (from the server's
    /// standpoint) had to be created to handle a client message. If, after
    /// this, the server receives a message with an even smaller `msg_id`
    /// within the same session, a similar notification will be generated for
    /// this `msg_id` as well. No such notifications are generated for high
    /// `msg_id` values.
    ///
    /// ```tl
    /// new_session_created#9ec20908 first_msg_id:long unique_id:long server_salt:long = NewSession
    /// ```
    ///
    /// The `unique_id` parameter is generated by the server every time a
    /// session is (re-)created.
    ///
    /// This notification must be acknowledged by the client. It is necessary,
    /// for instance, for the client to understand that there is, in fact, a
    /// "gap" in the stream of long poll notifications received from the
    /// server (the user may have failed to receive notifications during some
    /// period of time).
    ///
    /// Notice that the server may unilaterally destroy (close) any existing
    /// client sessions with all pending messages and notifications, without
    /// sending any notifications. This happens, for example, if the session
    /// is inactive for a long time, and the server runs out of memory. If the
    /// client at some point decides to send new messages to the server using
    /// the old session, already forgotten by the server, such a "new session
    /// created" notification will be generated. The client is expected to
    /// handle such situations gracefully.
    ///
    /// [New Session Creation Notification]: https://core.telegram.org/mtproto/service_messages#new-session-creation-notification
    fn handle_new_session_created(
        &mut self,
        message: manual_tl::Message,
    ) -> Result<(), DeserializeError> {
        let new_session = tl::enums::NewSession::from_bytes(&message.body)?;
        match new_session {
            tl::enums::NewSession::Created(x) => {
                self.salts.clear();
                self.salts.push(tl::types::FutureSalt {
                    valid_since: 0,
                    valid_until: i32::MAX,
                    salt: x.server_salt,
                });
            }
        }
        // The gap the documentation names: the caller fetches the difference.
        self.deserialization.push(Deserialization::NewSession);
        Ok(())
    }

    /// **[Containers]**
    ///
    /// *Containers* are messages containing several other messages. Used
    /// for the ability to transmit several RPC queries and/or service
    /// messages at the same time, using HTTP or even TCP or UDP protocol.
    /// A container may only be accepted or rejected by the other party as
    /// a whole.
    ///
    /// **[Simple Container]**
    ///
    /// A simple container carries several messages as follows:
    ///
    /// ```tl
    /// msg_container#73f1f8dc messages:vector message = MessageContainer;
    /// ```
    ///
    /// Here message refers to any message together with its length and
    /// `msg_id`:
    ///
    /// ```tl
    /// message msg_id:long seqno:int bytes:int body:Object = Message;
    /// ```
    ///
    /// `bytes` is the number of bytes in the body serialization.
    ///
    /// All messages in a container must have `msg_id` lower than that of the
    /// container itself. A container does not require an acknowledgment and
    /// may not carry other simple containers. When messages are re-sent,
    /// they may be combined into a container in a different manner or sent
    /// individually.
    ///
    /// Empty containers are also allowed. They are used by the server,
    /// for example, to respond to an HTTP request when the timeout specified
    /// in `http_wait` expires, and there are no messages to transmit.
    ///
    /// A container inside another one, also inside a `gzip_packed` one, is an error, so the
    /// nesting, and the recursion that processes it, stays bounded.
    ///
    /// [Containers]: https://core.telegram.org/mtproto/service_messages#containers
    /// [Simple Container]: https://core.telegram.org/mtproto/service_messages#simple-container
    fn handle_container(
        &mut self,
        message: manual_tl::Message,
        nesting: Nesting,
    ) -> Result<(), DeserializeError> {
        if nesting.in_container {
            return Err(DeserializeError::UnexpectedConstructor {
                id: manual_tl::MessageContainer::CONSTRUCTOR_ID,
            });
        }
        let container = manual_tl::MessageContainer::from_body(&message.body)?;
        let nesting = Nesting {
            in_container: true,
            ..nesting
        };
        for inner_message in container.messages {
            self.process_message(inner_message, nesting)?;
        }

        Ok(())
    }

    /// **[Message Copies]**
    ///
    /// In some situations, an old message with a `msg_id` that is no longer
    /// valid needs to be re-sent. Then, it is wrapped in a copy container:
    ///
    /// ```tl
    /// msg_copy#e06046b2 orig_message:Message = MessageCopy;
    /// ```
    ///
    /// Once received, the message is processed as if the wrapper were not
    /// there. However, if it is known for certain that the message
    /// `orig_message.msg_id` was received, then the new message is not
    /// processed (while at the same time, it and `orig_message.msg_id`
    /// are acknowledged). The value of `orig_message.msg_id` must be lower
    /// than the container's `msg_id`.
    ///
    /// This is not used at this time, because an old message can be wrapped
    /// in a simple container with the same result. Receiving one is an error.
    ///
    /// [Message Copies]: https://core.telegram.org/mtproto/service_messages#message-copies
    fn handle_msg_copy(&self, _message: manual_tl::Message) -> Result<(), DeserializeError> {
        Err(DeserializeError::UnexpectedConstructor {
            id: manual_tl::MessageCopy::CONSTRUCTOR_ID,
        })
    }

    /// **[Packed Object]**
    ///
    /// Used to replace any other object (or rather, a serialization thereof)
    /// with its archived (gzipped) representation:
    ///
    /// ```tl
    /// gzip_packed#3072cfa1 packed_data:string = Object;
    /// ```
    ///
    /// At the present time, it is supported in the body of an RPC response
    /// (i.e., as result in rpc_result) and generated by the server for a
    /// limited number of high-level queries. In addition, in the future it
    /// may be used to transmit non-service messages (i.e. RPC queries) from
    /// client to server.
    ///
    /// The packed object draws on the packet's budget for inflated bytes, and may be packed in at
    /// most [`manual_tl::MAX_GZIP_NESTING`] others; past either limit, it is
    /// [`DeserializeError::DecompressionFailed`]. The packed bytes are freed before the inflated
    /// message is processed, so a nested object doesn't keep the levels above it alive.
    ///
    /// [Packed Object]: https://core.telegram.org/mtproto/service_messages#packed-object
    fn handle_gzip_packed(
        &mut self,
        message: manual_tl::Message,
        nesting: Nesting,
    ) -> Result<(), DeserializeError> {
        if nesting.gzip_depth >= manual_tl::MAX_GZIP_NESTING {
            return Err(DeserializeError::DecompressionFailed);
        }
        let manual_tl::Message {
            msg_id,
            seq_no,
            body: packed,
        } = message;
        let body = manual_tl::GzipPacked::decompress(&packed, &mut self.unpacked_budget)?;
        drop(packed);
        self.process_message(
            manual_tl::Message {
                msg_id,
                seq_no,
                body,
            },
            Nesting {
                gzip_depth: nesting.gzip_depth + 1,
                ..nesting
            },
        )
    }

    /// **[HTTP Wait/Long Poll]**
    ///
    /// The following special service query not requiring an acknowledgement
    /// (which must be transmitted only through an HTTP connection) is used to
    /// enable the server to send messages in the future to the client using
    /// HTTP protocol:
    ///
    /// ```tl
    /// http_wait#9299359f max_delay:int wait_after:int max_wait:int = HttpWait;
    /// ```
    ///
    /// When such a message (or a container carrying such a message) is
    /// received, the server either waits `max_delay` milliseconds, whereupon
    /// it forwards all the messages that it is holding on to the client if
    /// there is at least one message queued in session (if needed, by placing
    /// them into a container to which acknowledgments may also be added); or
    /// else waits no more than `max_wait` milliseconds until such a message
    /// is available. If a message never appears, an empty container is
    /// transmitted.
    ///
    /// The `max_delay` parameter denotes the maximum number of milliseconds
    /// that has elapsed between the first message for this session and the
    /// transmission of an HTTP response. The wait_after parameter works as
    /// follows: after the receipt of the latest message for a particular
    /// session, the server waits another wait_after milliseconds in case
    /// there are more messages. If there are no additional messages, the
    /// result is transmitted (a container with all the messages). If more
    /// messages appear, the `wait_after` timer is reset.
    ///
    /// At the same time, the max_delay parameter has higher priority than
    /// `wait_after`, and `max_wait` has higher priority than `max_delay`.
    ///
    /// This message does not require a response or an acknowledgement. If
    /// the container transmitted over HTTP carries several such messages,
    /// the behavior is undefined (in fact, the latest parameter will be
    /// used).
    ///
    /// If no `http_wait` is present in container, default values
    /// `max_delay=0` (milliseconds), `wait_after=0` (milliseconds), and
    /// `max_wait=25000` (milliseconds) are used.
    ///
    /// If the client's ping of the server takes a long time, it may make
    /// sense to set `max_delay` to a value that is comparable in magnitude
    /// to ping time.
    ///
    /// [HTTP Wait/Long Poll]: https://core.telegram.org/mtproto/service_messages#http-wait-long-poll
    fn handle_http_wait(&mut self, _message: manual_tl::Message) -> Result<(), DeserializeError> {
        // TODO implement
        Ok(())
    }

    /// Anything else that's not Service Message will be `Updates`.
    ///
    /// Since we handle all the possible service messages, we can
    /// safely treat whatever message body we received as `Updates`.
    fn handle_update(&mut self, message: manual_tl::Message) -> Result<(), DeserializeError> {
        // TODO if this `Updates` cannot be deserialized, `getDifference` should be used
        self.deserialization
            .push(Deserialization::Update(message.body));
        Ok(())
    }
}

/// The maximum header length used by any available transport.
///
/// The largest transport is `Full`, needing 4 bytes for each of `len` and `seq`.
///
/// The `crc32` is appended at the end, so it is not part of the leading header.
pub const MAX_TRANSPORT_HEADER_LEN: usize = 4 + 4;

/// Amount of bytes used by the [Encrypted Message](https://core.telegram.org/mtproto/description#encrypted-message).
///
/// This accounts for both the `auth_key_id` (8 bytes) and `msg_key` (16 bytes).
pub const ENCRYPTED_PACKET_HEADER_LEN: usize = 8 + 16;

/// Amount of bytes used by the [`encrypted_data`](https://core.telegram.org/mtproto/description#encrypted-message-encrypted-data).
///
/// This accounts for both `salt` (8 bytes) and `session_id` (8 bytes).
///
/// The `message_id`, `seq_no` and `message_data_length` are not included
/// because those will have been appended at the current buffer position.
pub const PLAIN_PACKET_HEADER_LEN: usize = 8 + 8;

/// Amount of bytes used by a `msg_container`.
///
/// The message header for the container occupies the size of the message header
/// (`msg_id`, `seq_no` and `size`) followed by the container header (`constructor`, `len`).
pub const MESSAGE_CONTAINER_HEADER_LEN: usize = (8 + 4 + 4) + (4 + 4);

impl Mtp for Encrypted {
    /// Pushes a request into the internal buffer.
    ///
    /// Multiple requests will be buffered and sent as a single container to reduce roundtrips.
    /// If the buffer is full (request does not fit in the container), returns `None`.
    ///
    /// [MTProto 2.0 guidelines]: https://core.telegram.org/mtproto/description.
    fn push(&mut self, buffer: &mut DequeBuffer<u8>, request: &[u8]) -> Option<MsgId> {
        // TODO rather than taking in bytes, take requests, serialize them in place, and if too large drop the last part of the buffer

        // Check to see if the next salt can be used already. If it can, drop the current one and,
        // if the next salt is the last one, fetch more.
        //
        // The times come from the server, so the arithmetic saturates.
        if let Some((start_secs, start_instant)) = self.start_salt_time
            && let [.., next, _current] = self.salts.as_slice()
        {
            let now = start_secs.saturating_add(start_instant.elapsed().as_secs() as i32);
            if now >= next.valid_since.saturating_add(SALT_USE_DELAY) {
                self.salts.pop();
            }
        }

        self.try_request_salts(buffer);
        if self.salt_request_msg_id.is_some() {
            // Don't add anything else to the container while we still need new salts.
            return None;
        }

        // If we need to acknowledge messages, this notification goes in with the rest of requests
        // so that we can also include it. It has priority over user requests because these should
        // be sent out as soon as possible.
        if !self.pending_ack.is_empty() {
            let later = self
                .pending_ack
                .split_off(self.pending_ack.len().min(MAX_ACKS_PER_MESSAGE));
            let ack = tl::enums::MsgsAck::Ack(tl::types::MsgsAck {
                msg_ids: mem::replace(&mut self.pending_ack, later),
            });

            // This should take priority during the next packet.
            // Don't allow other requests through until this can be sent.
            if self
                .try_serialize_msg(buffer, &ack.to_bytes(), false)
                .is_none()
            {
                // Not sent: the identifiers stay pending, first in line.
                let tl::enums::MsgsAck::Ack(tl::types::MsgsAck { mut msg_ids }) = ack;
                msg_ids.append(&mut self.pending_ack);
                self.pending_ack = msg_ids;
                return None;
            }
        }

        // Requests that are too large would cause Telegram to close the
        // connection but are so uncommon it's not worth returning `Err`.
        // Without this, failing to serialize would repeatedly return `None`,
        // leading to an infinite loop as well.
        assert!(request.len() <= manual_tl::Message::MAX_BODY_SIZE);

        // Serialized requests will always be correctly padded.
        assert!(request.len().is_multiple_of(4));

        // Payload provided by the user is always considered to be
        // content-related, which means we can apply compression.
        //
        // Compression is only worth it if it makes the request smaller; if it fails, the request
        // goes out as it is.
        let mut body = request;
        let compressed;
        if let Some(threshold) = self.compression_threshold
            && request.len() >= threshold
            && let Ok(gzip) = manual_tl::GzipPacked::new(request)
        {
            compressed = gzip.to_bytes();
            if compressed.len() < request.len() {
                body = &compressed;
            }
        }

        self.try_serialize_msg(buffer, body, true)
    }

    fn finalize(&mut self, buffer: &mut DequeBuffer<u8>) -> Option<MsgId> {
        self.finalize_plain(buffer);
        if buffer.is_empty() {
            None
        } else {
            // The length is 17 to 32, so it never takes the fallback, which is the whole array.
            let (padding, padding_len) = get_padding_data_v2(buffer.len());
            buffer.extend_back(padding.get(..padding_len).unwrap_or(&padding));

            let msg_key = encrypt_data_v2(buffer.as_mut(), &self.auth_key);

            buffer.extend_front(&msg_key);
            buffer.extend_front(&self.auth_key.key_id());

            Some(MsgId(self.last_msg_id))
        }
    }

    /// Processes an encrypted response from the server.
    fn deserialize(
        &mut self,
        payload: &mut [u8],
    ) -> Result<Vec<Deserialization>, DeserializeError> {
        crate::utils::check_message_buffer(payload)?;

        let plaintext = decrypt_data_v2(payload, &self.auth_key)?;
        self.deserialize_plaintext(plaintext)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // salt + session_id
    const MESSAGE_PREFIX_LEN: usize = 8 + 8;

    // gzip_packed#3072cfa1 packed_data:string = Object;
    const GZIP_PACKED_HEADER: [u8; 4] = [0xa1, 0xcf, 0x72, 0x30];

    // msg_container#73f1f8dc messages:vector<message> = MessageContainer;
    const MSG_CONTAINER_HEADER: [u8; 4] = [0xdc, 0xf8, 0xf1, 0x73];

    const REQUEST: &[u8] = b"Hey!";
    const REQUEST_B: &[u8] = b"Bye!";

    fn auth_key() -> [u8; 256] {
        [0; 256]
    }

    fn ensure_buffer_is_message(buffer: &[u8], body: &[u8], seq_no: u8) {
        // buffer[0..8] is the msg_id, based on `SystemTime::now()`
        assert_ne!(&buffer[0..8], [0, 0, 0, 0, 0, 0, 0, 0]);
        // buffer[8..12] is the seq_no, ever-increasing odd number (little endian)
        assert_eq!(&buffer[8..12], [seq_no, 0, 0, 0]);
        // buffer[12..16] is the bytes, the len of the body (little endian)
        assert_eq!(&buffer[12..16], [body.len() as u8, 0, 0, 0]);
        // buffer[16..] is the body, which is padded to 4 bytes
        assert_eq!(&buffer[16..], body);
    }

    #[test]
    fn ensure_serialization_has_salt_session_id() {
        let mut buffer = DequeBuffer::with_capacity(0, 0);
        let mut mtproto = Encrypted::build().finish(auth_key());

        mtproto.push(&mut buffer, REQUEST);
        mtproto.finalize_plain(&mut buffer);

        // salt comes first, it's zero by default.
        assert_eq!(&buffer[0..8], [0, 0, 0, 0, 0, 0, 0, 0]);

        // session_id should be random.
        assert_ne!(&buffer[8..16], [0, 0, 0, 0, 0, 0, 0, 0]);

        // Only one message should remain.
        ensure_buffer_is_message(&buffer[MESSAGE_PREFIX_LEN..], REQUEST, 1);
    }

    #[test]
    fn ensure_correct_single_serialization() {
        let mut buffer = DequeBuffer::with_capacity(0, 0);
        let mut mtproto = Encrypted::build().finish(auth_key());

        assert!(mtproto.push(&mut buffer, REQUEST).is_some());
        mtproto.finalize_plain(&mut buffer);

        let buffer = &buffer[MESSAGE_PREFIX_LEN..];
        ensure_buffer_is_message(buffer, REQUEST, 1);
    }

    #[test]
    fn ensure_correct_multi_serialization() {
        let mut buffer = DequeBuffer::with_capacity(0, 0);
        let mut mtproto = Encrypted::build()
            .compression_threshold(None)
            .finish(auth_key());

        assert!(mtproto.push(&mut buffer, REQUEST).is_some());
        assert!(mtproto.push(&mut buffer, REQUEST_B).is_some());
        mtproto.finalize_plain(&mut buffer);
        let buffer = &buffer[MESSAGE_PREFIX_LEN..];

        // buffer[0..8] is the msg_id for the container
        assert_ne!(&buffer[0..8], [0, 0, 0, 0, 0, 0, 0, 0]);
        // buffer[8..12] is the seq_no, maybe-increasing even number.
        // after two messages (1, 3) the next non-content related is 4.
        assert_eq!(&buffer[8..12], [4, 0, 0, 0]);
        // buffer[12..16] is the bytes, the len of the body
        assert_eq!(&buffer[12..16], [48, 0, 0, 0]);

        // buffer[16..20] is the constructor id of the container
        assert_eq!(&buffer[16..20], MSG_CONTAINER_HEADER);
        // buffer[20..24] is how many messages are included
        assert_eq!(&buffer[20..24], [2, 0, 0, 0]);

        // buffer[24..44] is an inner message
        ensure_buffer_is_message(&buffer[24..44], REQUEST, 1);

        // buffer[44..] is the other inner message
        ensure_buffer_is_message(&buffer[44..], REQUEST_B, 3);
    }

    #[test]
    fn ensure_correct_single_large_serialization() {
        let mut buffer = DequeBuffer::with_capacity(0, 0);
        let mut mtproto = Encrypted::build()
            .compression_threshold(None)
            .finish(auth_key());
        let data = vec![0x7f; 768 * 1024];

        assert!(mtproto.push(&mut buffer, &data).is_some());
        mtproto.finalize_plain(&mut buffer);

        let buffer = &buffer[MESSAGE_PREFIX_LEN..];
        assert_eq!(buffer.len(), 16 + data.len());
    }

    #[test]
    fn ensure_correct_multi_large_serialization() {
        let mut buffer = DequeBuffer::with_capacity(0, 0);
        let mut mtproto = Encrypted::build()
            .compression_threshold(None)
            .finish(auth_key());
        let data = vec![0x7f; 768 * 1024];

        assert!(mtproto.push(&mut buffer, &data).is_some());
        assert!(mtproto.push(&mut buffer, &data).is_none());

        // No container should be used, only the `salt` + `session_id` (16 bytes) should count.
        mtproto.finalize_plain(&mut buffer);
        let buffer = &buffer[MESSAGE_PREFIX_LEN..];
        assert_eq!(buffer.len(), 16 + data.len());
    }

    #[test]
    #[should_panic]
    fn ensure_large_payload_panics() {
        let mut buffer = DequeBuffer::with_capacity(0, 0);
        let mut mtproto = Encrypted::build().finish(auth_key());

        mtproto.push(&mut buffer, &vec![0; 2 * 1024 * 1024]);
    }

    #[test]
    #[should_panic]
    fn ensure_non_padded_payload_panics() {
        let mut buffer = DequeBuffer::with_capacity(0, 0);
        let mut mtproto = Encrypted::build().finish(auth_key());

        mtproto.push(&mut buffer, &[1, 2, 3]);
    }

    #[test]
    fn ensure_no_compression_is_honored() {
        // A large vector of null bytes should compress
        let mut buffer = DequeBuffer::with_capacity(0, 0);
        let mut mtproto = Encrypted::build()
            .compression_threshold(None)
            .finish(auth_key());

        mtproto.push(&mut buffer, &vec![0; 512 * 1024]);
        mtproto.finalize_plain(&mut buffer);
        assert!(!buffer.as_ref().windows(4).any(|w| w == GZIP_PACKED_HEADER));
    }

    #[test]
    fn ensure_some_compression() {
        // A large vector of null bytes should compress
        {
            // High threshold not reached, should not compress
            let mut buffer = DequeBuffer::with_capacity(0, 0);
            let mut mtproto = Encrypted::build()
                .compression_threshold(Some(768 * 1024))
                .finish(auth_key());
            mtproto.push(&mut buffer, &vec![0; 512 * 1024]);
            mtproto.finalize_plain(&mut buffer);
            assert!(!buffer.as_ref().windows(4).any(|w| w == GZIP_PACKED_HEADER));
        }
        {
            // Low threshold is exceeded, should compress
            let mut buffer = DequeBuffer::with_capacity(0, 0);
            let mut mtproto = Encrypted::build()
                .compression_threshold(Some(256 * 1024))
                .finish(auth_key());
            mtproto.push(&mut buffer, &vec![0; 512 * 1024]);
            mtproto.finalize_plain(&mut buffer);
            assert!(buffer.as_ref().windows(4).any(|w| w == GZIP_PACKED_HEADER));
        }
        {
            // The default should compress
            let mut buffer = DequeBuffer::with_capacity(0, 0);
            let mut mtproto = Encrypted::build().finish(auth_key());
            mtproto.push(&mut buffer, &vec![0; 512 * 1024]);
            mtproto.finalize_plain(&mut buffer);
            assert!(buffer.as_ref().windows(4).any(|w| w == GZIP_PACKED_HEADER));
        }
    }
}
