// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The receiver keeps the session's state the message boxes': the account's state set from
//! `updates.getState` is stored with the channels' boxes loaded without it. It ends a failed
//! difference as the message boxes can: after the server's own failure (code 500, or a negative
//! code such as -503) unanswered, asked again after the boxes' backoff, the account's and a
//! channel's alike; after `PERSISTENT_TIMESTAMP_INVALID` the account's with the `pts`
//! `updates.getState` gives, the rest of the state kept. Any other error is still the caller's.
//! No network: a script plays the pool's runner ([`scripted`]), answering each request.

use std::sync::Mutex;
use std::time::Duration;

use grammers_session::Session;
use grammers_session::storages::{MemorySession, erase};
use grammers_session::types::{ChannelKind, ChannelState, PeerAuth, UpdateState, UpdatesState};
use grammers_tl_types::{Identifiable, Serializable};

use super::*;
use crate::RpcError;
use crate::sender_pool::tests::scripted;

/// The account's state the session holds: `pts` 100, `qts` 50, `date` 10, `seq` 5.
fn stored() -> UpdatesState {
    UpdatesState {
        pts: 100,
        qts: 50,
        date: 10,
        seq: 5,
        channels: Vec::new(),
    }
}

/// A receiver catching up from `state`, which its session holds, through `handle`, and that
/// session. The sender of its updates channel is returned too, kept open.
async fn receiver(
    handle: SenderPoolHandle,
    state: UpdatesState,
) -> (
    UpdatesReceiver,
    Arc<MemorySession>,
    mpsc::Sender<UpdatesLike>,
) {
    let session = Arc::new(MemorySession::default());
    session
        .set_update_state(UpdateState::All(state))
        .await
        .unwrap();
    let (updates_tx, updates) = mpsc::channel(1);
    let configuration = UpdatesConfiguration { catch_up: true };
    let receiver =
        UpdatesReceiver::create(handle, erase(Arc::clone(&session)), updates, configuration)
            .await
            .unwrap();
    (receiver, session, updates_tx)
}

fn rpc_error(code: i32, message: &str) -> InvocationError {
    InvocationError::Rpc(RpcError::from(tl::types::RpcError {
        error_code: code,
        error_message: message.into(),
    }))
}

#[tokio::test(start_paused = true)]
async fn the_accounts_state_is_stored_with_the_channels_loaded_without_it() {
    let channels = vec![ChannelState { id: 7000, pts: 20 }];
    let loaded = UpdatesState {
        pts: 0,
        qts: 0,
        date: 0,
        seq: 0,
        channels: channels.clone(),
    };
    let (handle, _) = scripted(Box::new(|_| Err(rpc_error(400, "UNEXPECTED"))));
    let (mut receiver, session, _updates) = receiver(handle, loaded).await;
    let state = tl::types::updates::State {
        pts: 100,
        qts: 50,
        date: 10,
        seq: 5,
        unread_count: 0,
    };
    receiver.set_account_state(state).await.unwrap();
    // The session holds the boxes' state: the account's, and the channel's still at its `pts`.
    let expected = UpdatesState {
        channels,
        ..stored()
    };
    assert_eq!(session.updates_state().await.unwrap(), expected);
    receiver.sync_update_state().await.unwrap();
    assert_eq!(session.updates_state().await.unwrap(), expected);
}

fn difference_empty() -> Vec<u8> {
    tl::enums::updates::Difference::Empty(tl::types::updates::DifferenceEmpty { date: 10, seq: 5 })
        .to_bytes()
}

const GET_DIFFERENCE: u32 = tl::functions::updates::GetDifference::CONSTRUCTOR_ID;
const GET_STATE: u32 = tl::functions::updates::GetState::CONSTRUCTOR_ID;
const GET_CHANNEL_DIFFERENCE: u32 = tl::functions::updates::GetChannelDifference::CONSTRUCTOR_ID;

/// The constructors of the requests `calls` recorded.
fn names(calls: &Mutex<Vec<(u32, Instant)>>) -> Vec<u32> {
    calls.lock().unwrap().iter().map(|(id, _)| *id).collect()
}

#[tokio::test(start_paused = true)]
async fn a_server_failure_ends_the_accounts_difference_for_a_retry() {
    for (code, name) in [(500, "PERSISTENT_TIMESTAMP_OUTDATED"), (-503, "Timeout")] {
        let failed = Mutex::new(false);
        let (handle, calls) = scripted(Box::new(move |body| {
            let mut failed = failed.lock().unwrap();
            match u32::from_le_bytes(body[..4].try_into().unwrap()) {
                GET_DIFFERENCE if !*failed => {
                    *failed = true;
                    Err(rpc_error(code, name))
                }
                GET_DIFFERENCE => Ok(difference_empty()),
                _ => Err(rpc_error(400, "UNEXPECTED")),
            }
        }));
        let (mut receiver, _session, _updates) = receiver(handle, stored()).await;

        // Ended unanswered: nothing to hand out, and no error for the caller.
        let (updates, _, _) = receiver.next().await.unwrap();
        assert!(updates.is_empty(), "{code}");
        assert_eq!(names(&calls), [GET_DIFFERENCE], "{code}");

        // Asked again after the boxes' first backoff step, 1 s, and answered.
        receiver.next().await.unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2, "{code}");
        assert_eq!(calls[1].0, GET_DIFFERENCE, "{code}");
        assert!(calls[1].1 - calls[0].1 >= Duration::from_secs(1), "{code}");
    }
}

/// The channel of [`a_server_failure_ends_a_channels_difference_for_a_retry`], at `pts` 20.
const CHANNEL: i64 = 7000;

fn channel_difference_empty() -> Vec<u8> {
    tl::enums::updates::ChannelDifference::Empty(tl::types::updates::ChannelDifferenceEmpty {
        r#final: true,
        pts: 20,
        timeout: None,
    })
    .to_bytes()
}

#[tokio::test(start_paused = true)]
async fn a_server_failure_ends_a_channels_difference_for_a_retry() {
    for (code, name) in [(500, "WORKER_BUSY_TOO_LONG_RETRY"), (-503, "Timeout")] {
        let failed = Mutex::new(false);
        let (handle, calls) = scripted(Box::new(move |body| {
            let mut failed = failed.lock().unwrap();
            match u32::from_le_bytes(body[..4].try_into().unwrap()) {
                GET_DIFFERENCE => Ok(difference_empty()),
                GET_CHANNEL_DIFFERENCE if !*failed => {
                    *failed = true;
                    Err(rpc_error(code, name))
                }
                GET_CHANNEL_DIFFERENCE => Ok(channel_difference_empty()),
                _ => Err(rpc_error(400, "UNEXPECTED")),
            }
        }));
        let state = UpdatesState {
            channels: vec![ChannelState {
                id: CHANNEL,
                pts: 20,
            }],
            ..stored()
        };
        let (mut receiver, session, _updates) = receiver(handle, state).await;
        // The channel's difference names it by its access hash.
        session
            .cache_peer(PeerInfo::Channel {
                id: CHANNEL,
                auth: Some(PeerAuth::from_hash(1)),
                kind: Some(ChannelKind::Broadcast),
            })
            .await
            .unwrap();
        // The account's catch-up, answered, then the channel's difference.
        receiver.next().await.unwrap();
        assert!(receiver.message_box.begin_channel_difference(CHANNEL));

        // Ended unanswered: nothing to hand out, and no error for the caller.
        let (updates, _, _) = receiver.next().await.unwrap();
        assert!(updates.is_empty(), "{code}");
        assert_eq!(
            names(&calls),
            [GET_DIFFERENCE, GET_CHANNEL_DIFFERENCE],
            "{code}"
        );

        // Asked again after the boxes' first backoff step, 1 s within 20 %, and answered.
        receiver.next().await.unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 3, "{code}");
        assert_eq!(calls[2].0, GET_CHANNEL_DIFFERENCE, "{code}");
        assert!(
            calls[2].1 - calls[1].1 >= Duration::from_millis(800),
            "{code}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_refused_state_starts_over_from_the_servers_pts() {
    let (handle, calls) = scripted(Box::new(|body| {
        match u32::from_le_bytes(body[..4].try_into().unwrap()) {
            GET_DIFFERENCE => Err(rpc_error(400, "PERSISTENT_TIMESTAMP_INVALID")),
            GET_STATE => Ok(tl::enums::updates::State::State(tl::types::updates::State {
                pts: 500,
                qts: 60,
                date: 20,
                seq: 9,
                unread_count: 0,
            })
            .to_bytes()),
            _ => Err(rpc_error(400, "UNEXPECTED")),
        }
    }));
    let (mut receiver, session, _updates) = receiver(handle, stored()).await;

    receiver.next().await.unwrap();
    assert_eq!(names(&calls), [GET_DIFFERENCE, GET_STATE]);
    // Only the `pts` is the server's: the `qts`, the `date` and the `seq` are kept.
    receiver.sync_update_state().await.unwrap();
    assert_eq!(
        session.updates_state().await.unwrap(),
        UpdatesState {
            pts: 500,
            ..stored()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn any_other_failure_is_the_callers() {
    let (handle, _) = scripted(Box::new(|_| {
        Err(InvocationError::Io(
            std::io::ErrorKind::ConnectionReset.into(),
        ))
    }));
    let (mut receiver, _session, _updates) = receiver(handle, stored()).await;
    assert!(matches!(receiver.next().await, Err(InvocationError::Io(_))));
}
