// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The receiver keeps the session's state the message boxes': the account's state set from
//! `updates.getState` is stored with the channels' boxes loaded without it. No network: a script
//! plays the pool's runner ([`scripted`]), answering each request.

use grammers_session::Session;
use grammers_session::storages::{MemorySession, erase};
use grammers_session::types::{ChannelState, UpdateState, UpdatesState};

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
