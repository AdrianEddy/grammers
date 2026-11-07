// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Answers from the server that match no sent request, or can't be read, never panic the sender.
//! The sender talks to an in-memory stream that never answers: the tests feed it what the MTP
//! returns.

use std::sync::Mutex;

use grammers_mtproto::mtp::{BadMessage, Deserialization, DeserializeError, Encrypted, RpcResult};
use grammers_mtproto::transport::Full;
use tokio::io::DuplexStream;
use tokio::sync::oneshot::Receiver;

use super::*;
use crate::net::{ConnKind, DialFuture};

type TestSender = Sender<Full, Encrypted>;
type Answer = Receiver<Result<Vec<u8>, InvocationError>>;

/// Dials the one stream it holds.
pub(crate) struct Pipe(Mutex<Option<Box<dyn MtStream>>>);

impl Pipe {
    pub(crate) fn new(stream: impl MtStream) -> Self {
        Self(Mutex::new(Some(Box::new(stream))))
    }
}

impl Dialer for Pipe {
    fn dial<'a>(&'a self, _: &'a DialTarget) -> DialFuture<'a> {
        let stream = self.0.lock().unwrap().take().unwrap();
        Box::pin(async move { Ok(stream) })
    }
}

/// A sender connected to an in-memory stream, whose other end is returned to keep it open.
async fn sender() -> (TestSender, DuplexStream) {
    let (client, server) = tokio::io::duplex(1 << 16);
    let target = DialTarget {
        dc_id: 2,
        kind: ConnKind::Main,
        ipv4: None,
        ipv6: None,
    };
    let sender = Sender::connect(
        Full::new(),
        Encrypted::build().finish([0; 256]),
        &Pipe::new(client),
        &target,
    )
    .await
    .unwrap();
    (sender, server)
}

/// Enqueue a request and serialize it, as [`Sender::step`] does before it writes; not sent yet.
fn serialized(sender: &mut TestSender) -> (MsgId, Answer) {
    let (tx, rx) = oneshot::channel();
    sender.enqueue_body(tl::functions::Ping { ping_id: 1 }.to_bytes(), tx);
    sender.try_fill_write();
    match sender.requests.as_slice() {
        [
            Request {
                state: RequestState::Serialized(pair),
                ..
            },
        ] => (pair.msg_id, rx),
        _ => panic!("one serialized request"),
    }
}

/// Write the whole buffer, as [`Sender::step`] does once the socket took it.
fn sent(sender: &mut TestSender) {
    let n = sender.state.io_buffers().1.len();
    sender.on_net_write(n);
    assert!(matches!(
        sender.requests.as_slice(),
        [Request {
            state: RequestState::Sent(_),
            ..
        }]
    ));
}

fn receive(sender: &mut TestSender, deserialization: Deserialization) {
    let mut updates = Vec::new();
    sender.process_mtp_buffer(vec![deserialization], &mut updates);
    assert!(updates.is_empty());
}

fn result(msg_id: MsgId, body: Vec<u8>) -> Deserialization {
    Deserialization::RpcResult(RpcResult { msg_id, body })
}

/// The answer to a ping, as the server sends it.
fn pong(msg_id: MsgId) -> Vec<u8> {
    tl::enums::Pong::Pong(tl::types::Pong {
        msg_id: msg_id.into_inner(),
        ping_id: 1,
    })
    .to_bytes()
}

#[tokio::test]
async fn bad_msg_for_unsent_request_is_ignored() {
    let (mut sender, _server) = sender().await;
    let (msg_id, mut answer) = serialized(&mut sender);

    // > The client must check that it has, in fact, recently sent a message with the specified
    // > msg_id
    // It hasn't, so the request stays as it was, and is still sent.
    for code in [16, 48, 64] {
        receive(
            &mut sender,
            Deserialization::BadMessage(BadMessage { msg_id, code }),
        );
        assert!(answer.try_recv().is_err());
    }
    sent(&mut sender);

    // Once sent, a notice about it is acted upon: here, one that can't be retried.
    receive(
        &mut sender,
        Deserialization::BadMessage(BadMessage { msg_id, code: 64 }),
    );
    assert!(matches!(
        answer.try_recv(),
        Ok(Err(InvocationError::Dropped(_)))
    ));
    assert!(sender.requests.is_empty());
}

#[tokio::test]
async fn response_for_unsent_request_is_ignored() {
    let (mut sender, _server) = sender().await;
    let (msg_id, mut answer) = serialized(&mut sender);

    // An answer to a request not sent yet can't be one.
    receive(&mut sender, result(msg_id, pong(msg_id)));
    receive(
        &mut sender,
        Deserialization::RpcError(mtp::RpcResultError {
            msg_id,
            error: tl::types::RpcError {
                error_code: 500,
                error_message: "INTERNAL".into(),
            },
        }),
    );
    receive(
        &mut sender,
        Deserialization::Failure(mtp::DeserializationFailure {
            msg_id,
            error: DeserializeError::DecompressionFailed,
        }),
    );
    assert!(answer.try_recv().is_err());
    assert_eq!(sender.requests.len(), 1);
    // An answer's own updates aren't matched with it either.
    assert!(sender.peek_request(msg_id).is_none());

    // The request is still sent, and its real answer delivered.
    sent(&mut sender);
    assert!(sender.peek_request(msg_id).is_some());
    receive(&mut sender, result(msg_id, pong(msg_id)));
    assert_eq!(answer.try_recv().unwrap().unwrap(), pong(msg_id));
    assert!(sender.requests.is_empty());
}

#[tokio::test]
async fn short_rpc_result_is_error() {
    for len in 0..4 {
        let (mut sender, _server) = sender().await;
        let (msg_id, mut answer) = serialized(&mut sender);
        sent(&mut sender);

        // Too short to hold any TL object's constructor identifier.
        receive(&mut sender, result(msg_id, vec![1; len]));
        assert!(matches!(
            answer.try_recv(),
            Ok(Err(InvocationError::Deserialize(
                DeserializeError::MessageBufferTooSmall
            )))
        ));
        assert!(sender.requests.is_empty());
    }
}

#[tokio::test]
async fn request_body_that_is_not_a_tl_object_is_dropped() {
    let (mut sender, _server) = sender().await;
    for body in [Vec::new(), vec![1, 2, 3], vec![1, 2, 3, 4, 5]] {
        let (tx, mut rx) = oneshot::channel();
        sender.enqueue_body(body, tx);
        assert!(matches!(
            rx.try_recv(),
            Ok(Err(InvocationError::Dropped(_)))
        ));
    }
    assert!(sender.requests.is_empty());
}

#[tokio::test]
async fn session_reset_sends_every_unanswered_request_again() {
    let (mut sender, _server) = sender().await;
    let (old, mut sent_answer) = serialized(&mut sender);
    sent(&mut sender);
    // A second request, serialized in the old session and not written yet.
    let (tx, mut unsent_answer) = oneshot::channel();
    sender.enqueue_body(tl::functions::Ping { ping_id: 2 }.to_bytes(), tx);
    sender.try_fill_write();
    assert!(matches!(
        sender.requests[1].state,
        RequestState::Serialized(_)
    ));

    // The server answers nothing of the old session in the new one: both go again.
    receive(&mut sender, Deserialization::SessionReset);
    assert!(
        sender
            .requests
            .iter()
            .all(|request| matches!(request.state, RequestState::NotSerialized))
    );
    // An answer named by an old message identifier is no answer to either now.
    receive(&mut sender, result(old, pong(old)));
    assert!(sent_answer.try_recv().is_err());
    assert!(unsent_answer.try_recv().is_err());

    // The old session's bytes still go out, then both requests again, with new identifiers.
    let n = sender.state.io_buffers().1.len();
    sender.on_net_write(n);
    sender.try_fill_write();
    let n = sender.state.io_buffers().1.len();
    sender.on_net_write(n);
    let resent: Vec<MsgId> = sender
        .requests
        .iter()
        .map(|request| match &request.state {
            RequestState::Sent(pair) => pair.msg_id,
            _ => panic!("every request sent again"),
        })
        .collect();
    assert_eq!(resent.len(), 2);
    assert!(!resent.contains(&old));
    for msg_id in resent {
        receive(&mut sender, result(msg_id, pong(msg_id)));
    }
    assert!(sent_answer.try_recv().unwrap().is_ok());
    assert!(unsent_answer.try_recv().unwrap().is_ok());
    assert!(sender.requests.is_empty());
}
