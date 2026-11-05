// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The server's datacenter options, stored without panicking on ones that can't be used;
//! updates that find the channel full, which become one `Lagged`, also when the pool drops the
//! connection; and answers read in the poll whose write or flush fails, which still reach their
//! requests.

use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Waker};

use grammers_mtproto::mtp::{Deserialization, DeserializeError, Mtp, Plain};
use grammers_mtproto::transport::Intermediate;
use grammers_mtproto::{DequeBuffer, MsgId};
use grammers_session::storages::MemorySession;
use tl::Serializable;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::task::JoinHandle;

use super::*;
use crate::net::MtStream;
use crate::sender::tests::Pipe;

fn dc_option(id: i32, ipv6: bool, ip_address: &str, port: i32) -> tl::enums::DcOption {
    tl::types::DcOption {
        ipv6,
        media_only: false,
        tcpo_only: false,
        cdn: false,
        r#static: true,
        this_port_only: false,
        id,
        ip_address: ip_address.into(),
        port,
        secret: None,
    }
    .into()
}

#[tokio::test]
async fn invalid_dc_address_is_skipped() {
    let session = Arc::new(MemorySession::default());
    let pool = SenderPool::new(Arc::clone(&session), 1);
    let stored = |id| session.dc_option(id).unwrap().unwrap();
    let before = [3, 4, 5].map(stored);

    pool.runner
        .update_dc_options(&[
            dc_option(2, false, "192.0.2.2", 443),
            dc_option(2, true, "2001:db8::2", 443),
            // Not an address; an IPv4 address where an IPv6 one is due; ports outside 0..=65535.
            dc_option(3, false, "not an address", 443),
            dc_option(3, true, "", 443),
            dc_option(4, true, "192.0.2.4", 443),
            dc_option(5, false, "192.0.2.5", 65536),
            dc_option(5, true, "2001:db8::5", -1),
        ])
        .await
        .unwrap();

    let dc2 = stored(2);
    assert_eq!(dc2.ipv4, "192.0.2.2:443".parse().unwrap());
    assert_eq!(dc2.ipv6, "[2001:db8::2]:443".parse().unwrap());
    assert_eq!([3, 4, 5].map(stored), before);
}

/// What the receiver of the updates channel can tell apart.
#[derive(Debug, PartialEq)]
enum Seen {
    Update(i32),
    Lagged,
    ConnectionClosed,
}

/// An update told apart by its `pts`.
fn update(pts: i32) -> UpdatesLike {
    UpdatesLike::AffectedMessages(tl::types::messages::AffectedMessages { pts, pts_count: 1 })
}

fn seen(updates: UpdatesLike) -> Seen {
    match updates {
        UpdatesLike::AffectedMessages(affected) => Seen::Update(affected.pts),
        UpdatesLike::Lagged => Seen::Lagged,
        UpdatesLike::ConnectionClosed => Seen::ConnectionClosed,
        other => panic!("unexpected {other:?}"),
    }
}

/// Everything in the channel now.
fn drain(rx: &mut mpsc::Receiver<UpdatesLike>) -> Vec<Seen> {
    std::iter::from_fn(|| rx.try_recv().ok())
        .map(seen)
        .collect()
}

/// Poll `future` once.
fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[tokio::test]
async fn overflow_sends_one_lagged() {
    let (tx, mut rx) = mpsc::channel(2);
    let mut forwarder = UpdatesForwarder::new(tx);

    forwarder.forward(update(1));
    forwarder.forward(update(2));
    assert!(!forwarder.owes_lagged());
    // The channel is full: these two are dropped, and one `Lagged` is owed for both.
    forwarder.forward(update(3));
    forwarder.forward(update(4));
    assert!(forwarder.owes_lagged());
    {
        let mut send = pin!(forwarder.send_lagged());
        assert!(poll_once(send.as_mut()).is_pending());
        // As soon as there is room, it goes out, with no update needed to carry it.
        assert_eq!(rx.recv().await.map(seen), Some(Seen::Update(1)));
        assert!(poll_once(send).is_ready());
    }
    assert!(!forwarder.owes_lagged());
    assert_eq!(drain(&mut rx), [Seen::Update(2), Seen::Lagged]);

    // Dropped again, and room comes with the next update: the marker goes first.
    forwarder.forward(update(5));
    forwarder.forward(update(6));
    forwarder.forward(update(7));
    assert!(forwarder.owes_lagged());
    assert_eq!(drain(&mut rx), [Seen::Update(5), Seen::Update(6)]);
    forwarder.forward(update(8));
    assert!(!forwarder.owes_lagged());
    assert_eq!(drain(&mut rx), [Seen::Lagged, Seen::Update(8)]);

    // Room for the marker alone: the update after it is dropped, and owes the next one.
    forwarder.forward(update(9));
    forwarder.forward(update(10));
    forwarder.forward(update(11));
    rx.recv().await.unwrap();
    forwarder.forward(update(12));
    assert!(forwarder.owes_lagged());
    assert_eq!(drain(&mut rx), [Seen::Update(10), Seen::Lagged]);
    forwarder.send_lagged().await;
    assert_eq!(drain(&mut rx), [Seen::Lagged]);
}

#[tokio::test]
async fn ended_connection_hands_its_owed_signal_to_the_runner() {
    // A connection that ends while it owes a `Lagged`.
    let (tx, mut rx) = mpsc::channel(1);
    let mut forwarder = UpdatesForwarder::new(tx);
    forwarder.forward(update(1));
    forwarder.forward(update(2));
    assert!(matches!(forwarder.finish(false), Some(UpdatesLike::Lagged)));
    assert_eq!(drain(&mut rx), [Seen::Update(1)]);

    // The home connection fails: `ConnectionClosed` goes out if there is room, and covers a
    // `Lagged` owed; with none, the runner sends it.
    let (tx, mut rx) = mpsc::channel(1);
    let mut forwarder = UpdatesForwarder::new(tx);
    forwarder.forward(update(1));
    forwarder.forward(update(2));
    rx.recv().await.unwrap();
    assert!(forwarder.finish(true).is_none());
    assert_eq!(drain(&mut rx), [Seen::ConnectionClosed]);

    let (tx, mut rx) = mpsc::channel(1);
    let mut forwarder = UpdatesForwarder::new(tx);
    forwarder.forward(update(1));
    assert!(matches!(
        forwarder.finish(true),
        Some(UpdatesLike::ConnectionClosed)
    ));
    assert_eq!(drain(&mut rx), [Seen::Update(1)]);

    // Nothing owed: a clean end, or a receiver that is gone.
    let (tx, _) = mpsc::channel(1);
    let mut forwarder = UpdatesForwarder::new(tx);
    forwarder.forward(update(1));
    assert!(forwarder.finish(true).is_none());
    let (tx, _rx) = mpsc::channel(1);
    assert!(UpdatesForwarder::new(tx).finish(false).is_none());
}

#[tokio::test]
async fn runner_sends_the_signal_an_ended_connection_owed() {
    let params = ConnectionParams {
        updates_channel_capacity: NonZeroUsize::MIN,
        ..Default::default()
    };
    let mut pool = SenderPool::with_configuration(Arc::new(MemorySession::default()), 1, params);
    pool.runner.updates_tx.try_send(update(1)).unwrap();
    // A connection ends owing a `Lagged`, while the channel is full.
    pool.runner
        .connection_pool
        .spawn(async { Some(UpdatesLike::Lagged) });
    let runner = tokio::spawn(pool.runner.run());

    let mut next = async || {
        let next = timeout(Duration::from_secs(10), pool.updates.recv());
        seen(next.await.unwrap().unwrap())
    };
    assert_eq!(next().await, Seen::Update(1));
    assert_eq!(next().await, Seen::Lagged);
    assert!(pool.handle.quit());
    runner.await.unwrap();
}

/// Reads every packet as one update, as `Encrypted` does a real one; it sends nothing.
struct UpdatesOnly;

impl Mtp for UpdatesOnly {
    fn push(&mut self, _: &mut DequeBuffer<u8>, _: &[u8]) -> Option<MsgId> {
        None
    }

    fn finalize(&mut self, _: &mut DequeBuffer<u8>) -> Option<MsgId> {
        None
    }

    fn deserialize(
        &mut self,
        payload: &mut [u8],
    ) -> Result<Vec<Deserialization>, DeserializeError> {
        Ok(vec![Deserialization::Update(payload.to_vec())])
    }
}

/// [`update`] as the server sends it, framed by the intermediate transport.
fn packet(pts: i32) -> Vec<u8> {
    let update =
        tl::enums::messages::AffectedMessages::Messages(tl::types::messages::AffectedMessages {
            pts,
            pts_count: 1,
        })
        .to_bytes();
    [(update.len() as u32).to_le_bytes().as_slice(), &update].concat()
}

type Answer = oneshot::Receiver<Result<InvokeResponse, InvocationError>>;

/// One connection running as the pool runs it, over an in-memory stream whose other end is the
/// test's, with an updates channel that holds one item.
struct Connection {
    server: DuplexStream,
    updates: mpsc::Receiver<UpdatesLike>,
    rpc_tx: mpsc::UnboundedSender<Rpc>,
    task: JoinHandle<Option<UpdatesLike>>,
}

impl Connection {
    async fn run(home_sender: bool) -> Self {
        let (client, server) = tokio::io::duplex(1 << 16);
        Self::over(client, server, UpdatesOnly, home_sender).await
    }

    /// A connection over `stream`, which leads to `server`.
    async fn over<M: Mtp + Send + 'static>(
        stream: impl MtStream,
        server: DuplexStream,
        mtp: M,
        home_sender: bool,
    ) -> Self {
        let target = DialTarget {
            dc_id: 2,
            kind: ConnKind::Main,
            ipv4: None,
            ipv6: None,
        };
        let sender = Sender::connect(Intermediate::new(), mtp, &Pipe::new(stream), &target)
            .await
            .unwrap();
        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        let (updates_tx, updates) = mpsc::channel(1);
        let task = tokio::spawn(run_sender(
            sender,
            rpc_rx,
            UpdatesForwarder::new(updates_tx),
            home_sender,
        ));
        Self {
            server,
            updates,
            rpc_tx,
            task,
        }
    }

    /// The next item in the updates channel. It comes by itself, or not at all.
    async fn next(&mut self) -> Seen {
        let next = timeout(Duration::from_secs(10), self.updates.recv());
        seen(next.await.unwrap().unwrap())
    }

    /// Hand the connection a request.
    fn send(&self, body: Vec<u8>) -> Answer {
        let (tx, rx) = oneshot::channel();
        self.rpc_tx.send(Rpc { body, tx }).unwrap();
        rx
    }
}

#[tokio::test]
async fn connection_sends_lagged_with_no_update_to_carry_it() {
    let mut connection = Connection::run(true).await;

    // One fits; the other two find the channel full.
    let packets = [packet(1), packet(2), packet(3)].concat();
    connection.server.write_all(&packets).await.unwrap();
    assert_eq!(connection.next().await, Seen::Update(1));
    // That made room, and the server sends nothing more: the marker goes out by itself.
    assert_eq!(connection.next().await, Seen::Lagged);

    // The connection goes on.
    connection.server.write_all(&packet(4)).await.unwrap();
    assert_eq!(connection.next().await, Seen::Update(4));

    // The server hangs up, and the home connection says so.
    drop(connection.server);
    assert_eq!(
        seen(connection.updates.recv().await.unwrap()),
        Seen::ConnectionClosed
    );
    assert!(connection.task.await.unwrap().is_none());
    drop(connection.rpc_tx);
}

#[tokio::test]
async fn connection_ending_on_a_full_channel_returns_what_it_owes() {
    // The home connection fails while the channel is full: its `ConnectionClosed` is owed.
    let mut connection = Connection::run(true).await;
    let packets = [packet(1), packet(2)].concat();
    connection.server.write_all(&packets).await.unwrap();
    drop(connection.server);
    assert!(matches!(
        connection.task.await.unwrap(),
        Some(UpdatesLike::ConnectionClosed)
    ));
    assert_eq!(connection.updates.try_recv().map(seen), Ok(Seen::Update(1)));

    // Any other connection says nothing of its closing, but still owes the updates it dropped.
    let mut connection = Connection::run(false).await;
    connection.server.write_all(&packets).await.unwrap();
    drop(connection.server);
    assert!(matches!(
        connection.task.await.unwrap(),
        Some(UpdatesLike::Lagged)
    ));
    assert_eq!(connection.updates.try_recv().map(seen), Ok(Seen::Update(1)));

    // The pool drops a connection that owes a `Lagged`: not a closed connection, but still owed.
    let mut connection = Connection::run(true).await;
    connection.server.write_all(&packets).await.unwrap();
    while connection.updates.is_empty() {
        tokio::task::yield_now().await;
    }
    drop(connection.rpc_tx);
    assert!(matches!(
        connection.task.await.unwrap(),
        Some(UpdatesLike::Lagged)
    ));
}

/// A pool whose updates channel holds one item, with one connection to `dc_id` in it, as
/// `create_connection` puts one there, over an in-memory stream that leads to the returned end.
async fn pool_with_connection(dc_id: i32) -> (SenderPool, DuplexStream) {
    let params = ConnectionParams {
        updates_channel_capacity: NonZeroUsize::MIN,
        ..Default::default()
    };
    let mut pool = SenderPool::with_configuration(Arc::new(MemorySession::default()), 1, params);
    let (client, server) = tokio::io::duplex(1 << 16);
    let target = DialTarget {
        dc_id,
        kind: ConnKind::Main,
        ipv4: None,
        ipv6: None,
    };
    let sender = Sender::connect(
        Intermediate::new(),
        UpdatesOnly,
        &Pipe::new(client),
        &target,
    )
    .await
    .unwrap();
    let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
    let abort_handle = pool.runner.connection_pool.spawn(run_sender(
        sender,
        rpc_rx,
        UpdatesForwarder::new(pool.runner.updates_tx.clone()),
        true,
    ));
    pool.runner.connections.push(ConnectionInfo {
        dc_id,
        rpc_tx,
        abort_handle,
    });
    (pool, server)
}

/// Wait until every other task waits: paused time moves on only then.
async fn idle() {
    sleep(Duration::from_secs(1)).await;
}

#[tokio::test(start_paused = true)]
async fn disconnected_connection_hands_back_the_lagged_it_owed() {
    let (pool, mut server) = pool_with_connection(2).await;
    let SenderPool {
        runner,
        handle,
        mut updates,
    } = pool;
    // One fits; the other finds the channel full, and a `Lagged` is owed.
    let packets = [packet(1), packet(2)].concat();
    server.write_all(&packets).await.unwrap();
    idle().await;

    // The pool drops the connection while the channel is still full.
    assert!(handle.disconnect_from_dc(2));
    let runner = tokio::spawn(runner.run());
    idle().await;

    assert_eq!(seen(updates.recv().await.unwrap()), Seen::Update(1));
    let lagged = timeout(Duration::from_secs(10), updates.recv()).await;
    assert_eq!(lagged.unwrap().map(seen), Some(Seen::Lagged));
    assert!(handle.quit());
    runner.await.unwrap();
}

#[tokio::test]
async fn quit_right_after_disconnect_ends_the_runner() {
    let (pool, _server) = pool_with_connection(2).await;
    assert!(pool.handle.disconnect_from_dc(2));
    assert!(pool.handle.quit());
    // An aborted connection's task would be a cancelled one, which the runner's last join fails on.
    timeout(Duration::from_secs(10), pool.runner.run())
        .await
        .unwrap();
}

fn ping() -> Vec<u8> {
    tl::functions::Ping { ping_id: 1 }.to_bytes()
}

fn pong() -> Vec<u8> {
    tl::enums::Pong::Pong(tl::types::Pong {
        msg_id: 1,
        ping_id: 1,
    })
    .to_bytes()
}

/// [`pong`] as the server's plain answer, framed by the intermediate transport.
fn pong_packet() -> Vec<u8> {
    let body = pong();
    let mut payload = Vec::new();
    payload.extend(0i64.to_le_bytes());
    // A server message's identifier answering a client's is 1 modulo 4.
    payload.extend(5i64.to_le_bytes());
    payload.extend((body.len() as i32).to_le_bytes());
    payload.extend(body);
    [(payload.len() as u32).to_le_bytes().as_slice(), &payload].concat()
}

/// What a request gets. It comes by itself, or not at all.
async fn answer(rx: Answer) -> Result<InvokeResponse, InvocationError> {
    timeout(Duration::from_secs(10), rx).await.unwrap().unwrap()
}

/// Let the connection's task go as far as it can.
async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn answer_read_with_a_failed_write_reaches_its_request() {
    let (client, server) = tokio::io::duplex(64);
    let mut connection = Connection::over(client, server, Plain::new(), true).await;
    let answered = connection.send(ping());
    // The transport's tag and length, the plain header and the ping.
    let mut request = [0; 4 + 4 + 20 + 12];
    connection.server.read_exact(&mut request).await.unwrap();
    settle().await;

    // Too big for the pipe, so its write stays pending.
    let unanswered = connection.send(
        tl::functions::upload::SaveFilePart {
            file_id: 1,
            file_part: 0,
            bytes: vec![0; 4096],
        }
        .to_bytes(),
    );
    settle().await;

    // One poll finds the answer to read and the write failed.
    connection.server.write_all(&pong_packet()).await.unwrap();
    drop(connection.server);

    assert_eq!(answer(answered).await.unwrap(), pong());
    assert!(matches!(
        answer(unanswered).await,
        Err(InvocationError::Io(_))
    ));
    let closed = timeout(Duration::from_secs(10), connection.updates.recv());
    assert_eq!(seen(closed.await.unwrap().unwrap()), Seen::ConnectionClosed);
    assert!(connection.task.await.unwrap().is_none());
}

/// A stream that fails every flush, and has nothing to read until one has failed.
struct FlushFails {
    stream: DuplexStream,
    failed: bool,
}

impl AsyncWrite for FlushFails {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.failed = true;
        Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl AsyncRead for FlushFails {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // No waker: the poll whose flush fails reads next.
        if !self.failed {
            return Poll::Pending;
        }
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

#[tokio::test]
async fn answer_read_with_a_failed_flush_reaches_its_request() {
    let (client, mut server) = tokio::io::duplex(1 << 16);
    // The answer waits in the pipe: the poll whose flush fails also reads it.
    server.write_all(&pong_packet()).await.unwrap();
    let stream = FlushFails {
        stream: client,
        failed: false,
    };
    let mut connection = Connection::over(stream, server, Plain::new(), true).await;

    let answered = connection.send(ping());
    assert_eq!(answer(answered).await.unwrap(), pong());
    assert_eq!(connection.next().await, Seen::ConnectionClosed);
    assert!(connection.task.await.unwrap().is_none());
}
