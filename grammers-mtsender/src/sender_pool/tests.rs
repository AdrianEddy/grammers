// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The server's datacenter options, stored without panicking on ones that can't be used;
//! updates that find the channel full, which become one `Lagged`, also when the pool drops the
//! connection; the connections' opens and closes, which keep their order while the channel is
//! full; a new session, a gap on the home connection alone; answers read in the poll whose
//! write or flush fails, which still reach their requests; a request that reaches a connection
//! as it ends, which goes to a new one, once; a connection the pool closes, which fails the
//! requests whose bytes left and hands back the others, those still queued for it unwritten, and
//! whose disconnect is told once its task has ended, before what it handed back dials, which a
//! later disconnect that waits for it too holds until it is told; a request held while the
//! dialer's permit is shut, which holds nothing else back, dials once the permit comes, and fails
//! once its wait runs out or the pool quits. A connection that fails gives every request it held
//! its error, cause and all. No pool here reaches anything but in-memory streams: every dial is
//! refused ([`RefusingDialer`]). Other modules' tests drive a handle whose runner is a script
//! ([`scripted`]), as the handle's channel and its requests are this module's alone.

use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use grammers_mtproto::mtp::{Deserialization, DeserializeError, Mtp, Plain};
use grammers_mtproto::transport::Intermediate;
use grammers_mtproto::{DequeBuffer, MsgId};
use grammers_session::storages::MemorySession;
use tl::{Identifiable, Serializable};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::*;
use crate::net::{DialFuture, Dialer, MtStream};
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
    ConnectionOpened,
    NewSession,
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
        UpdatesLike::ConnectionOpened => Seen::ConnectionOpened,
        UpdatesLike::NewSession => Seen::NewSession,
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
    let mut forwarder = UpdatesForwarder::new(ConnectionSignals::new(tx));

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

/// The signals owed now, each sent as the channel takes it, in order.
async fn owed(signals: &ConnectionSignals, rx: &mut mpsc::Receiver<UpdatesLike>) -> Vec<Seen> {
    let mut seen = Vec::new();
    while signals.owes() {
        signals.send_owed().await;
        seen.extend(drain(rx));
    }
    seen
}

#[tokio::test]
async fn ended_connection_owes_its_signal() {
    // A connection that ends while it owes a `Lagged`: it goes once there is room.
    let (tx, mut rx) = mpsc::channel(1);
    let signals = ConnectionSignals::new(tx);
    let mut forwarder = UpdatesForwarder::new(signals.clone());
    forwarder.forward(update(1));
    forwarder.forward(update(2));
    forwarder.finish(false);
    assert_eq!(drain(&mut rx), [Seen::Update(1)]);
    assert_eq!(owed(&signals, &mut rx).await, [Seen::Lagged]);

    // The home connection fails: `ConnectionClosed` goes out if there is room, and covers a
    // `Lagged` owed; with none, it is owed.
    let (tx, mut rx) = mpsc::channel(1);
    let signals = ConnectionSignals::new(tx);
    let mut forwarder = UpdatesForwarder::new(signals.clone());
    forwarder.forward(update(1));
    forwarder.forward(update(2));
    rx.recv().await.unwrap();
    forwarder.finish(true);
    assert!(!signals.owes());
    assert_eq!(drain(&mut rx), [Seen::ConnectionClosed]);

    let (tx, mut rx) = mpsc::channel(1);
    let signals = ConnectionSignals::new(tx);
    let mut forwarder = UpdatesForwarder::new(signals.clone());
    forwarder.forward(update(1));
    forwarder.finish(true);
    assert_eq!(drain(&mut rx), [Seen::Update(1)]);
    assert_eq!(owed(&signals, &mut rx).await, [Seen::ConnectionClosed]);

    // Nothing owed: a clean end, or a receiver that is gone.
    let (tx, _) = mpsc::channel(1);
    let signals = ConnectionSignals::new(tx);
    let mut forwarder = UpdatesForwarder::new(signals.clone());
    forwarder.forward(update(1));
    forwarder.finish(true);
    assert!(!signals.owes());
    let (tx, _rx) = mpsc::channel(1);
    let signals = ConnectionSignals::new(tx);
    UpdatesForwarder::new(signals.clone()).finish(false);
    assert!(!signals.owes());
}

#[tokio::test]
async fn opens_and_closes_keep_their_order_while_the_channel_is_full() {
    let (tx, mut rx) = mpsc::channel(1);
    let signals = ConnectionSignals::new(tx);
    // With room, an open goes at once.
    signals.send(Signal::Opened);
    assert_eq!(drain(&mut rx), [Seen::ConnectionOpened]);

    // Full: a close, then the next connection's open, are owed in that order; a `Lagged` the
    // close covers adds nothing; and once there is room, a signal still goes behind them.
    signals.tx.try_send(update(1)).unwrap();
    signals.send(Signal::Closed);
    signals.send(Signal::Opened);
    assert_eq!(drain(&mut rx), [Seen::Update(1)]);
    signals.send(Signal::Lagged);
    assert_eq!(drain(&mut rx), []);
    assert_eq!(
        owed(&signals, &mut rx).await,
        [Seen::ConnectionClosed, Seen::ConnectionOpened]
    );

    // An open owed, then its connection's close: the receiver never heard of it, and reads the
    // close alone, which also takes the place of an owed `Lagged`.
    signals.tx.try_send(update(2)).unwrap();
    signals.send(Signal::Lagged);
    signals.send(Signal::Opened);
    signals.send(Signal::Closed);
    assert_eq!(drain(&mut rx), [Seen::Update(2)]);
    assert_eq!(owed(&signals, &mut rx).await, [Seen::ConnectionClosed]);

    // A `Lagged` owed before an open is read before it, as it happened.
    signals.tx.try_send(update(3)).unwrap();
    signals.send(Signal::Lagged);
    signals.send(Signal::Opened);
    assert_eq!(drain(&mut rx), [Seen::Update(3)]);
    assert_eq!(
        owed(&signals, &mut rx).await,
        [Seen::Lagged, Seen::ConnectionOpened]
    );
    assert!(!signals.owes());
}

#[tokio::test]
async fn a_close_goes_behind_the_open_owed_before_it() {
    // The home connection's open is owed (the channel is full), and the connection fails before
    // the receiver reads anything: the close comes after the open would, so the receiver reads
    // the close alone, whatever room the channel has when the connection ends.
    let mut connection = Connection::run(true).await;
    connection.signals.tx.try_send(update(1)).unwrap();
    connection.signals.send(Signal::Opened);
    assert_eq!(connection.next().await, Seen::Update(1));
    drop(connection.server);
    connection.task.await.unwrap();
    let signals = connection.signals.clone();
    assert_eq!(
        owed(&signals, &mut connection.updates).await,
        [Seen::ConnectionClosed]
    );
    drop(connection.rpc_tx);
}

#[tokio::test]
async fn runner_sends_the_signal_an_ended_connection_owed() {
    let params = ConnectionParams {
        updates_channel_capacity: NonZeroUsize::MIN,
        ..Default::default()
    };
    let mut pool = SenderPool::with_configuration(Arc::new(MemorySession::default()), 1, params);
    pool.runner.signals.tx.try_send(update(1)).unwrap();
    // A connection ends owing a `Lagged`, while the channel is full.
    let signals = pool.runner.signals.clone();
    pool.runner.connection_pool.spawn(async move {
        signals.send(Signal::Lagged);
        ConnectionEnd {
            dc_id: 2,
            unsent: Vec::new(),
            error: None,
        }
    });
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

/// Reads every packet as one update, as `Encrypted` does a real one, but `new_session_created`,
/// which it reads as the new session it reports; it sends nothing.
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
        let new_session = tl::types::NewSessionCreated::CONSTRUCTOR_ID.to_le_bytes();
        Ok(vec![if payload.starts_with(&new_session) {
            Deserialization::NewSession
        } else {
            Deserialization::Update(payload.to_vec())
        }])
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

/// `new_session_created` as the server sends it, framed by the intermediate transport.
fn new_session_packet() -> Vec<u8> {
    let created = tl::enums::NewSession::Created(tl::types::NewSessionCreated {
        first_msg_id: 4,
        unique_id: 1,
        server_salt: 0,
    })
    .to_bytes();
    [(created.len() as u32).to_le_bytes().as_slice(), &created].concat()
}

type Answer = oneshot::Receiver<Result<InvokeResponse, InvocationError>>;

/// One connection running as the pool runs it, over an in-memory stream whose other end is the
/// test's, with an updates channel that holds one item.
struct Connection {
    server: DuplexStream,
    updates: mpsc::Receiver<UpdatesLike>,
    rpc_tx: mpsc::UnboundedSender<Rpc>,
    task: JoinHandle<ConnectionEnd>,
    /// The channel's sender and the signals it is owed, which the connection shares.
    signals: ConnectionSignals,
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
        let signals = ConnectionSignals::new(updates_tx);
        let task = tokio::spawn(run_sender(
            2,
            sender,
            rpc_rx,
            UpdatesForwarder::new(signals.clone()),
            home_sender,
        ));
        Self {
            server,
            updates,
            rpc_tx,
            task,
            signals,
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
        let rpc = Rpc {
            body,
            tx,
            handed_back: false,
        };
        self.rpc_tx.send(rpc).unwrap();
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
    connection.task.await.unwrap();
    assert!(!connection.signals.owes());
    drop(connection.rpc_tx);
}

#[tokio::test]
async fn new_session_is_a_gap_on_the_home_connection_alone() {
    // The home connection gets the account's updates: those the server sent to the session it
    // replaced may be lost.
    let mut home = Connection::run(true).await;
    home.server.write_all(&new_session_packet()).await.unwrap();
    assert_eq!(home.next().await, Seen::NewSession);

    // Any other connection's new session says nothing of them: the update after it comes first.
    let mut other = Connection::run(false).await;
    let packets = [new_session_packet(), packet(1)].concat();
    other.server.write_all(&packets).await.unwrap();
    assert_eq!(other.next().await, Seen::Update(1));
}

#[tokio::test]
async fn connection_ending_on_a_full_channel_owes_its_signal() {
    // The home connection fails while the channel is full: its `ConnectionClosed` is owed.
    let mut connection = Connection::run(true).await;
    let packets = [packet(1), packet(2)].concat();
    connection.server.write_all(&packets).await.unwrap();
    drop(connection.server);
    connection.task.await.unwrap();
    assert_eq!(connection.updates.try_recv().map(seen), Ok(Seen::Update(1)));
    let signals = connection.signals.clone();
    assert_eq!(
        owed(&signals, &mut connection.updates).await,
        [Seen::ConnectionClosed]
    );

    // Any other connection says nothing of its closing, but still owes the updates it dropped.
    let mut connection = Connection::run(false).await;
    connection.server.write_all(&packets).await.unwrap();
    drop(connection.server);
    connection.task.await.unwrap();
    assert_eq!(connection.updates.try_recv().map(seen), Ok(Seen::Update(1)));
    let signals = connection.signals.clone();
    assert_eq!(
        owed(&signals, &mut connection.updates).await,
        [Seen::Lagged]
    );

    // The pool drops a connection that owes a `Lagged`: not a closed connection, but still owed.
    let mut connection = Connection::run(true).await;
    connection.server.write_all(&packets).await.unwrap();
    while connection.updates.is_empty() {
        tokio::task::yield_now().await;
    }
    drop(connection.rpc_tx);
    connection.task.await.unwrap();
    assert_eq!(connection.updates.try_recv().map(seen), Ok(Seen::Update(1)));
    let signals = connection.signals.clone();
    assert_eq!(
        owed(&signals, &mut connection.updates).await,
        [Seen::Lagged]
    );
}

/// Refuses every dial, as a datacenter that is down does, and records the datacenter of each.
#[derive(Default)]
struct RefusingDialer {
    dialed: Mutex<Vec<i32>>,
}

impl Dialer for RefusingDialer {
    fn dial<'a>(&'a self, target: &'a DialTarget) -> DialFuture<'a> {
        self.dialed.lock().unwrap().push(target.dc_id);
        Box::pin(async { Err(io::ErrorKind::ConnectionRefused.into()) })
    }
}

/// A pool whose updates channel holds one item and whose dials are refused.
fn refusing_pool() -> (SenderPool, Arc<RefusingDialer>) {
    let dialer = Arc::new(RefusingDialer::default());
    let params = ConnectionParams {
        updates_channel_capacity: NonZeroUsize::MIN,
        dialer: Arc::clone(&dialer) as Arc<dyn Dialer>,
        ..Default::default()
    };
    let pool = SenderPool::with_configuration(Arc::new(MemorySession::default()), 1, params);
    (pool, dialer)
}

/// A [`refusing_pool`] with one home connection to `dc_id` in it, as `create_connection` puts one
/// there, over `stream`, its messages framed by `mtp`; `rpc` is the channel its requests come
/// through.
async fn pool_with_connection_over<M: Mtp + Send + 'static>(
    dc_id: i32,
    stream: impl MtStream,
    mtp: M,
    (rpc_tx, rpc_rx): (mpsc::UnboundedSender<Rpc>, mpsc::UnboundedReceiver<Rpc>),
) -> (SenderPool, Arc<RefusingDialer>) {
    let (mut pool, dialer) = refusing_pool();
    let target = DialTarget {
        dc_id,
        kind: ConnKind::Main,
        ipv4: None,
        ipv6: None,
    };
    let sender = Sender::connect(Intermediate::new(), mtp, &Pipe::new(stream), &target)
        .await
        .unwrap();
    let task = pool.runner.connection_pool.spawn(run_sender(
        dc_id,
        sender,
        rpc_rx,
        UpdatesForwarder::new(pool.runner.signals.clone()),
        true,
    ));
    pool.runner.tasks.push((task.id(), dc_id));
    pool.runner
        .connections
        .push(ConnectionInfo { dc_id, rpc_tx });
    (pool, dialer)
}

/// [`pool_with_connection_over`] an in-memory stream that leads to the returned end.
async fn pool_with_connection(dc_id: i32) -> (SenderPool, DuplexStream, Arc<RefusingDialer>) {
    let (client, server) = tokio::io::duplex(1 << 16);
    let (pool, dialer) =
        pool_with_connection_over(dc_id, client, UpdatesOnly, mpsc::unbounded_channel()).await;
    (pool, server, dialer)
}

/// Wait until every other task waits: paused time moves on only then.
async fn idle() {
    sleep(Duration::from_secs(1)).await;
}

#[tokio::test(start_paused = true)]
async fn disconnected_connection_hands_back_the_lagged_it_owed() {
    let (pool, mut server, _) = pool_with_connection(2).await;
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
    let disconnected = handle.disconnect_from_dc(2);
    let runner = tokio::spawn(runner.run());
    assert!(disconnected.await);

    assert_eq!(seen(updates.recv().await.unwrap()), Seen::Update(1));
    let lagged = timeout(Duration::from_secs(10), updates.recv()).await;
    assert_eq!(lagged.unwrap().map(seen), Some(Seen::Lagged));
    assert!(handle.quit());
    runner.await.unwrap();
}

#[tokio::test]
async fn quit_right_after_disconnect_ends_the_runner() {
    let (pool, _server, _) = pool_with_connection(2).await;
    let disconnected = pool.handle.disconnect_from_dc(2);
    assert!(pool.handle.quit());
    // An aborted connection's task would be a cancelled one, which the runner's last join fails on.
    timeout(Duration::from_secs(10), pool.runner.run())
        .await
        .unwrap();
    // The connection ended with the runner, which says so.
    assert!(disconnected.await);
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
    connection.task.await.unwrap();
    assert!(!connection.signals.owes());
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
    connection.task.await.unwrap();
    assert!(!connection.signals.owes());
}

#[tokio::test]
async fn retry_reaching_the_runner_before_it_joins_the_ended_connection_gets_a_new_one() {
    let (mut pool, server, dialer) = pool_with_connection(2).await;
    let invoke = |tx| Request::Invoke {
        dc_id: 2,
        body: ping(),
        tx,
    };

    // The server hangs up with a request in flight: its caller learns the connection failed.
    let (tx, first) = oneshot::channel();
    assert!(pool.runner.process_request(invoke(tx)).await.is_continue());
    settle().await;
    drop(server);
    assert!(matches!(answer(first).await, Err(InvocationError::Io(_))));
    settle().await;
    assert!(pool.runner.connections[0].rpc_tx.is_closed());

    // The caller retries at once, and the retry reaches the runner before the runner joins the
    // ended connection's task, which still lists it: the order a multi-thread runtime allows,
    // played here by a runner that doesn't run. It goes to a new connection, whose outcome is its
    // answer (the dial is refused), rather than into the ended one.
    let (tx, retry) = oneshot::channel();
    assert!(pool.runner.process_request(invoke(tx)).await.is_continue());
    assert!(matches!(
        answer(retry).await,
        Err(InvocationError::Io(error)) if error.kind() == io::ErrorKind::ConnectionRefused
    ));
    assert_eq!(*dialer.dialed.lock().unwrap(), [2]);
}

/// A stream whose first read fails, and first queues `rpc` in its connection's channel: as the
/// runner, on another thread, sends a retry to a connection whose task has just failed its
/// requests and is ending.
struct QueuesAndFails {
    rpc_tx: mpsc::UnboundedSender<Rpc>,
    rpc: Option<Rpc>,
}

impl AsyncRead for QueuesAndFails {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(rpc) = self.rpc.take() {
            assert!(
                self.rpc_tx.send(rpc).is_ok(),
                "the connection's channel is open"
            );
        }
        Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()))
    }
}

impl AsyncWrite for QueuesAndFails {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn request_queued_as_its_connection_ends_goes_to_a_new_one() {
    let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
    let (tx, queued) = oneshot::channel();
    let stream = QueuesAndFails {
        rpc_tx: rpc_tx.clone(),
        rpc: Some(Rpc {
            body: ping(),
            tx,
            handed_back: false,
        }),
    };
    let (pool, dialer) = pool_with_connection_over(2, stream, UpdatesOnly, (rpc_tx, rpc_rx)).await;
    let runner = tokio::spawn(pool.runner.run());

    // The request is queued in the poll the connection fails in, so the connection never takes
    // it: its task hands it back as it ends, and the runner sends it to a new connection, whose
    // outcome is its answer (the dial is refused), rather than drop it with the channel.
    assert!(matches!(
        answer(queued).await,
        Err(InvocationError::Io(error)) if error.kind() == io::ErrorKind::ConnectionRefused
    ));
    assert_eq!(*dialer.dialed.lock().unwrap(), [2]);
    assert!(pool.handle.quit());
    runner.await.unwrap();
}

#[tokio::test]
async fn request_handed_back_twice_fails_with_its_connection_error() {
    let (mut pool, dialer) = refusing_pool();
    // A second connection to datacenter 2, listed and open, which ends as soon as it gets a
    // request, before it takes it, on a `-404`.
    let (rpc_tx, mut rpc_rx) = mpsc::unbounded_channel();
    pool.runner.connection_pool.spawn(async move {
        let rpc = rpc_rx.recv().await.unwrap();
        ConnectionEnd {
            dc_id: 2,
            unsent: vec![rpc],
            error: Some(ReadError::Transport(transport::Error::BadStatus {
                status: 404,
            })),
        }
    });
    pool.runner
        .connections
        .push(ConnectionInfo { dc_id: 2, rpc_tx });
    // A first one ends with a request it never took.
    let (tx, handed_back) = oneshot::channel();
    pool.runner.connection_pool.spawn(async move {
        ConnectionEnd {
            dc_id: 2,
            unsent: vec![Rpc {
                body: ping(),
                tx,
                handed_back: false,
            }],
            error: Some(io::Error::from(io::ErrorKind::ConnectionReset).into()),
        }
    });
    let runner = tokio::spawn(pool.runner.run());

    // The runner sends the request on to the open connection, which hands it back too: it fails
    // with that connection's own error, as a request that connection took would, rather than go
    // to a third connection (the dial would be refused).
    assert!(matches!(
        answer(handed_back).await,
        Err(InvocationError::Transport(transport::Error::BadStatus {
            status: 404
        }))
    ));
    assert!(dialer.dialed.lock().unwrap().is_empty());
    assert!(pool.handle.quit());
    runner.await.unwrap();
}

/// Hand the runner a request for datacenter 2, as a caller's [`SenderPoolHandle::raw_invoke_in_dc`]
/// does, and let the connection take it.
async fn invoke(runner: &mut SenderPoolRunner, body: Vec<u8>) -> Answer {
    let (tx, rx) = oneshot::channel();
    let request = Request::Invoke { dc_id: 2, body, tx };
    assert!(runner.process_request(request).await.is_continue());
    settle().await;
    rx
}

/// A request whose packet is too big for a 64-byte pipe: the stream takes part of it, and its
/// write stays pending.
fn big_request() -> Vec<u8> {
    tl::functions::upload::SaveFilePart {
        file_id: 1,
        file_part: 0,
        bytes: vec![0; 4096],
    }
    .to_bytes()
}

/// A [`refusing_pool`] whose home connection to datacenter 2 holds a request of each kind, over a
/// 64-byte pipe whose other end is returned: `sent`, written whole; `partly`, whose packet the
/// stream took in part; `queued`, behind it, not serialized (the plain MTP sends one request at a
/// time); and one whose caller gave up.
async fn connection_holding_a_request_of_each_kind()
-> (SenderPool, DuplexStream, Arc<RefusingDialer>, [Answer; 3]) {
    let (client, mut server) = tokio::io::duplex(64);
    let (mut pool, dialer) =
        pool_with_connection_over(2, client, Plain::new(), mpsc::unbounded_channel()).await;
    let sent = invoke(&mut pool.runner, ping()).await;
    // The transport's tag and length, the plain header and the ping.
    let mut request = [0; 4 + 4 + 20 + 12];
    server.read_exact(&mut request).await.unwrap();
    let partly = invoke(&mut pool.runner, big_request()).await;
    let queued = invoke(&mut pool.runner, ping()).await;
    drop(invoke(&mut pool.runner, ping()).await);
    (pool, server, dialer, [sent, partly, queued])
}

#[tokio::test]
async fn disconnect_fails_what_left_and_sends_on_what_never_did() {
    let (pool, _server, dialer, [sent, partly, queued]) =
        connection_holding_a_request_of_each_kind().await;
    let disconnected = pool.handle.disconnect_from_dc(2);
    let runner = tokio::spawn(pool.runner.run());
    assert!(disconnected.await);

    // The server may have taken the request whose bytes left, and its answer could come only on
    // the closed connection: the caller learns it was aborted, as an I/O error its retry policy
    // reads, never that the pool dropped it.
    assert!(matches!(
        answer(sent).await,
        Err(InvocationError::Io(error)) if error.kind() == io::ErrorKind::ConnectionAborted
    ));
    // The server has none of the others: each goes to a new connection, whose outcome is its
    // answer (the dial is refused). The request whose caller gave up goes nowhere.
    for unsent in [partly, queued] {
        assert!(matches!(
            answer(unsent).await,
            Err(InvocationError::Io(error)) if error.kind() == io::ErrorKind::ConnectionRefused
        ));
    }
    assert_eq!(*dialer.dialed.lock().unwrap(), [2, 2]);
    assert!(pool.handle.quit());
    runner.await.unwrap();
}

/// Whether `server`'s other end, the client's stream, is closed now: its read returns the end at
/// once.
fn closed_at_once(server: &mut DuplexStream) -> bool {
    let mut buf = [0; 1];
    let mut read = pin!(server.read(&mut buf));
    matches!(poll_once(read.as_mut()), Poll::Ready(Ok(0)))
}

#[tokio::test]
async fn disconnect_resolves_once_its_connection_has_ended() {
    let (pool, mut server, _) = pool_with_connection(2).await;
    // Two disconnects before the runner runs: the second finds the connection ending, no longer
    // listed, and waits for it too.
    let first = pool.handle.disconnect_from_dc(2);
    let second = pool.handle.disconnect_from_dc(2);
    let runner = tokio::spawn(pool.runner.run());
    assert!(second.await);
    // The connection's task has ended, its socket with it.
    assert!(closed_at_once(&mut server));
    assert!(first.await);
    // Nothing to wait for: told at once.
    assert!(pool.handle.disconnect_from_dc(2).await);
    assert!(pool.handle.quit());
    runner.await.unwrap();
    // The runner is gone.
    assert!(!pool.handle.disconnect_from_dc(2).await);
}

/// Refuses every dial, as [`RefusingDialer`] does, and says whether the disconnect it was given
/// had been told by the time the first dial came.
#[derive(Default)]
struct DialsAfterTheDisconnect {
    disconnection: Mutex<Option<Disconnection>>,
    told_first: Mutex<Option<bool>>,
}

impl Dialer for DialsAfterTheDisconnect {
    fn dial<'a>(&'a self, _: &'a DialTarget) -> DialFuture<'a> {
        let mut disconnection = self.disconnection.lock().unwrap();
        if let Some(disconnection) = disconnection.as_mut() {
            let told = poll_once(Pin::new(disconnection)).is_ready();
            self.told_first.lock().unwrap().get_or_insert(told);
        }
        Box::pin(async { Err(io::ErrorKind::ConnectionRefused.into()) })
    }
}

#[tokio::test]
async fn disconnect_is_told_before_what_its_connection_handed_back_dials() {
    let dialer = Arc::new(DialsAfterTheDisconnect::default());
    let (client, mut server) = tokio::io::duplex(1 << 16);
    let (mut pool, _) =
        pool_with_connection_over(2, client, Plain::new(), mpsc::unbounded_channel()).await;
    pool.runner.connection_params.dialer = Arc::clone(&dialer) as Arc<dyn Dialer>;
    // A request queued for the connection, then the disconnect, before the connection's task
    // runs: the request goes back unwritten, and on through a new connection, which dials.
    let (tx, queued) = oneshot::channel();
    let request = Request::Invoke {
        dc_id: 2,
        body: ping(),
        tx,
    };
    assert!(pool.runner.process_request(request).await.is_continue());
    let (done, ended) = oneshot::channel();
    let disconnect = Request::Disconnect { dc_id: 2, done };
    assert!(pool.runner.process_request(disconnect).await.is_continue());
    *dialer.disconnection.lock().unwrap() = Some(Disconnection(Some(ended)));
    let runner = tokio::spawn(pool.runner.run());
    assert!(matches!(
        answer(queued).await,
        Err(InvocationError::Io(error)) if error.kind() == io::ErrorKind::ConnectionRefused
    ));
    assert_eq!(*dialer.told_first.lock().unwrap(), Some(true));
    assert!(closed_at_once(&mut server));
    assert!(pool.handle.quit());
    runner.await.unwrap();
}

#[tokio::test]
async fn a_later_disconnect_holds_what_its_connections_handed_back_until_it_is_told() {
    let dialer = Arc::new(DialsAfterTheDisconnect::default());
    let (client, _server) = tokio::io::duplex(1 << 16);
    let (mut pool, _) =
        pool_with_connection_over(2, client, Plain::new(), mpsc::unbounded_channel()).await;
    pool.runner.connection_params.dialer = Arc::clone(&dialer) as Arc<dyn Dialer>;
    // A request queued for the connection, then a disconnect, before the connection's task runs:
    // the request goes back unwritten, and the disconnect waits for that connection alone.
    let (tx, queued) = oneshot::channel();
    let request = Request::Invoke {
        dc_id: 2,
        body: ping(),
        tx,
    };
    assert!(pool.runner.process_request(request).await.is_continue());
    let (done, first) = oneshot::channel();
    let disconnect = Request::Disconnect { dc_id: 2, done };
    assert!(pool.runner.process_request(disconnect).await.is_continue());
    // A second connection to the datacenter, made after the first disconnect, which ends once it
    // is let go; then a second disconnect, which waits for both connections.
    let (let_go, ends) = oneshot::channel::<()>();
    let second_connection = pool.runner.connection_pool.spawn(async move {
        drop(ends.await);
        ConnectionEnd {
            dc_id: 2,
            unsent: Vec::new(),
            error: None,
        }
    });
    pool.runner.tasks.push((second_connection.id(), 2));
    let (done, second) = oneshot::channel();
    let disconnect = Request::Disconnect { dc_id: 2, done };
    assert!(pool.runner.process_request(disconnect).await.is_continue());
    *dialer.disconnection.lock().unwrap() = Some(Disconnection(Some(second)));
    let runner = tokio::spawn(pool.runner.run());

    // The first connection ends: the first disconnect is told, and what the connection handed
    // back waits for the second, which waits for the second connection too.
    assert!(Disconnection(Some(first)).await);
    settle().await;
    assert_eq!(*dialer.told_first.lock().unwrap(), None, "nothing dialed");
    drop(let_go);
    assert!(matches!(
        answer(queued).await,
        Err(InvocationError::Io(error)) if error.kind() == io::ErrorKind::ConnectionRefused
    ));
    assert_eq!(*dialer.told_first.lock().unwrap(), Some(true));
    assert!(pool.handle.quit());
    runner.await.unwrap();
}

/// Refuses every dial, as [`RefusingDialer`] does, and permits dials only while `open` holds.
struct GatedDialer {
    dialed: Mutex<Vec<i32>>,
    open: watch::Sender<bool>,
}

impl GatedDialer {
    /// One that permits no dial yet.
    fn shut() -> Arc<Self> {
        Arc::new(Self {
            dialed: Mutex::default(),
            open: watch::Sender::new(false),
        })
    }
}

impl Dialer for GatedDialer {
    fn dial<'a>(&'a self, target: &'a DialTarget) -> DialFuture<'a> {
        self.dialed.lock().unwrap().push(target.dc_id);
        Box::pin(async { Err(io::ErrorKind::ConnectionRefused.into()) })
    }

    fn permit(&self) -> PermitFuture {
        let mut open = self.open.subscribe();
        Box::pin(async move {
            drop(open.wait_for(|open| *open).await);
        })
    }
}

/// Ask the runner of `handle` for a ping in `dc_id`, on a task of its own.
fn ping_in(
    handle: &SenderPoolHandle,
    dc_id: i32,
) -> JoinHandle<Result<InvokeResponse, InvocationError>> {
    let handle = handle.clone();
    tokio::spawn(async move { handle.raw_invoke_in_dc(dc_id, ping()).await })
}

#[tokio::test(start_paused = true)]
async fn a_request_held_for_the_permit_holds_nothing_else() {
    let (mut pool, mut server, _) = pool_with_connection(2).await;
    let dialer = GatedDialer::shut();
    pool.runner.connection_params.dialer = Arc::clone(&dialer) as Arc<dyn Dialer>;
    let runner = tokio::spawn(pool.runner.run());
    let start = tokio::time::Instant::now();

    // A request for datacenter 4, to which no connection is open, while the permit is shut: the
    // runner holds it, and dials nothing.
    let held = ping_in(&pool.handle, 4);
    idle().await;
    // The runner goes on meanwhile: a disconnect of datacenter 2 is told once its connection's
    // task has ended, which the runner joins, and one of datacenter 4, which has none, at once,
    // the request staying held. Neither waits for the held request's wait to run out.
    assert!(pool.handle.disconnect_from_dc(2).await);
    assert!(closed_at_once(&mut server));
    assert!(pool.handle.disconnect_from_dc(4).await);
    assert!(start.elapsed() < SETUP_TIMEOUT, "{:?}", start.elapsed());
    assert!(!held.is_finished());
    assert!(dialer.dialed.lock().unwrap().is_empty());

    // The permit comes: the held request dials, once.
    dialer.open.send_replace(true);
    assert!(matches!(
        held.await.unwrap(),
        Err(InvocationError::Io(error)) if error.kind() == io::ErrorKind::ConnectionRefused
    ));
    assert_eq!(*dialer.dialed.lock().unwrap(), [4]);
    assert!(pool.handle.quit());
    runner.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_held_request_times_out_and_the_quit_drops_one_still_held() {
    let (mut pool, _) = refusing_pool();
    let dialer = GatedDialer::shut();
    pool.runner.connection_params.dialer = Arc::clone(&dialer) as Arc<dyn Dialer>;
    let runner = tokio::spawn(pool.runner.run());
    let start = tokio::time::Instant::now();

    // The permit never comes: a held request fails once its 60 s are over, as a connection's
    // setup past them does, with nothing dialed.
    let first = ping_in(&pool.handle, 2);
    idle().await;
    let second = ping_in(&pool.handle, 2);
    assert!(matches!(
        first.await.unwrap(),
        Err(InvocationError::Io(error)) if error.kind() == io::ErrorKind::TimedOut
    ));
    assert_eq!(start.elapsed(), SETUP_TIMEOUT);
    // One held a second later still waits; the quit drops it.
    assert!(!second.is_finished());
    assert!(pool.handle.quit());
    runner.await.unwrap();
    assert!(matches!(
        second.await.unwrap(),
        Err(InvocationError::Dropped(_))
    ));
    assert!(dialer.dialed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn requests_queued_right_before_a_disconnect_go_back_unwritten() {
    let (client, mut server) = tokio::io::duplex(1 << 16);
    let (mut pool, _) =
        pool_with_connection_over(2, client, Plain::new(), mpsc::unbounded_channel()).await;
    // The runner queues requests for the connection and then closes it, before the connection's
    // task runs: its channel holds them as it closes. Enough of them that a connection polling
    // its stream and its channel in a random order would all but surely write one.
    let mut queued = Vec::new();
    for _ in 0..32 {
        let (tx, rx) = oneshot::channel();
        let request = Request::Invoke {
            dc_id: 2,
            body: ping(),
            tx,
        };
        assert!(pool.runner.process_request(request).await.is_continue());
        queued.push(rx);
    }
    let (done, _) = oneshot::channel();
    let disconnect = Request::Disconnect { dc_id: 2, done };
    assert!(pool.runner.process_request(disconnect).await.is_continue());
    let runner = tokio::spawn(pool.runner.run());

    // None leaves: each goes to a new connection, whose outcome is its answer (the dial is
    // refused), rather than fail as aborted, and the server reads nothing but the end.
    for rx in queued {
        assert!(matches!(
            answer(rx).await,
            Err(InvocationError::Io(error)) if error.kind() == io::ErrorKind::ConnectionRefused
        ));
    }
    let mut written = Vec::new();
    let read = timeout(Duration::from_secs(10), server.read_to_end(&mut written));
    assert_eq!(read.await.unwrap().unwrap(), 0);
    assert!(pool.handle.quit());
    runner.await.unwrap();
}

#[tokio::test]
async fn quit_fails_what_its_connections_hold() {
    let (pool, _server, dialer, [sent, partly, queued]) =
        connection_holding_a_request_of_each_kind().await;
    assert!(pool.handle.quit());
    timeout(Duration::from_secs(10), pool.runner.run())
        .await
        .unwrap();

    // As a disconnect fails it; the others have no pool left to go on through.
    assert!(matches!(
        answer(sent).await,
        Err(InvocationError::Io(error)) if error.kind() == io::ErrorKind::ConnectionAborted
    ));
    for unsent in [partly, queued] {
        assert!(
            timeout(Duration::from_secs(10), unsent)
                .await
                .unwrap()
                .is_err()
        );
    }
    assert!(dialer.dialed.lock().unwrap().is_empty());
}

/// An error that names what failed and leaves its cause to `source()`, as a stream's may.
#[derive(Debug)]
struct TunnelError(io::Error);

impl fmt::Display for TunnelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the tunnel failed")
    }
}

impl std::error::Error for TunnelError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// A stream that takes every write, and whose first read fails with the error it holds.
struct FailsReading(Option<io::Error>);

impl AsyncRead for FailsReading {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(self.0.take().expect("one read")))
    }
}

impl AsyncWrite for FailsReading {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn connection_error_reaches_every_request_it_held_with_its_cause() {
    // Two requests, which the connection takes before its first read: that read fails.
    let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
    let held = [ping(), ping()].map(|body| {
        let (tx, rx) = oneshot::channel();
        let rpc = Rpc {
            body,
            tx,
            handed_back: false,
        };
        assert!(rpc_tx.send(rpc).is_ok());
        rx
    });
    let stream = FailsReading(Some(io::Error::new(
        io::ErrorKind::ConnectionReset,
        TunnelError(io::Error::other("the relay hung up")),
    )));
    let _pool = pool_with_connection_over(2, stream, Plain::new(), (rpc_tx, rpc_rx)).await;

    // Each fails with the connection's error, its kind, message and cause, which a copy shares.
    for rx in held {
        let error = answer(rx).await.unwrap_err();
        let chain: Vec<_> =
            std::iter::successors(Some(&error as &dyn std::error::Error), |&error| {
                error.source()
            })
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            chain,
            ["request error: the tunnel failed", "the relay hung up"]
        );
        assert!(matches!(
            error,
            InvocationError::Io(error) if error.kind() == io::ErrorKind::ConnectionReset
        ));
    }
}

/// What [`scripted`]'s runner answers a request with, by its body.
pub(crate) type Script = dyn Fn(&[u8]) -> Result<Vec<u8>, InvocationError> + Send + Sync;

/// The requests [`scripted`]'s runner took: each one's constructor, and when it came.
pub(crate) type Calls = Arc<Mutex<Vec<(u32, tokio::time::Instant)>>>;

/// A handle whose runner is `script`: each request is answered with what it says, and recorded by
/// its constructor and when it came; a failed one is never retried, and a disconnect is told at
/// once. For the tests of what drives a pool through its handle (the updates receiver's), which
/// reach neither the handle's channel nor the requests it carries.
pub(crate) fn scripted(script: Box<Script>) -> (SenderPoolHandle, Calls) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let calls: Calls = Arc::default();
    let seen = Arc::clone(&calls);
    tokio::spawn(async move {
        while let Some(request) = rx.recv().await {
            match request {
                Request::Invoke { body, tx, .. } => {
                    let id = u32::from_le_bytes(body[..4].try_into().unwrap());
                    seen.lock().unwrap().push((id, tokio::time::Instant::now()));
                    let _ = tx.send(script(&body));
                }
                Request::CheckRetry { error, tx, .. } => {
                    let _ = tx.send(ControlFlow::Break(error));
                }
                Request::Disconnect { done, .. } => {
                    let _ = done.send(());
                }
                Request::Quit => break,
            }
        }
    });
    (SenderPoolHandle(tx), calls)
}
