// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The byte-stream seam: every connection runs over a stream the caller's dialer opens, and the
//! auth-key handshake encrypts to the server RSA keys the caller supplies. The pool's connects
//! over it: one to a peer that never answers fails in time, one whose server gives the datacenter
//! a new address keeps it, and one whose server forgot the session's key makes a new one. A
//! transport error code fails a request as a transport error. A dial gives up on an address that
//! never answers and tries the next. The tests play the server on in-memory streams; nothing
//! leaves the machine.

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use grammers_crypto::{AuthKey, aes, rsa};
use grammers_mtproto::mtp::Plain;
use grammers_mtproto::transport::{self, Intermediate};
use grammers_mtsender::{
    ConnKind, ConnectionParams, DialFuture, DialTarget, Dialer, InvocationError, MtStream,
    NoRetries, Sender, SenderPool, TokioTcpDialer,
};
use grammers_session::Session;
use grammers_session::storages::MemorySession;
use grammers_tl_types::{self as tl, Deserializable, Serializable};
use sha2::{Digest, Sha256};
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter, DuplexStream, ReadBuf,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};

/// The modulus of an RSA key made for these tests, in decimal.
const TEST_RSA_N: &str = "31061566470730259279489667733084681556760687329465786664482053075283385752695398318670954403996188080210295550160319160616597041872894207691762622979283747283485639226402698352456692782338298534733490720927201364604470915682781526321186587797060286077250259555203220248590992731187431807354201024455330793205876852867537534839988043356364719437333460963565300672398085826393289466208888352980898963016147676019135375617566069611470363866053429222394903024621590709944904411843822000898167199135593139374450988589678005432444379189352301969314906419779690143747375740419891522246405756653712906618734395560224747115139";

/// 1206429347 × 1218991343.
const PQ: u64 = 1470626929934143021;

/// The server's end of a connection: reads the client's plain messages, framed by the
/// intermediate transport, and answers with its own.
struct Server {
    stream: DuplexStream,
    tag_read: bool,
}

impl Server {
    /// The client's next packet, unframed.
    async fn read_packet(&mut self) -> Vec<u8> {
        if !self.tag_read {
            assert_eq!(self.read_u32().await, 0xeeeeeeee, "intermediate tag");
            self.tag_read = true;
        }
        let len = self.read_u32().await as usize;
        let mut packet = vec![0; len];
        self.stream.read_exact(&mut packet).await.unwrap();
        packet
    }

    /// The body of the client's next plain message.
    async fn read_message(&mut self) -> Vec<u8> {
        let payload = self.read_packet().await;
        // auth_key_id = 0, msg_id, message_data_length, message_data.
        let (header, body) = payload.split_at(20);
        assert_eq!(header[..8], [0; 8], "a plain message");
        assert_eq!(
            u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize,
            body.len()
        );
        body.to_vec()
    }

    /// Send `body` as a plain message.
    async fn write_message(&mut self, body: &[u8]) {
        self.stream.write_all(&plain_frame(body)).await.unwrap();
    }

    /// Send the transport error `code`: a packet of one `int`, the code negated.
    async fn write_transport_error(&mut self, code: i32) {
        let mut frame = 4u32.to_le_bytes().to_vec();
        frame.extend((-code).to_le_bytes());
        self.stream.write_all(&frame).await.unwrap();
    }

    /// Answer the client's next message, encrypted with [`AUTH_KEY`], with `result` in an
    /// `rpc_result`.
    async fn answer_encrypted(&mut self, result: &[u8]) {
        let mut packet = self.read_packet().await;
        // auth_key_id, msg_key, then the encrypted salt, session_id, msg_id, seq_no, length, body
        // and padding.
        let (msg_key, encrypted) = packet[8..].split_at_mut(16);
        let (key, iv) = aes_key_iv(msg_key, 0);
        aes::ige_decrypt(encrypted, &key, &iv);
        assert_eq!(&sha256(&[&AUTH_KEY[88..120], encrypted])[8..24], msg_key);
        let (salt_and_session, msg_id) = (&encrypted[..16], &encrypted[16..24]);

        // rpc_result#f35c6d01 req_msg_id:long result:Object
        let mut body = 0xf35c6d01u32.to_le_bytes().to_vec();
        body.extend(msg_id);
        body.extend(result);

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let mut plain = salt_and_session.to_vec();
        // A server message's identifier answering a client's is 1 modulo 4.
        plain.extend(((now.as_secs() as i64) << 32 | 1).to_le_bytes());
        // The sequence number of a content-related message.
        plain.extend(1i32.to_le_bytes());
        plain.extend((body.len() as i32).to_le_bytes());
        plain.extend(body);
        // At least 12 bytes of padding, to a multiple of 16.
        plain.resize((plain.len() + 12).div_ceil(16) * 16, 0);

        let msg_key: [u8; 16] = sha256(&[&AUTH_KEY[96..128], &plain])[8..24]
            .try_into()
            .unwrap();
        let (key, iv) = aes_key_iv(&msg_key, 8);
        aes::ige_encrypt(&mut plain, &key, &iv);

        let mut reply = AuthKey::from_bytes(AUTH_KEY).key_id().to_vec();
        reply.extend(msg_key);
        reply.extend(plain);
        let mut frame = (reply.len() as u32).to_le_bytes().to_vec();
        frame.extend(reply);
        self.stream.write_all(&frame).await.unwrap();
    }

    async fn read_u32(&mut self) -> u32 {
        let mut bytes = [0; 4];
        self.stream.read_exact(&mut bytes).await.unwrap();
        u32::from_le_bytes(bytes)
    }
}

/// `body` as a plain message from the server, framed by the intermediate transport.
fn plain_frame(body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend(0i64.to_le_bytes());
    // A server message's identifier answering a client's is 1 modulo 4.
    payload.extend(5i64.to_le_bytes());
    payload.extend((body.len() as i32).to_le_bytes());
    payload.extend(body);
    let mut frame = (payload.len() as u32).to_le_bytes().to_vec();
    frame.extend(payload);
    frame
}

/// The authorization key the server holds in [`Server::answer_encrypted`].
const AUTH_KEY: [u8; 256] = [7; 256];

fn sha256(parts: &[&[u8]]) -> [u8; 32] {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update(part);
    }
    hash.finalize().into()
}

/// MTProto 2.0's AES key and IV for `msg_key` under [`AUTH_KEY`]: `x` is 0 for a message from the
/// client, 8 for one from the server.
fn aes_key_iv(msg_key: &[u8], x: usize) -> ([u8; 32], [u8; 32]) {
    let a = sha256(&[msg_key, &AUTH_KEY[x..x + 36]]);
    let b = sha256(&[&AUTH_KEY[40 + x..76 + x], msg_key]);
    let mut key = [0; 32];
    key[..8].copy_from_slice(&a[..8]);
    key[8..24].copy_from_slice(&b[8..24]);
    key[24..].copy_from_slice(&a[24..]);
    let mut iv = [0; 32];
    iv[..8].copy_from_slice(&b[..8]);
    iv[8..24].copy_from_slice(&a[8..24]);
    iv[24..].copy_from_slice(&b[24..]);
    (key, iv)
}

/// Opens in-memory streams, recording the target of each; the test plays the server on the
/// other ends, which it receives in order.
struct TestDialer {
    capacity: usize,
    targets: Mutex<Vec<DialTarget>>,
    servers: mpsc::UnboundedSender<Server>,
}

impl TestDialer {
    fn new(capacity: usize) -> (Arc<Self>, mpsc::UnboundedReceiver<Server>) {
        let (servers, rx) = mpsc::unbounded_channel();
        let dialer = Self {
            capacity,
            targets: Mutex::new(Vec::new()),
            servers,
        };
        (Arc::new(dialer), rx)
    }
}

impl Dialer for TestDialer {
    fn dial<'a>(&'a self, target: &'a DialTarget) -> DialFuture<'a> {
        Box::pin(async move {
            self.targets.lock().unwrap().push(target.clone());
            let (client, server) = tokio::io::duplex(self.capacity);
            self.servers
                .send(Server {
                    stream: server,
                    tag_read: false,
                })
                .unwrap();
            Ok(Box::new(client) as Box<dyn MtStream>)
        })
    }
}

fn target() -> DialTarget {
    DialTarget {
        dc_id: 2,
        kind: ConnKind::Main,
        ipv4: Some(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 2), 443)),
        ipv6: None,
    }
}

fn res_pq(nonce: [u8; 16], fingerprints: Vec<i64>) -> tl::enums::ResPq {
    tl::types::ResPq {
        nonce,
        server_nonce: [9; 16],
        pq: PQ.to_be_bytes().to_vec(),
        server_public_key_fingerprints: fingerprints,
    }
    .into()
}

#[tokio::test]
async fn step_reads_and_writes_without_split() {
    // A stream that holds 16 bytes: the request leaves in several writes, and the answer arrives
    // in several reads, all through the one boxed stream.
    let (dialer, mut servers) = TestDialer::new(16);
    let mut sender = Sender::connect(Intermediate::new(), Plain::new(), &*dialer, &target())
        .await
        .unwrap();
    let mut server = servers.recv().await.unwrap();

    let server = tokio::spawn(async move {
        let request =
            tl::functions::ReqPqMulti::from_boxed_bytes(&server.read_message().await).unwrap();
        server
            .write_message(&res_pq(request.nonce, vec![1, 2]).to_bytes())
            .await;
        server
    });
    let response = sender
        .invoke(&tl::functions::ReqPqMulti { nonce: [7; 16] })
        .await
        .unwrap();
    assert_eq!(response, res_pq([7; 16], vec![1, 2]));
    drop(server.await.unwrap());
}

/// A stream that has its answer ready the moment a request is written to it: both are ready in
/// the one poll of [`Sender::step`]. Unless `takes_bytes`, it reports writing nothing, and fails
/// the test if it is written to again: a sender that keeps trying spins inside one poll, where no
/// timeout can stop it.
struct AnswersAtOnce {
    answer: Vec<u8>,
    written: bool,
    takes_bytes: bool,
}

impl AsyncWrite for AnswersAtOnce {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        assert!(
            self.takes_bytes || !self.written,
            "written to again after it took no bytes"
        );
        self.written = true;
        Poll::Ready(Ok(if self.takes_bytes { buf.len() } else { 0 }))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for AnswersAtOnce {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.written || self.answer.is_empty() {
            return Poll::Pending;
        }
        buf.put_slice(&std::mem::take(&mut self.answer));
        Poll::Ready(Ok(()))
    }
}

/// Dials the one stream it holds.
struct Holds(Mutex<Option<Box<dyn MtStream>>>);

impl Dialer for Holds {
    fn dial<'a>(&'a self, _: &'a DialTarget) -> DialFuture<'a> {
        let stream = self.0.lock().unwrap().take().unwrap();
        Box::pin(async move { Ok(stream) })
    }
}

#[tokio::test]
async fn answer_in_the_poll_of_its_request_is_matched() {
    let answer = res_pq([7; 16], vec![1]);
    let dialer = Holds(Mutex::new(Some(Box::new(AnswersAtOnce {
        answer: plain_frame(&answer.to_bytes()),
        written: false,
        takes_bytes: true,
    }))));
    let mut sender = Sender::connect(Intermediate::new(), Plain::new(), &dialer, &target())
        .await
        .unwrap();

    // Handling the read before the write would take this for an answer to a request not sent
    // yet, ignore it, and wait forever.
    let call = sender.invoke(&tl::functions::ReqPqMulti { nonce: [7; 16] });
    let response = timeout(Duration::from_secs(10), call).await.unwrap();
    assert_eq!(response.unwrap(), answer);
}

/// A sender over `stream`.
async fn sender_over(stream: Box<dyn MtStream>) -> Sender<Intermediate, Plain> {
    let dialer = Holds(Mutex::new(Some(stream)));
    Sender::connect(Intermediate::new(), Plain::new(), &dialer, &target())
        .await
        .unwrap()
}

#[tokio::test]
async fn request_reaches_a_stream_that_buffers() {
    // The pipe takes 16 bytes at a time, so the flush goes `Pending` and has to be woken.
    let (client, server) = tokio::io::duplex(16);
    let mut sender = sender_over(Box::new(BufWriter::new(client))).await;
    let mut server = Server {
        stream: server,
        tag_read: false,
    };

    let answers = tokio::spawn(async move {
        let request =
            tl::functions::ReqPqMulti::from_boxed_bytes(&server.read_message().await).unwrap();
        server
            .write_message(&res_pq(request.nonce, vec![1, 2]).to_bytes())
            .await;
        server
    });
    let call = sender.invoke(&tl::functions::ReqPqMulti { nonce: [7; 16] });
    let response = timeout(Duration::from_secs(10), call).await.unwrap();
    assert_eq!(response.unwrap(), res_pq([7; 16], vec![1, 2]));
    drop(answers.await.unwrap());
}

/// A stream that takes every write, and fails every flush.
struct FlushFails(DuplexStream);

impl AsyncWrite for FlushFails {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for FlushFails {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

#[tokio::test]
async fn flush_error_fails_the_request() {
    let (client, _server) = tokio::io::duplex(1024);
    let mut sender = sender_over(Box::new(FlushFails(client))).await;
    let call = sender.invoke(&tl::functions::ReqPqMulti { nonce: [7; 16] });
    let response = timeout(Duration::from_secs(10), call).await.unwrap();
    assert!(
        matches!(response, Err(InvocationError::Io(_))),
        "{response:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn flush_survives_a_cancelled_step() {
    let (client, server) = tokio::io::duplex(16);
    let mut sender = sender_over(Box::new(BufWriter::new(client))).await;
    let mut server = Server {
        stream: server,
        tag_read: false,
    };

    // Nobody reads yet, so the flush can't finish; `timeout` drops `invoke` and its `step`.
    let request = tl::functions::ReqPqMulti { nonce: [7; 16] };
    let cancelled = timeout(Duration::from_millis(100), sender.invoke(&request)).await;
    assert!(cancelled.is_err());

    let message = timeout(Duration::from_secs(10), async {
        tokio::select! {
            message = server.read_message() => message,
            _ = async { loop { sender.step().await.unwrap(); } } => unreachable!(),
        }
    })
    .await
    .expect("the cancelled request never reached the server");
    let received = tl::functions::ReqPqMulti::from_boxed_bytes(&message).unwrap();
    assert_eq!(received.nonce, [7; 16]);
}

#[tokio::test]
async fn stream_that_takes_no_bytes_is_an_error() {
    let dialer = Holds(Mutex::new(Some(Box::new(AnswersAtOnce {
        answer: Vec::new(),
        written: false,
        takes_bytes: false,
    }))));
    let mut sender = Sender::connect(Intermediate::new(), Plain::new(), &dialer, &target())
        .await
        .unwrap();

    // Treated as progress, this would write the same bytes forever.
    let call = sender.invoke(&tl::functions::ReqPqMulti { nonce: [7; 16] });
    match call.await {
        Err(InvocationError::Io(error)) => assert_eq!(error.kind(), io::ErrorKind::WriteZero),
        other => panic!("an I/O error, not {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn ping_fires_on_paused_time() {
    let (dialer, mut servers) = TestDialer::new(1 << 16);
    let mut sender = Sender::connect(Intermediate::new(), Plain::new(), &*dialer, &target())
        .await
        .unwrap();
    let mut server = servers.recv().await.unwrap();
    let start = Instant::now();

    // Nothing to read or write: the step waits for the ping timer, which paused time reaches.
    assert!(sender.step().await.unwrap().is_empty());
    assert_eq!(start.elapsed(), Duration::from_secs(60));
    // The next step sends the ping.
    assert!(sender.step().await.unwrap().is_empty());
    let ping =
        tl::functions::PingDelayDisconnect::from_boxed_bytes(&server.read_message().await).unwrap();
    assert_eq!(ping.disconnect_delay, 75);
    assert_eq!(start.elapsed(), Duration::from_secs(60));

    // And the next one a minute later.
    assert!(sender.step().await.unwrap().is_empty());
    assert_eq!(start.elapsed(), Duration::from_secs(120));
}

/// Parameters that dial through `dialer` and ask the server's handshake for the given
/// `server_rsa_keys`, with no retries.
fn params(dialer: Arc<TestDialer>, server_rsa_keys: Vec<rsa::Key>) -> ConnectionParams {
    ConnectionParams {
        dialer,
        server_rsa_keys: server_rsa_keys.into(),
        transport: Box::new(|| Box::new(Intermediate::new())),
        retry_policy: Box::new(NoRetries),
        ..Default::default()
    }
}

/// A pool on the default session (no auth keys) with [`params`].
fn pool(
    dialer: Arc<TestDialer>,
    server_rsa_keys: Vec<rsa::Key>,
) -> (SenderPool, Arc<MemorySession>) {
    let session = Arc::new(MemorySession::default());
    let params = params(dialer, server_rsa_keys);
    (
        SenderPool::with_configuration(Arc::clone(&session), 1, params),
        session,
    )
}

#[tokio::test]
async fn dialer_receives_dc_and_kind() {
    let (dialer, mut servers) = TestDialer::new(1 << 16);
    let (pool, session) = pool(Arc::clone(&dialer), Vec::new());
    let runner = tokio::spawn(pool.runner.run());

    let handle = pool.handle.clone();
    let call = tokio::spawn(async move {
        handle
            .invoke_in_dc(4, &tl::functions::help::GetNearestDc {})
            .await
    });
    // The pool's first request to datacenter 4 dials it; the server hangs up at once.
    drop(servers.recv().await.unwrap());
    assert!(call.await.unwrap().is_err());

    let dc4 = session.dc_option(4).unwrap().unwrap();
    assert_eq!(
        *dialer.targets.lock().unwrap(),
        [DialTarget {
            dc_id: 4,
            kind: ConnKind::Main,
            ipv4: Some(dc4.ipv4),
            ipv6: Some(dc4.ipv6),
        }]
    );
    assert!(pool.handle.quit());
    runner.await.unwrap();
}

#[tokio::test]
async fn handshake_uses_supplied_rsa_keys() {
    let key = rsa::Key::new(TEST_RSA_N, "65537").unwrap();
    let fingerprint = key.fingerprint();
    let (dialer, mut servers) = TestDialer::new(1 << 16);
    let (pool, _) = pool(dialer, vec![key]);
    let runner = tokio::spawn(pool.runner.run());

    let handle = pool.handle.clone();
    let call = tokio::spawn(async move {
        handle
            .invoke_in_dc(2, &tl::functions::help::GetNearestDc {})
            .await
    });
    // The server holds only the test key, and says so.
    let mut server = servers.recv().await.unwrap();
    let request =
        tl::functions::ReqPqMulti::from_boxed_bytes(&server.read_message().await).unwrap();
    server
        .write_message(&res_pq(request.nonce, vec![fingerprint]).to_bytes())
        .await;
    // The client encrypts to it: Telegram's keys alone would have failed with no request.
    let request =
        tl::functions::ReqDhParams::from_boxed_bytes(&server.read_message().await).unwrap();
    assert_eq!(request.public_key_fingerprint, fingerprint);
    assert_eq!(request.encrypted_data.len(), 256);
    drop(server);

    assert!(call.await.unwrap().is_err());
    assert!(pool.handle.quit());
    runner.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn silent_peer_fails_the_request_and_frees_the_runner() {
    let (dialer, mut servers) = TestDialer::new(1 << 16);
    let (pool, _) = pool(Arc::clone(&dialer), Vec::new());
    let runner = tokio::spawn(pool.runner.run());
    let start = Instant::now();
    let call = |dc_id| {
        let handle = pool.handle.clone();
        tokio::spawn(async move {
            handle
                .invoke_in_dc(dc_id, &tl::functions::help::GetNearestDc {})
                .await
        })
    };

    // Each peer takes the connection and never answers the handshake.
    let first = call(2);
    let _silent = servers.recv().await.unwrap();
    // The runner is busy connecting to datacenter 2: this request waits until it gives up.
    let second = call(4);
    let _silent_too = timeout(Duration::from_secs(3600), servers.recv())
        .await
        .expect("the runner never took the next request")
        .unwrap();
    assert_eq!(start.elapsed(), Duration::from_secs(60));

    for call in [first, second] {
        match call.await.unwrap() {
            Err(InvocationError::Io(error)) => assert_eq!(error.kind(), io::ErrorKind::TimedOut),
            other => panic!("a timeout, not {other:?}"),
        }
    }
    assert!(pool.handle.quit());
    runner.await.unwrap();
    assert_eq!(start.elapsed(), Duration::from_secs(120));
    let dialed: Vec<_> = dialer
        .targets
        .lock()
        .unwrap()
        .iter()
        .map(|target| target.dc_id)
        .collect();
    assert_eq!(dialed, [2, 4]);
}

/// A static option for datacenter `id` at `address`, as `help.getConfig` lists one.
fn listed_dc_option(id: i32, address: SocketAddrV4) -> tl::enums::DcOption {
    tl::types::DcOption {
        ipv6: false,
        media_only: false,
        tcpo_only: false,
        cdn: false,
        r#static: true,
        this_port_only: false,
        id,
        ip_address: address.ip().to_string(),
        port: address.port().into(),
        secret: None,
    }
    .into()
}

/// `help.getConfig`'s answer, listing `dc_options`.
fn config(dc_options: Vec<tl::enums::DcOption>) -> tl::enums::Config {
    tl::types::Config {
        default_p2p_contacts: false,
        preload_featured_stickers: false,
        revoke_pm_inbox: false,
        blocked_mode: false,
        force_try_ipv6: false,
        date: 0,
        expires: 0,
        test_mode: false,
        this_dc: 2,
        dc_options,
        dc_txt_domain_name: String::new(),
        chat_size_max: 0,
        megagroup_size_max: 0,
        forwarded_count_max: 0,
        online_update_period_ms: 0,
        offline_blur_timeout_ms: 0,
        offline_idle_timeout_ms: 0,
        online_cloud_timeout_ms: 0,
        notify_cloud_delay_ms: 0,
        notify_default_delay_ms: 0,
        push_chat_period_ms: 0,
        push_chat_limit: 0,
        edit_time_limit: 0,
        revoke_time_limit: 0,
        revoke_pm_time_limit: 0,
        rating_e_decay: 0,
        stickers_recent_limit: 0,
        channels_read_media_period: 0,
        tmp_sessions: None,
        call_receive_timeout_ms: 0,
        call_ring_timeout_ms: 0,
        call_connect_timeout_ms: 0,
        call_packet_timeout_ms: 0,
        me_url_prefix: String::new(),
        autoupdate_url_prefix: None,
        gif_search_username: None,
        venue_search_username: None,
        img_search_username: None,
        static_maps_provider: None,
        caption_length_max: 0,
        message_length_max: 0,
        webfile_dc_id: 0,
        suggested_lang_code: None,
        lang_pack_version: None,
        base_lang_pack_version: None,
        reactions_default: None,
        autologin_token: None,
    }
    .into()
}

/// One run of a pool on `session` that connects to datacenter 2, whose server holds
/// [`AUTH_KEY`], answers `initConnection` with `config` and hangs up. Returns the address dialed.
async fn connect_to_dc2(session: &Arc<MemorySession>, config: &tl::enums::Config) -> SocketAddrV4 {
    let (dialer, mut servers) = TestDialer::new(1 << 16);
    let params = params(Arc::clone(&dialer), Vec::new());
    let pool = SenderPool::with_configuration(Arc::clone(session), 1, params);
    let runner = tokio::spawn(pool.runner.run());

    let call = pool
        .handle
        .invoke_in_dc(2, &tl::functions::help::GetNearestDc {});
    let serve = async {
        let mut server = servers.recv().await.unwrap();
        server.answer_encrypted(&config.to_bytes()).await;
    };
    // The pool stores what the connect learnt before it sends the request, which finds the
    // server gone.
    let (result, ()) = tokio::join!(call, serve);
    assert!(result.is_err());
    assert!(pool.handle.quit());
    runner.await.unwrap();

    let targets = dialer.targets.lock().unwrap();
    assert_eq!(targets.len(), 1);
    targets[0].ipv4.unwrap()
}

#[tokio::test]
async fn own_address_change_is_kept() {
    let session = Arc::new(MemorySession::default());
    let mut dc2 = session.dc_option(2).unwrap().unwrap();
    let old = dc2.ipv4;
    dc2.auth_key = Some(AUTH_KEY);
    session.set_dc_option(&dc2).await.unwrap();
    let new = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 2), 443);
    assert_ne!(old, new);
    let config = config(vec![listed_dc_option(2, new)]);

    // The connect dials the address the session has, and the server gives the datacenter its
    // new one, which the pool keeps when it stores the connection's key.
    assert_eq!(connect_to_dc2(&session, &config).await, old);
    let stored = session.dc_option(2).unwrap().unwrap();
    assert_eq!(stored.ipv4, new);
    assert!(stored.auth_key == Some(AUTH_KEY));
    // So the next run dials the new one.
    assert_eq!(connect_to_dc2(&session, &config).await, new);
}

#[tokio::test]
async fn transport_error_is_reported_as_one() {
    let (client, server) = tokio::io::duplex(1 << 10);
    let mut sender = sender_over(Box::new(client)).await;
    let mut server = Server {
        stream: server,
        tag_read: false,
    };

    // The server answers with a transport error code, as it does for a key it doesn't know: the
    // request fails with it as a transport error, the one the pool and retry policies match.
    let answers = tokio::spawn(async move {
        server.read_message().await;
        server.write_transport_error(404).await;
        server
    });
    let call = sender.invoke(&tl::functions::ReqPqMulti { nonce: [7; 16] });
    let response = timeout(Duration::from_secs(10), call).await.unwrap();
    assert!(
        matches!(
            response,
            Err(InvocationError::Transport(transport::Error::BadStatus {
                status: 404
            }))
        ),
        "{response:?}"
    );
    drop(answers.await.unwrap());
}

#[tokio::test(start_paused = true)]
async fn forgotten_key_starts_a_new_handshake() {
    let session = Arc::new(MemorySession::default());
    let mut dc2 = session.dc_option(2).unwrap().unwrap();
    dc2.auth_key = Some(AUTH_KEY);
    session.set_dc_option(&dc2).await.unwrap();
    let (dialer, mut servers) = TestDialer::new(1 << 16);
    let params = params(Arc::clone(&dialer), Vec::new());
    let pool = SenderPool::with_configuration(Arc::clone(&session), 1, params);
    let runner = tokio::spawn(pool.runner.run());
    let handle = pool.handle.clone();
    let call = tokio::spawn(async move {
        handle
            .invoke_in_dc(2, &tl::functions::help::GetNearestDc {})
            .await
    });

    // The connection opens under the session's key, which the server doesn't know.
    let mut first = servers.recv().await.unwrap();
    first.read_packet().await;
    first.write_transport_error(404).await;
    // So the pool connects again and makes a new key: the new connection starts the handshake.
    let mut second = timeout(Duration::from_secs(3600), servers.recv())
        .await
        .expect("no second connection: the -404 made no new key")
        .unwrap();
    tl::functions::ReqPqMulti::from_boxed_bytes(&second.read_message().await).unwrap();
    // This server can't finish a handshake, so the request fails with the handshake.
    drop(second);

    assert!(call.await.unwrap().is_err());
    assert!(pool.handle.quit());
    runner.await.unwrap();
    let dialed: Vec<_> = dialer
        .targets
        .lock()
        .unwrap()
        .iter()
        .map(|target| target.dc_id)
        .collect();
    assert_eq!(dialed, [2, 2]);
}

#[tokio::test]
async fn ipv6_used_when_ipv4_absent() {
    let listener = TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).await.unwrap();
    let SocketAddr::V6(ipv6) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    // A port nothing listens on: the connection is refused.
    let refused = {
        let closed = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let SocketAddr::V4(address) = closed.local_addr().unwrap() else {
            unreachable!()
        };
        address
    };

    for ipv4 in [None, Some(refused)] {
        let target = DialTarget {
            dc_id: 2,
            kind: ConnKind::Main,
            ipv4,
            ipv6: Some(ipv6),
        };
        let accept = timeout(Duration::from_secs(10), listener.accept());
        let (dialed, accepted) = tokio::join!(TokioTcpDialer.dial(&target), accept);
        let mut stream = dialed.unwrap();
        let (mut accepted, peer) = accepted.unwrap().unwrap();
        assert!(peer.is_ipv6(), "{ipv4:?}");
        stream.write_all(b"ping").await.unwrap();
        let mut bytes = [0; 4];
        accepted.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"ping");
    }

    let nowhere = DialTarget {
        dc_id: 2,
        kind: ConnKind::Main,
        ipv4: None,
        ipv6: None,
    };
    assert_eq!(
        TokioTcpDialer.dial(&nowhere).await.err().unwrap().kind(),
        io::ErrorKind::AddrNotAvailable
    );
}

/// A target with both families, on documentation addresses: the tests that dial it never open a
/// socket.
fn dual_stack_target() -> DialTarget {
    DialTarget {
        dc_id: 2,
        kind: ConnKind::Main,
        ipv4: Some(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 2), 443)),
        ipv6: Some(SocketAddrV6::new(
            Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 2),
            443,
            0,
            0,
        )),
    }
}

#[tokio::test(start_paused = true)]
async fn silent_ipv4_falls_back_to_ipv6_in_10_s() {
    let target = dual_stack_target();
    let start = Instant::now();
    let mut tried = Vec::new();
    // IPv4 never answers; IPv6 connects at once.
    let dial = target.connect_any(|address| {
        tried.push((address, start.elapsed()));
        async move {
            if address.is_ipv4() {
                std::future::pending().await
            } else {
                Ok(address)
            }
        }
    });
    let connected = timeout(Duration::from_secs(3600), dial)
        .await
        .expect("the silent IPv4 address held the dial")
        .unwrap();

    let ipv4 = SocketAddr::from(target.ipv4.unwrap());
    let ipv6 = SocketAddr::from(target.ipv6.unwrap());
    assert_eq!(connected, ipv6);
    assert_eq!(
        tried,
        [(ipv4, Duration::ZERO), (ipv6, Duration::from_secs(10))]
    );
    assert_eq!(start.elapsed(), Duration::from_secs(10));
}

#[tokio::test(start_paused = true)]
async fn two_silent_addresses_time_out_in_20_s() {
    let target = dual_stack_target();
    let start = Instant::now();
    let dial = target.connect_any(|_| std::future::pending::<io::Result<()>>());
    let error = timeout(Duration::from_secs(3600), dial)
        .await
        .expect("a silent address held the dial")
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(start.elapsed(), Duration::from_secs(20));
}

#[test]
#[cfg(not(feature = "os-detect"))]
fn default_params_are_static_without_os_detect() {
    let params = ConnectionParams::default();
    let os = std::env::consts::OS;
    assert_eq!(
        params.device_model,
        format!("{os} {}", std::env::consts::ARCH)
    );
    assert_eq!(params.system_version, os);
    assert_eq!(params.system_lang_code, "en");
    assert_eq!(params.lang_code, "en");
    assert_eq!(params.app_version, env!("CARGO_PKG_VERSION"));
    for value in [
        &params.device_model,
        &params.system_version,
        &params.app_version,
    ] {
        assert!(!value.is_empty());
    }
    // Telegram's keys, production and test.
    assert_eq!(
        params
            .server_rsa_keys
            .iter()
            .map(rsa::Key::fingerprint)
            .collect::<Vec<_>>(),
        [-3414540481677951611, -5595554452916591101]
    );
}

#[tokio::test]
#[cfg(feature = "proxy")]
async fn socks5_dialer_connects_through_the_proxy() {
    use grammers_mtsender::Socks5Dialer;
    use socks5_server::auth::Password;
    use socks5_server::proto::{Address, Reply};
    use socks5_server::{Command, Server};

    let datacenter = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let SocketAddr::V4(datacenter_address) = datacenter.local_addr().unwrap() else {
        unreachable!()
    };
    let proxy_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    // A password with characters a URL's userinfo can't hold as themselves.
    let proxy = Server::new(
        proxy_listener,
        Arc::new(Password::new(b"user".to_vec(), b"se@cr:et/".to_vec())),
    );
    let proxy = tokio::spawn(async move {
        let (conn, _) = proxy.accept().await.unwrap();
        let (conn, _) = conn.authenticate().await.unwrap();
        let Command::Connect(connect, Address::SocketAddress(address)) = conn.wait().await.unwrap()
        else {
            panic!("a connect to a socket address");
        };
        let mut upstream = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut conn = connect
            .reply(Reply::Succeeded, Address::unspecified())
            .await
            .unwrap();
        tokio::io::copy_bidirectional(&mut upstream, &mut conn)
            .await
            .unwrap();
        address
    });

    // The proxy receives the password decoded: written as is, it would refuse the dial.
    let dialer =
        Socks5Dialer::new(&format!("socks5://user:se%40cr%3Aet%2F@{proxy_address}")).unwrap();
    let debug = format!("{dialer:?}");
    assert!(debug.contains("authenticated: true"), "{debug}");
    assert!(!debug.contains("user"), "{debug}");
    assert!(!debug.contains("se@cr"), "{debug}");
    let target = DialTarget {
        dc_id: 2,
        kind: ConnKind::Main,
        ipv4: Some(datacenter_address),
        ipv6: None,
    };
    let mut stream = dialer.dial(&target).await.unwrap();
    let (mut accepted, _) = datacenter.accept().await.unwrap();
    stream.write_all(b"ping").await.unwrap();
    let mut bytes = [0; 4];
    accepted.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"ping");

    drop((stream, accepted));
    assert_eq!(proxy.await.unwrap(), SocketAddr::V4(datacenter_address));

    assert!(Socks5Dialer::new("http://127.0.0.1:1080").is_err());
    // The errors don't repeat the URL, which holds the password.
    for (url, message) in [
        (
            "socks5://user:se%40cr%3Aet%2F@127.0.0.1",
            "proxy url has no port",
        ),
        (
            "socks5:user:se%40cr%3Aet%2F@127.0.0.1:1080",
            "proxy url has no host",
        ),
    ] {
        let error = Socks5Dialer::new(url).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), message);
    }
    // A password that isn't UTF-8 once decoded can't be sent.
    let error = Socks5Dialer::new("socks5://user:%FF@127.0.0.1:1080").unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}
