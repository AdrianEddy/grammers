// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// The pool stores the server's configuration: an option it can't use is skipped, never a panic.
#![cfg_attr(
    not(test),
    deny(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing
    )
)]

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
use std::num::NonZeroU32;
use std::ops::{ControlFlow, Deref};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fmt, io, panic};

use grammers_mtproto::{MAX_REQUEST_SIZE, mtp, transport};
use grammers_session::Session;
use grammers_session::storages::{ErasedSession, erase};
use grammers_session::types::DcOption;
use grammers_session::updates::UpdatesLike;
use grammers_tl_types::{self as tl, Deserializable, enums};
use tokio::sync::mpsc::error::TrySendError;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::{sleep, timeout},
};
use tracing::{debug, info, warn};

use crate::configuration::{ConnectionParams, RetryContext};
use crate::errors::{InvocationError, ReadError};
use crate::net::{ConnKind, DialTarget};
use crate::{Sender, connect, connect_with_auth};

/// How long to wait after warning the user that updates were dropped before warning again.
const UPDATES_DROPPED_LOG_COOLDOWN: Duration = Duration::from_secs(300);

/// How long a new connection has to answer `initConnection`, from the dial on. The runner waits
/// for it, so a peer that accepts and then says nothing can't hold the pool for longer. It leaves
/// room for the slowest dial through [`DialTarget::connect_any`], 10 s on each of a target's two
/// addresses, and for the handshake, including the second dial and handshake a `-404` asks for.
const SETUP_TIMEOUT: Duration = Duration::from_secs(60);

type InvokeResponse = Vec<u8>;

enum Request {
    Invoke {
        dc_id: i32,
        body: Vec<u8>,
        tx: oneshot::Sender<Result<InvokeResponse, InvocationError>>,
    },
    CheckRetry {
        error: InvocationError,
        fail_count: NonZeroU32,
        slept_so_far: Duration,
        tx: oneshot::Sender<ControlFlow<InvocationError, Duration>>,
    },
    Disconnect {
        dc_id: i32,
    },
    Quit,
}

struct Rpc {
    body: Vec<u8>,
    tx: oneshot::Sender<Result<InvokeResponse, InvocationError>>,
    /// An ended connection handed it back once already ([`ConnectionEnd::unsent`]).
    handed_back: bool,
}

struct ConnectionInfo {
    dc_id: i32,
    rpc_tx: mpsc::UnboundedSender<Rpc>,
}

/// What a connection's task hands back to the runner as it ends ([`run_sender`]).
struct ConnectionEnd {
    /// The datacenter it was connected to.
    dc_id: i32,
    /// The requests the runner sent it that it never took, so never sent. A request a connection
    /// hands back is sent on once, through a new connection ([`SenderPoolRunner::dispatch`]);
    /// handed back again, it fails with that connection's [`error`](ConnectionEnd::error), as the
    /// requests that connection took do, and the caller's retry policy decides.
    unsent: Vec<Rpc>,
    /// The error that ended the connection, or `None` if the runner dropped its end of the
    /// channel, which leaves nothing unsent.
    error: Option<ReadError>,
    /// The gap signal the updates channel is still owed ([`UpdatesForwarder::finish`]).
    owed_signal: Option<UpdatesLike>,
}

/// A fat [`SenderPoolHandle`] with additional metadata from its attached [`SenderPoolRunner`].
#[derive(Clone)]
pub struct SenderPoolFatHandle {
    /// The inner thin handle that self can be derefed into.
    ///
    /// The rest of fields can be dropped if they are no longer needed.
    pub thin: SenderPoolHandle,
    /// The session in use by the attached [`SenderPoolRunner`].
    ///
    /// The runner will read and persist datacenter options in it.
    pub session: Arc<ErasedSession>,
    /// Developer's [Application Identifier](https://core.telegram.org/myapp).
    ///
    /// The [`SenderPoolRunner`] will make use of this value when it needs
    /// to invoke [`tl::functions::InitConnection`] after creating a new connection.
    pub api_id: i32,
}

/// Cheaply cloneable handle to interact with its [`SenderPoolRunner`].
#[derive(Clone)]
pub struct SenderPoolHandle(mpsc::UnboundedSender<Request>);

/// Builder to configure the runner to drive I/O and linked handles.
pub struct SenderPool {
    /// The single mutable instance responsible for driving I/O.
    ///
    /// Connections are created on-demand, so any errors while the pool
    /// is running can only be retrieved with one of the [`SenderPool::handle`]s.
    pub runner: SenderPoolRunner,
    /// Starting fat handle attached to the [`SenderPool::runner`].
    ///
    /// Handles are the only way to interact with the runner once it's running.
    pub handle: SenderPoolFatHandle,
    /// The single mutable channel through which updates received
    /// from the network by the [`SenderPool::runner`] are delivered.
    ///
    /// Update handling must be processed in a sequential manner,
    /// so this is a separate instance with no way to clone it.
    pub updates: mpsc::Receiver<UpdatesLike>,
}

/// Manages and runs a pool of zero or more [`Sender`]s.
///
/// Use [`SenderPool::new`] to create an instance of this type and associated channels.
pub struct SenderPoolRunner {
    session: Arc<ErasedSession>,
    api_id: i32,
    connection_params: ConnectionParams,
    request_rx: mpsc::UnboundedReceiver<Request>,
    updates_tx: mpsc::Sender<UpdatesLike>,
    connections: Vec<ConnectionInfo>,
    /// The connections' tasks, each ending with what it hands back ([`ConnectionEnd`]).
    connection_pool: JoinSet<ConnectionEnd>,
    /// A gap signal an ended connection owed the updates channel, sent once it has room. One
    /// covers any number: each makes the receiver fetch the difference.
    owed_signal: Option<UpdatesLike>,
}

impl Deref for SenderPoolFatHandle {
    type Target = SenderPoolHandle;

    fn deref(&self) -> &Self::Target {
        &self.thin
    }
}

impl SenderPoolHandle {
    /// Communicate with the running [`SenderPoolRunner`] instance
    /// to invoke the request in the specified datacenter.
    pub async fn invoke_in_dc<R: tl::RemoteCall>(
        &self,
        dc_id: i32,
        request: &R,
    ) -> Result<R::Return, InvocationError> {
        self.do_invoke_in_dc(dc_id, request.to_bytes())
            .await
            .and_then(|body| R::Return::from_bytes(&body).map_err(|e| e.into()))
    }

    /// Communicate with the running [`SenderPoolRunner`] instance
    /// to invoke the serialized request body in the specified datacenter.
    pub async fn raw_invoke_in_dc(
        &self,
        dc_id: i32,
        body: Vec<u8>,
    ) -> Result<InvokeResponse, InvocationError> {
        if body.len() > MAX_REQUEST_SIZE {
            return Err(InvocationError::Dropped("request body is too large"));
        }

        let (tx, rx) = oneshot::channel();
        self.0
            .send(Request::Invoke { dc_id, body, tx })
            .map_err(|_| InvocationError::Dropped("SenderPoolRunner was dropped at send"))?;
        rx.await
            .map_err(|_| InvocationError::Dropped("SenderPoolRunner was dropped during receive"))?
    }

    /// Communicate with the running [`SenderPoolRunner`] instance
    /// to drop any active connections to the given datacenter.
    ///
    /// Has no effect if there was no connection to the datacenter.
    ///
    /// This is useful after datacenter migrations during sign in,
    /// when the old connection is known to not be needed anymore.
    pub fn disconnect_from_dc(&self, dc_id: i32) -> bool {
        self.0.send(Request::Disconnect { dc_id }).is_ok()
    }

    /// Communicate with the running [`SenderPoolRunner`] instance
    /// to drop all active connections and gracefully stop running.
    ///
    /// The runner takes it after the requests ahead of it, each of which may need a new
    /// connection's dial and `initConnection` (up to 60 seconds).
    pub fn quit(&self) -> bool {
        self.0.send(Request::Quit).is_ok()
    }

    /// do [`Self::raw_invoke_in_dc`] with [`Self::check_retry`]
    pub(crate) async fn do_invoke_in_dc(
        &self,
        dc_id: i32,
        request_body: Vec<u8>,
    ) -> Result<Vec<u8>, InvocationError> {
        let mut fail_count = NonZeroU32::MIN;
        let mut slept_so_far = Duration::default();

        loop {
            match self.raw_invoke_in_dc(dc_id, request_body.clone()).await {
                Ok(response) => break Ok(response),
                Err(e) => {
                    let error_info = format!("{}", e);
                    match self.check_retry(e, fail_count, slept_so_far).await {
                        ControlFlow::Continue(delay) => {
                            info!(?delay, "sleeping on {} before retrying", error_info);
                            sleep(delay).await;
                            fail_count = fail_count.saturating_add(1);
                            slept_so_far += delay;
                            continue;
                        }
                        ControlFlow::Break(final_error) => break Err(final_error),
                    }
                }
            }
        }
    }

    /// Communicate with the running [`SenderPoolRunner`] instance
    /// to check whether the request needs to retry.
    async fn check_retry(
        &self,
        error: InvocationError,
        fail_count: NonZeroU32,
        slept_so_far: Duration,
    ) -> ControlFlow<InvocationError, Duration> {
        let (tx, rx) = oneshot::channel();
        if self
            .0
            .send(Request::CheckRetry {
                error,
                fail_count,
                slept_so_far,
                tx,
            })
            .is_err()
        {
            return ControlFlow::Break(InvocationError::Dropped(
                "SenderPoolRunner was dropped during retry at send",
            ));
        }

        match rx.await {
            Ok(flow) => flow,
            Err(_) => ControlFlow::Break(InvocationError::Dropped(
                "SenderPooLRunner was dropped during retry during receive",
            )),
        }
    }
}

impl SenderPool {
    /// Creates a new sender pool instance with default configuration,
    /// attached to the given session and using the provided
    /// [Application Identifier](https://core.telegram.org/myapp)
    /// belonging to the developer.
    ///
    /// Session instance **should not** be reused by multiple pools at the same time.
    /// The session instance will only be used to query datacenter options and persist
    /// any permanent Authorization Keys generated for previously-unconncected datacenters.
    pub fn new<S>(session: Arc<S>, api_id: i32) -> Self
    where
        S: Session + Sized,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        Self::with_configuration(session, api_id, Default::default())
    }

    /// Creates a new sender pool with non-[`ConnectionParams::default`] configuration.
    pub fn with_configuration<S>(
        session: Arc<S>,
        api_id: i32,
        connection_params: ConnectionParams,
    ) -> Self
    where
        S: Session + Sized,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let session = erase(session);
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (updates_tx, updates_rx) =
            mpsc::channel(connection_params.updates_channel_capacity.get());

        Self {
            runner: SenderPoolRunner {
                session: Arc::clone(&session),
                api_id,
                connection_params,
                request_rx,
                updates_tx,
                connections: Vec::new(),
                connection_pool: JoinSet::new(),
                owed_signal: None,
            },
            handle: SenderPoolFatHandle {
                thin: SenderPoolHandle(request_tx),
                session: Arc::clone(&session),
                api_id,
            },
            updates: updates_rx,
        }
    }
}

impl SenderPoolRunner {
    /// Run the sender pool until [`SenderPoolHandle::quit`] is called or the returned future is dropped.
    ///
    /// Connections will be initiated on-demand whenever the first request to a datacenter is made.
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                Some(completion) = self.connection_pool.join_next(), if !self.connection_pool.is_empty() => {
                    // A connection whose task ended, or is ending, closed its end of the channel
                    // (`run_sender`), so it takes no more requests.
                    self.connections
                        .retain(|connection| !connection.rpc_tx.is_closed());
                    match completion {
                        Ok(end) => {
                            if let Some(signal) = end.owed_signal {
                                self.owed_signal.get_or_insert(signal);
                            }
                            for mut rpc in end.unsent {
                                match &end.error {
                                    // Its second connection ended before taking it too.
                                    Some(error) if rpc.handed_back => {
                                        drop(rpc.tx.send(Err(error.clone().into())));
                                    }
                                    _ => {
                                        rpc.handed_back = true;
                                        self.dispatch(end.dc_id, rpc).await;
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            if let Ok(reason) = err.try_into_panic() {
                                panic::resume_unwind(reason);
                            }
                        }
                    }
                }
                () = send_owed_signal(&self.updates_tx, &mut self.owed_signal), if self.owed_signal.is_some() => {}
                request = self.request_rx.recv() => {
                    let flow = if let Some(request) = request {
                        self.process_request(request).await
                    } else {
                        ControlFlow::Break(())
                    };
                    match flow {
                        ControlFlow::Continue(_) => continue,
                        ControlFlow::Break(_) => break,
                    }
                }
            }
        }

        self.connections.clear(); // drop all channels to cause the `run_sender` loops to stop
        self.connection_pool.join_all().await;
    }

    async fn process_request(&mut self, request: Request) -> ControlFlow<()> {
        match request {
            Request::Invoke { dc_id, body, tx } => {
                let rpc = Rpc {
                    body,
                    tx,
                    handed_back: false,
                };
                self.dispatch(dc_id, rpc).await;
                ControlFlow::Continue(())
            }
            Request::CheckRetry {
                error,
                fail_count,
                slept_so_far,
                tx,
            } => {
                let retry_context = RetryContext {
                    fail_count,
                    slept_so_far,
                    error,
                };
                let flow = match self
                    .connection_params
                    .retry_policy
                    .should_retry(&retry_context)
                {
                    ControlFlow::Continue(delay) => ControlFlow::Continue(delay),
                    ControlFlow::Break(()) => ControlFlow::Break(retry_context.error),
                };
                let _ = tx.send(flow);
                ControlFlow::Continue(())
            }
            Request::Disconnect { dc_id } => {
                // Dropping `rpc_tx` ends `run_sender`, which hands back the signal it owes.
                self.connections
                    .retain(|connection| connection.dc_id != dc_id);
                ControlFlow::Continue(())
            }
            Request::Quit => ControlFlow::Break(()),
        }
    }

    /// Hand `rpc` to the connection to `dc_id`, making one if none is open.
    ///
    /// A connection's task wakes the callers of its requests as its connection fails, before the
    /// runner joins it, so a retry can reach the runner while the connection is still listed. An
    /// ended or ending connection takes nothing more ([`run_sender`] closes its end of the channel
    /// first): the request goes to a new connection, rather than fail with the old one. A request
    /// a connection hands back comes here once: handed back again, it fails with that connection's
    /// error ([`ConnectionEnd::unsent`]).
    async fn dispatch(&mut self, dc_id: i32, mut rpc: Rpc) {
        loop {
            let open = self
                .connections
                .iter()
                .find(|connection| connection.dc_id == dc_id && !connection.rpc_tx.is_closed());
            let connection = match open {
                Some(connection) => connection,
                None => match self.create_connection(dc_id).await {
                    Ok(connection) => connection,
                    Err(error) => {
                        drop(rpc.tx.send(Err(error)));
                        return;
                    }
                },
            };
            match connection.rpc_tx.send(rpc) {
                Ok(()) => return,
                // It closed its end since it was found: it is ending too.
                Err(mpsc::error::SendError(unsent)) => rpc = unsent,
            }
        }
    }

    async fn create_connection(&mut self, dc_id: i32) -> Result<&ConnectionInfo, InvocationError> {
        let dc_option = match self.session.dc_option(dc_id)? {
            Some(x) => x,
            None => return Err(InvocationError::InvalidDc),
        };

        let sender = self.connect_sender(&dc_option).await?;

        // `connect_sender` stored the addresses the server gave for this datacenter.
        let mut dc_option = self.session.dc_option(dc_id)?.unwrap_or(dc_option);
        dc_option.auth_key = Some(sender.auth_key());
        self.session.set_dc_option(&dc_option).await?;

        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        self.connection_pool.spawn(run_sender(
            dc_id,
            sender,
            rpc_rx,
            UpdatesForwarder::new(self.updates_tx.clone()),
            dc_option.id == self.session.home_dc_id()?,
        ));
        Ok(self.connections.push_mut(ConnectionInfo { dc_id, rpc_tx }))
    }

    async fn connect_sender(
        &mut self,
        dc_option: &DcOption,
    ) -> Result<Sender<Box<dyn transport::Transport + Send>, mtp::Encrypted>, InvocationError> {
        let transport = &self.connection_params.transport;
        let dialer = &*self.connection_params.dialer;
        let rsa_keys = &self.connection_params.server_rsa_keys;
        let target = DialTarget::new(dc_option, ConnKind::Main);

        let init_connection = tl::functions::InvokeWithLayer {
            layer: tl::LAYER,
            query: tl::functions::InitConnection {
                api_id: self.api_id,
                device_model: self.connection_params.device_model.clone(),
                system_version: self.connection_params.system_version.clone(),
                app_version: self.connection_params.app_version.clone(),
                system_lang_code: self.connection_params.system_lang_code.clone(),
                lang_pack: "".into(),
                lang_code: self.connection_params.lang_code.clone(),
                proxy: None,
                params: None,
                query: tl::functions::help::GetConfig {},
            },
        };

        // Only the network part has a deadline: a session write is never cancelled.
        let (sender, enums::Config::Config(remote_config)) = timeout(SETUP_TIMEOUT, async {
            let mut sender = if let Some(auth_key) = dc_option.auth_key {
                connect_with_auth(transport(), dialer, &target, auth_key)
                    .await
                    .map_err(InvocationError::Io)?
            } else {
                connect(transport(), dialer, &target, rsa_keys).await?
            };

            let config = match sender.invoke(&init_connection).await {
                Ok(config) => config,
                Err(InvocationError::Transport(transport::Error::BadStatus { status: 404 })) => {
                    sender = connect(transport(), dialer, &target, rsa_keys).await?;
                    sender.invoke(&init_connection).await?
                }
                Err(e) => return Err(e),
            };
            Ok((sender, config))
        })
        .await
        .map_err(|_| InvocationError::Io(io::ErrorKind::TimedOut.into()))??;

        self.update_dc_options(&remote_config.dc_options).await?;

        Ok(sender)
    }

    /// Store the addresses of the datacenters in the server's `dc_options`.
    ///
    /// An option whose address or port isn't valid is skipped, with a warning: the session keeps
    /// what it knew of that datacenter.
    async fn update_dc_options(
        &self,
        dc_options: &[tl::enums::DcOption],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for option in dc_options
            .iter()
            .map(|tl::enums::DcOption::Option(option)| option)
            .filter(|option| !option.media_only && !option.tcpo_only && option.r#static)
        {
            let Ok(port) = u16::try_from(option.port) else {
                warn!(
                    dc_id = option.id,
                    port = option.port,
                    "skipping a datacenter option with an invalid port"
                );
                continue;
            };
            let mut dc_option = self
                .session
                .dc_option(option.id)?
                .unwrap_or_else(|| DcOption {
                    id: option.id,
                    ipv4: SocketAddrV4::new(Ipv4Addr::from_bits(0), 0),
                    ipv6: SocketAddrV6::new(Ipv6Addr::from_bits(0), 0, 0, 0),
                    auth_key: None,
                });
            if option.ipv6 {
                let Ok(ip) = option.ip_address.parse::<Ipv6Addr>() else {
                    warn!(
                        dc_id = option.id,
                        address = option.ip_address,
                        "skipping a datacenter option with an invalid IPv6 address"
                    );
                    continue;
                };
                dc_option.ipv6 = SocketAddrV6::new(ip, port, 0, 0);
            } else {
                let Ok(ip) = option.ip_address.parse::<Ipv4Addr>() else {
                    warn!(
                        dc_id = option.id,
                        address = option.ip_address,
                        "skipping a datacenter option with an invalid IPv4 address"
                    );
                    continue;
                };
                dc_option.ipv4 = SocketAddrV4::new(ip, port);
                if dc_option.ipv6.ip().to_bits() == 0 {
                    dc_option.ipv6 = SocketAddrV6::new(
                        dc_option.ipv4.ip().to_ipv6_mapped(),
                        dc_option.ipv4.port(),
                        0,
                        0,
                    )
                }
            }
            self.session.set_dc_option(&dc_option).await?;
        }
        Ok(())
    }
}

/// Wait for room in `updates`, then send it the `owed` signal. Without a receiver, the signal has
/// nobody to reach, and is dropped.
async fn send_owed_signal(updates: &mpsc::Sender<UpdatesLike>, owed: &mut Option<UpdatesLike>) {
    let permit = updates.reserve().await;
    if let (Ok(permit), Some(signal)) = (permit, owed.take()) {
        permit.send(signal);
    }
}

/// Drive `sender`, the connection to `dc_id`, feeding it the requests from `rpc_rx` and forwarding
/// its updates, until its connection fails or the runner drops the other end of `rpc_rx`. Then
/// close `rpc_rx`, so the runner can send it nothing more, and hand back, for the runner to send
/// on, the requests still queued in it, which the sender never took, with the error that ended
/// the connection and the gap signal the updates channel is still owed ([`ConnectionEnd`]).
///
/// A new session ([`UpdatesLike::NewSession`]) is a gap in the account's updates only on the home
/// connection, which they come on, so only the home sender forwards it, as only it reports its
/// closing (TDLib also fetches the difference on the main session's new session alone).
async fn run_sender<T: transport::Transport, M: mtp::Mtp>(
    dc_id: i32,
    mut sender: Sender<T, M>,
    mut rpc_rx: mpsc::UnboundedReceiver<Rpc>,
    mut updates: UpdatesForwarder,
    home_sender: bool,
) -> ConnectionEnd {
    let (owed_signal, error) = loop {
        tokio::select! {
            step = sender.step() => match step {
                Ok(all_new_updates) => {
                    for new_updates in all_new_updates {
                        if home_sender || !matches!(new_updates, UpdatesLike::NewSession) {
                            updates.forward(new_updates);
                        }
                    }
                }
                // `Sender::step` logged the error and failed the requests with it.
                Err(error) => break (updates.finish(home_sender), Some(error)),
            },
            () = updates.send_lagged(), if updates.owes_lagged() => {}
            rpc = rpc_rx.recv() => match rpc {
                Some(rpc) => sender.enqueue_body(rpc.body, rpc.tx),
                None => break (updates.finish(false), None),
            },
        }
    };

    // From now on a send to this connection fails, and gives the runner its request back; one
    // sent before is in the channel, which `recv` drains until it reports the end.
    rpc_rx.close();
    let mut unsent = Vec::new();
    while let Some(rpc) = rpc_rx.recv().await {
        unsent.push(rpc);
    }
    ConnectionEnd {
        dc_id,
        unsent,
        error,
        owed_signal,
    }
}

/// Forwards one connection's updates into the bounded updates channel.
///
/// An update that finds the channel full is dropped, and the receiver is owed an
/// [`UpdatesLike::Lagged`]: it is sent as soon as the channel has room
/// ([`UpdatesForwarder::send_lagged`]), and always before this connection's next update, so the
/// receiver fetches the difference instead of waiting for the next gap to show.
struct UpdatesForwarder {
    tx: mpsc::Sender<UpdatesLike>,
    /// Updates were dropped since the last [`UpdatesLike::Lagged`] went out.
    lagged: bool,
    /// The receiver is gone: nothing more is forwarded.
    closed: bool,
    /// Updates dropped since the last warning.
    dropped: usize,
    last_warning: Option<Instant>,
}

impl UpdatesForwarder {
    fn new(tx: mpsc::Sender<UpdatesLike>) -> Self {
        Self {
            tx,
            lagged: false,
            closed: false,
            dropped: 0,
            last_warning: None,
        }
    }

    /// Forward `updates`, after the [`UpdatesLike::Lagged`] owed, if one is.
    fn forward(&mut self, updates: UpdatesLike) {
        if self.closed {
            return;
        }
        if self.lagged {
            match self.tx.try_send(UpdatesLike::Lagged) {
                Ok(()) => self.lagged = false,
                Err(TrySendError::Full(_)) => {
                    self.count_dropped();
                    return;
                }
                Err(TrySendError::Closed(_)) => {
                    self.close();
                    return;
                }
            }
        }
        match self.tx.try_send(updates) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.lagged = true;
                self.count_dropped();
            }
            Err(TrySendError::Closed(_)) => self.close(),
        }
    }

    /// Whether an [`UpdatesLike::Lagged`] is owed to a receiver that is still there.
    fn owes_lagged(&self) -> bool {
        self.lagged && !self.closed
    }

    /// Wait for room in the channel, then send the [`UpdatesLike::Lagged`] owed. Call only while
    /// [`Self::owes_lagged`]; cancelling it loses nothing.
    async fn send_lagged(&mut self) {
        if let Ok(permit) = self.tx.reserve().await {
            permit.send(UpdatesLike::Lagged);
            self.lagged = false;
            return;
        }
        self.close();
    }

    /// End forwarding as the connection ends, and return the gap signal the channel is still
    /// owed, which only the runner can send now. If `closed_connection`, that is an
    /// [`UpdatesLike::ConnectionClosed`] that found the channel full (it also covers a
    /// [`UpdatesLike::Lagged`] owed); otherwise, the `Lagged` owed, if one is.
    fn finish(self, closed_connection: bool) -> Option<UpdatesLike> {
        if self.closed {
            return None;
        }
        if closed_connection {
            return match self.tx.try_send(UpdatesLike::ConnectionClosed) {
                Ok(()) | Err(TrySendError::Closed(_)) => None,
                Err(TrySendError::Full(signal)) => Some(signal),
            };
        }
        self.lagged.then_some(UpdatesLike::Lagged)
    }

    fn count_dropped(&mut self) {
        self.dropped += 1;
        if self
            .last_warning
            .is_none_or(|last| last.elapsed() >= UPDATES_DROPPED_LOG_COOLDOWN)
        {
            warn!(
                capacity = self.tx.max_capacity(),
                dropped_count = self.dropped,
                "updates channel full; the receiver will be told to fetch the difference",
            );
            self.dropped = 0;
            self.last_warning = Some(Instant::now());
        }
    }

    fn close(&mut self) {
        debug!("updates channel closed, stopping update forwarding");
        self.closed = true;
    }
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invoke { dc_id, body, tx } => f
                .debug_struct("Invoke")
                .field("dc_id", dc_id)
                .field(
                    "request",
                    &body
                        .first_chunk()
                        .map(|constructor_id| tl::name_for_id(u32::from_le_bytes(*constructor_id)))
                        .unwrap_or("?"),
                )
                .field("tx", tx)
                .finish(),
            Self::CheckRetry {
                error,
                fail_count,
                slept_so_far,
                tx,
            } => f
                .debug_struct("CheckRetry")
                .field("error", error)
                .field("fail_count", fail_count)
                .field("slept_so_far", slept_so_far)
                .field("tx", tx)
                .finish(),
            Self::Disconnect { dc_id } => {
                f.debug_struct("Disconnect").field("dc_id", dc_id).finish()
            }
            Self::Quit => write!(f, "Quit"),
        }
    }
}

#[cfg(test)]
mod tests;
