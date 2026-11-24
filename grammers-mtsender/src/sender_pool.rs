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
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use std::{fmt, io, mem, panic};

use grammers_mtproto::{MAX_REQUEST_SIZE, mtp, transport};
use grammers_session::Session;
use grammers_session::storages::{ErasedSession, erase};
use grammers_session::types::DcOption;
use grammers_session::updates::UpdatesLike;
use grammers_tl_types::{self as tl, Deserializable, enums};
use tokio::sync::mpsc::error::TrySendError;
use tokio::{
    sync::{mpsc, oneshot},
    task::{self, JoinSet},
    time::{sleep, sleep_until, timeout},
};
use tracing::{debug, info, warn};

use crate::configuration::{ConnectionParams, RetryContext};
use crate::errors::{InvocationError, ReadError};
use crate::net::{ConnKind, DialTarget, PermitFuture};
use crate::{Sender, connect, connect_with_auth};

/// How long to wait after warning the user that updates were dropped before warning again.
const UPDATES_DROPPED_LOG_COOLDOWN: Duration = Duration::from_secs(300);

/// How long a new connection's network steps have, together, to answer `initConnection`, from the
/// dial on ([`SetupBudget`]). The runner waits for them, so a peer that accepts and then says
/// nothing can't hold the pool for longer. It leaves room for the slowest dial through
/// [`DialTarget::connect_any`], 10 s on each of a target's two addresses, and for the handshake,
/// including the second dial and handshake a `-404` asks for. The session writes between the
/// steps don't count: a session write is never cancelled. A request held for the dialer's permit
/// ([`crate::Dialer::permit`]) waits as long at most, then fails as a setup past it does
/// ([`SenderPoolRunner::expire_held`]).
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
        /// Told once the connections to `dc_id` the runner found as it read the disconnect have
        /// ended ([`Disconnection`]).
        done: oneshot::Sender<()>,
    },
    Quit,
}

struct Rpc {
    body: Vec<u8>,
    tx: oneshot::Sender<Result<InvokeResponse, InvocationError>>,
    /// An ended connection handed it back before ([`ConnectionEnd::unsent`]).
    handed_back: bool,
}

struct ConnectionInfo {
    dc_id: i32,
    rpc_tx: mpsc::UnboundedSender<Rpc>,
}

/// A request held for the dialer's permit ([`crate::Dialer::permit`]): it needs a new connection
/// to its datacenter, which can't be dialed yet ([`SenderPoolRunner::dispatch`]).
struct Held {
    dc_id: i32,
    rpc: Rpc,
    /// When its wait runs out: [`SETUP_TIMEOUT`] after it was first held.
    expires: tokio::time::Instant,
}

/// A disconnect waiting for the connections to its datacenter to end
/// ([`SenderPoolHandle::disconnect_from_dc`]).
struct PendingDisconnect {
    /// The tasks of the connections it waits for that haven't ended.
    left: Vec<task::Id>,
    done: oneshot::Sender<()>,
    /// What those that ended handed back, held until it is told: sent on then, which may dial.
    unsent: Vec<(i32, Rpc)>,
}

/// What a connection's task hands back to the runner as it ends ([`run_sender`]).
struct ConnectionEnd {
    /// The datacenter it was connected to.
    dc_id: i32,
    /// The requests it never sent whose callers still wait: the ones the runner sent it that it
    /// never took, and, if the runner closed it, the ones it took and never wrote
    /// ([`Sender::close`]). A request a connection hands back is sent on through a new connection
    /// ([`SenderPoolRunner::dispatch`]). If a connection that failed hands back a request that was
    /// handed back before, the request fails with that connection's [`error`](ConnectionEnd::error)
    /// instead, as the requests that connection took do, and the caller's retry policy decides.
    unsent: Vec<Rpc>,
    /// The error that ended the connection, or `None` if the runner closed it: it dropped its end
    /// of the channel ([`SenderPoolHandle::disconnect_from_dc`], or the pool quitting).
    error: Option<ReadError>,
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

/// A disconnect the runner was asked for ([`SenderPoolHandle::disconnect_from_dc`]).
///
/// Awaited, it resolves once the connections to the datacenter that the runner found as it read
/// the disconnect have ended, those it dropped and any that was ending already: their tasks have
/// ended, and their sockets with them. That comes before the requests they never sent go on
/// through a new connection, which may dial. It resolves to `true` then, or to `false` if the
/// runner was gone first, its connections with it.
///
/// It doesn't wait for a connection that a request made after the disconnect opens meanwhile: such
/// a request goes on as any does, through a new connection if none is open. While the dialer's
/// permit is shut ([`crate::Dialer::permit`]) the runner opens none, but holds the request, so
/// the disconnect resolves with no connection to the datacenter left. A caller that needs none
/// open once it resolves shuts the permit before it disconnects.
#[derive(Debug)]
pub struct Disconnection(Option<oneshot::Receiver<()>>);

impl Future for Disconnection {
    type Output = bool;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<bool> {
        match &mut self.0 {
            Some(ended) => Pin::new(ended).poll(cx).map(|ended| ended.is_ok()),
            None => Poll::Ready(false),
        }
    }
}

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
    ///
    /// Besides the updates, it carries what the receiver needs to know of the connections:
    ///
    /// - [`UpdatesLike::ConnectionOpened`] once a connection to the home datacenter, which the
    ///   account's updates come on, has its `initConnection` answered, before any request goes to
    ///   it, and [`UpdatesLike::ConnectionClosed`] once such a connection fails. The opens and the
    ///   closes come in the order they happened, each behind the signals before it, also while the
    ///   channel is full, so the last of them the receiver has read says whether the home
    ///   connection is up. While the channel is full they are owed, and the receiver then reads
    ///   what it would have made of them one by one: a close takes the place of an open it follows
    ///   (that connection ended before the receiver heard of it), and of a
    ///   [`UpdatesLike::Lagged`] (both make it fetch the difference); an open goes after them.
    /// - [`UpdatesLike::NewSession`] when the server starts a new session on a home connection
    ///   that is up. A connection's first session, which the answer to the pool's own
    ///   `initConnection` creates, isn't forwarded: the pool reads no update of that call.
    /// - [`UpdatesLike::Lagged`] when updates were dropped because the channel was full.
    ///
    /// The updates aren't ordered against the signals owed: a connection's updates can be read
    /// before its open while that is owed, or before the close owed for the connection before
    /// it. The message boxes order the updates by their `pts`, `qts` and `seq`.
    ///
    /// A connection the runner drops on request ([`SenderPoolHandle::disconnect_from_dc`], or the
    /// pool quitting) gets no close: its caller knows it is gone, and no gap is owed for it.
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
    /// The updates channel's sender, and the connection signals it is owed, which the
    /// connections' tasks share.
    signals: ConnectionSignals,
    connections: Vec<ConnectionInfo>,
    /// The connections' tasks, each ending with what it hands back ([`ConnectionEnd`]).
    connection_pool: JoinSet<ConnectionEnd>,
    /// Each task in `connection_pool`, with its connection's datacenter: those still listed in
    /// `connections`, and those ending, which no longer are.
    tasks: Vec<(task::Id, i32)>,
    /// The disconnects waiting for their datacenter's connections to end.
    disconnects: Vec<PendingDisconnect>,
    /// The requests held for the dialer's permit, in the order they came ([`Self::dispatch`]).
    held: Vec<Held>,
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
    ///
    /// The requests the connections hold aren't dropped. One whose bytes left fails with an I/O
    /// error of kind [`std::io::ErrorKind::ConnectionAborted`]: the server may have taken it, and
    /// its answer could come only on the closed connection. Any other goes to a new connection to
    /// the datacenter, as a request made after the disconnect would.
    ///
    /// The request goes to the runner at once. The [`Disconnection`] it returns may be awaited
    /// until the connections have ended, their sockets closed; dropping it cancels nothing. The
    /// requests the runner holds for the dialer's permit ([`crate::Dialer::permit`]) stay held:
    /// they go to the next connection.
    pub fn disconnect_from_dc(&self, dc_id: i32) -> Disconnection {
        let (done, ended) = oneshot::channel();
        let sent = self.0.send(Request::Disconnect { dc_id, done }).is_ok();
        Disconnection(sent.then_some(ended))
    }

    /// Communicate with the running [`SenderPoolRunner`] instance
    /// to drop all active connections and gracefully stop running.
    ///
    /// The runner takes it after the requests ahead of it, those a closed or failed connection
    /// handed back included, each of which may need a new connection's dial and `initConnection`
    /// (up to 60 seconds). The requests the connections still hold then fail: one whose bytes left
    /// with an I/O error of kind [`std::io::ErrorKind::ConnectionAborted`], as
    /// [`Self::disconnect_from_dc`] fails it, and any other with [`InvocationError::Dropped`], as
    /// those held for the dialer's permit ([`crate::Dialer::permit`]) do.
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
                signals: ConnectionSignals::new(updates_tx),
                connections: Vec::new(),
                connection_pool: JoinSet::new(),
                tasks: Vec::new(),
                disconnects: Vec::new(),
                held: Vec::new(),
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
    /// Connections will be initiated on-demand whenever the first request to a datacenter is made,
    /// once the dialer permits it ([`crate::Dialer::permit`]).
    pub async fn run(mut self) {
        // The dialer's permit, waited for while requests are held for it.
        let mut permit: Option<PermitFuture> = None;
        loop {
            if self.held.is_empty() {
                permit = None;
            } else if permit.is_none() {
                permit = Some(self.connection_params.dialer.permit());
            }
            let expiry = self.held.iter().map(|held| held.expires).min();
            tokio::select! {
                biased;
                Some(completion) = self.connection_pool.join_next_with_id(), if !self.connection_pool.is_empty() => {
                    // A connection whose task ended, or is ending, closed its end of the channel
                    // (`run_sender`), so it takes no more requests.
                    self.connections
                        .retain(|connection| !connection.rpc_tx.is_closed());
                    let (id, unsent) = match completion {
                        Ok((id, end)) => {
                            let mut unsent = Vec::new();
                            for mut rpc in end.unsent {
                                match &end.error {
                                    // Its second connection ended before taking it too.
                                    Some(error) if rpc.handed_back => {
                                        drop(rpc.tx.send(Err(error.clone().into())));
                                    }
                                    _ => {
                                        rpc.handed_back = true;
                                        unsent.push((end.dc_id, rpc));
                                    }
                                }
                            }
                            (id, unsent)
                        }
                        Err(err) => {
                            let id = err.id();
                            if let Ok(reason) = err.try_into_panic() {
                                panic::resume_unwind(reason);
                            }
                            (id, Vec::new())
                        }
                    };
                    for (dc_id, rpc) in self.ended(id, unsent) {
                        self.dispatch(dc_id, rpc, None).await;
                    }
                }
                // A connection's task owes its signal as it ends, so the join above wakes this.
                () = self.signals.send_owed(), if self.signals.owes() => {}
                () = sleep_until_some(expiry), if expiry.is_some() => self.expire_held(),
                () = permit_given(&mut permit), if permit.is_some() => {
                    permit = None;
                    self.release_held().await;
                }
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

        // The held requests' callers are told `Dropped`, as those of the requests the connections
        // never sent are.
        self.held.clear();
        self.connections.clear(); // drop all channels to cause the `run_sender` loops to stop
        self.connection_pool.join_all().await;
        // No connection is left, and nothing they handed back goes on.
        for pending in self.disconnects.drain(..) {
            let _ = pending.done.send(());
        }
    }

    /// The connection task `id` has ended (its socket with it), handing back `unsent`: each
    /// disconnect that waited for it is told once every connection it waits for has ended, and
    /// then what they handed back is returned, to go on, which may dial. While a disconnect still
    /// waits for another of its connections, what this one handed back is held with it: with the
    /// last disconnect that waited for this one, which is told no earlier than any other that did
    /// (each waits for every connection to its datacenter not ended as it came, so a later one
    /// waits for all an earlier one still waits for).
    fn ended(&mut self, id: task::Id, unsent: Vec<(i32, Rpc)>) -> Vec<(i32, Rpc)> {
        self.tasks.retain(|(task, _)| *task != id);
        let mut unsent = Some(unsent);
        for pending in self.disconnects.iter_mut().rev() {
            if let Some(at) = pending.left.iter().position(|left| *left == id) {
                pending.left.swap_remove(at);
                pending.unsent.extend(unsent.take().into_iter().flatten());
            }
        }
        let mut go_on = unsent.unwrap_or_default();
        for pending in self
            .disconnects
            .extract_if(.., |pending| pending.left.is_empty())
        {
            let _ = pending.done.send(());
            go_on.extend(pending.unsent);
        }
        go_on
    }

    async fn process_request(&mut self, request: Request) -> ControlFlow<()> {
        match request {
            Request::Invoke { dc_id, body, tx } => {
                let rpc = Rpc {
                    body,
                    tx,
                    handed_back: false,
                };
                self.dispatch(dc_id, rpc, None).await;
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
            Request::Disconnect { dc_id, done } => {
                // Dropping `rpc_tx` ends `run_sender`, which fails the requests whose bytes left
                // and hands back the others, which the join arm sends on, with the signal it owes,
                // once the disconnect has been told. The requests held for the dialer's permit stay
                // held: none has a connection to drop.
                self.connections
                    .retain(|connection| connection.dc_id != dc_id);
                let left: Vec<task::Id> = self
                    .tasks
                    .iter()
                    .filter(|(_, dc)| *dc == dc_id)
                    .map(|(task, _)| *task)
                    .collect();
                if left.is_empty() {
                    let _ = done.send(());
                } else {
                    self.disconnects.push(PendingDisconnect {
                        left,
                        done,
                        unsent: Vec::new(),
                    });
                }
                ControlFlow::Continue(())
            }
            Request::Quit => ControlFlow::Break(()),
        }
    }

    /// Hand `rpc` to the connection to `dc_id`, making one if none is open, or else hold it while
    /// the dialer's permit is shut ([`crate::Dialer::permit`]), behind those held for `dc_id`
    /// already, which go first. `expires` is when the wait of a request held before runs out
    /// ([`Self::hold`]).
    ///
    /// The runner never waits for the permit: it goes on with every other request, disconnects
    /// among them, and with the connections that end, and the held requests go on once the permit
    /// comes ([`Self::release_held`]). Waiting for it here would hold all of that: a caller that
    /// awaits a disconnect while it keeps the permit shut would wait until the wait ran out.
    ///
    /// A connection's task wakes the callers of its requests as its connection fails, before the
    /// runner joins it, so a retry can reach the runner while the connection is still listed. An
    /// ended or ending connection takes nothing more ([`run_sender`] closes its end of the channel
    /// first): the request goes to a new connection, rather than fail with the old one. A request
    /// a connection hands back comes here too; handed back again by a connection that failed, it
    /// fails with that connection's error instead ([`ConnectionEnd::unsent`]).
    async fn dispatch(&mut self, dc_id: i32, mut rpc: Rpc, expires: Option<tokio::time::Instant>) {
        loop {
            let open = self
                .connections
                .iter()
                .find(|connection| connection.dc_id == dc_id && !connection.rpc_tx.is_closed());
            let connection = match open {
                Some(connection) => connection,
                None if self.held.iter().any(|held| held.dc_id == dc_id) || !self.permitted() => {
                    self.hold(dc_id, rpc, expires);
                    return;
                }
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

    /// Whether the dialer permits a dial now ([`crate::Dialer::permit`]).
    fn permitted(&self) -> bool {
        let mut permit = self.connection_params.dialer.permit();
        permit
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    }

    /// Hold `rpc` for the dialer's permit, behind every request held before it, until `expires`
    /// if it was held before, or else for [`SETUP_TIMEOUT`]: the wait counts against a new
    /// connection's setup time, as it did when a dialer waited inside its dial.
    fn hold(&mut self, dc_id: i32, rpc: Rpc, expires: Option<tokio::time::Instant>) {
        let expires = expires.unwrap_or_else(|| tokio::time::Instant::now() + SETUP_TIMEOUT);
        self.held.push(Held {
            dc_id,
            rpc,
            expires,
        });
    }

    /// The dialer's permit came: the held requests go on in the order they came
    /// ([`Self::dispatch`]), so a datacenter's first one makes the new connection and the others
    /// go to it. One whose caller no longer waits is dropped: it could only make a connection for
    /// nobody. One that finds the permit shut again is held again, its wait running out when it
    /// would have.
    async fn release_held(&mut self) {
        for held in mem::take(&mut self.held) {
            if !held.rpc.tx.is_closed() {
                self.dispatch(held.dc_id, held.rpc, Some(held.expires))
                    .await;
            }
        }
    }

    /// Fail the held requests whose wait for the dialer's permit has run out with an I/O error of
    /// kind [`io::ErrorKind::TimedOut`], as a new connection's setup past [`SETUP_TIMEOUT`] fails.
    fn expire_held(&mut self) {
        let now = tokio::time::Instant::now();
        for held in self.held.extract_if(.., |held| held.expires <= now) {
            drop(
                held.rpc
                    .tx
                    .send(Err(InvocationError::Io(io::ErrorKind::TimedOut.into()))),
            );
        }
    }

    async fn create_connection(&mut self, dc_id: i32) -> Result<&ConnectionInfo, InvocationError> {
        let dc_option = match self.session.dc_option(dc_id)? {
            Some(x) => x,
            None => return Err(InvocationError::InvalidDc),
        };

        // `connect_sender` stored the connection's key, if it made one, and the addresses the
        // server gave.
        let sender = self.connect_sender(&dc_option).await?;

        // The account's updates come on the home connection, which says it is up before anything
        // goes to it, behind the signals the connections before it owe.
        let home_sender = dc_id == self.session.home_dc_id()?;
        if home_sender {
            self.signals.send(Signal::Opened);
        }
        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        let task = self.connection_pool.spawn(run_sender(
            dc_id,
            sender,
            rpc_rx,
            UpdatesForwarder::new(self.signals.clone()),
            home_sender,
        ));
        self.tasks.push((task.id(), dc_id));
        Ok(self.connections.push_mut(ConnectionInfo { dc_id, rpc_tx }))
    }

    /// Connect to `dc_option`'s datacenter and invoke `initConnection` on the new connection.
    ///
    /// A new authorization key, made by the handshake when the session holds none for the
    /// datacenter or when the server answers `-404` for the one it holds, is stored before
    /// anything goes out under it, as TDLib hands a new key to its storage once the handshake is
    /// ready, before the session's first query. So every key a packet was sent under is one the
    /// session holds: a connection that ends between the handshake and the answer to
    /// `initConnection` (the pool's runner dropped, the setup timed out, the connection failed)
    /// leaves no packet under a key that was never stored. The network steps share
    /// [`SETUP_TIMEOUT`]; the session writes between them aren't timed.
    async fn connect_sender(
        &self,
        dc_option: &DcOption,
    ) -> Result<Sender<Box<dyn transport::Transport + Send>, mtp::Encrypted>, InvocationError> {
        let target = DialTarget::new(dc_option, ConnKind::Main);
        let mut budget = SetupBudget {
            left: SETUP_TIMEOUT,
        };

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

        let mut sender = match dc_option.auth_key {
            Some(auth_key) => {
                let transport = (self.connection_params.transport)();
                let dialer = &*self.connection_params.dialer;
                budget
                    .run(async {
                        connect_with_auth(transport, dialer, &target, auth_key)
                            .await
                            .map_err(InvocationError::Io)
                    })
                    .await?
            }
            None => {
                self.connect_new_key(&mut budget, dc_option, &target)
                    .await?
            }
        };
        let enums::Config::Config(remote_config) =
            match budget.run(sender.invoke(&init_connection)).await {
                Ok(config) => config,
                // The server doesn't know the session's key: a new one replaces it.
                Err(InvocationError::Transport(transport::Error::BadStatus { status: 404 })) => {
                    sender = self
                        .connect_new_key(&mut budget, dc_option, &target)
                        .await?;
                    budget.run(sender.invoke(&init_connection)).await?
                }
                Err(e) => return Err(e),
            };

        self.update_dc_options(&remote_config.dc_options).await?;

        Ok(sender)
    }

    /// Dial `target` and make a new authorization key on the connection, within what is left of
    /// `budget`, then store the key in `dc_option`'s row before the connection carries anything
    /// under it. The write isn't timed: a session write is never cancelled.
    async fn connect_new_key(
        &self,
        budget: &mut SetupBudget,
        dc_option: &DcOption,
        target: &DialTarget,
    ) -> Result<Sender<Box<dyn transport::Transport + Send>, mtp::Encrypted>, InvocationError> {
        let transport = (self.connection_params.transport)();
        let dialer = &*self.connection_params.dialer;
        let rsa_keys = &self.connection_params.server_rsa_keys;
        let sender = budget
            .run(connect(transport, dialer, target, rsa_keys))
            .await?;
        let mut keyed = dc_option.clone();
        keyed.auth_key = Some(sender.auth_key());
        self.session.set_dc_option(&keyed).await?;
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

/// Wait for the dialer's permit, if one is waited for ([`SenderPoolRunner::run`]).
async fn permit_given(permit: &mut Option<PermitFuture>) {
    match permit {
        Some(permit) => permit.await,
        None => std::future::pending().await,
    }
}

/// Sleep until `at`, if there is one ([`SenderPoolRunner::run`]).
async fn sleep_until_some(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// What is left of [`SETUP_TIMEOUT`] for a new connection's network steps. Each step is timed
/// against it and uses up what it took, so the session writes between the steps, which are never
/// cancelled, take none of it.
struct SetupBudget {
    left: Duration,
}

impl SetupBudget {
    /// Run `step` within what is left, failing it with an I/O error of kind
    /// [`io::ErrorKind::TimedOut`] once that runs out.
    async fn run<T>(
        &mut self,
        step: impl Future<Output = Result<T, InvocationError>>,
    ) -> Result<T, InvocationError> {
        let started = tokio::time::Instant::now();
        let result = timeout(self.left, step).await;
        self.left = self.left.saturating_sub(started.elapsed());
        result.map_err(|_| InvocationError::Io(io::ErrorKind::TimedOut.into()))?
    }
}

/// A signal of the connections for the updates channel ([`SenderPool::updates`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Signal {
    /// A home connection opened: [`UpdatesLike::ConnectionOpened`].
    Opened,
    /// A home connection failed: [`UpdatesLike::ConnectionClosed`], a gap.
    Closed,
    /// An ended connection dropped updates the channel had no room for: [`UpdatesLike::Lagged`].
    Lagged,
}

impl Signal {
    /// What the receiver reads of it.
    fn updates(self) -> UpdatesLike {
        match self {
            Signal::Opened => UpdatesLike::ConnectionOpened,
            Signal::Closed => UpdatesLike::ConnectionClosed,
            Signal::Lagged => UpdatesLike::Lagged,
        }
    }
}

/// The connection signals the updates channel is owed, as what the receiver makes of them: a gap
/// signal, then an open. They are owed only while the channel is full, or while others are owed,
/// so the opens and the closes reach the receiver in the order they happened. Each takes what it
/// covers of those before it ([`Owed::add`]), so at most two are ever owed, however long the
/// receiver doesn't read: it acts only on whether to fetch the difference and on whether the home
/// connection is up, and these two keep both.
#[derive(Debug, Default)]
struct Owed {
    /// [`Signal::Closed`] or [`Signal::Lagged`], never [`Signal::Opened`].
    gap: Option<Signal>,
    /// A home connection opened after it.
    opened: bool,
}

impl Owed {
    fn is_empty(&self) -> bool {
        self.gap.is_none() && !self.opened
    }

    /// Owe `signal` with those owed: a close takes the place of an owed open, whose connection
    /// ended before the receiver heard of it, and of an owed `Lagged` (both make it fetch the
    /// difference); an open goes after the gap; a `Lagged` an owed gap covers adds nothing, and
    /// one that finds only an open owed goes ahead of it, so the open is still the last
    /// connection signal the receiver reads.
    fn add(&mut self, signal: Signal) {
        match signal {
            Signal::Opened => self.opened = true,
            Signal::Closed => {
                self.opened = false;
                self.gap = Some(Signal::Closed);
            }
            Signal::Lagged => {
                self.gap.get_or_insert(Signal::Lagged);
            }
        }
    }

    /// The first signal owed, no longer owed.
    fn take(&mut self) -> Option<UpdatesLike> {
        match self.gap.take() {
            Some(gap) => Some(gap.updates()),
            None => mem::take(&mut self.opened).then_some(UpdatesLike::ConnectionOpened),
        }
    }
}

/// The updates channel's sender, with the connection signals it is owed, shared by the runner,
/// which sends the opens and what is owed, and the connections' tasks, which owe how they ended
/// ([`UpdatesForwarder::finish`]). Every signal goes behind those owed, so the opens and the
/// closes keep their order whichever side sends them: the runner makes a connection to a
/// datacenter only once the one before it has stopped taking requests, which it does after it
/// has owed its end.
#[derive(Clone)]
struct ConnectionSignals {
    tx: mpsc::Sender<UpdatesLike>,
    /// Never locked across an `.await`.
    owed: Arc<Mutex<Owed>>,
}

impl ConnectionSignals {
    fn new(tx: mpsc::Sender<UpdatesLike>) -> Self {
        Self {
            tx,
            owed: Arc::default(),
        }
    }

    /// No section panics midway, so a poisoned lock still guards a whole state.
    fn owed(&self) -> MutexGuard<'_, Owed> {
        self.owed.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Send `signal` at once if nothing is owed and the channel has room, or else owe it.
    fn send(&self, signal: Signal) {
        let mut owed = self.owed();
        if owed.is_empty() {
            match self.tx.try_send(signal.updates()) {
                Ok(()) | Err(TrySendError::Closed(_)) => return,
                Err(TrySendError::Full(_)) => {}
            }
        }
        owed.add(signal);
    }

    /// Whether a signal is owed.
    fn owes(&self) -> bool {
        !self.owed().is_empty()
    }

    /// Wait for room in the channel, then send the first signal owed. Without a receiver, the
    /// signals have nobody to reach, and are dropped. Cancelling it loses nothing.
    async fn send_owed(&self) {
        match self.tx.reserve().await {
            Ok(permit) => {
                if let Some(signal) = self.owed().take() {
                    permit.send(signal);
                }
            }
            Err(_) => *self.owed() = Owed::default(),
        }
    }
}

/// Drive `sender`, the connection to `dc_id`, feeding it the requests from `rpc_rx` and forwarding
/// its updates, until its connection fails or the runner drops the other end of `rpc_rx`. Then
/// close `rpc_rx`, so the runner can send it nothing more, and hand back, for the runner to send
/// on, the requests that never left: those the sender never wrote, when the runner closed the
/// connection (a failed connection fails them with its error), and those still queued in
/// `rpc_rx`, which the sender never took. They go with the error that ended the connection
/// ([`ConnectionEnd`]), after the signal its end owes the updates channel
/// ([`UpdatesForwarder::finish`]), which goes behind every signal before it. A request whose
/// caller no longer waits, such as a keepalive ping, is dropped instead: sent on, it could only
/// make a new connection for nobody.
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
    let error = loop {
        tokio::select! {
            // The channel first: once the runner closes it, what is still in it reaches the
            // sender and goes back unwritten (`Sender::close`), and a burst is serialized
            // together. It can't starve the step: every caller waits for an answer only the step
            // brings. The owed `Lagged` goes ahead of the step, which a stream busy with a
            // download keeps ready.
            biased;
            rpc = rpc_rx.recv() => match rpc {
                Some(rpc) => sender.enqueue_body(rpc.body, rpc.tx),
                None => {
                    updates.finish(false);
                    break None;
                }
            },
            () = updates.send_lagged(), if updates.owes_lagged() => {}
            step = sender.step() => match step {
                Ok(all_new_updates) => {
                    for new_updates in all_new_updates {
                        if home_sender || !matches!(new_updates, UpdatesLike::NewSession) {
                            updates.forward(new_updates);
                        }
                    }
                }
                // `Sender::step` logged the error and failed the requests with it.
                Err(error) => {
                    updates.finish(home_sender);
                    break Some(error);
                }
            },
        }
    };

    // What the sender holds and never wrote, ahead of what it never took. After a failed step it
    // holds nothing: `Sender::step` failed every request with the error. A sender keeps no
    // `handed_back`, so these start at `false`. That is safe only because the join arm reads the
    // flag on a failed connection's end, and this is empty then.
    let mut unsent = Vec::new();
    sender.close(|body, tx| {
        unsent.push(Rpc {
            body,
            tx,
            handed_back: false,
        })
    });
    // From now on a send to this connection fails, and gives the runner its request back; one
    // sent before is in the channel, which `recv` drains until it reports the end.
    rpc_rx.close();
    while let Some(rpc) = rpc_rx.recv().await {
        unsent.push(rpc);
    }
    unsent.retain(|rpc| !rpc.tx.is_closed());
    ConnectionEnd {
        dc_id,
        unsent,
        error,
    }
}

/// Forwards one connection's updates into the bounded updates channel.
///
/// An update that finds the channel full is dropped, and the receiver is owed an
/// [`UpdatesLike::Lagged`]: it is sent as soon as the channel has room
/// ([`UpdatesForwarder::send_lagged`]), and always before this connection's next update, so the
/// receiver fetches the difference instead of waiting for the next gap to show.
struct UpdatesForwarder {
    /// The channel, and the connection signals it is owed.
    signals: ConnectionSignals,
    /// Updates were dropped since the last [`UpdatesLike::Lagged`] went out.
    lagged: bool,
    /// The receiver is gone: nothing more is forwarded.
    closed: bool,
    /// Updates dropped since the last warning.
    dropped: usize,
    last_warning: Option<Instant>,
}

impl UpdatesForwarder {
    fn new(signals: ConnectionSignals) -> Self {
        Self {
            signals,
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
            match self.signals.tx.try_send(UpdatesLike::Lagged) {
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
        match self.signals.tx.try_send(updates) {
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
        if let Ok(permit) = self.signals.tx.reserve().await {
            permit.send(UpdatesLike::Lagged);
            self.lagged = false;
            return;
        }
        self.close();
    }

    /// End forwarding as the connection ends, with the signal its end owes the channel, behind
    /// every signal before it ([`ConnectionSignals::send`]), which the runner sends once the
    /// channel has room if it can't go now: if `closed_connection`, an
    /// [`UpdatesLike::ConnectionClosed`] (it also covers a [`UpdatesLike::Lagged`] owed);
    /// otherwise, the `Lagged` owed, if one is.
    fn finish(self, closed_connection: bool) {
        if self.closed {
            return;
        }
        if closed_connection {
            self.signals.send(Signal::Closed);
        } else if self.lagged {
            self.signals.send(Signal::Lagged);
        }
    }

    fn count_dropped(&mut self) {
        self.dropped += 1;
        if self
            .last_warning
            .is_none_or(|last| last.elapsed() >= UPDATES_DROPPED_LOG_COOLDOWN)
        {
            warn!(
                capacity = self.signals.tx.max_capacity(),
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
            Self::Disconnect { dc_id, done } => f
                .debug_struct("Disconnect")
                .field("dc_id", dc_id)
                .field("done", done)
                .finish(),
            Self::Quit => write!(f, "Quit"),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
