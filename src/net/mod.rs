use std::{
    collections::{BTreeMap, HashMap, hash_map::Entry},
    future::pending,
    mem,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use blake3::Hash;
use bytes::BytesMut;
use chrono::DateTime;
use futures::StreamExt;
use iroh::{
    Endpoint, NodeAddr, NodeId, PublicKey, endpoint::Connection, protocol::ProtocolHandler,
};
use iroh_blobs::{
    net_protocol::Blobs,
    rpc::client::blobs::{BlobStatus, MemClient},
    store::Store,
};
use iroh_gossip::net::util::Timers;
use n0_future::{
    task::{AbortOnDropHandle, JoinSet},
    time::Instant,
};
use reader::read_message;
use tokio::{
    join, select, spawn,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;
use writer::write_message;

use crate::proto::{Config, InEvent, Message, OutEvent, State};

mod reader;
mod writer;

pub const KREATILAS_ALPN: &[u8] = b"/kreatilas/1";

#[derive(Debug, Clone)]
pub struct Kreatilas {
    inner: Arc<Inner>,
}

#[derive(Debug, Clone)]
pub struct Builder {
    config: Config,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            config: Config {
                max_storage_use: 100 * 1024 * 1024,
                max_message_size: 65536,
            },
        }
    }
}

impl Builder {
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.config.max_message_size = size;
        self
    }
    pub fn max_storage_use(mut self, size: u64) -> Self {
        self.config.max_storage_use = size;
        self
    }
    pub async fn spawn<S: Store>(
        self,
        endpoint: Endpoint,
        blobs: &Blobs<S>,
    ) -> anyhow::Result<Kreatilas> {
        let node_addr = endpoint.node_addr().await?;
        let blobs_client = blobs.client().clone();
        let config = self.config;

        let (actor, to_actor_tx) = Actor::new(endpoint, node_addr, blobs_client, config).await;
        let actor_handle = spawn(async move {
            _ = actor.run().await;
        });
        let _actor_handle = AbortOnDropHandle::new(actor_handle);

        let inner = Inner {
            to_actor_tx,
            _actor_handle,
        };

        Ok(Kreatilas {
            inner: Arc::new(inner),
        })
    }
}

impl Kreatilas {
    pub fn builder() -> Builder {
        Builder::default()
    }
    pub async fn get(&self, key: Hash) -> anyhow::Result<bool> {
        let (send, recv) = oneshot::channel();
        self.inner
            .to_actor_tx
            .send(ToActor::Get { key, result: send })
            .await?;
        let res = recv.await?;
        Ok(res)
    }

    pub async fn put(&self, key: Hash) -> anyhow::Result<bool> {
        let (send, recv) = oneshot::channel();
        self.inner
            .to_actor_tx
            .send(ToActor::Put { key, result: send })
            .await?;
        let res = recv.await?;
        Ok(res)
    }

    pub async fn add_peer(&self, node_addr: NodeAddr) -> anyhow::Result<()> {
        let (send, recv) = oneshot::channel();
        self.inner
            .to_actor_tx
            .send(ToActor::AddPeer {
                node_addr,
                result: send,
            })
            .await?;
        let res = recv.await?;
        Ok(res)
    }
}

#[derive(Debug)]
struct Inner {
    to_actor_tx: Sender<ToActor>,
    _actor_handle: AbortOnDropHandle<()>,
}

#[derive(derive_more::Debug)]
enum ToActor {
    HandleConnection(PublicKey, #[debug("Connection")] Connection, ConnOrigin),
    Put {
        key: Hash,
        #[debug("Sender")]
        result: oneshot::Sender<bool>,
    },
    Get {
        key: Hash,
        #[debug("Sender")]
        result: oneshot::Sender<bool>,
    },
    AddPeer {
        node_addr: NodeAddr,
        #[debug("Sender")]
        result: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
struct Actor {
    state: State,
    dialer: Dialer,
    blobs_client: MemClient,
    to_actor_rx: Receiver<ToActor>,
    in_event_tx: Sender<InEvent>,
    in_event_rx: Receiver<InEvent>,
    results: HashMap<Hash, oneshot::Sender<bool>>,
    peers: HashMap<NodeId, PeerState>,
    timers: Timers<(Uuid, PublicKey)>,
    connection_tasks: JoinSet<(NodeId, Connection, anyhow::Result<()>)>,
}

impl Actor {
    async fn new(
        endpoint: Endpoint,
        node_addr: NodeAddr,
        blobs_client: MemClient,
        config: Config,
    ) -> (Self, Sender<ToActor>) {
        let (to_actor_tx, to_actor_rx) = channel(16);
        let (in_event_tx, in_event_rx) = channel(16);

        let state = State::new(config, node_addr);

        let actor = Actor {
            state,
            dialer: Dialer::new(endpoint),
            blobs_client,
            to_actor_rx,
            in_event_tx,
            in_event_rx,
            results: HashMap::new(),
            peers: HashMap::new(),
            timers: Timers::default(),
            connection_tasks: JoinSet::default(),
        };

        (actor, to_actor_tx)
    }
    async fn run(mut self) -> anyhow::Result<()> {
        let mut i = 0;
        while self.event_loop(i).await? {
            i += 1;
        }
        Ok(())
    }
    async fn event_loop(&mut self, _i: u64) -> anyhow::Result<bool> {
        select! {
            biased;
            msg = self.to_actor_rx.recv() => {
                debug!(?msg, "actor received message");
                match msg {
                    Some(ToActor::HandleConnection(node_id, conn, origin)) => {
                        self.handle_connection(node_id, conn, origin).await;
                    }
                    Some(ToActor::Put { key, result }) => {
                        if let BlobStatus::Complete { size } = self.blobs_client.status(key.into()).await? {
                            self.results.insert(key, result);
                            self.handle_in_event(InEvent::Insert(key, size), Instant::now()).await?;
                        } else {
                            _ = result.send(false);
                        }
                    }
                    Some(ToActor::Get { key, result }) => {
                        self.results.insert(key, result);
                        self.handle_in_event(InEvent::Find(key), Instant::now()).await?;
                    }
                    Some(ToActor::AddPeer { node_addr, result }) => {
                        self.state.add_peer(node_addr.node_id);
                        let state = self.peers.entry(node_addr.node_id).or_default();
                        match state {
                            PeerState::Active { .. } => {
                            }
                            PeerState::Pending { queue } => {
                                if queue.is_empty() {
                                    let node_id = node_addr.node_id;
                                    if let Err(err) = self.dialer.endpoint.add_node_addr(node_addr) {
                                        warn!(%err, "failed to add node address");
                                    }
                                    self.dialer.queue_dial(node_id, KREATILAS_ALPN, Some(result));
                                }
                                queue.push(Message::pulse());
                            }
                        }
                    }
                    None => return Ok(false),
                }
            }
            (peer_id, res) = self.dialer.next_conn() => {
                match res {
                    Some(Ok(conn)) => {
                        debug!(%peer_id, "successfully dialed");
                        self.handle_connection(peer_id, conn, ConnOrigin::Dial).await;
                    }
                    Some(Err(err)) => {
                        warn!(?err, %peer_id, "failed to dial");
                    }
                    None => {}
                }
            }
            in_event = self.in_event_rx.recv() => {
                let in_event = in_event.expect("in_event_tx is never dropped before receiver");
                self.handle_in_event(in_event, Instant::now()).await?;
            }
            timers = self.timers.wait_and_drain() => {
                let now = Instant::now();
                for (_instant, (tx_id, peer_id)) in timers {
                    self.handle_in_event(InEvent::Timeout(tx_id, peer_id), now).await?;
                }
            }
            Some(res) = self.connection_tasks.join_next(), if !self.connection_tasks.is_empty() => {
                let (peer_id, conn, res) = res.expect("connection task panicked");
                debug!(%peer_id, "connection task completed");
                self.handle_conn_task_end(peer_id, conn, res).await?;
            }
        }
        Ok(true)
    }
    async fn handle_connection(
        &mut self,
        node_id: PublicKey,
        conn: Connection,
        origin: ConnOrigin,
    ) {
        let (send_tx, send_rx) = channel(16);
        let conn_id = conn.stable_id();

        let queue = match self.peers.entry(node_id) {
            Entry::Occupied(mut entry) => entry.get_mut().accept_conn(send_tx, conn_id),
            Entry::Vacant(entry) => {
                entry.insert(PeerState::Active {
                    send_tx,
                    conn_id,
                    other_conns: Vec::new(),
                });
                Vec::new()
            }
        };

        let in_event_tx = self.in_event_tx.clone();
        let max_message_size = self.state.max_message_size();
        self.state.add_peer(node_id);

        self.connection_tasks.spawn(async move {
            let res = connection_loop(
                node_id,
                &conn,
                origin,
                send_rx,
                &in_event_tx,
                max_message_size,
                queue,
            )
            .await;
            (node_id, conn, res)
        });
    }
    async fn handle_conn_task_end(
        &mut self,
        peer_id: NodeId,
        conn: Connection,
        _task_result: anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        if conn.close_reason().is_none() {
            conn.close(0u32.into(), b"close from disconnect");
        }
        if let Some(PeerState::Active {
            conn_id,
            other_conns,
            ..
        }) = self.peers.get_mut(&peer_id)
        {
            if conn.stable_id() == *conn_id {
                self.handle_in_event(InEvent::PeerDisconnected(peer_id), Instant::now())
                    .await?;
            } else {
                other_conns.retain(|x| *x != conn.stable_id());
            }
        }
        Ok(())
    }
    async fn handle_in_event(&mut self, event: InEvent, now: Instant) -> anyhow::Result<()> {
        debug!(?event, "handling input event");
        let max_storage_use = self.state.config.max_storage_use;
        let out = self.state.handle(event, now);
        for event in out {
            debug!(?event, "handling output event");
            match event {
                OutEvent::SendMessage(peer_id, msg) => {
                    self.timers.insert(
                        Instant::now() + Duration::from_secs(3),
                        (msg.transaction(), peer_id),
                    );
                    let state = self.peers.entry(peer_id).or_default();
                    match state {
                        PeerState::Active { send_tx, .. } => {
                            if let Err(err) = send_tx.send(msg).await {
                                warn!(%err, "failed to send message to connection task");
                            }
                        }
                        PeerState::Pending { queue } => {
                            if queue.is_empty() {
                                self.dialer.queue_dial(peer_id, KREATILAS_ALPN, None);
                            }
                            queue.push(msg);
                        }
                    }
                }
                OutEvent::CheckBlob(tx_id, key) => {
                    let in_event_tx = self.in_event_tx.clone();
                    let client = self.blobs_client.clone();
                    spawn(async move {
                        if let Ok(status) = client.status(key.into()).await {
                            _ = in_event_tx.send(InEvent::CheckedBlob(tx_id, status)).await;
                        }
                    });
                }
                OutEvent::DownloadBlob(tx_id, from, key, size) => {
                    let client = self.blobs_client.clone();
                    let in_event_tx = self.in_event_tx.clone();
                    spawn(async move {
                        let mut listing = match client.list().await {
                            Ok(listing) => listing,
                            Err(err) => {
                                warn!(%err, "failed to list blobs");
                                return;
                            }
                        };
                        let mut storage_use = 0;
                        let mut sizes = HashMap::new();

                        while let Some(info) = listing.next().await {
                            if let Ok(info) = info {
                                storage_use += info.size;
                                sizes.insert(info.hash, info.size);
                            }
                        }

                        if storage_use + size > max_storage_use {
                            let to_reclaim = storage_use + size - max_storage_use;
                            let mut reclaimed = 0;

                            let mut listing = match client.tags().list().await {
                                Ok(listing) => listing,
                                Err(err) => {
                                    warn!(%err, "failed to list tags");
                                    return;
                                }
                            };

                            let mut tags = BTreeMap::new();
                            while let Some(tag) = listing.next().await {
                                let tag = match tag {
                                    Ok(tag) => tag,
                                    Err(err) => {
                                        warn!(%err, "failed to list tags");
                                        return;
                                    }
                                };

                                if let Some(time) =
                                    std::str::from_utf8(&tag.name.0).ok().and_then(|t| {
                                        DateTime::parse_from_rfc3339(t.trim_start_matches("auto-"))
                                            .ok()
                                    })
                                {
                                    if let Some(size) = sizes.get(&tag.hash) {
                                        tags.insert(time, (tag.hash, *size));
                                    }
                                }
                            }

                            while reclaimed < to_reclaim {
                                let Some((_time, (hash, size))) = tags.pop_first() else {
                                    warn!("not enough storage");
                                    return;
                                };

                                if let Err(err) = client.delete_blob(hash).await {
                                    warn!(%err, "failed to delete blob");
                                    return;
                                }

                                reclaimed += size;
                            }
                        }

                        let progress = match client.download(key.into(), from).await {
                            Ok(progress) => progress,
                            Err(err) => {
                                warn!(%err, "failed to download blob");
                                return;
                            }
                        };
                        let outcome = match progress.await {
                            Ok(outcome) => outcome,
                            Err(err) => {
                                warn!(%err, "failed to download blob");
                                return;
                            }
                        };
                        debug!(?outcome, "download concluded");
                        if let Err(err) = in_event_tx
                            .send(InEvent::DownloadedBlob(tx_id, outcome.local_size))
                            .await
                        {
                            warn!(%err, "failed to send download notification");
                        }
                    });
                }
                OutEvent::PeerInfo(info) => {
                    if let Err(err) = self.dialer.endpoint.add_node_addr(info) {
                        warn!(%err, "failed to add node address");
                    }
                }
                OutEvent::Downloaded(key) | OutEvent::Inserted(key) => {
                    if let Some(res) = self.results.remove(&key) {
                        _ = res.send(true);
                    }
                }
                OutEvent::Found(_key) => {}
                OutEvent::NotFound(key) | OutEvent::NotInserted(key) => {
                    if let Some(res) = self.results.remove(&key) {
                        _ = res.send(false);
                    }
                }
                OutEvent::DisconnectPeer(peer_id) => {
                    self.peers.remove(&peer_id);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Dialer {
    endpoint: Endpoint,
    pending: JoinSet<(NodeId, Option<anyhow::Result<Connection>>)>,
    pending_dials: HashMap<NodeId, CancellationToken>,
}

impl Dialer {
    fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            pending: JoinSet::default(),
            pending_dials: HashMap::new(),
        }
    }
    fn queue_dial(
        &mut self,
        node_id: NodeId,
        alpn: &'static [u8],
        result: Option<oneshot::Sender<()>>,
    ) {
        if self.is_pending(node_id) {
            return;
        }
        let cancel = CancellationToken::new();
        self.pending_dials.insert(node_id, cancel.clone());
        let endpoint = self.endpoint.clone();
        self.pending.spawn(async move {
            let res = select! {
                biased;
                _ = cancel.cancelled() => None,
                res = endpoint.connect(node_id, alpn) => {
                    if let Some(send) = result {
                        _ = send.send(());
                    }
                    Some(res.context("failed to dial"))
                }
            };
            (node_id, res)
        });
    }
    fn is_pending(&self, node_id: NodeId) -> bool {
        self.pending_dials.contains_key(&node_id)
    }
    async fn next_conn(&mut self) -> (NodeId, Option<anyhow::Result<Connection>>) {
        match self.pending_dials.is_empty() {
            false => {
                let (node_id, res) = loop {
                    match self.pending.join_next().await {
                        Some(Ok((node_id, res))) => {
                            self.pending_dials.remove(&node_id);
                            break (node_id, res);
                        }
                        Some(Err(_)) => {}
                        None => pending().await,
                    }
                };

                (node_id, res)
            }
            true => pending().await,
        }
    }
}

type ConnId = usize;

#[derive(Debug, Clone)]
enum PeerState {
    Pending {
        queue: Vec<Message>,
    },
    Active {
        send_tx: Sender<Message>,
        conn_id: ConnId,
        other_conns: Vec<ConnId>,
    },
}

impl Default for PeerState {
    fn default() -> Self {
        Self::Pending { queue: Vec::new() }
    }
}

impl PeerState {
    fn accept_conn(&mut self, send_tx: Sender<Message>, conn_id: ConnId) -> Vec<Message> {
        match self {
            PeerState::Pending { queue } => {
                let queue = mem::take(queue);
                *self = PeerState::Active {
                    send_tx,
                    conn_id,
                    other_conns: Vec::new(),
                };
                queue
            }
            PeerState::Active {
                send_tx: active_tx,
                conn_id: active_id,
                other_conns,
            } => {
                other_conns.push(*active_id);
                *active_tx = send_tx;
                *active_id = conn_id;
                Vec::new()
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ConnOrigin {
    Accept,
    Dial,
}

async fn connection_loop(
    node_id: PublicKey,
    conn: &Connection,
    origin: ConnOrigin,
    mut send_rx: Receiver<Message>,
    in_event_tx: &Sender<InEvent>,
    max_message_size: usize,
    queue: Vec<Message>,
) -> anyhow::Result<()> {
    let (mut send, mut recv) = match origin {
        ConnOrigin::Accept => conn.accept_bi().await?,
        ConnOrigin::Dial => conn.open_bi().await?,
    };

    let mut send_buf = BytesMut::new();
    let mut recv_buf = BytesMut::new();

    let send_loop = async {
        for msg in queue {
            write_message(&mut send, &mut send_buf, &msg, max_message_size).await?;
        }
        while let Some(msg) = send_rx.recv().await {
            write_message(&mut send, &mut send_buf, &msg, max_message_size).await?;
        }
        let _ = send.finish();
        let _ = send.stopped().await;
        anyhow::Ok(())
    };

    let recv_loop = async {
        loop {
            let msg = read_message(&mut recv, &mut recv_buf, max_message_size).await?;

            match msg {
                None => {
                    debug!(%node_id, "connection ended");
                    break;
                }
                Some(msg) => in_event_tx.send(InEvent::RecvMessage(node_id, msg)).await?,
            }
        }
        anyhow::Ok(())
    };

    let res = join!(send_loop, recv_loop);
    res.0.context("send_loop").and(res.1.context("recv_loop"))
}

impl Inner {
    async fn handle_connection(&self, conn: Connection) -> anyhow::Result<()> {
        let node_id = conn.remote_node_id()?;
        self.to_actor_tx
            .send(ToActor::HandleConnection(node_id, conn, ConnOrigin::Accept))
            .await?;
        Ok(())
    }
}

impl ProtocolHandler for Kreatilas {
    fn accept(
        &self,
        conn: Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'static>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            inner.handle_connection(conn).await?;
            Ok(())
        })
    }
}
