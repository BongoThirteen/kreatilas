use std::collections::{HashMap, HashSet};

use blake3::Hash;
use iroh::{NodeAddr, PublicKey};
use iroh_blobs::rpc::client::blobs::{BlobStatus, DownloadOutcome};
use n0_future::time::Instant;
use rand::{Rng, rng};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Protocol message
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message {
    /// [`Uuid`] associated with this transaction.
    tx_id: Uuid,
    /// Approximate number of hops that this message will still travel.
    hops_to_live: u64,
    /// Approximate number of nodes which have already relayed this message.
    depth: u64,
    /// Message payload
    data: MessageData,
}

impl Message {
    /// Message that does nothing but notifies the receiving node that this connection
    /// is still active.
    pub fn pulse() -> Self {
        Self {
            tx_id: Uuid::new_v4(),
            hops_to_live: 1,
            depth: 0,
            data: MessageData::Pulse,
        }
    }
    /// The transaction ID of this message.
    pub fn transaction(&self) -> Uuid {
        self.tx_id
    }
}

/// Payload of a message
#[derive(Debug, Clone, Deserialize, Serialize)]
enum MessageData {
    /// This node is looking for `key`.
    Find { key: Hash },
    /// This node found data at node `from` of `size` bytes.
    Found { from: NodeAddr, size: u64 },
    /// The [`MessageData::Find`] or [`MessageData::Insert`] message used up all its `hops_to_live`
    /// without finding a corresponding data blob.
    NotFound,
    /// No more nodes are available.
    Continue { hops_remaining: u64 },
    /// This node wants to insert some data into the network.
    Insert {
        from: NodeAddr,
        key: Hash,
        size: u64,
    },
    /// We've traversed enough nodes and not found a colliding blob.
    Inserted,
    /// `from` has the blob associated with `hash` of `size` bytes.
    Ready {
        addr: NodeAddr,
        data: Hash,
        size: u64,
    },
    /// This connection is active.
    Pulse,
    /// You sent an incorrect message.
    Error,
}

/// Node state
#[derive(Debug, Clone)]
pub struct State {
    pub config: Config,
    node_addr: NodeAddr,
    transactions: HashMap<Uuid, TxState>,
    peers: HashSet<PublicKey>,
    outbox: Vec<OutEvent>,
}

impl State {
    /// Constructs the initial state of a new node.
    pub fn new(config: Config, node_addr: NodeAddr) -> Self {
        Self {
            config,
            node_addr,
            transactions: HashMap::new(),
            peers: HashSet::new(),
            outbox: Vec::new(),
        }
    }
    /// Add a peer's public key to this node's state. You should already have some way to
    /// contact this peer if sent an [`OutEvent::SendMessage`].
    pub fn add_peer(&mut self, node_id: PublicKey) {
        self.peers.insert(node_id);
    }
    /// Notifies this node of an incoming event. Make sure to handle all the [`OutEvent`]s produced.
    pub fn handle(&mut self, event: InEvent, _now: Instant) -> impl Iterator<Item = OutEvent> + '_ {
        match event {
            InEvent::RecvMessage(peer_id, msg) => 'handling: {
                if matches!(msg.data, MessageData::Pulse) {
                    break 'handling;
                }
                if msg.hops_to_live == 1 && rng().random() {
                    if let MessageData::Insert { from, key, size } = msg.data {
                        self.peers.insert(from.node_id);
                        self.outbox.push(OutEvent::PeerInfo(from.clone()));
                        self.transactions
                            .entry(msg.tx_id)
                            .or_insert(TxState::Inserting {
                                origin: from,
                                from: peer_id,
                                hops_to_live: msg.hops_to_live,
                                depth: msg.depth,
                                key,
                                size,
                                tried: HashSet::new(),
                            });
                    }
                    self.outbox.push(OutEvent::SendMessage(
                        peer_id,
                        Message {
                            tx_id: msg.tx_id,
                            hops_to_live: msg.depth + 2,
                            depth: 0,
                            data: MessageData::NotFound,
                        },
                    ));
                    break 'handling;
                }
                let tx = if matches!(
                    msg.data,
                    MessageData::Find { .. } | MessageData::Insert { .. }
                ) {
                    self.transactions
                        .entry(msg.tx_id)
                        .or_insert(TxState::Starting { from: peer_id })
                } else {
                    match self.transactions.get_mut(&msg.tx_id) {
                        Some(tx) => tx,
                        None => {
                            send_error(peer_id, &msg, &mut self.outbox);
                            break 'handling;
                        }
                    }
                };
                self.peers.insert(peer_id);
                handle_message(
                    self.node_addr.clone(),
                    peer_id,
                    &mut self.peers,
                    tx,
                    msg,
                    &mut self.outbox,
                );
            }
            InEvent::CheckedBlob(tx_id, status) => {
                if let Some(tx) = self.transactions.get_mut(&tx_id) {
                    match tx {
                        TxState::Starting { .. } => {}
                        TxState::Finding {
                            from,
                            hops_to_live,
                            depth,
                            key,
                            tried,
                        } => {
                            if let BlobStatus::Complete { size } = status {
                                self.outbox.push(OutEvent::SendMessage(
                                    *from,
                                    Message {
                                        tx_id,
                                        hops_to_live: *depth + 2,
                                        depth: 0,
                                        data: MessageData::Found {
                                            from: self.node_addr.clone(),
                                            size,
                                        },
                                    },
                                ));
                                self.outbox.push(OutEvent::SendMessage(
                                    *from,
                                    Message {
                                        tx_id,
                                        hops_to_live: *depth + 2,
                                        depth: 0,
                                        data: MessageData::Ready {
                                            addr: self.node_addr.clone(),
                                            data: *key,
                                            size,
                                        },
                                    },
                                ));
                            } else {
                                if let Some(send_to) = self
                                    .peers
                                    .iter()
                                    .find(|p| !tried.contains(*p) && *p != from)
                                {
                                    self.outbox.push(OutEvent::SendMessage(
                                        *send_to,
                                        Message {
                                            tx_id,
                                            hops_to_live: (*hops_to_live - 1).max(1),
                                            depth: *depth + 1,
                                            data: MessageData::Find { key: *key },
                                        },
                                    ));
                                } else {
                                    self.outbox.push(OutEvent::SendMessage(
                                        *from,
                                        Message {
                                            tx_id,
                                            hops_to_live: *depth + 2,
                                            depth: 0,
                                            data: MessageData::Continue {
                                                hops_remaining: *hops_to_live,
                                            },
                                        },
                                    ));
                                }
                            }
                        }
                        TxState::Inserting {
                            origin,
                            hops_to_live,
                            depth,
                            key,
                            size,
                            tried,
                            from,
                        } => {
                            if let BlobStatus::Complete { size } = status {
                                self.outbox.push(OutEvent::SendMessage(
                                    origin.node_id,
                                    Message {
                                        tx_id,
                                        hops_to_live: *depth + 2,
                                        depth: 0,
                                        data: MessageData::Found {
                                            from: self.node_addr.clone(),
                                            size,
                                        },
                                    },
                                ));
                            } else {
                                if let Some(send_to) = self
                                    .peers
                                    .iter()
                                    .filter(|p| {
                                        !tried.contains(*p) && **p != origin.node_id && *p != from
                                    })
                                    .min_by_key(|p| distance(**p, *key))
                                {
                                    self.outbox.push(OutEvent::SendMessage(
                                        *send_to,
                                        Message {
                                            tx_id,
                                            hops_to_live: (*hops_to_live - 1).max(1),
                                            depth: *depth + 1,
                                            data: MessageData::Insert {
                                                from: origin.clone(),
                                                key: *key,
                                                size: *size,
                                            },
                                        },
                                    ));
                                } else {
                                    self.outbox.push(OutEvent::SendMessage(
                                        origin.node_id,
                                        Message {
                                            tx_id,
                                            hops_to_live: *depth + 2,
                                            depth: 0,
                                            data: MessageData::Continue {
                                                hops_remaining: *hops_to_live,
                                            },
                                        },
                                    ));
                                }
                            }
                        }
                        TxState::Errored { .. } | TxState::Finished { .. } => {}
                    }
                }
            }
            InEvent::DownloadedBlob(tx_id, outcome) => {
                if let Some(tx) = self.transactions.get_mut(&tx_id) {
                    match tx {
                        TxState::Finding {
                            from, depth, key, ..
                        } => {
                            if *from == self.node_addr.node_id {
                                self.outbox
                                    .push(OutEvent::Downloaded(*key, outcome.clone()));
                            } else {
                                self.outbox.push(OutEvent::SendMessage(
                                    *from,
                                    Message {
                                        tx_id,
                                        hops_to_live: *depth + 2,
                                        depth: 0,
                                        data: MessageData::Ready {
                                            addr: self.node_addr.clone(),
                                            data: *key,
                                            size: outcome.local_size,
                                        },
                                    },
                                ));
                            }
                            *tx = TxState::Finished { from: *from };
                        }
                        TxState::Inserting {
                            from,
                            key,
                            depth,
                            tried,
                            ..
                        } => {
                            let mut sent = false;
                            for next in tried.iter() {
                                sent = true;
                                self.outbox.push(OutEvent::SendMessage(
                                    *next,
                                    Message {
                                        tx_id,
                                        hops_to_live: 2,
                                        depth: 0,
                                        data: MessageData::Ready {
                                            addr: self.node_addr.clone(),
                                            data: *key,
                                            size: outcome.local_size,
                                        },
                                    },
                                ));
                            }
                            if !sent {
                                self.outbox.push(OutEvent::SendMessage(
                                    *from,
                                    Message {
                                        tx_id,
                                        hops_to_live: *depth + 2,
                                        depth: 0,
                                        data: MessageData::Inserted,
                                    },
                                ));
                            }
                        }
                        _ => {}
                    }
                }
            }
            InEvent::Timeout(tx_id, timeout_peer) => {
                if let Some(tx) = self.transactions.get_mut(&tx_id) {
                    match tx {
                        TxState::Finding {
                            from,
                            hops_to_live,
                            depth,
                            key,
                            tried,
                        } if !tried.contains(&timeout_peer) => {
                            if let Some(send_to) = self
                                .peers
                                .iter()
                                .filter(|p| !tried.contains(*p) && *p != from)
                                .min_by_key(|p| distance(**p, *key))
                            {
                                self.outbox.push(OutEvent::SendMessage(
                                    *send_to,
                                    Message {
                                        tx_id,
                                        hops_to_live: (*hops_to_live - 1).max(1),
                                        depth: *depth + 1,
                                        data: MessageData::Find { key: *key },
                                    },
                                ));
                            } else if *from == self.node_addr.node_id {
                                self.outbox.push(OutEvent::NotFound(*key));
                            } else {
                                self.outbox.push(OutEvent::SendMessage(
                                    *from,
                                    Message {
                                        tx_id,
                                        hops_to_live: *depth + 2,
                                        depth: 0,
                                        data: MessageData::Continue {
                                            hops_remaining: *hops_to_live,
                                        },
                                    },
                                ));
                            }
                        }
                        TxState::Inserting {
                            origin,
                            hops_to_live,
                            depth,
                            key,
                            size,
                            tried,
                            ..
                        } if !tried.contains(&timeout_peer) => {
                            if let Some(send_to) = self
                                .peers
                                .iter()
                                .filter(|p| !tried.contains(*p) && **p != origin.node_id)
                                .min_by_key(|p| distance(**p, *key))
                            {
                                self.outbox.push(OutEvent::SendMessage(
                                    *send_to,
                                    Message {
                                        tx_id,
                                        hops_to_live: *depth + 2,
                                        depth: *depth + 1,
                                        data: MessageData::Insert {
                                            from: origin.clone(),
                                            key: *key,
                                            size: *size,
                                        },
                                    },
                                ));
                            } else if origin.node_id == self.node_addr.node_id {
                                self.outbox.push(OutEvent::NotInserted(*key));
                            } else {
                                self.outbox.push(OutEvent::SendMessage(
                                    origin.node_id,
                                    Message {
                                        tx_id,
                                        hops_to_live: *depth + 2,
                                        depth: 0,
                                        data: MessageData::Continue {
                                            hops_remaining: *hops_to_live,
                                        },
                                    },
                                ));
                            }
                        }
                        _ => {}
                    }
                }
            }
            InEvent::Find(key) => 'finding: {
                let tx_id = Uuid::new_v4();
                self.transactions.insert(
                    tx_id,
                    TxState::Finding {
                        from: self.node_addr.node_id,
                        hops_to_live: 5,
                        depth: 0,
                        key,
                        tried: HashSet::new(),
                    },
                );
                let Some(send_to) = self.peers.iter().next() else {
                    self.outbox.push(OutEvent::NotFound(key));
                    break 'finding;
                };
                self.outbox.push(OutEvent::SendMessage(
                    *send_to,
                    Message {
                        tx_id,
                        hops_to_live: 5,
                        depth: 0,
                        data: MessageData::Find { key },
                    },
                ));
            }
            InEvent::Insert(key, size) => 'inserting: {
                let Some(send_to) = self.peers.iter().min_by_key(|p| distance(**p, key)) else {
                    self.outbox.push(OutEvent::NotInserted(key));
                    break 'inserting;
                };
                let tx_id = Uuid::new_v4();
                self.transactions.insert(
                    tx_id,
                    TxState::Inserting {
                        origin: self.node_addr.clone(),
                        from: self.node_addr.node_id,
                        hops_to_live: 2,
                        depth: 0,
                        key,
                        size,
                        tried: HashSet::new(),
                    },
                );
                self.outbox.push(OutEvent::SendMessage(
                    *send_to,
                    Message {
                        tx_id,
                        hops_to_live: 2,
                        depth: 0,
                        data: MessageData::Insert {
                            from: self.node_addr.clone(),
                            key,
                            size,
                        },
                    },
                ));
            }
            InEvent::PeerDisconnected(peer_id) => {
                self.peers.remove(&peer_id);
                self.transactions.retain(|_, t| *t.from() != peer_id);
            }
        }

        self.transactions
            .retain(|_, t| !matches!(t, TxState::Finished { .. }));
        self.outbox.drain(..)
    }
    /// Maximum size of messages this node will send or accept.
    pub fn max_message_size(&self) -> usize {
        self.config.max_message_size
    }
}

fn send_error(peer_id: PublicKey, msg: &Message, outbox: &mut Vec<OutEvent>) {
    outbox.push(OutEvent::SendMessage(
        peer_id,
        Message {
            tx_id: msg.tx_id,
            hops_to_live: msg.depth,
            depth: 0,
            data: MessageData::Error,
        },
    ));
}

fn handle_message(
    node_addr: NodeAddr,
    peer_id: PublicKey,
    peers: &mut HashSet<PublicKey>,
    tx: &mut TxState,
    msg: Message,
    outbox: &mut Vec<OutEvent>,
) {
    match msg.data {
        MessageData::Find { key } => {
            if !matches!(tx, TxState::Starting { .. }) {
                *tx = TxState::Errored { from: peer_id };
                send_error(peer_id, &msg, outbox);
                return;
            }

            *tx = TxState::Finding {
                from: peer_id,
                hops_to_live: msg.hops_to_live,
                depth: msg.depth,
                key,
                tried: HashSet::new(),
            };

            outbox.push(OutEvent::CheckBlob(msg.tx_id, key));
        }
        MessageData::Insert {
            ref from,
            key,
            size,
        } => {
            if !matches!(tx, TxState::Starting { .. }) {
                *tx = TxState::Errored { from: peer_id };
                send_error(peer_id, &msg, outbox);
                return;
            }

            peers.insert(from.node_id);
            *tx = TxState::Inserting {
                origin: from.clone(),
                from: peer_id,
                hops_to_live: msg.hops_to_live,
                depth: msg.depth,
                key,
                size,
                tried: HashSet::new(),
            };

            outbox.push(OutEvent::CheckBlob(msg.tx_id, key));
        }
        MessageData::Found {
            from: ref download_from,
            size,
        } => match tx {
            TxState::Finding { from, key, .. } if *from != peer_id => {
                peers.insert(download_from.node_id);
                if *from == node_addr.node_id {
                    outbox.push(OutEvent::Found(*key));
                } else {
                    outbox.push(OutEvent::SendMessage(
                        *from,
                        Message {
                            tx_id: msg.tx_id,
                            hops_to_live: (msg.hops_to_live - 1).max(1),
                            depth: msg.depth + 1,
                            data: MessageData::Found {
                                from: node_addr,
                                size,
                            },
                        },
                    ));
                }
            }
            TxState::Inserting { origin, key, .. } if origin.node_id != peer_id => {
                peers.insert(download_from.node_id);
                outbox.push(OutEvent::DownloadBlob(
                    msg.tx_id,
                    download_from.clone(),
                    *key,
                    size,
                ));
                if origin.node_id == node_addr.node_id {
                    outbox.push(OutEvent::NotInserted(*key));
                } else {
                    outbox.push(OutEvent::SendMessage(
                        origin.node_id,
                        Message {
                            tx_id: msg.tx_id,
                            hops_to_live: (msg.hops_to_live - 1).max(1),
                            depth: msg.depth + 1,
                            data: MessageData::Found {
                                from: node_addr,
                                size,
                            },
                        },
                    ));
                }
            }
            _ => {
                send_error(peer_id, &msg, outbox);
            }
        },
        MessageData::NotFound => match tx {
            TxState::Finding { from, key, .. } if *from != peer_id => {
                if *from == node_addr.node_id {
                    outbox.push(OutEvent::NotFound(*key));
                } else {
                    outbox.push(OutEvent::SendMessage(
                        *from,
                        Message {
                            tx_id: msg.tx_id,
                            hops_to_live: (msg.hops_to_live - 1).max(1),
                            depth: msg.depth + 1,
                            data: MessageData::NotFound,
                        },
                    ));
                }

                *tx = TxState::Finished { from: *from };
            }
            TxState::Inserting {
                origin,
                key,
                from,
                tried,
                size,
                ..
            } => {
                tried.insert(peer_id);
                if origin.node_id == node_addr.node_id {
                    for next in tried.iter() {
                        outbox.push(OutEvent::SendMessage(
                            *next,
                            Message {
                                tx_id: msg.tx_id,
                                hops_to_live: 5,
                                depth: 0,
                                data: MessageData::Ready {
                                    addr: node_addr.clone(),
                                    data: *key,
                                    size: *size,
                                },
                            },
                        ));
                    }
                } else {
                    outbox.push(OutEvent::SendMessage(
                        *from,
                        Message {
                            tx_id: msg.tx_id,
                            hops_to_live: (msg.hops_to_live - 1).max(1),
                            depth: msg.depth + 1,
                            data: MessageData::NotFound,
                        },
                    ));
                }
            }
            _ => {
                send_error(peer_id, &msg, outbox);
            }
        },
        MessageData::Continue { hops_remaining } => match tx {
            TxState::Finding {
                from,
                hops_to_live,
                depth,
                key,
                tried,
            } if *from != peer_id => {
                tried.insert(peer_id);
                if let Some(send_to) = peers
                    .iter()
                    .filter(|p| !tried.contains(*p) && *p != from && **p != peer_id)
                    .min_by_key(|p| distance(**p, *key))
                {
                    outbox.push(OutEvent::SendMessage(
                        *send_to,
                        Message {
                            tx_id: msg.tx_id,
                            hops_to_live: (*hops_to_live - 1).max(1),
                            depth: *depth + 1,
                            data: MessageData::Find { key: *key },
                        },
                    ));
                } else if *from == node_addr.node_id {
                    outbox.push(OutEvent::NotFound(*key));
                } else {
                    outbox.push(OutEvent::SendMessage(
                        *from,
                        Message {
                            tx_id: msg.tx_id,
                            hops_to_live: (msg.hops_to_live - 1).max(1),
                            depth: msg.depth + 1,
                            data: MessageData::Continue {
                                hops_remaining: *hops_to_live,
                            },
                        },
                    ));
                }
            }
            TxState::Inserting {
                origin,
                depth,
                key,
                size,
                tried,
                ..
            } if origin.node_id != peer_id => {
                tried.insert(peer_id);
                if let Some(send_to) = peers
                    .iter()
                    .filter(|p| !tried.contains(*p) && **p != origin.node_id && **p != peer_id)
                    .min_by_key(|p| distance(**p, *key))
                {
                    outbox.push(OutEvent::SendMessage(
                        *send_to,
                        Message {
                            tx_id: msg.tx_id,
                            hops_to_live: (hops_remaining - 1).max(1),
                            depth: *depth + 1,
                            data: MessageData::Insert {
                                from: origin.clone(),
                                key: *key,
                                size: *size,
                            },
                        },
                    ));
                } else if origin.node_id == node_addr.node_id {
                    outbox.push(OutEvent::NotInserted(*key));
                } else {
                    outbox.push(OutEvent::SendMessage(
                        origin.node_id,
                        Message {
                            tx_id: msg.tx_id,
                            hops_to_live: (msg.hops_to_live - 1).max(1),
                            depth: msg.depth + 1,
                            data: MessageData::Continue { hops_remaining },
                        },
                    ));
                }
            }
            _ => {
                send_error(peer_id, &msg, outbox);
            }
        },
        MessageData::Inserted => match tx {
            TxState::Inserting { origin, key, .. } if origin.node_id != peer_id => {
                if origin.node_id == node_addr.node_id {
                    outbox.push(OutEvent::Inserted(*key));
                } else {
                    outbox.push(OutEvent::SendMessage(
                        origin.node_id,
                        Message {
                            tx_id: msg.tx_id,
                            hops_to_live: (msg.hops_to_live - 1).max(1),
                            depth: msg.depth + 1,
                            data: MessageData::Inserted,
                        },
                    ));
                }
            }
            _ => {
                send_error(peer_id, &msg, outbox);
            }
        },
        MessageData::Ready {
            ref addr,
            data,
            size,
        } => match tx {
            TxState::Inserting { origin, key, .. } => {
                peers.insert(addr.node_id);
                outbox.push(OutEvent::DownloadBlob(
                    msg.tx_id,
                    origin.clone(),
                    *key,
                    size,
                ));
            }
            TxState::Finding { .. } => {
                peers.insert(addr.node_id);
                outbox.push(OutEvent::DownloadBlob(msg.tx_id, addr.clone(), data, size));
            }
            _ => {
                send_error(peer_id, &msg, outbox);
            }
        },
        MessageData::Pulse => {}
        MessageData::Error => {
            let &from = tx.from();
            *tx = TxState::Errored { from };
        }
    }
}

fn distance(a: PublicKey, b: Hash) -> (u128, u128) {
    let (a1, a2) = (
        u128::from_be_bytes(a.as_bytes()[..16].try_into().unwrap()),
        u128::from_be_bytes(a.as_bytes()[16..].try_into().unwrap()),
    );
    let (b1, b2) = (
        u128::from_be_bytes(b.as_bytes()[..16].try_into().unwrap()),
        u128::from_be_bytes(b.as_bytes()[16..].try_into().unwrap()),
    );

    let d1 = if a1 >= b1 { a1 - b1 } else { b1 - a1 };

    let d2 = if a2 >= b2 { a2 - b2 } else { b2 - a2 };

    (d1, d2)
}

/// Node configuration
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Maximum amount of memory or storage space, in bytes, that this node will allow its blob
    /// store to use before deleting unused data.
    pub max_storage_use: u64,
    /// Maximum size of messages this node will accept or send.
    pub max_message_size: usize,
}

/// Event sent to this node's state machine.
///
/// This event should be produced by some networking code and sent to the [`State::handle`] function.
#[derive(Debug, Clone)]
pub enum InEvent {
    /// We've received a [`Message`] from another peer with this [`PublicKey`].
    RecvMessage(PublicKey, Message),
    /// We've checked the local status of some data in our [`Blobs`](iroh_blobs::net_protocol::Blobs) instance,
    /// and received this [`BlobStatus`].
    CheckedBlob(Uuid, BlobStatus),
    /// We've successfully downloaded a blob as part of the transaction identified by this [`Uuid`].
    /// This also specifies the size of this blob.
    DownloadedBlob(Uuid, DownloadOutcome),
    /// The connection to the peer with this [`PublicKey`], for the transaction identified by this [`Uuid`], timed out.
    Timeout(Uuid, PublicKey),
    /// We want to find the data associated with this [`struct@Hash`].
    Find(Hash),
    /// We have the blob associated with this [`struct@Hash`] in our [`Blobs`](iroh_blobs::net_protocol::Blobs) instance,
    /// of this size in bytes, and we want to insert it into the network.
    Insert(Hash, u64),
    /// The peer with this [`PublicKey`] has disconnected from us.
    PeerDisconnected(PublicKey),
}

/// Event produced by this node's state machine.
///
/// This is produced by the [`Iterator`] returned by [`State::handle`], and provides instructions
/// or feedback to a networking implementation.
#[derive(Debug, Clone)]
pub enum OutEvent {
    /// You must send this [`Message`] to the peer with this [`PublicKey`]. The peer may reply
    /// with other [`Message`]s, in which case you must return those to this [`State`] with an
    /// [`InEvent::RecvMessage`].
    ///
    /// This [`PublicKey`] may come from a [`State::add_peer`] call, in which case you should
    /// have dialing information for this peer, or it may have been automatically discovered
    /// from a [`Message`] received by this peer, in which case it will be accompanied by an
    /// [`OutEvent::PeerInfo`] providing this information.
    SendMessage(PublicKey, Message),
    /// You must check your local [`Blobs`](iroh_blobs::net_protocol::Blobs) store for a blob
    /// with this [`struct@Hash`] using [`Client::status`](iroh_blobs::rpc::client::blobs::Client::status).
    /// Return the resulting [`BlobStatus`] to this [`State`] by supplying an
    /// [`InEvent::CheckedBlob`] to [`State::handle`].
    CheckBlob(Uuid, Hash),
    /// You must download the data blob with this [`struct@Hash`] from this [`NodeAddr`] using
    /// [`Client::download`](iroh_blobs::rpc::client::blobs::Client::download). This `u64` is
    /// the size of the blob as claimed by the peer who provides it.
    DownloadBlob(Uuid, NodeAddr, Hash, u64),
    /// You must save this [`NodeAddr`] and be ready to use it to make connections to using its [`PublicKey`].
    PeerInfo(NodeAddr),
    /// We've found data associated with this [`struct@Hash`].
    Found(Hash),
    /// We've finished downloading the blob with this [`struct@Hash`]. You may now retrieve it
    /// from the [`Blobs`](iroh_blobs::net_protocol::Blobs) instance.
    Downloaded(Hash, DownloadOutcome),
    /// We checked enough peers and didn't find the data with this [`struct@Hash`].
    NotFound(Hash),
    /// We successfully inserted data with this [`struct@Hash`] into the network.
    Inserted(Hash),
    /// We couldn't send the data with this [`struct@Hash`] to enough peers to insert it.
    NotInserted(Hash),
    /// You must disconnect from the peer with this [`PublicKey`].
    DisconnectPeer(PublicKey),
}

#[derive(Debug, Clone)]
enum TxState {
    Starting {
        from: PublicKey,
    },
    Finding {
        from: PublicKey,
        hops_to_live: u64,
        depth: u64,
        key: Hash,
        tried: HashSet<PublicKey>,
    },
    Inserting {
        origin: NodeAddr,
        from: PublicKey,
        hops_to_live: u64,
        depth: u64,
        key: Hash,
        size: u64,
        tried: HashSet<PublicKey>,
    },
    Errored {
        from: PublicKey,
    },
    Finished {
        from: PublicKey,
    },
}

impl TxState {
    fn from(&self) -> &PublicKey {
        match self {
            TxState::Starting { from } => from,
            TxState::Finding { from, .. } => from,
            TxState::Inserting { origin, .. } => &origin.node_id,
            TxState::Errored { from } => from,
            TxState::Finished { from } => from,
        }
    }
}
