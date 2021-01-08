use bytes::Bytes;
use chrono::Utc;
use once_cell::sync::Lazy;
use parking_lot::Mutex as SyncMutex;
use rand::{rngs::SmallRng, seq::IteratorRandom, SeedableRng};
use tokio::{
    sync::{mpsc, Mutex},
    time::sleep,
};
use tracing::*;

use pea2pea::{
    connections::ConnectionSide,
    protocols::{Handshaking, Reading, Writing},
    *,
};
use snarkos_network::external::{GetPeers, Message, MessageHeader, Peers, Verack, Version};

use std::{
    collections::HashMap,
    fmt, io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

pub const DESIRED_CONNECTION_COUNT: u8 = 5;
pub static RNG: Lazy<SyncMutex<SmallRng>> = Lazy::new(|| SyncMutex::new(SmallRng::from_entropy()));

const VERSION: u64 = 1;
const MESSAGE_HEADER_LEN: usize = 16;

#[derive(Clone)]
pub struct FakeNode {
    pub node: Node,
    // a map of listening addresses to the actual addresses of connected nodes
    pub peers: Arc<Mutex<HashMap<SocketAddr, SocketAddr>>>,
    pub desired_connection_count: u8,
    pub current_block_height: Arc<AtomicU32>,
}

impl From<Node> for FakeNode {
    fn from(node: Node) -> Self {
        Self {
            node,
            peers: Default::default(),
            desired_connection_count: DESIRED_CONNECTION_COUNT,
            current_block_height: Default::default(),
        }
    }
}

impl FakeNode {
    fn produce_version(&self, receiver_addr: SocketAddr) -> Version {
        Version::new(
            VERSION,
            self.current_block_height.load(Ordering::SeqCst),
            self.node.listening_addr().port() as u64, // for simplicity,
            self.node.listening_addr(),
            receiver_addr,
        )
    }
}

impl Pea2Pea for FakeNode {
    fn node(&self) -> &Node {
        &self.node
    }
}

#[derive(Debug)]
pub struct SnarkosFullMessage {
    header: MessageHeader,
    message: SnarkosMessage,
}

#[derive(Debug)]
enum SnarkosMessage {
    Version(Version),
    Verack(Verack),
    GetPeers(GetPeers),
    Peers(Peers),
    Unsupported,
}

impl fmt::Display for SnarkosMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Version(_) => "Version",
            Self::Verack(_) => "Verack",
            Self::GetPeers(_) => "GetPeers",
            Self::Peers(_) => "Peers",
            Self::Unsupported => "unsupported message",
        };

        write!(f, "{}", name)
    }
}

pub fn prepare_packet<M: Message>(message: &M) -> Bytes {
    let serialized = message.serialize().unwrap();
    let header = MessageHeader::new(M::name(), serialized.len() as u32);

    let mut ret = Vec::with_capacity(MESSAGE_HEADER_LEN + serialized.len());
    ret.extend_from_slice(&header.serialize().unwrap());
    ret.extend_from_slice(&serialized);

    ret.into()
}

macro_rules! unwrap_or_bail {
    ($action: expr, $sender: expr) => {{
        let ret = $action;

        match ret {
            Ok(ret) => ret,
            Err(_) => {
                let err = io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "already connected, not handshaking",
                );

                if $sender.send(Err(err)).is_err() {
                    error!("\npanic!\n");
                    unreachable!(); // can't recover if this happens
                }

                continue;
            }
        }
    }};
}

impl Handshaking for FakeNode {
    fn enable_handshaking(&self) {
        let (from_node_sender, mut from_node_receiver) = mpsc::channel(1);
        self.node().set_handshake_handler(from_node_sender.into());

        // spawn a background task dedicated to handling the handshakes
        let self_clone = self.clone();
        tokio::spawn(async move {
            loop {
                if let Some((mut conn, result_sender)) = from_node_receiver.recv().await {
                    let mut locked_peers = self_clone.peers.lock().await;
                    // extra safeguard against double connections
                    if locked_peers.contains_key(&conn.addr) {
                        let err = io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            "already connected, not handshaking",
                        );
                        if result_sender.send(Err(err)).is_err() {
                            error!("\npanic!\n");
                            unreachable!(); // can't recover if this happens
                        }
                        continue;
                    }

                    let peer_listening_addr = match !conn.side {
                        ConnectionSide::Initiator => {
                            debug!(parent: conn.node.span(), "handshaking with {} as the initiator", conn.addr);

                            // send own Version
                            let version = self_clone.produce_version(conn.addr);
                            let packeted = prepare_packet(&version);
                            unwrap_or_bail!(
                                conn.writer().write_all(&packeted).await,
                                result_sender
                            );

                            // receive a Verack
                            let header = unwrap_or_bail!(
                                conn.reader().read_exact(MESSAGE_HEADER_LEN).await,
                                result_sender
                            );
                            let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                            header_arr.copy_from_slice(header);
                            let header = MessageHeader::from(header_arr);
                            assert_eq!(header.name, Verack::name());
                            let message_len = header.len as usize;
                            let message = unwrap_or_bail!(
                                conn.reader().read_exact(message_len).await,
                                result_sender
                            );
                            unwrap_or_bail!(Verack::deserialize(message), result_sender);

                            // receive a Version
                            let header = unwrap_or_bail!(
                                conn.reader().read_exact(MESSAGE_HEADER_LEN).await,
                                result_sender
                            );
                            let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                            header_arr.copy_from_slice(header);
                            let header = MessageHeader::from(header_arr);
                            assert_eq!(header.name, Version::name());
                            let message_len = header.len as usize;
                            let message = unwrap_or_bail!(
                                conn.reader().read_exact(message_len).await,
                                result_sender
                            );
                            let peer_version =
                                unwrap_or_bail!(Version::deserialize(message), result_sender);

                            // send a Verack
                            let nonce = conn.node.listening_addr().port() as u64; // for simplicity
                            let verack = Verack::new(nonce, conn.node.listening_addr(), conn.addr);
                            let packeted = prepare_packet(&verack);
                            unwrap_or_bail!(
                                conn.writer().write_all(&packeted).await,
                                result_sender
                            );

                            peer_version.sender
                        }
                        ConnectionSide::Responder => {
                            debug!(parent: conn.node.span(), "handshaking with {} as the responder", conn.addr);

                            // receive a Version
                            let header = unwrap_or_bail!(
                                conn.reader().read_exact(MESSAGE_HEADER_LEN).await,
                                result_sender
                            );
                            let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                            header_arr.copy_from_slice(header);
                            let header = MessageHeader::from(header_arr);
                            assert_eq!(header.name, Version::name());
                            let message_len = header.len as usize;
                            let message = unwrap_or_bail!(
                                conn.reader().read_exact(message_len).await,
                                result_sender
                            );
                            let peer_version =
                                unwrap_or_bail!(Version::deserialize(message), result_sender);

                            // send a Verack
                            let nonce = conn.node.listening_addr().port() as u64; // for simplicity
                            let verack = Verack::new(nonce, conn.node.listening_addr(), conn.addr);
                            let packeted = prepare_packet(&verack);
                            unwrap_or_bail!(
                                conn.writer().write_all(&packeted).await,
                                result_sender
                            );

                            // send own Version
                            let version = self_clone.produce_version(conn.addr);
                            let packeted = prepare_packet(&version);
                            unwrap_or_bail!(
                                conn.writer().write_all(&packeted).await,
                                result_sender
                            );

                            // receive a Verack
                            let header = unwrap_or_bail!(
                                conn.reader().read_exact(MESSAGE_HEADER_LEN).await,
                                result_sender
                            );
                            let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                            header_arr.copy_from_slice(header);
                            let header = MessageHeader::from(header_arr);
                            assert_eq!(header.name, Verack::name());
                            let message_len = header.len as usize;
                            let message = unwrap_or_bail!(
                                conn.reader().read_exact(message_len).await,
                                result_sender
                            );
                            unwrap_or_bail!(Verack::deserialize(message), result_sender);

                            peer_version.sender
                        }
                    };

                    locked_peers.insert(peer_listening_addr, conn.addr);

                    debug!(parent: conn.node.span(), "handshake with {} ({}) was a success", conn.addr, peer_listening_addr);

                    // return the Connection to the node
                    if result_sender.send(Ok(conn)).is_err() {
                        error!("\npanic!\n");
                        unreachable!(); // can't recover if this happens
                    }
                }
            }
        });
    }
}

#[async_trait::async_trait]
impl Reading for FakeNode {
    type Message = SnarkosFullMessage;

    fn read_message(
        &self,
        source: SocketAddr,
        buffer: &[u8],
    ) -> io::Result<Option<(Self::Message, usize)>> {
        // parse the header
        let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
        header_arr.copy_from_slice(&buffer[..MESSAGE_HEADER_LEN]);
        let header = MessageHeader::from(header_arr);

        // read payload length
        let message_len = header.len as usize;

        // read message payload
        let message = &buffer[MESSAGE_HEADER_LEN..][..message_len];

        let message = if header.name == Version::name() {
            info!(parent: self.node().span(), "got a Version from {}", source);
            SnarkosMessage::Version(
                Version::deserialize(message).map_err(|_| io::ErrorKind::InvalidData)?,
            )
        } else if header.name == Verack::name() {
            info!(parent: self.node().span(), "got a Verack from {}", source);
            SnarkosMessage::Verack(
                Verack::deserialize(message).map_err(|_| io::ErrorKind::InvalidData)?,
            )
        } else if header.name == GetPeers::name() {
            info!(parent: self.node().span(), "got a GetPeers from {}", source);
            SnarkosMessage::GetPeers(
                GetPeers::deserialize(message).map_err(|_| io::ErrorKind::InvalidData)?,
            )
        } else if header.name == Peers::name() {
            info!(parent: self.node().span(), "got a Peers from {}", source);
            SnarkosMessage::Peers(
                Peers::deserialize(message).map_err(|_| io::ErrorKind::InvalidData)?,
            )
        } else {
            info!(parent: self.node().span(), "got an unsupported message from {}", source);
            SnarkosMessage::Unsupported
        };

        let full_message = SnarkosFullMessage { header, message };

        Ok(Some((full_message, MESSAGE_HEADER_LEN + message_len)))
    }

    async fn process_message(
        &self,
        source: SocketAddr,
        full_message: Self::Message,
    ) -> io::Result<()> {
        let named_response = match full_message.message {
            SnarkosMessage::Version(_) => {
                // only used for sync purposes post-handshake
                None
            }
            SnarkosMessage::Verack(_) => {
                // only used for the handshake
                None
            }
            SnarkosMessage::GetPeers(_) => {
                let peers = self
                    .peers
                    .lock()
                    .await
                    .keys()
                    .filter(|&addr| *addr != source)
                    .map(|addr| (*addr, Utc::now()))
                    .collect::<Vec<_>>();

                if !peers.is_empty() {
                    let peers = Peers::new(peers);
                    let packet = prepare_packet(&peers);

                    Some(("Peers", packet))
                } else {
                    None
                }
            }
            SnarkosMessage::Peers(peers) => {
                // comment out the bootstrapper condition in order for all nodes to keep sending GetPeers requests
                // if node.name() == "bootstrapper" {
                if !peers.addresses.is_empty()
                    && self.node().num_connected() < self.desired_connection_count as usize
                {
                    // connect to only one candidate at once to avoid maxing the node's connection limit
                    let addr = peers
                        .addresses
                        .iter()
                        .filter(|&(addr, _)| *addr != self.node().listening_addr())
                        .choose(&mut *RNG.lock())
                        .unwrap()
                        .0;
                    if let Err(e) = self.node().connect(addr).await {
                        error!(parent: self.node().span(), "couldn't connect to {}: {}", addr, e);
                    }
                }
                // }

                None
            }
            SnarkosMessage::Unsupported => None,
        };

        if let Some((name, response)) = named_response {
            self.node()
                .send_direct_message(source, response)
                .await
                .unwrap();

            info!(parent: self.node().span(), "sent a {} to {}", name, source);
        }

        Ok(())
    }
}

impl Writing for FakeNode {
    fn write_message(&self, _: SocketAddr, payload: &[u8], buffer: &mut [u8]) -> io::Result<usize> {
        buffer[..payload.len()].copy_from_slice(payload);

        Ok(payload.len())
    }
}

impl FakeNode {
    pub fn run_periodic_maintenance(&self) {
        const BROADCAST_INTERVAL_SECS: u64 = 5;

        let self_clone = self.clone();
        tokio::spawn(async move {
            let node = self_clone.node();

            loop {
                sleep(Duration::from_secs(BROADCAST_INTERVAL_SECS)).await;
                debug!(parent: node.span(), "running maintenance");

                // disconnect from misbehaving peers if still connected
                let mut to_disconnect = Vec::new();
                for (addr, stats) in node.known_peers().write().iter_mut() {
                    if stats.failures != 0 {
                        to_disconnect.push(*addr);
                        stats.failures = 0;
                    }
                }
                for addr in to_disconnect {
                    node.disconnect(addr);
                }

                let num_connected = node.num_connected();

                if num_connected < self_clone.desired_connection_count as usize {
                    // broadcast GetPeers
                    info!(parent: node.span(), "broadcasting GetPeers (I only have {}/{})", num_connected, self_clone.desired_connection_count);

                    let packeted = prepare_packet(&GetPeers);
                    node.send_broadcast(packeted).await.unwrap();
                } else {
                    debug!(parent: node.span(), "I don't need any more peers (I have {}/{})", num_connected, self_clone.desired_connection_count);
                }

                if num_connected != 0 {
                    // broadcast Version
                    info!(parent: node.span(), "broadcasting Version");

                    let version = self_clone.produce_version("127.0.0.1:9".parse().unwrap()); // the discard protocol port
                    let packeted = prepare_packet(&version);
                    node.send_broadcast(packeted).await.unwrap();
                }
            }
        });
    }
}
