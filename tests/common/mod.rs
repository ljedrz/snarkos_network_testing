use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
};
use tracing::*;

use pea2pea::{
    connections::{Connection, ConnectionSide},
    protocols::{Handshaking, Reading, Writing},
    *,
};
use snarkos_network::external::{GetPeers, Message, MessageHeader, Peers, Verack, Version};

use std::{collections::HashMap, fmt, io, net::SocketAddr, ops::Deref, sync::Arc};

#[derive(Clone)]
pub struct FakeNode {
    pub node: Arc<Node>,
    // a map of listening addresses to the actual addresses of connected nodes
    pub peers: Arc<RwLock<HashMap<SocketAddr, SocketAddr>>>,
    pub desired_connection_count: u8,
}

const DESIRED_CONNECTION_COUNT: u8 = 100;

impl FakeNode {
    #[allow(dead_code)]
    pub async fn new(config: Option<NodeConfig>) -> Arc<Self> {
        Arc::new(Self {
            node: Node::new(config).await.unwrap(),
            peers: Default::default(),
            desired_connection_count: DESIRED_CONNECTION_COUNT,
        })
    }
}

impl From<Arc<Node>> for FakeNode {
    fn from(node: Arc<Node>) -> Self {
        Self {
            node,
            peers: Default::default(),
            desired_connection_count: DESIRED_CONNECTION_COUNT,
        }
    }
}

impl Deref for FakeNode {
    type Target = Node;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl Pea2Pea for FakeNode {
    fn node(&self) -> &Arc<Node> {
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

const VERSION: u64 = 1;
const MESSAGE_HEADER_LEN: usize = 16;

pub fn prepare_packet<M: Message>(message: &M) -> Bytes {
    let serialized = message.serialize().unwrap();
    let header = MessageHeader::new(M::name(), serialized.len() as u32);

    let mut ret = Vec::with_capacity(MESSAGE_HEADER_LEN + serialized.len());
    ret.extend_from_slice(&header.serialize().unwrap());
    ret.extend_from_slice(&serialized);

    ret.into()
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
                    let peer_listening_addr = match !conn.side {
                        ConnectionSide::Initiator => {
                            debug!(parent: conn.node.span(), "handshaking with {} as the initiator", conn.addr);

                            // send own Version
                            let block_height = 1; // TODO: 1 or 0?
                            let nonce = conn.node.listening_addr.port() as u64; // for simplicity
                            let version = Version::new(
                                VERSION,
                                block_height,
                                nonce,
                                conn.node.listening_addr,
                                conn.addr,
                            );
                            let packeted = prepare_packet(&version);
                            conn.writer().write_all(&packeted).await.unwrap();

                            // receive a Verack
                            let header =
                                conn.reader().read_exact(MESSAGE_HEADER_LEN).await.unwrap();
                            let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                            header_arr.copy_from_slice(header);
                            let header = MessageHeader::from(header_arr);
                            assert_eq!(header.name, Verack::name());
                            let message_len = header.len as usize;
                            let message = conn.reader().read_exact(message_len).await.unwrap();
                            Verack::deserialize(message).unwrap();

                            // receive a Version
                            let header =
                                conn.reader().read_exact(MESSAGE_HEADER_LEN).await.unwrap();
                            let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                            header_arr.copy_from_slice(header);
                            let header = MessageHeader::from(header_arr);
                            assert_eq!(header.name, Version::name());
                            let message_len = header.len as usize;
                            let message = conn.reader().read_exact(message_len).await.unwrap();
                            let peer_version = Version::deserialize(message).unwrap();

                            // send a Verack
                            let verack = Verack::new(nonce, conn.node.listening_addr, conn.addr);
                            let packeted = prepare_packet(&verack);
                            conn.writer().write_all(&packeted).await.unwrap();

                            peer_version.sender
                        }
                        ConnectionSide::Responder => {
                            debug!(parent: conn.node.span(), "handshaking with {} as the responder", conn.addr);

                            // receive a Version
                            let header =
                                conn.reader().read_exact(MESSAGE_HEADER_LEN).await.unwrap();
                            let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                            header_arr.copy_from_slice(header);
                            let header = MessageHeader::from(header_arr);
                            assert_eq!(header.name, Version::name());
                            let message_len = header.len as usize;
                            let message = conn.reader().read_exact(message_len).await.unwrap();
                            let peer_version = Version::deserialize(message).unwrap();

                            // send a Verack
                            let nonce = conn.node.listening_addr.port() as u64; // for simplicity
                            let verack = Verack::new(nonce, conn.node.listening_addr, conn.addr);
                            let packeted = prepare_packet(&verack);
                            conn.writer().write_all(&packeted).await.unwrap();

                            // send own Version
                            let block_height = 1; // TODO: 1 or 0?
                            let version = Version::new(
                                VERSION,
                                block_height,
                                nonce,
                                conn.node.listening_addr,
                                conn.addr,
                            );
                            let packeted = prepare_packet(&version);
                            conn.writer().write_all(&packeted).await.unwrap();

                            // receive a Verack
                            let header =
                                conn.reader().read_exact(MESSAGE_HEADER_LEN).await.unwrap();
                            let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                            header_arr.copy_from_slice(header);
                            let header = MessageHeader::from(header_arr);
                            assert_eq!(header.name, Verack::name());
                            let message_len = header.len as usize;
                            let message = conn.reader().read_exact(message_len).await.unwrap();
                            Verack::deserialize(message).unwrap();

                            peer_version.sender
                        }
                    };

                    self_clone
                        .peers
                        .write()
                        .await
                        .insert(peer_listening_addr, conn.addr);

                    // return the Connection to the node
                    if result_sender.send(Ok(conn)).is_err() {
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
        _source: SocketAddr,
        buffer: &[u8],
    ) -> io::Result<Option<(Self::Message, usize)>> {
        // parse the header
        let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
        header_arr.copy_from_slice(&buffer[..MESSAGE_HEADER_LEN]);
        let header = MessageHeader::from(header_arr);

        // read payload length
        let message_len = header.len as usize;

        // read message payload
        if message_len == 0 {
            return Err(io::ErrorKind::InvalidData.into());
        }

        let message = &buffer[MESSAGE_HEADER_LEN..][..message_len];

        let message = if header.name == Version::name() {
            SnarkosMessage::Version(
                Version::deserialize(message).map_err(|_| io::ErrorKind::InvalidData)?,
            )
        } else if header.name == Verack::name() {
            SnarkosMessage::Verack(
                Verack::deserialize(message).map_err(|_| io::ErrorKind::InvalidData)?,
            )
        } else if header.name == GetPeers::name() {
            SnarkosMessage::GetPeers(
                GetPeers::deserialize(message).map_err(|_| io::ErrorKind::InvalidData)?,
            )
        } else if header.name == Peers::name() {
            SnarkosMessage::Peers(
                Peers::deserialize(message).map_err(|_| io::ErrorKind::InvalidData)?,
            )
        } else {
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
            SnarkosMessage::Version(version) => {
                let self_clone = self.clone();
                let sender = version.sender;
                tokio::spawn(async move {
                    self_clone.peers.write().await.insert(sender, source);
                });

                let nonce = self.node().listening_addr.port() as u64; // for simplicity
                let verack = Verack::new(nonce, self.node().listening_addr, source);
                let packet = prepare_packet(&verack);

                Some(("Verack", packet))
            }
            SnarkosMessage::GetPeers(_) => {
                let peers = self
                    .peers
                    .read()
                    .await
                    .iter()
                    .map(|(addr, _)| (*addr, Utc::now()))
                    .collect();
                let peers = Peers::new(peers);
                let packet = prepare_packet(&peers);

                Some(("Peers", packet))
            }
            SnarkosMessage::Peers(peers) => {
                let peers = peers
                    .addresses
                    .iter()
                    .map(|(addr, _)| *addr)
                    .collect::<Vec<_>>();

                let self_clone = self.clone();
                tokio::spawn(async move {
                    let node = self_clone.node();
                    let mut peers = peers.into_iter();

                    while let Some(addr) = peers.next() {
                        if node.num_connected() < self_clone.desired_connection_count as usize {
                            let mut peers_lock = self_clone.peers.write().await;
                            if addr != node.listening_addr && !peers_lock.contains_key(&addr) {
                                if let Err(e) = node.connect(addr).await {
                                    error!(parent: node.span(), "couldn't connect to {}: {}", addr, e);
                                } else {
                                    peers_lock.insert(addr, source);
                                }
                            }
                        }
                    }
                });

                None
            }
            _ => None,
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

impl FakeNode {
    pub fn start_broadcasting(&self) {
        const INTERVAL_MS: u64 = 10_000;
        /*
        let node = self.node();

        if node.handshaken_addrs().len() != 0 {
            info!(parent: node.span(), "broadcasting Version");

            let block_height = 1; // TODO: keep a state that updates based on the highest received value
            let nonce = node.listening_addr.port() as u64; // for simplicity

            // provide the discard protocol as the port on the receiver side
            // TODO: check if that value is not already ignored by snarkOS (it's probable)
            let version = Version::new(
                VERSION,
                block_height,
                nonce,
                node.listening_addr,
                "127.0.0.1:9".parse().unwrap(),
            );
            let packeted = prepare_packet(&version);

            node.send_broadcast(packeted).await;

            let num_connected = node.handshaken_addrs().len();
            if num_connected < self.desired_connection_count as usize {
                info!(parent: node.span(), "broadcasting GetPeers (I only have {}, and I want {})", num_connected, self.desired_connection_count);

                let get_peers = GetPeers;
                let packeted = prepare_packet(&get_peers);
                node.send_broadcast(packeted).await;
            } else {
                debug!(parent: node.span(), "I don't need any more peers - not broadcasting GetPeers");
            }
        }*/
    }
}
