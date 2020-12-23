use chrono::{DateTime, Utc};
use tokio::{io::AsyncReadExt, task::JoinHandle};
use tracing::*;

use pea2pea::*;
use snarkos_network::external::{GetPeers, Message, MessageHeader, Peers, Verack, Version};
use tokio::sync::RwLock;

use std::{collections::HashMap, fmt, io, net::SocketAddr, ops::Deref, sync::Arc};

#[derive(Clone)]
pub struct FakeNode {
    pub node: Arc<Node>,
    // a map of *listening* addresses and last-seen timestamps for handshaken nodes
    pub peers: Arc<RwLock<HashMap<SocketAddr, DateTime<Utc>>>>,
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

impl ContainsNode for FakeNode {
    fn node(&self) -> &Arc<Node> {
        &self.node
    }
}

#[derive(Debug)]
pub enum SnarkosMessage {
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

pub fn prepare_packet<M: Message>(message: &M) -> Vec<u8> {
    let serialized = message.serialize().unwrap();
    let header = MessageHeader::new(M::name(), serialized.len() as u32);

    let mut ret = Vec::with_capacity(MESSAGE_HEADER_LEN + serialized.len());
    ret.extend_from_slice(&header.serialize().unwrap());
    ret.extend_from_slice(&serialized);

    ret
}

#[async_trait::async_trait]
impl MessagingProtocol for FakeNode {
    type Message = SnarkosMessage;

    async fn read_message(connection_reader: &mut ConnectionReader) -> io::Result<Vec<u8>> {
        let buffer = &mut connection_reader.buffer;

        // read message header
        connection_reader
            .reader
            .read_exact(&mut buffer[..MESSAGE_HEADER_LEN])
            .await?;

        // read payload length
        let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
        header_arr.copy_from_slice(&buffer[..MESSAGE_HEADER_LEN]);
        let header = MessageHeader::from(header_arr);
        let message_len = header.len as usize;
        let packet_len = MESSAGE_HEADER_LEN + message_len;

        // read message payload
        if message_len != 0 {
            connection_reader
                .reader
                .read_exact(&mut buffer[MESSAGE_HEADER_LEN..packet_len])
                .await?;
        }

        Ok(buffer[..packet_len].to_vec())
    }

    fn parse_message(&self, buffer: &[u8]) -> Option<Self::Message> {
        let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
        header_arr.copy_from_slice(&buffer[..MESSAGE_HEADER_LEN]);
        let header = MessageHeader::from(header_arr);

        let message_len = header.len as usize;
        let packet_len = MESSAGE_HEADER_LEN + message_len;
        let message = &buffer[MESSAGE_HEADER_LEN..packet_len];

        let message = if header.name == Version::name() {
            SnarkosMessage::Version(Version::deserialize(message).ok()?)
        } else if header.name == Verack::name() {
            SnarkosMessage::Verack(Verack::deserialize(message).ok()?)
        } else if header.name == GetPeers::name() {
            SnarkosMessage::GetPeers(GetPeers::deserialize(message).ok()?)
        } else if header.name == Peers::name() {
            SnarkosMessage::Peers(Peers::deserialize(message).ok()?)
        } else {
            SnarkosMessage::Unsupported
        };

        Some(message)
    }

    fn process_message(&self, message: &Self::Message) {
        match message {
            SnarkosMessage::Version(version) => {
                let self_clone = self.clone();
                let sender = version.sender;
                tokio::spawn(async move {
                    self_clone.peers.write().await.insert(sender, Utc::now());
                });
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
                                if let Err(e) = node.initiate_connection(addr).await {
                                    error!(parent: node.span(), "couldn't connect to {}: {}", addr, e);
                                } else {
                                    peers_lock.insert(addr, Utc::now());
                                }
                            }
                        }
                    }
                });
            }
            _ => {}
        }
    }

    fn respond_to_message(
        &self,
        message: Self::Message,
        source_addr: SocketAddr,
    ) -> io::Result<()> {
        info!(parent: self.node().span(), "got a {} from {}", message, source_addr);

        let self_clone = self.clone();
        tokio::spawn(async move {
            let node = self_clone.node();

            let named_packet = match message {
                SnarkosMessage::Version(_) => {
                    let nonce = node.listening_addr.port() as u64; // for simplicity
                    let verack = Verack::new(nonce, node.listening_addr, source_addr);
                    let packet = prepare_packet(&verack);

                    Some(("Verack", packet))
                }
                SnarkosMessage::Verack(_) => None,
                SnarkosMessage::GetPeers(_) => {
                    let peers = self_clone
                        .peers
                        .read()
                        .await
                        .iter()
                        .map(|(addr, time)| (*addr, *time))
                        .collect();
                    let peers = Peers::new(peers);
                    let packet = prepare_packet(&peers);

                    Some(("Peers", packet))
                }
                _ => None,
            };

            if let Some((name, packet)) = named_packet {
                self_clone
                    .send_direct_message(source_addr, packet)
                    .await
                    .unwrap();

                info!(parent: node.span(), "sent a {} to {}", name, source_addr);
            }
        });

        Ok(())
    }
}

impl HandshakeProtocol for FakeNode {
    fn enable_handshake_protocol(&self) {
        let initiator = |peer_addr: SocketAddr,
                         mut connection_reader: ConnectionReader,
                         connection: Arc<Connection>|
         -> JoinHandle<ConnectionReader> {
            tokio::spawn(async move {
                let node = Arc::clone(&connection_reader.node);
                debug!(parent: node.span(), "spawned a task to handshake with {}", peer_addr);

                // send own Version
                let block_height = 1; // TODO: 1 or 0?
                let nonce = node.listening_addr.port() as u64; // for simplicity
                let version =
                    Version::new(VERSION, block_height, nonce, node.listening_addr, peer_addr);
                let packeted = prepare_packet(&version);
                connection.write_bytes(&packeted).await.unwrap();

                // receive a Verack
                assert_eq!(
                    connection_reader
                        .read_bytes(MESSAGE_HEADER_LEN)
                        .await
                        .unwrap(),
                    MESSAGE_HEADER_LEN
                );
                let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                header_arr.copy_from_slice(&connection_reader.buffer[..MESSAGE_HEADER_LEN]);
                let header = MessageHeader::from(header_arr);
                assert_eq!(header.name, Verack::name());
                let message_len = header.len as usize;
                assert_eq!(
                    connection_reader.read_bytes(message_len).await.unwrap(),
                    message_len
                );
                Verack::deserialize(&connection_reader.buffer[..message_len]).unwrap();

                // receive a Version
                assert_eq!(
                    connection_reader
                        .read_bytes(MESSAGE_HEADER_LEN)
                        .await
                        .unwrap(),
                    MESSAGE_HEADER_LEN
                );
                let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                header_arr.copy_from_slice(&connection_reader.buffer[..MESSAGE_HEADER_LEN]);
                let header = MessageHeader::from(header_arr);
                assert_eq!(header.name, Version::name());
                let message_len = header.len as usize;
                assert_eq!(
                    connection_reader.read_bytes(message_len).await.unwrap(),
                    message_len
                );
                Version::deserialize(&connection_reader.buffer[..message_len]).unwrap();

                // send a Verack
                let verack = Verack::new(nonce, node.listening_addr, peer_addr);
                let packeted = prepare_packet(&verack);
                connection.write_bytes(&packeted).await.unwrap();

                connection_reader
            })
        };

        let responder = |peer_addr: SocketAddr,
                         mut connection_reader: ConnectionReader,
                         connection: Arc<Connection>|
         -> JoinHandle<ConnectionReader> {
            tokio::spawn(async move {
                let node = Arc::clone(&connection_reader.node);
                debug!(parent: node.span(), "spawned a task to handshake with {}", peer_addr);

                // receive a Version
                assert_eq!(
                    connection_reader
                        .read_bytes(MESSAGE_HEADER_LEN)
                        .await
                        .unwrap(),
                    MESSAGE_HEADER_LEN
                );
                let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                header_arr.copy_from_slice(&connection_reader.buffer[..MESSAGE_HEADER_LEN]);
                let header = MessageHeader::from(header_arr);
                assert_eq!(header.name, Version::name());
                let message_len = header.len as usize;
                assert_eq!(
                    connection_reader.read_bytes(message_len).await.unwrap(),
                    message_len
                );
                Version::deserialize(&connection_reader.buffer[..message_len]).unwrap();

                // send a Verack
                let nonce = node.listening_addr.port() as u64; // for simplicity
                let verack = Verack::new(nonce, node.listening_addr, peer_addr);
                let packeted = prepare_packet(&verack);
                connection.write_bytes(&packeted).await.unwrap();

                // send own Version
                let block_height = 1; // TODO: 1 or 0?
                let version =
                    Version::new(VERSION, block_height, nonce, node.listening_addr, peer_addr);
                let packeted = prepare_packet(&version);
                connection.write_bytes(&packeted).await.unwrap();

                // receive a Verack
                assert_eq!(
                    connection_reader
                        .read_bytes(MESSAGE_HEADER_LEN)
                        .await
                        .unwrap(),
                    MESSAGE_HEADER_LEN
                );
                let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                header_arr.copy_from_slice(&connection_reader.buffer[..MESSAGE_HEADER_LEN]);
                let header = MessageHeader::from(header_arr);
                assert_eq!(header.name, Verack::name());
                let message_len = header.len as usize;
                assert_eq!(
                    connection_reader.read_bytes(message_len).await.unwrap(),
                    message_len
                );
                Verack::deserialize(&connection_reader.buffer[..message_len]).unwrap();

                connection_reader
            })
        };

        let handshake_closures = HandshakeClosures {
            initiator: Box::new(initiator),
            responder: Box::new(responder),
        };

        self.node().set_handshake_closures(handshake_closures);
    }
}

#[async_trait::async_trait]
impl BroadcastProtocol for FakeNode {
    const INTERVAL_MS: u64 = 10_000;

    async fn perform_broadcast(&self) -> io::Result<()> {
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
        }

        Ok(())
    }
}
