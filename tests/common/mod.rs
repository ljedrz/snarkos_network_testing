use bytes::Bytes;
use chrono::Utc;
use once_cell::sync::Lazy;
use parking_lot::Mutex as SyncMutex;
use rand::{rngs::SmallRng, seq::IteratorRandom, SeedableRng};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, Mutex},
    time::sleep,
};
use tracing::*;

use pea2pea::{
    connections::{Connection, ConnectionSide},
    protocols::{Handshaking, Reading, ReturnableConnection, Writing},
    *,
};
use snarkos_network::external::*;

use std::{
    collections::HashMap,
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

pub const DESIRED_CONNECTION_COUNT: u8 = 5;
pub static RNG: Lazy<SyncMutex<SmallRng>> = Lazy::new(|| SyncMutex::new(SmallRng::from_entropy()));

const VERSION: u64 = 1;
const MESSAGE_HEADER_LEN: usize = 4;

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
    fn produce_version(&self) -> Version {
        Version::new(
            VERSION,
            self.current_block_height.load(Ordering::SeqCst),
            0, // for simplicity,
            self.node.listening_addr().port(),
        )
    }

    async fn perform_handshake(&self, conn: &mut Connection) -> io::Result<SocketAddr> {
        let mut locked_peers = self.peers.lock().await;
        // extra safeguard against double connections
        if locked_peers.contains_key(&conn.addr) {
            return Err(io::ErrorKind::Other.into());
        }

        let mut temp_buffer = [0u8; 128];

        let peer_listening_port = match !conn.side {
            ConnectionSide::Initiator => {
                debug!(parent: conn.node.span(), "handshaking with {} as the initiator", conn.addr);

                // send own Version
                let version = self.produce_version();
                let packeted = prepare_packet(&Payload::Version(version));
                conn.writer().write_all(&packeted).await?;

                // receive a Verack
                let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                conn.reader().read_exact(&mut header_arr).await?;
                let header = MessageHeader::from(header_arr);
                let message_len = header.len as usize;
                conn.reader()
                    .read_exact(&mut temp_buffer[..message_len])
                    .await?;
                bincode::deserialize(&temp_buffer[..message_len])
                    .map_err(|_| io::ErrorKind::InvalidData)?;

                // receive a Version
                let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                conn.reader().read_exact(&mut header_arr).await?;
                let header = MessageHeader::from(header_arr);
                let message_len = header.len as usize;
                conn.reader()
                    .read_exact(&mut temp_buffer[..message_len])
                    .await?;
                let peer_version = bincode::deserialize(&temp_buffer[..message_len])
                    .map_err(|_| io::ErrorKind::InvalidData)?;
                let peer_port = if let Payload::Version(ref version) = peer_version {
                    version.listening_port
                } else {
                    return Err(io::ErrorKind::InvalidData.into());
                };

                // send a Verack
                let nonce = 0; // for simplicity
                let verack = Verack::new(nonce);
                let packeted = prepare_packet(&Payload::Verack(verack));
                conn.writer().write_all(&packeted).await?;

                peer_port
            }
            ConnectionSide::Responder => {
                debug!(parent: conn.node.span(), "handshaking with {} as the responder", conn.addr);

                // receive a Version
                let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
                conn.reader().read_exact(&mut header_arr).await?;
                let header = MessageHeader::from(header_arr);
                let message_len = header.len as usize;
                conn.reader()
                    .read_exact(&mut temp_buffer[..message_len])
                    .await?;
                let peer_version = bincode::deserialize(&temp_buffer[..message_len])
                    .map_err(|_| io::ErrorKind::InvalidData)?;

                // send a Verack
                let (peer_port, nonce) = if let Payload::Version(ref version) = peer_version {
                    (version.listening_port, version.nonce)
                } else {
                    return Err(io::ErrorKind::InvalidData.into());
                };
                let verack = Verack::new(nonce);
                let packeted = prepare_packet(&Payload::Verack(verack));
                conn.writer().write_all(&packeted).await?;

                // send own Version
                let version = self.produce_version();
                let packeted = prepare_packet(&Payload::Version(version));
                conn.writer().write_all(&packeted).await?;

                // receive a Verack
                conn.reader().read_exact(&mut header_arr).await?;
                let header = MessageHeader::from(header_arr);
                let message_len = header.len as usize;
                conn.reader()
                    .read_exact(&mut temp_buffer[..message_len])
                    .await?;
                let verack = bincode::deserialize(&temp_buffer[..message_len])
                    .map_err(|_| io::ErrorKind::InvalidData)?;
                if !matches!(verack, Payload::Verack(_)) {
                    return Err(io::ErrorKind::InvalidData.into());
                }

                peer_port
            }
        };

        let peer_listening_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, peer_listening_port));

        locked_peers.insert(peer_listening_addr, conn.addr);

        Ok(peer_listening_addr)
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
    message: Payload,
}

pub fn prepare_packet(payload: &Payload) -> Bytes {
    let serialized = bincode::serialize(payload).unwrap();
    let header = MessageHeader {
        len: serialized.len() as u32,
    };

    let mut ret = Vec::with_capacity(MESSAGE_HEADER_LEN + serialized.len());
    ret.extend_from_slice(&header.as_bytes()[..]);
    ret.extend_from_slice(&serialized);

    ret.into()
}

impl Handshaking for FakeNode {
    fn enable_handshaking(&self) {
        let (from_node_sender, mut from_node_receiver) = mpsc::channel::<ReturnableConnection>(
            self.node().config().protocol_handler_queue_depth,
        );

        // spawn a background task dedicated to handling the handshakes
        let self_clone = self.clone();
        let handshake_task = tokio::spawn(async move {
            loop {
                if let Some((mut conn, result_sender)) = from_node_receiver.recv().await {
                    match self_clone.perform_handshake(&mut conn).await {
                        Ok(peer_listening_addr) => {
                            debug!(parent: conn.node.span(), "handshake with {} ({}) was a success", conn.addr, peer_listening_addr);

                            // return the Connection to the node
                            if result_sender.send(Ok(conn)).is_err() {
                                error!("\npanic!\n");
                                unreachable!(); // can't recover if this happens
                            }
                        }
                        Err(e) => {
                            if result_sender.send(Err(e)).is_err() {
                                error!("\npanic!\n");
                                unreachable!(); // can't recover if this happens
                            }
                        }
                    }
                }
            }
        });

        self.node()
            .set_handshake_handler((from_node_sender, handshake_task).into());
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

        if buffer[MESSAGE_HEADER_LEN..].len() >= message_len {
            // read message payload
            let message = bincode::deserialize(&buffer[MESSAGE_HEADER_LEN..][..message_len])
                .map_err(|e| {
                    error!("can't deserialize: {}", e);
                    io::ErrorKind::InvalidData
                })?;

            let full_message = SnarkosFullMessage { header, message };

            Ok(Some((full_message, MESSAGE_HEADER_LEN + message_len)))
        } else {
            Ok(None)
        }
    }

    async fn process_message(
        &self,
        source: SocketAddr,
        full_message: Self::Message,
    ) -> io::Result<()> {
        let named_response = match full_message.message {
            Payload::Version(_) => {
                // only used for sync purposes post-handshake
                None
            }
            Payload::Verack(_) => {
                // only used for the handshake
                None
            }
            Payload::GetPeers => {
                let peers = self
                    .peers
                    .lock()
                    .await
                    .keys()
                    .filter(|&addr| *addr != source)
                    .map(|addr| (*addr, Utc::now()))
                    .collect::<Vec<_>>();

                if !peers.is_empty() {
                    let peers = Payload::Peers(peers);
                    let packet = prepare_packet(&peers);

                    Some(("Peers", packet))
                } else {
                    None
                }
            }
            Payload::Peers(peers) => {
                // comment out the bootstrapper condition in order for all nodes to keep sending GetPeers requests
                // if node.name() == "bootstrapper" {
                if !peers.is_empty()
                    && self.node().num_connected() < self.desired_connection_count as usize
                {
                    // connect to only one candidate at once to avoid maxing the node's connection limit
                    let addr = peers
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
                /*
                let mut to_disconnect = Vec::new();
                for (addr, stats) in node.known_peers().write().iter_mut() {
                    if stats.failures != 0 {
                        to_disconnect.push(*addr);
                        stats.failures = 0;
                    }
                }
                for addr in to_disconnect {
                    node.disconnect(addr);
                }*/

                let num_connected = node.num_connected();

                if num_connected < self_clone.desired_connection_count as usize {
                    // broadcast GetPeers
                    info!(parent: node.span(), "broadcasting GetPeers (I only have {}/{})", num_connected, self_clone.desired_connection_count);

                    let packeted = prepare_packet(&Payload::GetPeers);
                    node.send_broadcast(packeted).await.unwrap();
                } else {
                    debug!(parent: node.span(), "I don't need any more peers (I have {}/{})", num_connected, self_clone.desired_connection_count);
                }

                if num_connected != 0 {
                    // broadcast Version
                    info!(parent: node.span(), "broadcasting Version");

                    let version = self_clone.produce_version();
                    let packeted = prepare_packet(&Payload::Version(version));
                    node.send_broadcast(packeted).await.unwrap();
                }
            }
        });
    }
}
