use bytes::Bytes;
use once_cell::sync::Lazy;
use parking_lot::{Mutex as SyncMutex, RwLock};
use rand::{rngs::SmallRng, seq::IteratorRandom, SeedableRng};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
    time::sleep,
};
use tracing::*;

use pea2pea::{
    connections::{Connection, ConnectionSide},
    protocols::{Handshake, Reading, Writing},
    *,
};
use snarkos_network::{MessageHeader, Payload, Version, PROTOCOL_VERSION};

use std::{
    collections::{HashMap, HashSet},
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

const MESSAGE_HEADER_LEN: usize = 4;

#[derive(Clone)]
pub struct FakeNode {
    pub node: Node,
    // a map of listening addresses to the actual addresses of connected nodes
    pub peers: Arc<Mutex<HashSet<SocketAddr>>>,
    pub handshakes: Arc<RwLock<HashMap<SocketAddr, Arc<SyncMutex<snow::TransportState>>>>>,
    pub desired_connection_count: u8,
    pub current_block_height: Arc<AtomicU32>,
}

impl From<Node> for FakeNode {
    fn from(node: Node) -> Self {
        Self {
            node,
            peers: Default::default(),
            handshakes: Default::default(),
            desired_connection_count: DESIRED_CONNECTION_COUNT,
            current_block_height: Default::default(),
        }
    }
}

impl Pea2Pea for FakeNode {
    fn node(&self) -> &Node {
        &self.node
    }
}

pub fn prepare_packet(payload: &Payload) -> Bytes {
    Payload::serialize(payload).unwrap().into()
}

#[async_trait::async_trait]
impl Handshake for FakeNode {
    async fn perform_handshake(&self, mut conn: Connection) -> io::Result<Connection> {
        let mut locked_peers = self.peers.lock().await;
        // extra safeguard against double connections
        if locked_peers.contains(&conn.addr) {
            return Err(io::ErrorKind::AlreadyExists.into());
        }

        let (noise, peer_listening_port) = match !conn.side {
            ConnectionSide::Initiator => {
                let builder = snow::Builder::with_resolver(
                    snarkos_network::HANDSHAKE_PATTERN.parse().unwrap(),
                    Box::new(snow::resolvers::SodiumResolver),
                );
                let static_key = builder.generate_keypair().unwrap().private;
                let noise_builder = builder
                    .local_private_key(&static_key)
                    .psk(3, snarkos_network::HANDSHAKE_PSK);
                let mut noise = noise_builder.build_initiator().unwrap();
                let mut buffer: Box<[u8]> = vec![0u8; snarkos_network::MAX_MESSAGE_SIZE].into();
                let mut buf = [0u8; snarkos_network::NOISE_BUF_LEN]; // a temporary intermediate buffer to decrypt from

                // -> e
                let len = noise.write_message(&[], &mut buffer).unwrap();
                println!("len: {}", len);
                conn.writer().write_all(&[len as u8]).await?;
                conn.writer().write_all(&buffer[..len]).await?;
                trace!("sent e (XX handshake part 1/3)");

                // <- e, ee, s, es
                conn.reader().read_exact(&mut buf[..1]).await?;
                let len = buf[0] as usize;
                let len = conn.reader().read_exact(&mut buf[..len]).await?;
                let len = noise
                    .read_message(&buf[..len], &mut buffer)
                    .map_err(|_| io::ErrorKind::InvalidData)?;
                let peer_version =
                    Version::deserialize(&buffer[..len]).map_err(|_| io::ErrorKind::InvalidData)?;
                trace!("received e, ee, s, es (XX handshake part 2/3)");

                // -> s, se, psk
                let own_version = Version::serialize(&Version::new(
                    PROTOCOL_VERSION,
                    self.node().listening_addr().unwrap().port(),
                    0,
                ))
                .unwrap();
                let len = noise.write_message(&own_version, &mut buffer).unwrap();
                conn.writer().write_all(&[len as u8]).await?;
                conn.writer().write_all(&buffer[..len]).await?;
                trace!("sent s, se, psk (XX handshake part 3/3)");

                (noise, peer_version.listening_port)
            }
            ConnectionSide::Responder => {
                let builder = snow::Builder::with_resolver(
                    snarkos_network::HANDSHAKE_PATTERN
                        .parse()
                        .expect("Invalid noise handshake pattern!"),
                    Box::new(snow::resolvers::SodiumResolver),
                );
                let static_key = builder.generate_keypair().unwrap().private;
                let noise_builder = builder
                    .local_private_key(&static_key)
                    .psk(3, snarkos_network::HANDSHAKE_PSK);
                let mut noise = noise_builder.build_responder().unwrap();
                let mut buffer: Box<[u8]> = vec![0u8; snarkos_network::MAX_MESSAGE_SIZE].into();
                let mut buf = [0u8; snarkos_network::NOISE_BUF_LEN]; // a temporary intermediate buffer to decrypt from

                // <- e
                conn.reader().read_exact(&mut buf[..1]).await?;
                let len = buf[0] as usize;
                let len = conn.reader().read_exact(&mut buf[..len]).await?;
                noise
                    .read_message(&buf[..len], &mut buffer)
                    .map_err(|_| io::ErrorKind::InvalidData)?;
                trace!("received e (XX handshake part 1/3)");

                // -> e, ee, s, es
                let own_version = Version::serialize(&Version::new(
                    PROTOCOL_VERSION,
                    self.node().listening_addr().unwrap().port(),
                    0,
                ))
                .unwrap();
                let len = noise.write_message(&own_version, &mut buffer).unwrap();
                conn.writer().write_all(&[len as u8]).await?;
                conn.writer().write_all(&buffer[..len]).await?;
                trace!("sent e, ee, s, es (XX handshake part 2/3)");

                // <- s, se, psk
                conn.reader().read_exact(&mut buf[..1]).await?;
                let len = buf[0] as usize;
                let len = conn.reader().read_exact(&mut buf[..len]).await?;
                let len = noise
                    .read_message(&buf[..len], &mut buffer)
                    .map_err(|_| io::ErrorKind::InvalidData)?;
                let peer_version =
                    Version::deserialize(&buffer[..len]).map_err(|_| io::ErrorKind::InvalidData)?;
                trace!("received s, se, psk (XX handshake part 3/3)");

                (noise, peer_version.listening_port)
            }
        };

        let peer_listening_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, peer_listening_port));

        locked_peers.insert(peer_listening_addr);
        self.handshakes.write().insert(
            conn.addr,
            Arc::new(SyncMutex::new(noise.into_transport_mode().unwrap())),
        );

        Ok(conn)
    }
}

#[async_trait::async_trait]
impl Reading for FakeNode {
    type Message = Payload;

    fn read_message<R: io::Read>(
        &self,
        source: SocketAddr,
        reader: &mut R,
    ) -> io::Result<Option<Self::Message>> {
        // parse the header
        let mut header_arr = [0u8; MESSAGE_HEADER_LEN];
        reader.read_exact(&mut header_arr).unwrap();
        let header = MessageHeader::from(header_arr);

        // read payload length
        let payload_len = header.len as usize;

        // the easy way
        let mut decrypted = vec![0u8; payload_len];

        let noise = Arc::clone(self.handshakes.read().get(&source).unwrap());
        let noise = &mut *noise.lock();

        let mut decrypted_len = 0;
        let mut processed_len = 0;

        while processed_len < payload_len {
            let chunk_len =
                std::cmp::min(snarkos_network::NOISE_BUF_LEN, payload_len - processed_len);
            let mut buffer = vec![0u8; chunk_len];
            if reader.read_exact(&mut buffer).is_err() {
                return Ok(None);
            }

            decrypted_len += noise
                .read_message(&buffer, &mut decrypted[decrypted_len..])
                .map_err(|_| io::ErrorKind::InvalidData)?;
            processed_len += chunk_len;
        }

        let payload = Payload::deserialize(&decrypted).map_err(|_| io::ErrorKind::InvalidData)?;

        Ok(Some(payload))
    }

    async fn process_message(&self, source: SocketAddr, payload: Self::Message) -> io::Result<()> {
        info!(parent: self.node().span(), "got a {} from {}", payload, source);

        let response = match payload {
            Payload::GetPeers => {
                let peers = self
                    .peers
                    .lock()
                    .await
                    .iter()
                    .copied()
                    .filter(|&addr| addr != source)
                    .collect::<Vec<_>>();

                if !peers.is_empty() {
                    Some(Payload::Peers(peers))
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
                        .copied()
                        .filter(|&addr| addr != self.node().listening_addr().unwrap())
                        .choose(&mut *RNG.lock())
                        .unwrap();
                    let _ = self.node().connect(addr).await;
                }
                // }

                None
            }
            Payload::Ping(_block_height) => Some(Payload::Pong),
            Payload::Sync(hashes) => Some(Payload::GetBlocks(hashes)),
            Payload::SyncBlock(_block) => None,
            _ => None,
        };

        if let Some(response) = response {
            info!(parent: self.node().span(), "seding a {} to {}", response, source);

            let _ = self.send_direct_message(source, response);
        }

        Ok(())
    }
}

impl Writing for FakeNode {
    type Message = Payload;

    fn write_message<W: io::Write>(
        &self,
        source: SocketAddr,
        payload: &Self::Message,
        writer: &mut W,
    ) -> io::Result<()> {
        let noise = Arc::clone(self.handshakes.read().get(&source).unwrap());
        let noise = &mut *noise.lock();

        let mut buffer = [0u8; snarkos_network::NOISE_BUF_LEN + snarkos_network::NOISE_TAG_LEN];

        let mut encrypted_len = MESSAGE_HEADER_LEN;
        let mut processed_len = 0;

        let payload = payload.serialize().unwrap();

        while processed_len < payload.len() {
            let chunk_len = std::cmp::min(
                snarkos_network::NOISE_BUF_LEN - snarkos_network::NOISE_TAG_LEN,
                payload[processed_len..].len(),
            );
            let chunk = &payload[processed_len..][..chunk_len];

            encrypted_len += noise
                .write_message(chunk, &mut buffer[encrypted_len..])
                .unwrap();
            processed_len += chunk_len;
        }
        encrypted_len -= MESSAGE_HEADER_LEN;

        let header = MessageHeader::from(encrypted_len);
        buffer[..MESSAGE_HEADER_LEN].copy_from_slice(&header.as_bytes()[..]);

        writer.write_all(&buffer[..MESSAGE_HEADER_LEN + encrypted_len])
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
                debug!(parent: node.span(), "running periodic tasks");

                let num_connected = node.num_connected();

                if num_connected < self_clone.desired_connection_count as usize {
                    // broadcast GetPeers
                    info!(parent: node.span(), "broadcasting requests for peers (I only have {}/{})", num_connected, self_clone.desired_connection_count);

                    let _ = self_clone.send_broadcast(Payload::GetPeers);
                } else {
                    trace!(parent: node.span(), "I don't need any more peers (I have {}/{})", num_connected, self_clone.desired_connection_count);
                }

                if num_connected != 0 {
                    // broadcast Ping
                    info!(parent: node.span(), "broadcasting Ping");

                    let message =
                        Payload::Ping(self_clone.current_block_height.load(Ordering::SeqCst));

                    /*/ broadcast GetSync
                    info!(parent: node.span(), "broadcasting GetSync");

                    let packeted = prepare_packet(&Payload::GetSync(vec![]));
                    */
                    let _ = self_clone.send_broadcast(message);
                }
            }
        });
    }
}
