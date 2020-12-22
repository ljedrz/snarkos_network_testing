use tokio::stream::{self, StreamExt};

mod common;
use common::*;
use pea2pea::*;

use std::iter;

#[tokio::test]
async fn initiate_handshake() {
    tracing_subscriber::fmt::init();

    let fake_node = FakeNode::new(None).await;
    fake_node.enable_handshake_protocol();

    fake_node
        .node
        .initiate_connection("127.0.0.1:4131".parse().unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn pose_as_bootstrapper() {
    tracing_subscriber::fmt::init();

    let mut config = NodeConfig::default();
    config.name = Some("bootstrapper".into());
    config.desired_listening_port = Some(4141);
    config.inbound_message_queue_depth = 1024;
    let fake_bootstrapper = Node::new(Some(config)).await.unwrap();

    let fake_nodes = spawn_nodes(999, None).await.unwrap();
    let fake_nodes = iter::once(fake_bootstrapper)
        .chain(fake_nodes.into_iter())
        .map(|node| FakeNode::from(node))
        .collect::<Vec<_>>();

    for node in &fake_nodes {
        node.enable_messaging_protocol();
        node.enable_handshake_protocol();
        node.enable_broadcast_protocol();
    }

    connect_nodes(&fake_nodes, Topology::Star).await.unwrap();

    stream::pending::<()>().next().await;
}
