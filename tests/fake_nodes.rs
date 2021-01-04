use tokio::time::sleep;

mod common;
use common::*;
use pea2pea::{
    protocols::{Handshaking, Reading, Writing},
    *,
};

use std::{iter, sync::Arc, time::Duration};

async fn start_nodes(count: usize, config: Option<NodeConfig>) -> Vec<Arc<Node>> {
    let mut nodes = Vec::with_capacity(count);

    for _ in 0..count {
        let node = Node::new(config.clone()).await.unwrap();
        nodes.push(node);
    }

    nodes
}

#[tokio::test]
async fn initiate_handshake() {
    tracing_subscriber::fmt::init();

    let fake_node = FakeNode::new(None).await;
    fake_node.enable_handshaking();

    fake_node
        .node
        .connect("127.0.0.1:4131".parse().unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn pose_as_bootstrapper() {
    tracing_subscriber::fmt::init();

    let mut config = NodeConfig::default();
    config.name = Some("bootstrapper".into());
    config.desired_listening_port = Some(4141);
    let fake_bootstrapper = Node::new(Some(config)).await.unwrap();

    let fake_nodes = start_nodes(9, None).await;
    let fake_nodes = iter::once(fake_bootstrapper)
        .chain(fake_nodes.into_iter())
        .map(|node| FakeNode::from(node))
        .collect::<Vec<_>>();

    for node in &fake_nodes {
        node.enable_handshaking();
        node.enable_reading();
        // node.enable_writing();
    }

    connect_nodes(&fake_nodes, Topology::Star).await.unwrap();

    for node in &fake_nodes {
        node.start_broadcasting();
    }

    std::future::pending::<()>().await;
}
