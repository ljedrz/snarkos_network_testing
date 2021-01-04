mod common;
use common::*;
use pea2pea::{
    protocols::{Handshaking, Reading, Writing},
    *,
};
use tokio::time::sleep;
use tracing::*;

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

    const NUM_NON_BOOTSTRAPPERS: usize = 49;

    let config = NodeConfig {
        name: Some("bootstrapper".into()),
        desired_listening_port: Some(4141),
        ..Default::default()
    };
    let fake_bootstrapper = Node::new(Some(config)).await.unwrap();

    let config = NodeConfig {
        max_connections: common::DESIRED_CONNECTION_COUNT as u16 + 5,
        ..Default::default()
    };
    let fake_nodes = start_nodes(NUM_NON_BOOTSTRAPPERS, Some(config)).await;
    let fake_nodes = iter::once(fake_bootstrapper)
        .chain(fake_nodes.into_iter())
        .map(FakeNode::from)
        .collect::<Vec<_>>();

    for node in &fake_nodes {
        node.enable_handshaking();
        node.enable_reading();
        node.enable_writing();
    }

    connect_nodes(&fake_nodes, Topology::Star).await.unwrap();

    for node in &fake_nodes {
        node.run_periodic_maintenance();
        sleep(Duration::from_millis(50)).await;
    }

    loop {
        let bootstrapper_peer_count = fake_nodes[0].node().num_connected();
        if bootstrapper_peer_count < common::DESIRED_CONNECTION_COUNT as usize {
            error!(
                "the bootstrapper isn't connected to all the fake nodes ({}/{})!",
                bootstrapper_peer_count,
                common::DESIRED_CONNECTION_COUNT
            );
        }

        if fake_nodes
            .iter()
            .skip(1)
            .any(|fake| fake.node().num_connected() < common::DESIRED_CONNECTION_COUNT as usize)
        {
            error!("not all the peers have the desired number of peers!");
        }

        sleep(Duration::from_secs(5)).await;
    }
}
