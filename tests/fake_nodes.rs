use pea2pea::{
    protocols::{Handshake, Reading, Writing},
    *,
};
use snarkos_network_testing::*;
use tokio::time::sleep;
use tracing::*;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

use std::{future, iter, time::Duration};

fn start_logger() {
    let filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter.add_directive("mio=off".parse().unwrap()),
        _ => EnvFilter::default()
            .add_directive(LevelFilter::INFO.into())
            .add_directive("mio=off".parse().unwrap()),
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .without_time()
        .with_target(false)
        .init();
}

async fn start_nodes(count: usize, config: Option<Config>) -> Vec<Node> {
    let mut nodes = Vec::with_capacity(count);

    for _ in 0..count {
        let node = Node::new(config.clone()).await.unwrap();
        nodes.push(node);
    }

    nodes
}

#[tokio::test]
async fn pose_as_bootstrapper_with_peers() {
    start_logger();

    const NUM_NON_BOOTSTRAPPERS: usize = 49;

    let config = Config {
        name: Some("bootstrapper".into()),
        desired_listening_port: Some(4141),
        ..Default::default()
    };
    let fake_bootstrapper = Node::new(Some(config)).await.unwrap();

    let config = Config {
        max_connections: DESIRED_CONNECTION_COUNT as u16 + 5,
        ..Default::default()
    };
    let fake_nodes = start_nodes(NUM_NON_BOOTSTRAPPERS, Some(config)).await;
    let fake_nodes = iter::once(fake_bootstrapper)
        .chain(fake_nodes.into_iter())
        .map(FakeNode::from)
        .collect::<Vec<_>>();

    for node in &fake_nodes {
        node.enable_handshake();
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
        if bootstrapper_peer_count < NUM_NON_BOOTSTRAPPERS {
            error!(
                "the bootstrapper isn't connected to all the fake nodes ({}/{})!",
                bootstrapper_peer_count, DESIRED_CONNECTION_COUNT
            );
        }
        /*
                if fake_nodes
                    .iter()
                    .skip(1)
                    .any(|fake| fake.node().num_connected() < DESIRED_CONNECTION_COUNT as usize)
                {
                    warn!("not all the peers have the desired number of peers!");
                }
        */
        sleep(Duration::from_secs(5)).await;
    }
}

#[tokio::test]
async fn stress_test_snarkos_bootstrapper() {
    start_logger();

    const NUM_FAKE_NODES: usize = 50;

    let config = Config {
        max_connections: DESIRED_CONNECTION_COUNT as u16 + 5,
        ..Default::default()
    };
    let fake_nodes = start_nodes(NUM_FAKE_NODES, Some(config)).await;
    let fake_nodes = fake_nodes
        .into_iter()
        .map(FakeNode::from)
        .collect::<Vec<_>>();

    for node in &fake_nodes {
        node.enable_handshake();
        node.enable_reading();
        node.enable_writing();
    }

    for node in &fake_nodes {
        let _ = node.node().connect("0.0.0.0:4144".parse().unwrap()).await;
    }

    for node in &fake_nodes {
        node.run_periodic_maintenance();
        sleep(Duration::from_millis(50)).await;
    }

    loop {
        if fake_nodes
            .iter()
            .any(|node| node.node().num_connected() < DESIRED_CONNECTION_COUNT as usize)
        {
            warn!(
                "a fake node doesn't have the desired number of peers ({})!",
                DESIRED_CONNECTION_COUNT
            );
        }

        sleep(Duration::from_secs(5)).await;
    }
}

#[tokio::test]
async fn single_plain_node() {
    start_logger();

    let fake_node = FakeNode::from(Node::new(None).await.unwrap());
    fake_node.enable_handshake();
    fake_node.enable_reading();
    fake_node.enable_writing();
    fake_node.run_periodic_maintenance();

    fake_node
        .node()
        .connect("0.0.0.0:4141".parse().unwrap())
        .await
        .unwrap();

    future::pending::<()>().await;
}

#[tokio::test]
async fn single_bootstrapper_node() {
    start_logger();

    let config = Config {
        name: Some("bootstrapper".into()),
        desired_listening_port: Some(4141),
        ..Default::default()
    };
    let fake_node = FakeNode::from(Node::new(Some(config)).await.unwrap());
    fake_node.enable_handshake();
    fake_node.enable_reading();
    fake_node.enable_writing();
    fake_node.run_periodic_maintenance();

    future::pending::<()>().await;
}
