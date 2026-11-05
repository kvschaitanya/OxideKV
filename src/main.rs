use oxidekv::raft_pb::raft_service_server::RaftServiceServer;
use oxidekv::{NodeConfig, OxideNode, start};
use std::path::PathBuf;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("OXIDEKV_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".into());
    let peers = std::env::var("OXIDEKV_PEERS")
        .unwrap_or_else(|_| address.clone())
        .split(',')
        .map(str::trim)
        .filter(|peer| !peer.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let local_index = peers
        .iter()
        .position(|peer| peer == &address)
        .ok_or("OXIDEKV_ADDR must be present in OXIDEKV_PEERS")?;
    let data_dir = std::env::var_os("OXIDEKV_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("./data/node-{local_index}")));
    let raft = start(NodeConfig {
        address: address.clone(),
        peers,
        data_dir,
    })
    .await?;
    let addr = address.parse()?;

    println!("OxideKV node {local_index} is listening on {address}");

    Server::builder()
        .add_service(RaftServiceServer::new(OxideNode::new(raft)))
        .serve(addr)
        .await?;

    Ok(())
}
