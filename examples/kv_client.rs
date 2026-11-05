use oxidekv::raft_pb::{PutRequest, raft_service_client::RaftServiceClient};
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("OXIDEKV_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".into());
    let mut client = RaftServiceClient::connect(format!("http://{address}")).await?;
    let result = client
        .client_write(Request::new(PutRequest {
            key: "example".into(),
            value: "committed".into(),
        }))
        .await?
        .into_inner();
    if result.success {
        println!("write committed by leader {}", result.leader_id);
    } else {
        println!("not leader; known leader is {}", result.leader_id);
    }
    Ok(())
}
