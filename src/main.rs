pub mod raft_pb {
    tonic::include_proto!("raft");
}

use raft_pb::raft_service_server::{RaftService, RaftServiceServer};
use raft_pb::{AppendArgs, AppendReply, PutReply, PutRequest, VoteArgs, VoteReply};

use tonic::{Request, Response, Status, transport::Server};

#[derive(Default)]
pub struct OxideNode {}

#[tonic::async_trait]
impl RaftService for OxideNode {
    async fn request_vote(
        &self,
        request: Request<VoteArgs>,
    ) -> Result<Response<VoteReply>, Status> {
        let args = request.into_inner();
        println!("Received RequestVote from Node {}", args.candidate_id);

        // Dummy response: always grant the vote for now
        let reply = VoteReply {
            term: args.term,
            vote_granted: true,
        };
        Ok(Response::new(reply))
    }

    async fn append_entries(
        &self,
        request: Request<AppendArgs>,
    ) -> Result<Response<AppendReply>, Status> {
        let args = request.into_inner();
        println!("Received AppendEntries from Leader {}", args.leader_id);

        // Dummy response: always accept the logs
        let reply = AppendReply {
            term: args.term,
            success: true,
        };
        Ok(Response::new(reply))
    }

    async fn client_write(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<PutReply>, Status> {
        let args = request.into_inner();
        println!("Received ClientWrite: {} -> {}", args.key, args.value);

        let reply = PutReply {
            success: true,
            leader_id: 1, // refactor this to the semantics
        };
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    let node = OxideNode::default();

    println!("OxideNode is listening on {}", addr);

    Server::builder()
        .add_service(RaftServiceServer::new(node))
        .serve(addr)
        .await?;

    Ok(())
}
