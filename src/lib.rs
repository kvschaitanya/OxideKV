pub mod raft_pb {
    tonic::include_proto!("raft");
}

mod raft;

pub use raft::{NodeConfig, RaftHandle, start};

pub struct OxideNode {
    raft: RaftHandle,
}

impl OxideNode {
    pub fn new(raft: RaftHandle) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl raft_pb::raft_service_server::RaftService for OxideNode {
    async fn request_vote(
        &self,
        request: tonic::Request<raft_pb::VoteArgs>,
    ) -> Result<tonic::Response<raft_pb::VoteReply>, tonic::Status> {
        Ok(tonic::Response::new(
            self.raft.request_vote(request.into_inner()).await?,
        ))
    }

    async fn append_entries(
        &self,
        request: tonic::Request<raft_pb::AppendArgs>,
    ) -> Result<tonic::Response<raft_pb::AppendReply>, tonic::Status> {
        Ok(tonic::Response::new(
            self.raft.append_entries(request.into_inner()).await?,
        ))
    }

    async fn client_write(
        &self,
        request: tonic::Request<raft_pb::PutRequest>,
    ) -> Result<tonic::Response<raft_pb::PutReply>, tonic::Status> {
        Ok(tonic::Response::new(
            self.raft.client_write(request.into_inner()).await?,
        ))
    }
}
