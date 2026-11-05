use crate::raft_pb::raft_service_client::RaftServiceClient;
use crate::raft_pb::{
    AppendArgs, AppendReply, LogEntry, PutReply, PutRequest, VoteArgs, VoteReply,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
const ELECTION_TIMEOUT: Duration = Duration::from_millis(350);

#[derive(Clone)]
pub struct NodeConfig {
    pub address: String,
    pub peers: Vec<String>,
    pub data_dir: PathBuf,
}

#[derive(Clone)]
pub struct RaftHandle {
    event_tx: mpsc::Sender<Event>,
}

impl RaftHandle {
    pub async fn request_vote(&self, args: VoteArgs) -> Result<VoteReply, Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.event_tx
            .send(Event::RequestVote(args, reply_tx))
            .await
            .map_err(|_| Status::unavailable("Raft runtime stopped"))?;
        reply_rx
            .await
            .map_err(|_| Status::unavailable("Raft runtime stopped"))
    }

    pub async fn append_entries(&self, args: AppendArgs) -> Result<AppendReply, Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.event_tx
            .send(Event::AppendEntries(args, reply_tx))
            .await
            .map_err(|_| Status::unavailable("Raft runtime stopped"))?;
        reply_rx
            .await
            .map_err(|_| Status::unavailable("Raft runtime stopped"))
    }

    pub async fn client_write(&self, args: PutRequest) -> Result<PutReply, Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.event_tx
            .send(Event::ClientWrite(args, reply_tx))
            .await
            .map_err(|_| Status::unavailable("Raft runtime stopped"))?;
        reply_rx
            .await
            .map_err(|_| Status::unavailable("Raft runtime stopped"))
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Default)]
struct RaftState {
    id: u32,
    current_term: u64,
    voted_for: Option<u32>,
    role: Role,
    leader_id: Option<u32>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    votes_received: HashSet<u32>,
    next_index: Vec<u64>,
    match_index: Vec<u64>,
    store: HashMap<String, String>,
    pending_writes: HashMap<u64, oneshot::Sender<PutReply>>,
    pending_client_requests: Vec<(PutRequest, oneshot::Sender<PutReply>)>,
}

#[derive(Serialize, Deserialize, Default)]
struct DiskState {
    current_term: u64,
    voted_for: Option<u32>,
    log: Vec<DiskEntry>,
    commit_index: u64,
    last_applied: u64,
    store: HashMap<String, String>,
}

#[derive(Serialize, Deserialize)]
struct DiskEntry {
    term: u64,
    index: u64,
    key: String,
    value: String,
}

impl From<&LogEntry> for DiskEntry {
    fn from(entry: &LogEntry) -> Self {
        Self {
            term: entry.term,
            index: entry.index,
            key: entry.key.clone(),
            value: entry.value.clone(),
        }
    }
}

enum Event {
    RequestVote(VoteArgs, oneshot::Sender<VoteReply>),
    AppendEntries(AppendArgs, oneshot::Sender<AppendReply>),
    ClientWrite(PutRequest, oneshot::Sender<PutReply>),
    VoteResponse {
        peer_id: u32,
        sent_term: u64,
        reply: Result<VoteReply, String>,
    },
    AppendResponse {
        peer_id: u32,
        sent_term: u64,
        sent_prev_index: u64,
        sent_last_index: u64,
        reply: Result<AppendReply, String>,
    },
}

struct Node {
    config: NodeConfig,
    state: RaftState,
    clients: Vec<RaftServiceClient<Channel>>,
    event_tx: mpsc::Sender<Event>,
    election_deadline: Instant,
    heartbeat_deadline: Instant,
}

pub async fn start(config: NodeConfig) -> Result<RaftHandle> {
    let id = config
        .peers
        .iter()
        .position(|peer| peer == &config.address)
        .context("local address must appear in OXIDEKV_PEERS")? as u32;
    let clients = config
        .peers
        .iter()
        .map(|peer| {
            let endpoint = Endpoint::from_shared(format!("http://{peer}"))?;
            Ok(RaftServiceClient::new(endpoint.connect_lazy()))
        })
        .collect::<Result<Vec<_>>>()?;
    let (event_tx, event_rx) = mpsc::channel(256);
    let mut node = Node::load(config, id, clients, event_tx.clone()).await?;
    tokio::spawn(async move { node.run(event_rx).await });
    Ok(RaftHandle { event_tx })
}

impl Node {
    async fn load(
        config: NodeConfig,
        id: u32,
        clients: Vec<RaftServiceClient<Channel>>,
        event_tx: mpsc::Sender<Event>,
    ) -> Result<Self> {
        tokio::fs::create_dir_all(&config.data_dir).await?;
        let disk = read_disk_state(&config.data_dir.join("raft_state.json")).await?;
        let log = disk
            .log
            .into_iter()
            .map(|entry| LogEntry {
                term: entry.term,
                index: entry.index,
                key: entry.key,
                value: entry.value,
            })
            .collect::<Vec<_>>();
        let mut state = RaftState {
            id,
            current_term: disk.current_term,
            voted_for: disk.voted_for,
            role: Role::Follower,
            leader_id: None,
            log,
            commit_index: disk.commit_index,
            last_applied: disk.last_applied,
            votes_received: HashSet::new(),
            next_index: vec![1; clients.len()],
            match_index: vec![0; clients.len()],
            store: disk.store,
            pending_writes: HashMap::new(),
            pending_client_requests: Vec::new(),
        };
        if state.last_applied > state.log.len() as u64 {
            anyhow::bail!("persisted applied index exceeds log length");
        }
        state.commit_index = state.commit_index.min(state.log.len() as u64);
        Ok(Self {
            config,
            state,
            clients,
            event_tx,
            election_deadline: Instant::now() + election_timeout(),
            heartbeat_deadline: Instant::now() + HEARTBEAT_INTERVAL,
        })
    }

    async fn run(&mut self, mut event_rx: mpsc::Receiver<Event>) {
        let mut tick = time::interval(Duration::from_millis(20));
        loop {
            tokio::select! {
                event = event_rx.recv() => match event {
                    Some(event) => self.handle_event(event).await,
                    None => break,
                },
                _ = tick.tick() => self.on_tick().await,
            }
        }
    }

    async fn handle_event(&mut self, event: Event) {
        match event {
            Event::RequestVote(args, tx) => {
                let _ = tx.send(self.handle_vote_request(args).await);
            }
            Event::AppendEntries(args, tx) => {
                let _ = tx.send(self.handle_append_entries(args).await);
            }
            Event::ClientWrite(args, tx) => self.handle_client_write(args, tx).await,
            Event::VoteResponse {
                peer_id,
                sent_term,
                reply,
            } => self.handle_vote_response(peer_id, sent_term, reply).await,
            Event::AppendResponse {
                peer_id,
                sent_term,
                sent_prev_index,
                sent_last_index,
                reply,
            } => {
                self.handle_append_response(
                    peer_id,
                    sent_term,
                    sent_prev_index,
                    sent_last_index,
                    reply,
                )
                .await
            }
        }
    }

    async fn handle_vote_request(&mut self, args: VoteArgs) -> VoteReply {
        if args.term > self.state.current_term {
            self.step_down(args.term, None).await;
        }
        let (last_index, last_term) = last_log_position(&self.state.log);
        let up_to_date = (args.last_log_term, args.last_log_index) >= (last_term, last_index);
        let granted = args.term == self.state.current_term
            && up_to_date
            && (self.state.voted_for.is_none() || self.state.voted_for == Some(args.candidate_id));
        if granted {
            self.state.role = Role::Follower;
            self.state.voted_for = Some(args.candidate_id);
            self.state.leader_id = None;
            self.reset_election_deadline();
            self.persist().await;
        }
        VoteReply {
            term: self.state.current_term,
            vote_granted: granted,
        }
    }

    async fn handle_append_entries(&mut self, args: AppendArgs) -> AppendReply {
        if args.term < self.state.current_term {
            return AppendReply {
                term: self.state.current_term,
                success: false,
            };
        }
        if args.term > self.state.current_term || self.state.role != Role::Follower {
            self.step_down(args.term, Some(args.leader_id)).await;
        } else {
            self.state.leader_id = Some(args.leader_id);
            self.forward_buffered_writes(args.leader_id);
        }
        self.reset_election_deadline();
        let prefix_matches = if args.prev_log_index == 0 {
            args.prev_log_term == 0
        } else {
            usize::try_from(args.prev_log_index - 1)
                .ok()
                .and_then(|i| self.state.log.get(i))
                .is_some_and(|entry| {
                    entry.index == args.prev_log_index && entry.term == args.prev_log_term
                })
        };
        let indexes_are_sequential = args
            .entries
            .iter()
            .try_fold(args.prev_log_index, |index, entry| {
                index
                    .checked_add(1)
                    .and_then(|next| (entry.index == next).then_some(next))
            })
            .is_some();
        if !prefix_matches || !indexes_are_sequential {
            self.persist().await;
            return AppendReply {
                term: self.state.current_term,
                success: false,
            };
        }
        for (offset, entry) in args.entries.iter().enumerate() {
            let index = entry.index as usize - 1;
            if index < self.state.log.len() {
                if self.state.log[index].term != entry.term {
                    if entry.index <= self.state.commit_index {
                        self.persist().await;
                        return AppendReply {
                            term: self.state.current_term,
                            success: false,
                        };
                    }
                    self.state.log.truncate(index);
                    self.state
                        .log
                        .extend(args.entries[offset..].iter().cloned());
                    self.fail_truncated_writes(entry.index);
                    break;
                }
            } else {
                self.state.log.push(entry.clone());
            }
        }
        self.state.commit_index = self
            .state
            .commit_index
            .max(args.leader_commit.min(self.state.log.len() as u64));
        self.apply_committed();
        self.persist().await;
        AppendReply {
            term: self.state.current_term,
            success: true,
        }
    }

    async fn handle_client_write(&mut self, args: PutRequest, tx: oneshot::Sender<PutReply>) {
        if self.state.role == Role::Leader {
            self.append_client_write(args, tx);
            self.persist().await;
            self.advance_commit().await;
            self.replicate_to_all();
        } else if let Some(leader_id) = self.state.leader_id {
            self.forward_write(leader_id, args, tx);
        } else {
            self.state.pending_client_requests.push((args, tx));
        }
    }

    fn append_client_write(&mut self, args: PutRequest, tx: oneshot::Sender<PutReply>) {
        let index = self.state.log.len() as u64 + 1;
        self.state.log.push(LogEntry {
            term: self.state.current_term,
            index,
            key: args.key,
            value: args.value,
        });
        self.state.pending_writes.insert(index, tx);
        self.state.match_index[self.state.id as usize] = index;
    }

    async fn handle_vote_response(
        &mut self,
        peer_id: u32,
        sent_term: u64,
        reply: Result<VoteReply, String>,
    ) {
        let Ok(reply) = reply else { return };
        if reply.term > self.state.current_term {
            self.step_down(reply.term, None).await;
            return;
        }
        if self.state.role != Role::Candidate
            || self.state.current_term != sent_term
            || reply.term != sent_term
            || !reply.vote_granted
        {
            return;
        }
        self.state.votes_received.insert(peer_id);
        if self.state.votes_received.len() >= majority(self.clients.len()) {
            self.become_leader().await;
        }
    }

    async fn handle_append_response(
        &mut self,
        peer_id: u32,
        sent_term: u64,
        sent_prev_index: u64,
        sent_last_index: u64,
        reply: Result<AppendReply, String>,
    ) {
        let Ok(reply) = reply else { return };
        if reply.term > self.state.current_term {
            self.step_down(reply.term, None).await;
            return;
        }
        if self.state.role != Role::Leader
            || sent_term != self.state.current_term
            || reply.term != self.state.current_term
        {
            return;
        }
        let peer = peer_id as usize;
        if reply.success {
            self.state.match_index[peer] = self.state.match_index[peer].max(sent_last_index);
            self.state.next_index[peer] = self.state.match_index[peer] + 1;
            self.advance_commit().await;
        } else if self.state.next_index[peer] == sent_prev_index + 1 {
            self.state.next_index[peer] = self.state.next_index[peer].saturating_sub(1).max(1);
            self.send_append(peer_id);
        }
    }

    async fn on_tick(&mut self) {
        let now = Instant::now();
        if self.state.role == Role::Leader {
            if now >= self.heartbeat_deadline {
                self.heartbeat_deadline = now + HEARTBEAT_INTERVAL;
                self.replicate_to_all();
            }
        } else if now >= self.election_deadline {
            self.start_election().await;
        }
    }

    async fn start_election(&mut self) {
        self.state.current_term += 1;
        self.state.role = Role::Candidate;
        self.state.leader_id = None;
        self.state.voted_for = Some(self.state.id);
        self.state.votes_received.clear();
        self.state.votes_received.insert(self.state.id);
        self.reset_election_deadline();
        self.persist().await;
        if self.state.votes_received.len() >= majority(self.clients.len()) {
            self.become_leader().await;
            return;
        }
        let (last_log_index, last_log_term) = last_log_position(&self.state.log);
        let args = VoteArgs {
            term: self.state.current_term,
            candidate_id: self.state.id,
            last_log_index,
            last_log_term,
        };
        for peer in 0..self.clients.len() {
            if peer != self.state.id as usize {
                self.send_vote(peer as u32, args.clone());
            }
        }
    }

    async fn become_leader(&mut self) {
        self.state.role = Role::Leader;
        self.state.leader_id = Some(self.state.id);
        let next = self.state.log.len() as u64 + 1;
        self.state.next_index.fill(next);
        self.state.match_index.fill(0);
        self.state.match_index[self.state.id as usize] = next - 1;
        self.heartbeat_deadline = Instant::now();
        let pending = std::mem::take(&mut self.state.pending_client_requests);
        for (request, reply_tx) in pending {
            self.append_client_write(request, reply_tx);
        }
        self.persist().await;
        self.advance_commit().await;
        self.replicate_to_all();
    }

    async fn step_down(&mut self, term: u64, leader_id: Option<u32>) {
        if term > self.state.current_term {
            self.state.current_term = term;
            self.state.voted_for = None;
        }
        self.state.role = Role::Follower;
        self.state.leader_id = leader_id;
        self.state.votes_received.clear();
        for (_, pending) in self.state.pending_writes.drain() {
            let _ = pending.send(PutReply {
                success: false,
                leader_id: leader_id.unwrap_or_default(),
            });
        }
        self.reset_election_deadline();
        self.persist().await;
        if let Some(leader_id) = leader_id {
            self.forward_buffered_writes(leader_id);
        }
    }

    fn reset_election_deadline(&mut self) {
        self.election_deadline = Instant::now() + election_timeout();
    }

    fn replicate_to_all(&self) {
        for peer in 0..self.clients.len() {
            if peer != self.state.id as usize {
                self.send_append(peer as u32);
            }
        }
    }

    fn forward_buffered_writes(&mut self, leader_id: u32) {
        if leader_id == self.state.id {
            return;
        }
        let pending = std::mem::take(&mut self.state.pending_client_requests);
        for (request, reply_tx) in pending {
            self.forward_write(leader_id, request, reply_tx);
        }
    }

    fn forward_write(
        &self,
        leader_id: u32,
        request: PutRequest,
        reply_tx: oneshot::Sender<PutReply>,
    ) {
        let Some(mut client) = self.clients.get(leader_id as usize).cloned() else {
            let _ = reply_tx.send(PutReply {
                success: false,
                leader_id,
            });
            return;
        };
        tokio::spawn(async move {
            let result = time::timeout(
                Duration::from_secs(5),
                client.client_write(Request::new(request)),
            )
            .await;
            let response = match result {
                Ok(Ok(response)) => response.into_inner(),
                _ => PutReply {
                    success: false,
                    leader_id,
                },
            };
            let _ = reply_tx.send(response);
        });
    }

    fn send_vote(&self, peer_id: u32, args: VoteArgs) {
        let mut client = self.clients[peer_id as usize].clone();
        let event_tx = self.event_tx.clone();
        let sent_term = args.term;
        tokio::spawn(async move {
            let reply = client
                .request_vote(Request::new(args))
                .await
                .map(|r| r.into_inner())
                .map_err(|e| e.to_string());
            let _ = event_tx
                .send(Event::VoteResponse {
                    peer_id,
                    sent_term,
                    reply,
                })
                .await;
        });
    }

    fn send_append(&self, peer_id: u32) {
        let peer = peer_id as usize;
        let next = self.state.next_index[peer].clamp(1, self.state.log.len() as u64 + 1);
        let prev_log_index = next - 1;
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            self.state.log[prev_log_index as usize - 1].term
        };
        let entries = self.state.log[next as usize - 1..].to_vec();
        let sent_last_index = entries.last().map_or(prev_log_index, |entry| entry.index);
        let args = AppendArgs {
            term: self.state.current_term,
            leader_id: self.state.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.state.commit_index,
        };
        let mut client = self.clients[peer].clone();
        let event_tx = self.event_tx.clone();
        let sent_term = self.state.current_term;
        tokio::spawn(async move {
            let reply = client
                .append_entries(Request::new(args))
                .await
                .map(|r| r.into_inner())
                .map_err(|e| e.to_string());
            let _ = event_tx
                .send(Event::AppendResponse {
                    peer_id,
                    sent_term,
                    sent_prev_index: prev_log_index,
                    sent_last_index,
                    reply,
                })
                .await;
        });
    }

    async fn advance_commit(&mut self) {
        let mut commit_to = self.state.commit_index;
        for index in (self.state.commit_index + 1)..=self.state.log.len() as u64 {
            if self.state.log[index as usize - 1].term != self.state.current_term {
                continue;
            }
            let replicated = self
                .state
                .match_index
                .iter()
                .filter(|matched| **matched >= index)
                .count();
            if replicated >= majority(self.clients.len()) {
                commit_to = index;
            }
        }
        if commit_to > self.state.commit_index {
            self.state.commit_index = commit_to;
            self.apply_committed();
            self.persist().await;
            self.replicate_to_all();
        }
    }

    fn apply_committed(&mut self) {
        while self.state.last_applied < self.state.commit_index {
            self.state.last_applied += 1;
            let entry = &self.state.log[self.state.last_applied as usize - 1];
            self.state
                .store
                .insert(entry.key.clone(), entry.value.clone());
            if let Some(tx) = self.state.pending_writes.remove(&entry.index) {
                let _ = tx.send(PutReply {
                    success: true,
                    leader_id: self.state.id,
                });
            }
        }
    }

    fn fail_truncated_writes(&mut self, from_index: u64) {
        let indexes = self
            .state
            .pending_writes
            .keys()
            .copied()
            .filter(|index| *index >= from_index)
            .collect::<Vec<_>>();
        for index in indexes {
            if let Some(tx) = self.state.pending_writes.remove(&index) {
                let _ = tx.send(PutReply {
                    success: false,
                    leader_id: self.state.leader_id.unwrap_or_default(),
                });
            }
        }
    }

    async fn persist(&self) {
        let disk = DiskState {
            current_term: self.state.current_term,
            voted_for: self.state.voted_for,
            log: self.state.log.iter().map(DiskEntry::from).collect(),
            commit_index: self.state.commit_index,
            last_applied: self.state.last_applied,
            store: self.state.store.clone(),
        };
        let path = self.config.data_dir.join("raft_state.json");
        write_disk_state(&path, &disk)
            .await
            .expect("failed to persist Raft state");
    }
}

fn majority(server_count: usize) -> usize {
    server_count / 2 + 1
}

fn last_log_position(log: &[LogEntry]) -> (u64, u64) {
    log.last().map_or((0, 0), |entry| (entry.index, entry.term))
}

fn election_timeout() -> Duration {
    let jitter = rand::random::<u64>() % ELECTION_TIMEOUT.as_millis() as u64;
    ELECTION_TIMEOUT + Duration::from_millis(jitter)
}

async fn read_disk_state(path: &Path) -> Result<DiskState> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(DiskState::default()),
        Err(error) => Err(error.into()),
    }
}

async fn write_disk_state(path: &Path, state: &DiskState) -> Result<()> {
    let temporary_path = path.with_extension("json.tmp");
    let mut file = tokio::fs::File::create(&temporary_path).await?;
    file.write_all(&serde_json::to_vec(state)?).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(temporary_path, path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OxideNode;
    use crate::raft_pb::raft_service_server::RaftServiceServer;
    use tonic::transport::Server;

    fn log_entry(index: u64, term: u64, key: &str, value: &str) -> LogEntry {
        LogEntry {
            term,
            index,
            key: key.into(),
            value: value.into(),
        }
    }

    async fn test_node() -> (Node, PathBuf) {
        let directory = std::env::temp_dir().join(format!(
            "oxidekv-append-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let address = "127.0.0.1:50051".to_string();
        let config = NodeConfig {
            address: address.clone(),
            peers: vec![address],
            data_dir: directory.clone(),
        };
        let client =
            RaftServiceClient::new(Endpoint::from_static("http://127.0.0.1:50051").connect_lazy());
        let (event_tx, _event_rx) = mpsc::channel(8);
        let node = Node::load(config, 0, vec![client], event_tx).await.unwrap();
        (node, directory)
    }

    #[test]
    fn election_timeouts_are_randomized_and_quorums_are_majorities() {
        assert!((ELECTION_TIMEOUT..ELECTION_TIMEOUT * 2).contains(&election_timeout()));
        assert_eq!(majority(1), 1);
        assert_eq!(majority(3), 2);
        assert_eq!(majority(4), 3);
    }

    #[tokio::test]
    async fn append_entries_rejects_stale_terms_and_mismatched_prefixes() {
        let (mut node, directory) = test_node().await;
        node.state.current_term = 3;
        node.state.log.push(log_entry(1, 2, "a", "old"));

        let stale = node
            .handle_append_entries(AppendArgs {
                term: 2,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            })
            .await;
        assert!(!stale.success);
        assert_eq!(stale.term, 3);

        let mismatch = node
            .handle_append_entries(AppendArgs {
                term: 3,
                leader_id: 1,
                prev_log_index: 1,
                prev_log_term: 9,
                entries: Vec::new(),
                leader_commit: 0,
            })
            .await;
        assert!(!mismatch.success);
        assert_eq!(node.state.log.len(), 1);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn append_entries_replaces_conflicts_and_caps_commit_index() {
        let (mut node, directory) = test_node().await;
        node.state.current_term = 1;
        node.state.log = vec![log_entry(1, 1, "a", "first"), log_entry(2, 1, "b", "old")];

        let reply = node
            .handle_append_entries(AppendArgs {
                term: 2,
                leader_id: 0,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![log_entry(2, 2, "b", "new")],
                leader_commit: 9,
            })
            .await;

        assert!(reply.success);
        assert_eq!(node.state.commit_index, 2);
        assert_eq!(node.state.store.get("b").map(String::as_str), Some("new"));
        assert_eq!(node.state.log[1].term, 2);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn disk_state_round_trips_term_vote_log_and_store() {
        let dir = std::env::temp_dir().join(format!("oxidekv-test-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("raft_state.json");
        let expected = DiskState {
            current_term: 4,
            voted_for: Some(2),
            log: vec![DiskEntry {
                term: 4,
                index: 1,
                key: "k".into(),
                value: "v".into(),
            }],
            commit_index: 1,
            last_applied: 1,
            store: HashMap::from([("k".into(), "v".into())]),
        };
        write_disk_state(&path, &expected).await.unwrap();
        let loaded = read_disk_state(&path).await.unwrap();
        assert_eq!(loaded.current_term, expected.current_term);
        assert_eq!(loaded.voted_for, expected.voted_for);
        assert_eq!(loaded.log.len(), 1);
        assert_eq!(loaded.store.get("k").map(String::as_str), Some("v"));
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }

    #[tokio::test]
    async fn three_nodes_elect_a_leader_and_commit_a_write() {
        let addresses = (0..3)
            .map(|_| {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                listener.local_addr().unwrap().to_string()
            })
            .collect::<Vec<_>>();
        let peers = addresses.clone();
        let root = std::env::temp_dir().join(format!("oxidekv-cluster-{}", std::process::id()));
        let mut handles = Vec::new();
        let mut servers = Vec::new();

        for (index, address) in addresses.iter().enumerate() {
            let handle = start(NodeConfig {
                address: address.clone(),
                peers: peers.clone(),
                data_dir: root.join(format!("node-{index}")),
            })
            .await
            .unwrap();
            let server_address = address.parse().unwrap();
            let service = OxideNode::new(handle.clone());
            servers.push(tokio::spawn(async move {
                Server::builder()
                    .add_service(RaftServiceServer::new(service))
                    .serve(server_address)
                    .await
                    .unwrap();
            }));
            handles.push(handle);
        }

        let reply = time::timeout(
            Duration::from_secs(8),
            handles[0].client_write(PutRequest {
                key: "cluster-test".into(),
                value: "committed".into(),
            }),
        )
        .await
        .expect("client write timed out")
        .expect("Raft runtime stopped");
        assert!(reply.success, "buffered client write was not committed");
        let convergence_deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let mut converged = true;
            for index in 0..addresses.len() {
                let path = root.join(format!("node-{index}")).join("raft_state.json");
                let state = read_disk_state(&path).await.unwrap();
                converged &= state.commit_index == 1
                    && state.store.get("cluster-test").map(String::as_str) == Some("committed");
            }
            if converged || Instant::now() >= convergence_deadline {
                break;
            }
            time::sleep(Duration::from_millis(25)).await;
        }
        for index in 0..addresses.len() {
            let path = root.join(format!("node-{index}")).join("raft_state.json");
            let state = read_disk_state(&path).await.unwrap();
            assert_eq!(
                state.store.get("cluster-test").map(String::as_str),
                Some("committed")
            );
            assert_eq!(state.commit_index, 1);
        }

        for server in servers {
            server.abort();
        }
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
