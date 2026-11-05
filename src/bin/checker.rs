use stateright::actor::{Actor, ActorModel, Id, Network, Out, model_timeout};
use stateright::report::WriteReporter;
use stateright::{Checker, Expectation, Model};
use std::borrow::Cow;
use std::collections::HashSet;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct LogItem {
    term: u64,
    value: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct State {
    term: u64,
    role: Role,
    voted_for: Option<usize>,
    votes: Vec<usize>,
    log: Vec<LogItem>,
    commit_index: usize,
    applied: Vec<u64>,
    next_index: Vec<usize>,
    match_index: Vec<usize>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Message {
    VoteRequest {
        term: u64,
        candidate: usize,
        last_index: usize,
        last_term: u64,
    },
    VoteResponse {
        term: u64,
        granted: bool,
    },
    Append {
        term: u64,
        leader: usize,
        prev_index: usize,
        prev_term: u64,
        entries: Vec<LogItem>,
        leader_commit: usize,
    },
    AppendResponse {
        term: u64,
        success: bool,
        matched: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Timer {
    Election,
    Replication,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RaftActor {
    server_count: usize,
}

fn majority(server_count: usize) -> usize {
    server_count / 2 + 1
}

fn start_election(state: &mut State, id: Id, server_count: usize, out: &mut Out<RaftActor>) {
    if state.role == Role::Leader {
        return;
    }
    let self_id: usize = id.into();
    state.term += 1;
    state.role = Role::Candidate;
    state.voted_for = Some(self_id);
    state.votes.clear();
    state.votes.push(self_id);
    out.set_timer(Timer::Election, model_timeout());
    if state.votes.len() >= majority(server_count) {
        become_leader(state, id, server_count, out);
        return;
    }
    let (last_index, last_term) = state
        .log
        .last()
        .map_or((0, 0), |entry| (state.log.len(), entry.term));
    for peer in 0..server_count {
        if peer != self_id {
            out.send(
                Id::from(peer),
                Message::VoteRequest {
                    term: state.term,
                    candidate: self_id,
                    last_index,
                    last_term,
                },
            );
        }
    }
}

fn become_leader(state: &mut State, id: Id, server_count: usize, out: &mut Out<RaftActor>) {
    let self_id: usize = id.into();
    state.role = Role::Leader;
    state.next_index.fill(state.log.len() + 1);
    state.match_index.fill(0);
    state.match_index[self_id] = state.log.len();
    state.log.push(LogItem {
        term: state.term,
        value: self_id as u64,
    });
    state.match_index[self_id] = state.log.len();
    state.next_index.fill(state.log.len());
    replicate(state, self_id, server_count, out);
    out.set_timer(Timer::Replication, model_timeout());
}

fn replicate(state: &State, self_id: usize, server_count: usize, out: &mut Out<RaftActor>) {
    for peer in 0..server_count {
        if peer == self_id {
            continue;
        }
        let next = state.next_index[peer].clamp(1, state.log.len() + 1);
        let prev_index = next - 1;
        let prev_term = if prev_index == 0 {
            0
        } else {
            state.log[prev_index - 1].term
        };
        out.send(
            Id::from(peer),
            Message::Append {
                term: state.term,
                leader: self_id,
                prev_index,
                prev_term,
                entries: state.log[next - 1..].to_vec(),
                leader_commit: state.commit_index,
            },
        );
    }
}

fn apply_committed(state: &mut State) {
    while state.applied.len() < state.commit_index {
        state.applied.push(state.log[state.applied.len()].value);
    }
}

impl Actor for RaftActor {
    type Msg = Message;
    type Timer = Timer;
    type State = State;

    fn on_start(&self, _id: Id, out: &mut Out<Self>) -> Self::State {
        out.set_timer(Timer::Election, model_timeout());
        State {
            term: 0,
            role: Role::Follower,
            voted_for: None,
            votes: Vec::new(),
            log: Vec::new(),
            commit_index: 0,
            applied: Vec::new(),
            next_index: vec![1; self.server_count],
            match_index: vec![0; self.server_count],
        }
    }

    fn on_msg(
        &self,
        id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        out: &mut Out<Self>,
    ) {
        let state = state.to_mut();
        let self_id: usize = id.into();
        let src_id: usize = src.into();
        match msg {
            Message::VoteRequest {
                term,
                candidate,
                last_index,
                last_term,
            } => {
                if term > state.term {
                    state.term = term;
                    state.role = Role::Follower;
                    state.voted_for = None;
                    state.votes.clear();
                }
                let local_index = state.log.len();
                let local_term = state.log.last().map_or(0, |entry| entry.term);
                let up_to_date = (last_term, last_index) >= (local_term, local_index);
                let granted = term == state.term
                    && up_to_date
                    && state.voted_for.is_none_or(|voted| voted == candidate);
                if granted {
                    state.voted_for = Some(candidate);
                    state.role = Role::Follower;
                    out.set_timer(Timer::Election, model_timeout());
                }
                out.send(
                    src,
                    Message::VoteResponse {
                        term: state.term,
                        granted,
                    },
                );
            }
            Message::VoteResponse { term, granted } => {
                if term > state.term {
                    state.term = term;
                    state.role = Role::Follower;
                    state.voted_for = None;
                    state.votes.clear();
                } else if state.role == Role::Candidate && term == state.term && granted {
                    if !state.votes.contains(&src_id) {
                        state.votes.push(src_id);
                    }
                    if state.votes.len() >= majority(self.server_count) {
                        become_leader(state, id, self.server_count, out);
                    }
                }
            }
            Message::Append {
                term,
                leader,
                prev_index,
                prev_term,
                entries,
                leader_commit,
            } => {
                let mut success = false;
                if term >= state.term {
                    if term > state.term {
                        state.term = term;
                        state.voted_for = None;
                        state.votes.clear();
                    }
                    state.role = Role::Follower;
                    out.set_timer(Timer::Election, model_timeout());
                    let prefix_matches = if prev_index == 0 {
                        prev_term == 0
                    } else {
                        state
                            .log
                            .get(prev_index - 1)
                            .is_some_and(|entry| entry.term == prev_term)
                    };
                    if prefix_matches {
                        for (offset, entry) in entries.iter().enumerate() {
                            let index = prev_index + offset;
                            if index < state.log.len() {
                                if state.log[index].term != entry.term {
                                    state.log.truncate(index);
                                    state.log.extend(entries[offset..].iter().cloned());
                                    break;
                                }
                            } else {
                                state.log.push(entry.clone());
                            }
                        }
                        state.commit_index =
                            state.commit_index.max(leader_commit.min(state.log.len()));
                        apply_committed(state);
                        success = true;
                    }
                    let matched = prev_index + entries.len();
                    out.send(
                        src,
                        Message::AppendResponse {
                            term: state.term,
                            success,
                            matched,
                        },
                    );
                    let _ = leader;
                }
            }
            Message::AppendResponse {
                term,
                success,
                matched,
            } => {
                if term > state.term {
                    state.term = term;
                    state.role = Role::Follower;
                    state.voted_for = None;
                } else if state.role == Role::Leader && term == state.term && success {
                    state.match_index[src_id] = state.match_index[src_id].max(matched);
                    state.next_index[src_id] = state.match_index[src_id] + 1;
                    for index in (state.commit_index + 1)..=state.log.len() {
                        if state.log[index - 1].term != state.term {
                            continue;
                        }
                        if state
                            .match_index
                            .iter()
                            .filter(|matched| **matched >= index)
                            .count()
                            >= majority(self.server_count)
                        {
                            state.commit_index = index;
                        }
                    }
                    apply_committed(state);
                    replicate(state, self_id, self.server_count, out);
                } else if state.role == Role::Leader && term == state.term && !success {
                    state.next_index[src_id] = state.next_index[src_id].saturating_sub(1).max(1);
                    replicate(state, self_id, self.server_count, out);
                }
            }
        }
    }

    fn on_timeout(
        &self,
        id: Id,
        state: &mut Cow<Self::State>,
        timer: &Self::Timer,
        out: &mut Out<Self>,
    ) {
        match timer {
            Timer::Election => start_election(state.to_mut(), id, self.server_count, out),
            Timer::Replication => {
                if state.role == Role::Leader {
                    replicate(state, id.into(), self.server_count, out);
                    out.set_timer(Timer::Replication, model_timeout());
                }
            }
        }
    }
}

fn model(server_count: usize, network: Network<Message>) -> ActorModel<RaftActor, usize> {
    ActorModel::new(server_count, ())
        .max_crashes(server_count.saturating_sub(1) / 2)
        .actors((0..server_count).map(|_| RaftActor { server_count }))
        .init_network(network)
        .property(Expectation::Sometimes, "Election Liveness", |_, state| {
            state
                .actor_states
                .iter()
                .any(|node| node.role == Role::Leader)
        })
        .property(Expectation::Sometimes, "Log Liveness", |_, state| {
            state.actor_states.iter().any(|node| node.commit_index > 0)
        })
        .property(Expectation::Always, "Election Safety", |_, state| {
            let mut leaders = HashSet::new();
            state
                .actor_states
                .iter()
                .all(|node| node.role != Role::Leader || leaders.insert(node.term))
        })
        .property(Expectation::Always, "State Machine Safety", |_, state| {
            state.actor_states.iter().all(|left| {
                state.actor_states.iter().all(|right| {
                    let common = left.applied.len().min(right.applied.len());
                    left.applied[..common] == right.applied[..common]
                })
            })
        })
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "check".into());
    let server_count = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3);
    let depth = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);
    let network_kind = args
        .next()
        .unwrap_or_else(|| "unordered-nonduplicating".into());
    let network = match network_kind.as_str() {
        "unordered-duplicating" => Network::new_unordered_duplicating([]),
        "ordered" => Network::new_ordered([]),
        "unordered-nonduplicating" => Network::new_unordered_nonduplicating([]),
        _ => {
            eprintln!(
                "network must be ordered, unordered-duplicating, or unordered-nonduplicating"
            );
            return;
        }
    };
    let model = model(server_count, network);
    match mode.as_str() {
        "check" => {
            model
                .checker()
                .threads(1)
                .target_max_depth(depth)
                .spawn_bfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        "explore" => {
            println!("Exploring Raft model at http://127.0.0.1:3000");
            model.checker().threads(1).serve("127.0.0.1:3000");
        }
        _ => eprintln!("usage: checker [check|explore] [server-count] [depth] [network]"),
    }
}
