# OxideKV

OxideKV is a small Raft-backed key/value service. Nodes communicate over the existing Tonic gRPC service, elect a leader, replicate ordered log entries, and acknowledge writes after a quorum has committed them. Each node persists its term, vote, log, commit index, and applied key/value state under its data directory.

## Run a cluster

Start each node in a separate terminal. Every node must use the same comma-separated peer list and its own address and data directory:

```sh
OXIDEKV_ADDR=127.0.0.1:50051 \
OXIDEKV_PEERS=127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053 \
OXIDEKV_DATA_DIR=./data/node-0 cargo run --bin OxideKV
```

Repeat with addresses `127.0.0.1:50052` and `127.0.0.1:50053`, and distinct data directories. `OXIDEKV_ADDR` must exactly match an entry in `OXIDEKV_PEERS`. With no environment variables, a single-node cluster listens on `127.0.0.1:50051` and stores state under `./data/node-0`.

Use the example client against a node with a known leader:

```sh
OXIDEKV_ADDR=127.0.0.1:50051 cargo run --example kv_client
```

Writes sent to a follower are forwarded to its known leader. If no leader is known yet, the request is buffered until election resolves.

## Model checking

Run the bounded three-node Stateright search:

```sh
cargo run --bin checker -- check 3 10 unordered-nonduplicating
```

Start the interactive state explorer at `http://127.0.0.1:3000`:

```sh
cargo run --bin checker -- explore 3 10 unordered-nonduplicating
```

The checker supports `ordered`, `unordered-duplicating`, and `unordered-nonduplicating` message delivery, and checks election safety and state-machine safety.

## Tests

```sh
cargo test --all-targets
```