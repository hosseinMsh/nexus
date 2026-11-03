# ADR-006: Database and Persistence (SQLite & SQLx)

- **Status:** Approved
- **Date:** 2026-11-03
- **Deciders:** NEXUS Architecture Team

---

## 1. Context
NEXUS must persist download tasks, segment states, network paths, proxy profiles, and historical statistics across application restarts and unexpected system crashes.

## 2. Problem
Which storage engine and access pattern ensures transactional consistency, zero data corruption on sudden power loss, and asynchronous query handling?

## 3. Considered Alternatives
1. **Flat JSON/YAML Files:** Simple, human-readable, but lacks atomic transactions; risk of corrupted state files on power loss; slow search and complex concurrency.
2. **Embedded Key-Value Store (Sled / RocksDB):** High write speed, but lacks relational integrity checks and migration tooling.
3. **SQLite with SQLx:** Battle-tested embedded ACID database engine with write-ahead logging (WAL), rich relational queries, compile-time SQL verification, and async connection pooling.

## 4. Decision
We select **SQLite managed via SQLx** (in `nexus-storage`).
Key persistence rules:
- WAL mode enabled for concurrent reader/writer operations.
- Explicit schema migrations stored under `crates/nexus-storage/migrations/`.
- Segment progress checkpoints batched to avoid overwhelming disk I/O.
- Separation of metadata (stored in SQLite) from file payloads (stored in `.part` files).

## 5. Consequences
- **Positive:** Unmatched reliability and durability. Crash recovery is deterministic. Complex queries (e.g. filtering download queues by status or category) are fast.
- **Negative:** Requires C compiler toolchain for compiling `sqlite3` or using bundled C sources.

## 6. Risks & Mitigation
- **Risk:** High-frequency download progress updates could saturate SQLite transaction write logs.
- **Mitigation:** Memory-buffered progress tracking with periodic checkpointing (e.g. every 500ms or 1MB written) and flush on task state changes.

## 7. Validation Strategy
Power interruption simulation and crash recovery tests in `tests/recovery/`.
