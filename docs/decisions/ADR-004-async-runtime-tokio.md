# ADR-004: Async Runtime (Tokio)

- **Status:** Approved
- **Date:** 2026-11-03
- **Deciders:** NEXUS Architecture Team

---

## 1. Context
NEXUS manages hundreds of concurrent network connections, multi-path socket polling, timer events, disk streaming, and local IPC. An asynchronous I/O runtime is mandatory.

## 2. Problem
Which asynchronous runtime best meets the concurrency, reliability, and ecosystem compatibility requirements of NEXUS?

## 3. Considered Alternatives
1. **Tokio:** Industry-standard asynchronous runtime for Rust with multi-threaded work-stealing schedulers, mature timer systems, and rich crate ecosystem support.
2. **async-std:** Alternative async runtime mirroring the Rust standard library API.
3. **smol:** Minimalist async runtime.

## 4. Decision
We standardize on **Tokio** (version 1.x) across the entire workspace.

## 5. Consequences
- **Positive:** First-class compatibility with Hyper, Reqwest, SQLx, and socket bindings. Proven reliability under massive I/O loads. Comprehensive debugging tooling (e.g. `console-subscriber`).
- **Negative:** Slightly larger binary footprint than minimal runtimes.

## 6. Risks & Mitigation
- **Risk:** Blocking the async runtime worker threads with disk I/O or hashing calculations.
- **Mitigation:** Heavy hashing or CPU-intensive checks are explicitly offloaded to Tokio's blocking threadpool (`tokio::task::spawn_blocking`).

## 7. Validation Strategy
Verified by concurrency load tests simulating 100+ concurrent network streams without runtime stalls.
