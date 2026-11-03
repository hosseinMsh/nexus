# ADR-001: Modular Monolithic Architecture

- **Status:** Approved
- **Date:** 2026-11-03
- **Deciders:** NEXUS Architecture Team

---

## 1. Context
NEXUS is a native desktop download manager and transfer platform requiring high performance, complex orchestration between download logic, operating system network interfaces, proxies, scheduling, persistence, and desktop presentation.

## 2. Problem
Should NEXUS be built as a distributed set of independent microservices (e.g., daemon + UI + helper processes communicating over IPC) or as an integrated modular monolith within a single codebase?

## 3. Considered Alternatives
1. **Multi-process Microservices:** Separate daemon process for networking, separate storage service, separate UI binary communicating across local gRPC or WebSockets.
2. **Single Monolith without Modules:** One large application crate where all code resides together.
3. **Modular Monolith:** A single repository and unified binary compilation combining strictly separated, single-responsibility Rust crates.

## 4. Decision
We choose the **Modular Monolith**. The application compiles into a single integrated desktop binary (and standalone CLI), with internal modules structured as 19 decoupled Rust crates in a single workspace.

## 5. Consequences
- **Positive:** Zero inter-process latency for core data transfers, zero complex multi-process lifecycle supervision, strict crate boundaries enforce clean interfaces and domain separation, single binary distribution simplifies user installation.
- **Negative:** Large workspace compile times if dependency caching is not configured properly.

## 6. Risks & Mitigation
- **Risk:** Developers may introduce circular or leaky dependencies between crates.
- **Mitigation:** Strict topological hierarchy enforced by Cargo workspace dependency declarations; `nexus-domain` has zero OS or network dependencies.

## 7. Validation Strategy
Verified by `cargo check` and dependency graph validation scripts ensuring no circular dependencies and preserving layer isolation.
