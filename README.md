# NEXUS

<p align="center">
  <strong>Native, Multi-Path Transfer & Download Management Platform</strong><br>
  <em>One Application. Multiple Paths. Complete Control.</em>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"></a>
  <img src="https://img.shields.io/badge/language-Rust-orange.svg" alt="Language: Rust">
  <img src="https://img.shields.io/badge/platforms-macOS%20%7C%20Windows%20%7C%20Linux-lightgrey.svg" alt="Platforms: macOS | Windows | Linux">
  <img src="https://img.shields.io/badge/architecture-Modular%20Monolith-purple.svg" alt="Architecture: Modular Monolith">
</p>

---

## 1. Overview

**NEXUS** is a professional, open-source, native download and network transfer management application built from the ground up in Rust.

It integrates an advanced multi-connection download engine, multi-interface network discovery, per-path proxy routing, adaptive segment scheduling, crash-safe persistence, and a native desktop experience without relying on Electron or WebView.

NEXUS enables reliable, intelligent file transfers across multiple physical and logical network interfaces (e.g., simultaneous Wi-Fi + Ethernet + USB Tethering) with direct peer-to-server multi-path aggregation without requiring external relay servers.

---

## 2. Core Principles

| Principle | Commitment |
|---|---|
| **Native First** | 100% native desktop application without Electron, CEF, or WebView wrappers. |
| **Rust First** | Strict memory safety, high concurrency, and minimal resource footprints. |
| **Local First** | Downloads, metadata, paths, and configurations reside exclusively on the user's device. |
| **Direct Networking** | No mandatory cloud relay infrastructure or vendor lock-in. |
| **Modular Architecture** | 19 independently testable crates within a clean single workspace. |
| **Reliability First** | Crash-proof transaction persistence, non-overlapping segment commitments, and zero data corruption. |
| **Security by Design** | OS keychain integration, SSRF and path traversal guards, safe loopback IPC authentication. |

---

## 3. Workspace Architecture

NEXUS follows a modular monolithic architecture, dividing domains into distinct crates:

```text
nexus/
├── Cargo.toml                  # Unified workspace configuration & dependencies
├── rust-toolchain.toml         # Rust toolchain pinning
├── rustfmt.toml                # Formatting rules
├── clippy.toml                 # Static analysis linting configuration
│
├── apps/
│   ├── nexus-desktop/          # Native desktop application
│   └── nexus-cli/              # Fast, scriptable command-line interface
│
├── crates/
│   ├── nexus-core/             # Identifiers, configuration primitives, and shared utilities
│   ├── nexus-domain/           # Core domain models (DownloadTask, Segment, Path, Proxy)
│   ├── nexus-application/      # Application services and orchestration use cases
│   ├── nexus-download/         # Complete download lifecycle execution engine
│   ├── nexus-segments/         # Non-overlapping byte interval allocation & tracking
│   ├── nexus-network/          # Interface and path abstractions
│   ├── nexus-network-platform/ # OS-level network discovery (macOS, Windows, Linux)
│   ├── nexus-proxy/            # SOCKS4/5 & HTTP proxy profiles, health checks, credentials
│   ├── nexus-path-scheduler/   # Weighted adaptive multi-path scheduling
│   ├── nexus-transport/        # Bound socket transport & HTTP client abstractions
│   ├── nexus-protocols/        # Protocol adapters (HTTP, HTTPS, FTP, Torrent, etc.)
│   ├── nexus-storage/          # SQLite persistence and SQLx migrations
│   ├── nexus-recovery/         # Crash recovery and download state reconstruction
│   ├── nexus-integrity/        # File checksum validation (SHA-256, BLAKE3)
│   ├── nexus-security/         # SSRF protection, path traversal guards, credential safety
│   ├── nexus-automation/       # Queues, download rules, and clipboard monitoring
│   ├── nexus-ipc/              # Local loopback JSON-RPC server and protocol
│   ├── nexus-events/           # Decoupled cross-crate event bus
│   └── nexus-telemetry/        # Structured tracing and metrics collection
│
├── docs/                       # PRD, SAD, TDD, ADRs, Setup, Security
└── tests/                      # Integration, performance, recovery, and security tests
```

---

## 4. Getting Started

### Prerequisites

- **Rust:** 1.80+ (stable toolchain)
- **Cargo**
- **Platform C compiler / build essentials** (for SQLite / native socket APIs)

### Building the Project

```bash
# Clone the repository
git clone https://github.com/nexus-transfer/nexus.git
cd nexus

# Check formatting and lints
cargo fmt --check
cargo clippy --workspace --all-targets

# Run the full test suite
cargo test --workspace

# Build all binaries in release mode
cargo build --release
```

### Running the CLI

```bash
# Print general CLI help
cargo run -p nexus-cli -- --help

# Inspect detected network interfaces
cargo run -p nexus-cli -- network interfaces

# List configured network paths
cargo run -p nexus-cli -- network paths

# Add a download task
cargo run -p nexus-cli -- download add "https://example.com/sample.iso"
```

---

## 5. Development Roadmap

- **Milestone A — Foundation (Phase 0):** Rust workspace, coding standards, CI, ADRs, domain skeleton. *(Current)*
- **Milestone B — Reliable Downloads (Phases 1-3):** HTTP/HTTPS range engine, persistence, recovery, and multi-connection streaming.
- **Milestone C — Network Intelligence (Phase 4):** OS network discovery (macOS, Windows, Linux) and network path modeling.
- **Milestone D — Proxy Intelligence (Phase 5):** Per-path SOCKS/HTTP proxies with OS credential vault integration.
- **Milestone E — True Multi-Path (Phase 6):** Bound interface sockets, adaptive segment distribution, and seamless failover.
- **Milestone F — Product Experience (Phase 7):** Native desktop user interface (egui/iced evaluation).
- **Milestone G — Production Release (Phases 8-10):** Protocols, automation, security audits, and release packages.

---

## 6. Architecture Decision Records (ADRs)

All foundational architectural decisions are documented under [`docs/decisions/`](docs/decisions/):
- **ADR-001:** Modular Monolithic Architecture
- **ADR-002:** Rust Workspace Design
- **ADR-003:** Native UI Framework Evaluation
- **ADR-004:** Async Runtime (Tokio)
- **ADR-005:** HTTP Transport Architecture
- **ADR-006:** Database and Persistence (SQLite & SQLx)
- **ADR-007:** Cross-Platform Network Interface Binding
- **ADR-008:** Proxy Routing Architecture
- **ADR-009:** Multi-Path Adaptive Scheduling
- **ADR-010:** Local JSON-RPC IPC Interface
- **ADR-011:** Secure OS Credential Storage
- **ADR-012:** Release and Cross-Platform Packaging

---

## 7. License

This project is licensed under the terms of the [MIT License](LICENSE).
