# Master Development Roadmap

NEXUS development follows a phased, test-driven approach divided into major milestones.

---

## Milestone A — Foundation (Phase 0) [Current]
- [x] Initialized Git repository and branch governance.
- [x] Cargo workspace with 19 modular monolithic crates and shared dependencies.
- [x] Pinned toolchain, rustfmt, and strict clippy lint configurations.
- [x] Comprehensive PRD, SAD, TDD, and Architecture Decision Records (ADR-001 through ADR-012).
- [x] GitHub Actions CI matrix for Linux, macOS, and Windows.
- [x] Initial domain entities, state machines, and CLI application shell.

---

## Milestone B — Reliable Downloads (Phases 1-3)
- [ ] **Phase 1: Domain & Storage Persistence**
  - Implement SQLite database repository with SQLx migrations.
  - Implement atomic task transitions and checkpoint transactions.
- [ ] **Phase 2: HTTP Download Engine**
  - Implement robust HTTP/HTTPS range requests.
  - Automatic fallback for non-range capable servers.
  - Temporary file management (`.part`) with sparse allocations.
- [ ] **Phase 3: Multi-Connection Engine**
  - Segment file into dynamic non-overlapping intervals.
  - Parallel stream workers with backpressure and rate limiting.

---

## Milestone C — Network Intelligence (Phase 4)
- [ ] Platform-specific network interface discovery:
  - macOS: `getifaddrs` / SystemConfiguration.
  - Linux: Netlink / `/proc/net`.
  - Windows: `GetAdaptersAddresses` / IP Helper.
- [ ] Identification of Wi-Fi, Ethernet, and USB tethering interfaces.
- [ ] Interface status, latency monitoring, and state change callbacks.

---

## Milestone D — Proxy Intelligence (Phase 5)
- [ ] SOCKS4, SOCKS5, and HTTP proxy profile management.
- [ ] SOCKS5 username/password authentication.
- [ ] Secure credential storage using OS Keychain / DPAPI / Secret Service.
- [ ] Active proxy health checking and timeout management.

---

## Milestone E — True Multi-Path (Phase 6)
- [ ] OS socket binding to specific physical interfaces (e.g. `IP_BOUND_IF`, `SO_BINDTODEVICE`).
- [ ] Weighted adaptive segment scheduling across multiple concurrent interfaces.
- [ ] Seamless failure detection and segment reassignment to healthy paths.
- [ ] Aggregated throughput telemetry.

---

## Milestone F — Product Experience (Phase 7)
- [ ] Native Rust UI implementation (based on ADR-003 evaluation).
- [ ] Real-time transfer graphs, active download queues, and network path inspector.
- [ ] System tray integration, dark/light themes, and keyboard navigation.

---

## Milestone G — Production Release (Phases 8-10)
- [ ] Advanced protocol adapters (FTP, SFTP, BitTorrent, Metalink).
- [ ] Browser extension integration and local loopback JSON-RPC server.
- [ ] Security audits, crash recovery test validation, signed installer packages.
