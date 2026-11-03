# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased] - Phase 9: Desktop & CLI Applications

## [0.9.0] - Phase 8: Automation Engine

### Added
- **Automation Rule Engine (`nexus-automation::rules`):**
  - Declarative rule matching based on URL pattern regex/substrings, domain whitelist, compound file extensions (e.g. `.tar.gz`, `.iso`, `.zip`), and min/max file size thresholds.
  - Priority-based rule execution (`sort_by_key`) with actions applying custom destination directories, category assignment, connection limits, and per-download bandwidth throttles.
- **Off-Peak Scheduler & Time Windows (`nexus-automation::scheduler`):**
  - Implemented `TimeWindow` with support for standard daytime windows and overnight midnight-spanning off-peak windows (e.g., 23:00 to 07:00).
  - Configurable `SchedulePolicy` with speed throttling, deferral until next off-peak window, and maximum concurrency control.
- **Priority Concurrency Queue Manager (`nexus-automation::queue`):**
  - Multi-state queue management (`Pending`, `Active`, `Paused`, `Completed`, `Failed`).
  - Strict priority scheduling (`P0` Urgent through `P3` Low) with automatic task promotion up to maximum active concurrency slots.
- **Clipboard URL Extractor (`nexus-automation::clipboard`):**
  - Heuristic detection and extraction of URLs (`http`, `https`, `ftp`, `magnet`, `webdav`) from clipboard contents.
  - Automatic trailing punctuation sanitization (periods, commas, closing brackets) preserving query parameters.
- **JSON-RPC 2.0 Dispatcher (`nexus-ipc`):**
  - Spec-compliant JSON-RPC 2.0 protocol engine (`JsonRpcRequest`, `JsonRpcResponse`, `JsonRpcError`).
  - Thread-safe Bearer session token authentication guard (`check_auth`).
  - Modular `RpcHandler` trait for seamless integration with frontend, CLI, and browser extensions.
- **Integration Test Suite (`nexus-tests`):**
  - `automation_engine_test.rs`: 5 tests validating rule evaluation, overnight scheduling, clipboard parsing, priority concurrency queuing, and authenticated JSON-RPC dispatching.

## [0.8.0] - Phase 7: Protocol Extensions

### Added
- **Protocol Dispatcher & Manager (`nexus-protocols::manager`):**
  - Implemented `ProtocolManager` with thread-safe adapter registry (`register_adapter`, `get_adapter`).
  - Dynamic scheme-based transfer dispatching for metadata probing and byte-range streaming.
- **HTTP / HTTPS Protocol Adapter (`nexus-protocols::http`):**
  - Implemented `HttpProtocolAdapter` bridging `HttpTransportClient` to the unified `ProtocolAdapter` trait.
- **FTP / FTPS Protocol Adapter (`nexus-protocols::ftp`):**
  - RFC 959 session handling with USER/PASS authentication and binary transfer mode (`TYPE I`).
  - Implemented remote file size querying via `SIZE` command (RFC 3659).
  - Extended Passive (`EPSV`, RFC 2428) and Passive (`PASV`) data port negotiation.
  - Implemented Range resumption using `REST <offset>` and `RETR <path>`.
- **BitTorrent & Magnet Protocol Suite (`nexus-protocols::torrent`):**
  - Full Bencode encoder and decoder (BEP 3) supporting integers (`i...e`), byte strings (`len:str`), lists (`l...e`), and sorted dictionaries (`d...e`).
  - Implemented `MagnetUri` parser (BEP 9) extracting `urn:btih` infohash, display name (`dn`), trackers (`tr`), web seeds (`ws`), and exact length (`xl`).
  - Implemented `MagnetProtocolAdapter` with metadata probing and web-seed bridging.
- **Metalink 4.0 & 3.0 Document Parser (`nexus-protocols::metalink`):**
  - Implemented `MetalinkParser` extracting `<file>` definitions, cryptographic verification hashes (`sha-256`, `sha-1`, `md5`), and multiple mirror URLs with location and priority ranking.
  - Automatic mirror sorting preferring lowest numerical priority values.
- **WebDAV Protocol Adapter (`nexus-protocols::webdav`):**
  - WebDAV (RFC 4918) URL mapping (`webdav://` and `webdavs://`).
  - Implemented `parse_propfind_xml` extracting `getcontentlength`, `getlastmodified`, `getetag`, and `is_collection` flags.
- **Integration Test Suite (`nexus-tests`):**
  - `protocol_adapters_test.rs`: 5 integration tests covering:
    - ProtocolManager routing across HTTP, FTP, Magnet, and WebDAV with unsupported scheme rejection.
    - FTP metadata probing (`SIZE`) and range streaming (`REST`) against a live hermetic mock FTP server.
    - Bencode serialization/deserialization and Magnet URI parsing.
    - Metalink multi-mirror priority ordering and hash extraction.
    - WebDAV metadata extraction and range downloading.

## [0.7.0] - Phase 6: Multi-Path Networking

### Added
- **Multi-Path Transfer Engine & Worker Pool (`nexus-download`):**
  - Extended `WorkerPool` to coordinate parallel transfers simultaneously across multiple independent `NetworkPath`s.
  - Implemented dynamic path failover: when a path experiences connection drop, stream error, or proxy failure mid-transfer, workers dynamically detect the fault, record path degradation, and migrate in-flight segments to surviving healthy paths without aborting the overall transfer.
  - Implemented `download_multi_path` in `DownloadEngine` with fallback single-path delegation (`download`).
  - Strict absence of byte gaps or overlapping segment requests during multi-path execution.
- **Dynamic Path Metrics & Adaptive Scheduling (`nexus-path-scheduler`):**
  - Implemented `PathMetricsTracker` tracking real-time metrics per path:
    - Exponential moving average (EMA) throughput calculation ($\alpha = 0.3$).
    - Moving average round-trip latency ($\alpha = 0.2$).
    - Consecutive failure counting and automatic health status transitioning (`is_healthy`).
    - Active connection tracking with atomic increment/decrement.
    - Aggregated throughput computation across all active paths (`aggregate_throughput_bps`).
  - Implemented `select_best_candidate` and `select_alternative_path` in `AdaptivePathScheduler` taking into account path priority, throughput score, failure rate, and path connection limits (`PathLimits.max_connections`).
- **Interface-Bound Socket Routing (`nexus-transport`):**
  - Added interface IP registration (`register_interface_ip`) and socket binding (`builder.local_address`) per `InterfaceId`.
  - Upgraded HTTP transport caching to `PathId` resolution, ensuring dedicated client isolation for distinct interface bindings and proxies.
- **Integration Test Suite (`nexus-tests`):**
  - `multi_path_networking_test.rs`: 5 integration tests covering:
    - `test_multi_path_parallel_download_and_file_integrity`: Multi-path download across Direct and Proxied routes simultaneously with byte-for-byte SHA-256 integrity verification.
    - `test_multi_path_failover_when_one_path_drops`: Mid-stream path drop recovery where surviving healthy paths seamlessly absorb uncommitted segments.
    - `test_multi_path_priority_and_dynamic_scheduling`: Prioritization of high-priority paths with graceful fallback upon consecutive failures.
    - `test_multi_path_connection_limits_enforced`: Connection limit enforcement diverting excess workers to secondary paths.
    - `test_aggregate_throughput_accounting`: Accurate aggregate throughput tracking across active paths.

## [0.6.0] - Phase 5: Proxy Engine

### Added
- **Proxy Profile Coordinator (`nexus-proxy`):**
  - Implemented `ProxyManager` registering, updating, listing, and health-checking `ProxyProfile` entities.
  - SOCKS5 (RFC 1928) handshake and RFC 1929 username/password authentication client negotiation.
  - Remote vs Local DNS policy resolution: configured `socks5h://` schemes instructing client transports to resolve destination domains remotely via ATYP `0x03`.
  - Active latency and status health probing (`check_proxy_health`) reporting alive status, round-trip latency, and detailed failure diagnostics.
- **Secure Keyring & Credential Separation (`nexus-security`):**
  - Implemented `CredentialStore` trait and `InMemoryCredentialStore` (and OS keyring abstraction).
  - Enforced zero-plaintext-credential storage in the primary database: domain profiles store only cryptographic references (`credential_reference: Option<String>`), never passwords or usernames.
- **Per-Path Proxy Routing Transport (`nexus-transport`):**
  - Updated `HttpTransportClient` to resolve and cache dedicated proxied client instances on a per-`NetworkPath` basis.
  - Strict isolation guarantee: if a path specifies a proxy, traffic must strictly flow through that proxy; on connection failure or bad auth, the transport immediately errors with `TransportError` and **never** falls back silently to direct connections.
  - Independent per-path isolation: traffic on unproxied paths flows directly without impacting proxied paths.
- **Integration Test Suite (`nexus-tests`):**
  - `proxy_engine_test.rs`: 7 integration tests covering:
    - SOCKS5 authenticated downloads with RFC 1928 / RFC 1929 handshake and file integrity verification.
    - HTTP proxying with partial content `Range` request forwarding.
    - Remote DNS resolution (`socks5h://`, ATYP 0x03 domain forwarding).
    - Independent per-path proxy routing (proxied vs direct).
    - Strict isolation: zero fallback to direct unproxied connections on proxy failure.
    - Secure credential separation at rest (zero plaintext passwords in database serialization).
    - Live proxy health probing (healthy, bad auth, closed port).

## [0.5.0] - Phase 4: Network Discovery

### Added
- **OS Platform Adapters (`nexus-network-platform`):**
  - **macOS (`MacOsNetworkPlatform`):** Native hardware port discovery (`networksetup -listallhardwareports`), `ifconfig` flags/status/address parsing, and active default gateway route detection (`route -n get default`).
  - **Linux (`LinuxNetworkPlatform`):** Direct sysfs inspection (`/sys/class/net/*`), operstate/carrier reading, USB tethering driver identification (`rndis_host`, `cdc_ncm`, `cdc_ether`, `ipheth`), and `/proc/net/route` gateway parsing.
  - **Windows (`WindowsNetworkPlatform`):** Net adapter classification (802.11 Wi-Fi, GbE Ethernet, Remote NDIS / Apple Mobile Device USB tethering).
  - **Simulated Platform (`SimulatedNetworkPlatform`):** Hermetic testing platform for simulated connect/disconnect, tethering attach/detach, and sleep/wake state machines.
  - **Deterministic Interface IDs:** Generated reproducible UUIDv5 identifiers from platform prefix and system name, ensuring stable identities across reboots and scans.
- **Dynamic Network Monitoring (`nexus-network::monitor`):**
  - Implemented `NetworkMonitor` with broadcast stream delivering `NetworkChangeEvent`s:
    - `InterfaceDiscovered`, `InterfaceRemoved`, `InterfaceStatusChanged`, `DefaultRouteChanged`, `SleepDetected`, `WakeDetected`.
  - Distinguishes whether an interface is physically present, operational (`is_up`), and connected (`has_carrier` with IP).
- **Network Path Health Auditing (`nexus-network`):**
  - Implemented `inspect_path_health` reporting live path availability, carrier status, and IP addresses.
  - Added `remove_path` and `get_path` management methods to `NetworkManager`.
- **Integration Test Suite (`nexus-tests`):**
  - `network_discovery_test.rs`: 5 integration tests verifying:
    - Wi-Fi, Ethernet, Loopback, and live macOS interface enumeration.
    - USB tethering detection, dynamic classification, and path binding.
    - Interface disconnection and automatic recovery reflection.
    - System sleep and wake detection.
    - Download resilience: in-flight transfers survive mid-stream network drops and resume seamlessly.

## [0.4.0] - Phase 3: Multi-Connection Engine

### Added
- **Asynchronous Disk Writer Actor (`nexus-download::writer`):**
  - Implemented `AsyncFileWriterHandle` and background `spawn_disk_writer` actor with bounded Tokio `mpsc` queue backpressure.
  - Safe, lock-free, single-handle file write coordination with random-access seek-and-write for concurrent workers.
  - Asynchronous flush and sync guarantees (`WriterCommand::Sync`, `WriterCommand::Close`).
  - Thread-safe error propagation for disk I/O faults.
- **Parallel Multi-Connection Worker Pool (`nexus-download::worker`):**
  - Implemented `WorkerPool` with Tokio `JoinSet` managing up to 32 parallel worker streams.
  - Designed `AtomicSegment` with lock-free atomic offsets (`Arc<AtomicU64>`), byte progress, and status flags.
  - Dynamic end-of-transfer "work stealing": idle fast workers inspect active in-progress segments with $\ge 128\text{ KB}$ remaining, dynamically split the uncommitted range, and download the tail concurrently.
  - Exponential backoff retry logic on network drops, with immediate checkpoint resumption avoiding duplicate range transfers.
  - Integrated `validate_segments` ensuring strict absence of byte gaps or overlapping intervals.
- **Integration & Performance Test Suites (`nexus-tests`):**
  - `multi_connection_engine_test.rs`: 5 integration tests verifying:
    - 16-worker parallel downloads with SHA-256 integrity verification.
    - Strict coverage validation (`validate_segments`) with arbitrary non-power-of-two file sizes.
    - Dynamic work stealing across asymmetric / stalled connections.
    - Mid-stream network connection drop recovery from checkpoint.
    - Server connection concurrency limits and HTTP 429 backoff.
  - `multi_connection_benchmark_test.rs`: Performance benchmark against throttled mock HTTP server:
    - Single-connection vs 4-connection transfer benchmark showing **3.42x speedup** on throttled channels.

## [0.3.0] - Phase 2: HTTP Download Engine

### Added
- **HTTP Transport Engine (`nexus-transport`):**
  - Implemented `HttpTransportClient` wrapping `reqwest` with `rustls` TLS backend.
  - Implemented `probe_metadata` with standard `HEAD` request and robust fallback to `GET` with `Range: bytes=0-0` when servers return `405 Method Not Allowed` or `501 Not Implemented`.
  - Extracted headers for `Content-Length`, `Accept-Ranges`, `Content-Range`, `ETag`, `Last-Modified`, and RFC 5987 / standard `Content-Disposition` filename parsing.
  - Redirect tracking to capture and store the final CDN or target URL.
  - Implemented `fetch_range` returning an async streaming reader with streaming byte chunks.
- **Temporary File Manager (`nexus-download::file`):**
  - Deterministic `.part` file naming schema (`<dir>/<filename>.<task_id>.part`).
  - Sparse file pre-allocation via `file.set_len(size)`.
  - Concurrency-safe random-access writes using `tokio::fs::File` with `AsyncSeekExt::seek` and `AsyncWriteExt::write_all`.
  - Buffer synchronization via `file.sync_all()`.
  - Atomic file promotion from `.part` to final filename via `tokio::fs::rename`, with overwrite protection and parent directory auto-creation.
- **Download Execution Engine (`nexus-download::engine`):**
  - Comprehensive lifecycle coordinator managing transitions: `Queued -> Preparing -> Downloading -> Verifying -> Completed`.
  - HTTP Range segment partitioning via `nexus-segments::partition_segments`.
  - Real-time progress tracking, checkpointing segment completion, and publishing `SystemEvent::SegmentProgress`.
  - Graceful cancellation and pause handling via `tokio_util::sync::CancellationToken` saving partial bytes and transitioning to `DownloadStatus::Paused`.
  - Fallback streaming mechanism for non-Range servers or streams with unknown total file sizes.
  - Automatic retry with exponential backoff on transient transport errors.
- **Integration Test Suite (`nexus-tests`):**
  - `http_download_engine_test.rs`: 5 integration tests using hermetic local TCP HTTP mock servers:
    - `test_range_capable_download_and_file_assembly`: Multi-segment download, sparse file assembly, and byte-for-byte verification.
    - `test_head_rejection_probe_fallback`: Verifies range probe fallback when server rejects `HEAD` with HTTP 405.
    - `test_pause_and_resume_download_lifecycle`: Verifies pausing at mid-transfer, flushes to disk, and resumes to completion without re-downloading finished segments.
    - `test_non_range_server_fallback`: Verifies graceful single-stream fallback when `Accept-Ranges` is omitted or absent.
    - `test_redirect_handling`: Verifies 302 redirect following and metadata preservation.

## [0.2.0] - Phase 1: Domain and Storage Persistence

### Added
- **SQLite Storage Engine (`nexus-storage`):**
  - Integrated `sqlx 0.8` with embedded migrations (`migrations/0001_initial_schema.sql`).
  - Implemented `SqliteStorage` connection pool with automatic WAL mode (`PRAGMA journal_mode = WAL`), foreign keys enforcement (`PRAGMA foreign_keys = ON`), and normal synchronous durability.
  - Implemented `TaskRepository`, `SegmentRepository`, `PathRepository`, and `ProxyRepository` for SQLite.
  - Implemented `InMemoryStorage` mock repository with full feature parity.
- **Relational Schema:**
  - `downloads` table with full policy serialization, progress bytes, and status indices.
  - `segments` table with foreign key cascade to downloads and non-overlapping byte intervals.
  - `network_paths` table with priority, interface reference, and bandwidth constraints.
  - `proxy_profiles` table with protocol kind, host, port, credentials reference, and DNS policy.
  - `download_events` and `application_settings` tables for audit and state persistence.
- **Integration Test Suite (`nexus-tests`):**
  - `crash_persistence_test.rs`: Validates restart durability, physical file reopen, and crash recovery resetting in-flight downloads to `Paused`.
  - `storage_migration_test.rs`: Validates migration execution, filtering by status, and transactional consistency.
  - `security_guards_test.rs`: Validates SSRF blocking, filename path traversal prevention, and SHA-256/BLAKE3 hashing.

## [0.1.0] - Phase 0: Project Foundation

### Added
- **Repository & Workspace Setup:** Initialized 19-crate Rust modular monolithic workspace with resolver 2 and shared workspace dependencies.
- **Applications:**
  - `apps/nexus-desktop`: Native desktop application shell with decoupled architecture.
  - `apps/nexus-cli`: Command-line interface with subcommands for downloads, network management, and statistics.
- **Core Domain Crates:**
  - `crates/nexus-core`: Core types, identifiers, and configuration models.
  - `crates/nexus-domain`: Domain entities (DownloadTask, DownloadSegment, NetworkInterface, NetworkPath, ProxyProfile, DownloadPolicy, TransferStatistics).
  - `crates/nexus-application`: Use-case orchestration services and traits.
  - `crates/nexus-download`: Download engine state machine and lifecycle coordinator.
  - `crates/nexus-segments`: Byte-interval partitioning, boundary verification, and non-overlapping commitments.
  - `crates/nexus-network`: Multi-interface discovery and routing abstractions.
  - `crates/nexus-network-platform`: OS-level network discovery abstraction and cross-platform scaffolding.
  - `crates/nexus-proxy`: SOCKS4/SOCKS5/HTTP proxy configuration and health checking models.
  - `crates/nexus-path-scheduler`: Weighted adaptive multi-path scheduling algorithms.
  - `crates/nexus-transport`: Interface-bound socket and HTTP transport abstractions.
  - `crates/nexus-protocols`: Protocol adapter framework and dispatchers.
  - `crates/nexus-storage`: SQLite metadata repository and schema migrations.
  - `crates/nexus-recovery`: Crash-safe task reconstruction and partial file verification.
  - `crates/nexus-integrity`: SHA-256 and BLAKE3 checksum verification engines.
  - `crates/nexus-security`: SSRF defenses, path traversal guards, and filename sanitization.
  - `crates/nexus-automation`: Download queues, rule engines, and automation hooks.
  - `crates/nexus-ipc`: Local JSON-RPC 2.0 schema, protocol types, and token authentication.
  - `crates/nexus-events`: Decoupled asynchronous event bus.
  - `crates/nexus-telemetry`: Structured tracing and telemetry integration.
- **Architecture Documentation:**
  - PRD (Product Requirements Document), SAD (Software Architecture Document), TDD (Technical Design Document).
  - 12 comprehensive Architecture Decision Records (ADR-001 through ADR-012).
  - Threat modeling and developer setup documentation.
- **CI/CD:** Multi-platform GitHub Actions workflow covering Linux, macOS, and Windows.
- **Tooling:** Pinned toolchain, rustfmt configuration, clippy configuration with strict lints.
