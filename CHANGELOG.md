# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased] - Phase 3: Multi-Connection Engine

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
