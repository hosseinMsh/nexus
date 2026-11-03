# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased] - Phase 1: Domain and Storage Persistence

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
