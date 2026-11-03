# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased] - Phase 0: Project Foundation

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
