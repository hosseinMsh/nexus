# NEXUS — Product Requirements Document (PRD)

**Document Version:** 1.0  
**Status:** Approved Baseline  
**License:** MIT  
**Distribution:** Single Integrated Application  

---

## 1. Product Overview

### 1.1. Product Definition
NEXUS is a professional, open-source, native download and network transfer management application built primarily with Rust.
It combines an advanced download engine, multi-interface network management, per-path proxy configuration, intelligent bandwidth allocation, protocol support, automation, and a professional desktop experience.
NEXUS is designed to provide reliable, efficient, and intelligent file transfers across multiple network interfaces without requiring an external relay server.

### 1.2. Product Vision
Build a high-performance, extensible, privacy-oriented download management platform that gives users complete control over their downloads and network connections.

### 1.3. Product Principles
- **Native First:** Native desktop application without Electron or WebView.
- **Rust First:** Rust for the core and application logic.
- **Local First:** Data and configurations remain on the user's device.
- **Direct Networking:** No mandatory external relay infrastructure.
- **Modular Architecture:** Independently testable internal modules.
- **Open Source:** MIT-licensed source code.
- **Reliability First:** Recovery and correctness take priority over raw speed.
- **Security by Design:** Secure networking, credentials, and local IPC.
- **Performance:** Efficient CPU, memory, disk, and network utilization.
- **User Control:** Transparent network selection and transfer policies.

---

## 2. Target Users

### 2.1. Primary Users
- Users downloading large files.
- Developers downloading packages, repositories, and datasets.
- Users with multiple Internet connections.
- Users relying on VPN and proxy services.
- Users with unstable Internet connections.
- Researchers transferring large datasets.
- Advanced users requiring detailed network control.

### 2.2. Secondary Users
- System administrators.
- DevOps engineers.
- Automation developers.
- Users managing multiple download sources.
- Users requiring CLI-based download workflows.

---

## 3. Product Scope & Functional Requirements

### 3.1. Download Management
- **DL-001 [P0]:** Download files through HTTP and HTTPS.
- **DL-002 [P0]:** Support multiple simultaneous downloads.
- **DL-003 [P0]:** Pause and resume downloads.
- **DL-004 [P0]:** Support HTTP Range requests.
- **DL-005 [P0]:** Support multi-connection downloads.
- **DL-006 [P0]:** Recover interrupted downloads.
- **DL-007 [P0]:** Support automatic retry.
- **DL-008 [P0]:** Support configurable connection limits.
- **DL-009 [P0]:** Support download speed limits.
- **DL-010 [P1]:** Support download scheduling.
- **DL-011 [P1]:** Support batch downloads.
- **DL-012 [P1]:** Support mirror URLs.
- **DL-013 [P0]:** Support file integrity verification (SHA-256, BLAKE3).
- **DL-014 [P0]:** Detect incomplete or inconsistent files.
- **DL-015 [P1]:** Support download categories.
- **DL-016 [P0]:** Support download history.
- **DL-017 [P0]:** Support automatic filename detection.
- **DL-018 [P1]:** Support custom HTTP headers.
- **DL-019 [P1]:** Support authentication and cookies.
- **DL-020 [P0]:** Support configurable download directories.

### 3.2. Network Interface Management
- **NW-001 [P0]:** Discover available network interfaces.
- **NW-002 [P0]:** Identify Wi-Fi interfaces.
- **NW-003 [P0]:** Identify Ethernet interfaces.
- **NW-004 [P0]:** Identify USB tethering interfaces where exposed by the OS.
- **NW-005 [P0]:** Detect interface availability changes.
- **NW-006 [P1]:** Monitor per-interface traffic.
- **NW-007 [P1]:** Monitor interface latency.
- **NW-008 [P1]:** Estimate available throughput.
- **NW-009 [P0]:** Allow manual interface selection.
- **NW-010 [P1]:** Support interface priorities.
- **NW-011 [P1]:** Support interface-specific bandwidth limits.
- **NW-012 [P1]:** Support network health monitoring.
- **NW-013 [P0]:** Detect disconnected interfaces.
- **NW-014 [P1]:** Automatically recover failed paths.

### 3.3. Proxy Management
Supported proxy types: HTTP, HTTPS, SOCKS4, SOCKS4a, SOCKS5, SOCKS5 with authentication.
- **PX-001 [P0]:** Create multiple proxy profiles.
- **PX-002 [P0]:** Assign a proxy to a network path.
- **PX-003 [P0]:** Support username/password authentication.
- **PX-004 [P1]:** Support proxy health checks.
- **PX-005 [P1]:** Support proxy connection limits.
- **PX-006 [P1]:** Support proxy-specific timeout settings.
- **PX-007 [P1]:** Support proxy-specific DNS policies.
- **PX-008 [P2]:** Support proxy bypass rules.
- **PX-009 [P0]:** Securely store credentials.
- **PX-010 [P1]:** Display proxy connection status.

### 3.4. Network Path Management
A Network Path represents a configured route for transferring data:
`Path = Network Interface + Optional Proxy + Routing & DNS Policy + Connection Limits + Health Status`
- **NP-001 [P0]:** Create multiple network paths.
- **NP-002 [P0]:** Bind paths to specific interfaces.
- **NP-003 [P0]:** Assign independent proxies.
- **NP-004 [P0]:** Enable and disable paths.
- **NP-005 [P1]:** Monitor path health.
- **NP-006 [P1]:** Monitor path throughput.
- **NP-007 [P1]:** Support manual priorities.
- **NP-008 [P1]:** Support path-specific policies.
- **NP-009 [P1]:** Support automatic path selection.
- **NP-010 [P1]:** Support path failover.

### 3.5. Multi-Path Download Engine
- **MP-001 [P0]:** Download separate file ranges through different paths.
- **MP-002 [P0]:** Support independent connections per path.
- **MP-003 [P1]:** Dynamically allocate segments.
- **MP-004 [P1]:** Reallocate unfinished segments upon path failure.
- **MP-005 [P1]:** Detect failed paths.
- **MP-006 [P1]:** Continue transfers through healthy paths.
- **MP-007 [P1]:** Estimate path throughput.
- **MP-008 [P1]:** Balance transfer workloads.
- **MP-009 [P0]:** Avoid duplicate segment downloads.
- **MP-010 [P0]:** Validate received ranges.
- **MP-011 [P0]:** Detect inconsistent remote file versions (ETag / Last-Modified mismatch).
- **MP-012 [P0]:** Provide aggregated throughput statistics.

---

## 4. Non-Functional Requirements

### 4.1. Performance
- **Idle Memory:** Under 100 MB.
- **Typical Active Memory:** Under 300 MB.
- **UI Responsiveness:** 60 FPS target without blocking the main event loop.
- **Download Buffering:** Bounded memory via bounded streaming buffers.
- **Large Files:** Support files larger than 100 GB without loading entire files into memory.

### 4.2. Reliability
- No corrupted final files.
- No silent overwriting of user data.
- Crash-safe metadata persistence with SQLite WAL mode and atomic file promotion (`.part` to final file).
- Consistent state transitions through an explicit state machine.

### 4.3. Security & Privacy
- Zero plaintext credentials on disk.
- Zero mandatory cloud account or telemetry.
- Private network SSRF protection enabled by default.
