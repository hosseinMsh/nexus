# ADR-005: HTTP Transport Architecture

- **Status:** Approved
- **Date:** 2026-11-03
- **Deciders:** NEXUS Architecture Team

---

## 1. Context
NEXUS requires fine-grained control over HTTP/1.1 and HTTP/2 connections, including custom Range requests, header management, redirect inspection, and most importantly, binding outgoing TCP sockets to specific local IP addresses / physical interfaces.

## 2. Problem
Which HTTP client abstraction should be chosen to support custom socket binding and proxy chaining without sacrificing high-level features like TLS verification and streaming?

## 3. Considered Alternatives
1. **Raw Hyper with custom connector:** Maximum low-level control over socket creation and connection pools, but requires manual handling of TLS certificates, redirects, cookies, and HTTP connection management.
2. **Reqwest (custom client builder per path):** Mature, robust HTTP client built on Hyper and Rustls. Supports custom local address binding (`local_address`), connection timeouts, redirects, and proxy configuration.
3. **Curl bindings (`isahc` / `curl-rust`):** Wraps libcurl C library. Introduces C dependencies, cross-compilation complexity, and unsafe FFI boundaries.

## 4. Decision
We adopt **Reqwest with Rustls** for general HTTP/HTTPS operations, backed by custom client builders per `NetworkPath`. For advanced socket-level options requiring direct interface index binding (e.g. `SO_BINDTODEVICE` / `IP_BOUND_IF`), we implement custom connector hooks in `nexus-transport` bridging directly to socket configuration.

## 5. Consequences
- **Positive:** Pure Rust stack with Rustls eliminates OpenSSL cross-compilation headaches. Native async streaming support with zero extra FFI overhead.
- **Negative:** Certain OS-specific socket flags require custom Tokio socket builders before handing off to the transport connector.

## 6. Risks & Mitigation
- **Risk:** Some servers reject aggressive range requests or misreport byte intervals.
- **Mitigation:** Strict Range header validation and Content-Range parsing in `nexus-download` with automatic fallback to single-connection streaming.

## 7. Validation Strategy
Integration tests against local mock HTTP servers validating 206 Partial Content, Range headers, and redirect chains.
