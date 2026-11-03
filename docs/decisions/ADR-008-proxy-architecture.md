# ADR-008: Proxy Architecture

- **Status:** Approved
- **Date:** 2026-11-03
- **Deciders:** NEXUS Architecture Team

---

## 1. Context
Users require the ability to route traffic through various proxy types (HTTP, HTTPS, SOCKS4, SOCKS5 with authentication) and assign independent proxies per NetworkPath (e.g., Interface 1 Direct, Interface 2 via SOCKS5 proxy).

## 2. Problem
How should proxy connections be negotiated and combined with interface binding without leaking DNS or falling back silently to unproxied routes?

## 3. Considered Alternatives
1. **Global System Proxy:** Relying on OS system proxy settings. Disqualified because it prevents per-path routing.
2. **Independent Per-Path Proxy Pipeline:** Every `NetworkPath` maintains its own proxy client configuration. If a proxy is configured, the transport binds to the local interface first, connects to the proxy server, completes authentication/handshake, and then requests the remote destination.

## 4. Decision
We adopt the **Independent Per-Path Proxy Pipeline** in `nexus-proxy` and `nexus-transport`:
- Connection order: Select Path -> Bind Interface -> Connect Proxy -> Authenticate Proxy -> Connect Target -> TLS Handshake (if HTTPS) -> Transfer.
- Remote DNS resolution via SOCKS5 (domain name passed directly to proxy) to prevent local DNS leakage.
- Strict isolation: If a proxy handshake or connection fails, the engine **never** silently falls back to direct unproxied transfer. Failures are reported directly to the path scheduler.

## 5. Consequences
- **Positive:** Guarantees strict user privacy and predictable routing. Users can aggregate traffic across multiple proxies or mix direct and proxied paths.
- **Negative:** Additional latency during initial handshake per segment if connection pooling is not utilized.

## 6. Risks & Mitigation
- **Risk:** Stale proxy connections causing segment timeouts.
- **Mitigation:** Periodic health checks and aggressive connection timeout detection.

## 7. Validation Strategy
Integration tests running mock SOCKS5 and HTTP proxy instances in tests, validating authentication and DNS handling.
