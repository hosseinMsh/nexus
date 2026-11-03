# ADR-010: Local IPC (JSON-RPC 2.0)

- **Status:** Approved
- **Date:** 2026-11-03
- **Deciders:** NEXUS Architecture Team

---

## 1. Context
NEXUS must support CLI control, automation scripts, browser extensions, and future MCP integrations while maintaining strict local security.

## 2. Problem
What IPC protocol and transport should be used to communicate between the NEXUS desktop/daemon core and external local clients?

## 3. Considered Alternatives
1. **gRPC / Protocol Buffers:** High performance, but requires HTTP/2, protobuf compiler toolchains, and browser extensions cannot easily communicate without specialized adapters.
2. **REST API:** Familiar, but lacks standardized bi-directional notification and request-batching conventions.
3. **JSON-RPC 2.0:** Standardized, lightweight, human-readable, easily parsed in any language (Rust, JavaScript, Python, bash via curl), and natively supported by web extension messaging.

## 4. Decision
We adopt **JSON-RPC 2.0** over loopback HTTP / UNIX domain sockets with Bearer token authentication in `nexus-ipc`.

## 5. Consequences
- **Positive:** Universal client interoperability. CLI, browser extensions, automation scripts, and MCP agents all share a unified, well-typed schema.
- **Negative:** Slightly higher parsing overhead compared to binary protocols (negligible for control plane IPC).

## 6. Risks & Mitigation
- **Risk:** Cross-Origin Request Forgery (CSRF) or unauthorized local applications issuing commands.
- **Mitigation:** Unpredictable ephemeral token generated on application startup, stored with restricted file permissions (`0600`), and required on all requests via `Authorization` header.

## 7. Validation Strategy
Contract testing verifying all methods defined in `docs/api/RPC_SPEC.md` against JSON-RPC 2.0 conformance.
