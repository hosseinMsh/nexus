# ADR-011: Credential Storage

- **Status:** Approved
- **Date:** 2026-11-03
- **Deciders:** NEXUS Architecture Team

---

## 1. Context
NEXUS requires credentials for SOCKS5 proxies, HTTP proxies, private FTP/SFTP servers, and authenticated HTTP endpoints.

## 2. Problem
How should sensitive credentials be stored at rest and protected from unauthorized access or accidental leakage?

## 3. Considered Alternatives
1. **Plaintext in SQLite / Config file:** Disqualified immediately by product requirements.
2. **Symmetric Encryption with Hardcoded / Local Key:** Vulnerable if key is extracted from binary or user filesystem.
3. **OS-Native Keychains (Keychain / DPAPI / Secret Service):** Utilizes hardware-backed and OS-managed user session keyrings.

## 4. Decision
We use **OS-Native Keychains** via `nexus-security`:
- **macOS:** Apple Keychain Services via Security framework.
- **Windows:** Windows Credential Manager / DPAPI.
- **Linux:** Freedesktop Secret Service API (with headless encrypted fallback when D-Bus is unavailable).
- Database entities store only opaque references (e.g., `cred_ref: "proxy:fast-socks5"`).

## 5. Consequences
- **Positive:** Industry-standard protection against database dumps, malware inspecting config files, and unauthorized access.
- **Negative:** Platform-specific keyring dependencies must be handled gracefully in headless or continuous integration environments.

## 6. Risks & Mitigation
- **Risk:** Headless server or Linux container lacks a running D-Bus Secret Service daemon.
- **Mitigation:** Implement an encrypted file keyring backend secured with a master password or environment variable for headless mode.

## 7. Validation Strategy
Unit tests in `nexus-security` testing storage, retrieval, and redaction across mock and platform keyrings.
