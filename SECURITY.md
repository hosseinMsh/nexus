# Security Policy

NEXUS takes the security and privacy of its users very seriously. As a native network application capable of handling multi-interface routing, proxy authentication, and high-volume data transfers, security is designed into every layer.

---

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

---

## Reporting a Vulnerability

If you discover a security vulnerability in NEXUS, please report it privately. **Do not create public GitHub issues for security vulnerabilities.**

### Contact Information

Please email your security disclosure to `security@nexus-transfer.org` (or contact project maintainers securely) with:
- Description of the vulnerability.
- Steps or proof-of-concept to reproduce the vulnerability.
- Affected platforms (macOS, Windows, Linux).
- Any potential impact on user data or system integrity.

### Disclosure Policy

- Maintainers will acknowledge receipt within 48 hours.
- A remediation timeline will be provided within 5 business days.
- Fixes will be coordinated and released with an advisory.

---

## Security Architecture Principles

1. **Local-First & Privacy First:** Zero mandatory cloud connectivity, zero user telemetry without explicit consent, zero telemetry of URLs or downloaded content.
2. **SSRF Protection:** By default, loopback and private network IP ranges (RFC 1918, RFC 4193, CGNAT) are blocked from arbitrary URL fetches unless explicitly enabled for internal environments.
3. **Safe Path & Filename Sanitization:** Content-Disposition headers and URLs are strictly sanitized against path traversal (`../`, absolute paths, null bytes, alternate data streams).
4. **Credential Isolation:** Proxy passwords and server credentials are never stored in plaintext SQLite databases; they use OS Keychain (macOS), DPAPI/Credential Manager (Windows), and Secret Service/Keyutils (Linux).
5. **Memory Safety:** Unsafe code is forbidden (`#![deny(unsafe_code)]`).
