# Security Architecture & Threat Model

**Document Version:** 1.0  
**Scope:** Core Engine, Network Routing, IPC, Credential Vault  

---

## 1. Threat Landscape & Mitigations

### 1.1. Server-Side Request Forgery (SSRF)
- **Threat:** An attacker crafts a malicious download URL or redirect targeting internal network resources (e.g., `http://169.254.169.254`, `http://127.0.0.1:8080`, router administrative portals).
- **Mitigation:**
  - `nexus-security` performs IP resolution pre-validation.
  - Connections to IPv4 loopback (`127.0.0.0/8`), IPv6 loopback (`::1`), link-local (`169.254.0.0/16`), private networks (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), and carrier-grade NAT are rejected by default.
  - Redirect chains re-verify every hop against SSRF rules before following.

### 1.2. Path Traversal & Filename Spoofing
- **Threat:** Malicious HTTP `Content-Disposition` headers containing relative path sequences (`../../etc/shadow`, `..\..\Windows\System32\cmd.exe`) or hidden control characters.
- **Mitigation:**
  - Filename sanitization strictly extracts the base filename component.
  - Strips path separators (`/`, `\`), null bytes, Windows reserved characters (`:`, `*`, `?`, `"`, `<`, `>`, `|`), and reserved device names (`CON`, `PRN`, `AUX`, `NUL`, `COM1-9`, `LPT1-9`).
  - Forces final storage within the explicitly designated download directory.

### 1.3. Credential Exposure
- **Threat:** Leakage of proxy authentication passwords via log files, heap inspection, or unencrypted database dumps.
- **Mitigation:**
  - SQLite database never stores plaintext passwords; only reference keys.
  - Secret credentials resolve at runtime from the operating system's native secure credential storage (Apple Keychain, Windows DPAPI/Credential Manager, Linux Secret Service).
  - Credentials implement custom `Debug` redaction (`[REDACTED]`).

### 1.4. Local IPC Hijacking
- **Threat:** Unprivileged local processes or browser scripts attempting to control NEXUS via loopback IPC.
- **Mitigation:**
  - The local JSON-RPC server binds strictly to `127.0.0.1` or OS local domain sockets (`AF_UNIX` / Named Pipes).
  - Every IPC request requires a cryptographically random, per-session authentication token (`Authorization: Bearer <token>`).
  - Browser extensions use explicit native messaging channels with signed extension IDs.
