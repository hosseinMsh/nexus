# Security & Threat Verification Tests

This directory contains automated security regression tests:
- SSRF destination filters against RFC 1918, RFC 4193, loopback, and cloud metadata IPs.
- Filename sanitization against null byte injection, directory traversal (`..`), and reserved device names.
- Local JSON-RPC token authentication and unauthorized request rejection.
