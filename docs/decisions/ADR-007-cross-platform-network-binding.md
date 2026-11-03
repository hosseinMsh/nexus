# ADR-007: Cross-Platform Network Interface Binding

- **Status:** Approved
- **Date:** 2026-11-03
- **Deciders:** NEXUS Architecture Team

---

## 1. Context
Direct Multi-Path networking requires establishing concurrent TCP/UDP connections bound to specific physical or logical network interfaces (e.g., Wi-Fi `en0`, Ethernet `en1`, USB Cellular `en2`).

## 2. Problem
Operating systems provide fundamentally different APIs and security models for interface discovery and socket route binding. How can NEXUS abstract these cross-platform mechanisms reliably without requiring root/administrative privileges for basic transfers?

## 3. Considered Alternatives
1. **Rely on Default OS Routing:** Transmit all requests through the system default route. Disqualified because it prevents multi-path aggregation.
2. **Local Address Binding (IP level):** Bind the client socket to the specific local IP assigned to the desired interface (`bind()` to local IP).
3. **Interface Index / Name Socket Options:**
   - macOS: `IP_BOUND_IF` / `IPV6_BOUND_IF` socket options.
   - Linux: `SO_BINDTODEVICE` (requires CAP_NET_RAW on older kernels or unprivileged in newer Linux with unprivileged namespaces) or IP rule routing.
   - Windows: Bind to adapter local address or IP Helper API routing.

## 4. Decision
We implement a two-tiered binding strategy in `nexus-network-platform`:
1. **Tier 1 (Universal Non-Elevated):** Bind client TCP socket to the specific local IPv4/IPv6 address assigned to the target network interface.
2. **Tier 2 (Platform-Specific Socket Options):** Where supported without root (e.g. `IP_BOUND_IF` on macOS), set the interface index explicitly on the raw socket before connecting.

## 5. Consequences
- **Positive:** Multi-path downloads function out-of-the-box on macOS, Linux, and Windows for ordinary users without elevated permissions.
- **Negative:** IP address changes (e.g. DHCP renewal) require updating the interface-to-path mapping.

## 6. Risks & Mitigation
- **Risk:** Some routers/subnets drop packets if outbound source IP does not match the interface MAC route.
- **Mitigation:** Comprehensive path health pre-checks (`network.test_path`) that verify bidirectional connectivity before assigning segments.

## 7. Validation Strategy
Unit and platform tests checking interface enumeration and mock socket binding under `tests/network/`.
