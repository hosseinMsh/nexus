# ADR-009: Multi-Path Scheduling

- **Status:** Approved
- **Date:** 2026-11-03
- **Deciders:** NEXUS Architecture Team

---

## 1. Context
NEXUS manages concurrent downloads across multiple network paths with varying bandwidth, latency, and reliability characteristics. A naive round-robin or equal segment distribution severely degrades performance when one path is slower or lossy.

## 2. Problem
How should segments be scheduled and dynamically distributed across multiple paths to maximize aggregated throughput while preventing slower paths from stalling completion?

## 3. Considered Alternatives
1. **Static Equal Splitting:** Divide file into $N$ equal parts and assign one part to each interface. Flawed: the slowest interface blocks the entire download completion (head-of-line blocking).
2. **Fixed Round-Robin Chunking:** Fixed-size chunks requested sequentially across paths. Inefficient on asymmetric paths.
3. **Weighted Adaptive Segment Scheduling:** Continuously measure real-time path throughput, latency, and failure rates. Dynamically size upcoming segments according to path bandwidth capability, and steal/reallocate remaining tail bytes from slow/stalled paths.

## 4. Decision
We implement **Weighted Adaptive Segment Scheduling** with dynamic segment tail-splitting in `nexus-path-scheduler`:
- Dynamically calculate path scores based on EMA (Exponential Moving Average) throughput and latency.
- Fast paths receive larger continuous byte intervals; slower paths receive smaller intervals.
- End-of-transfer "work stealing": When healthy fast paths finish their segments and idle while a slow path is still transferring a large segment, the scheduler splits the remaining byte interval of the pending segment and transfers the latter portion over the fast path (cancelling the truncated tail on the slower worker).

## 5. Consequences
- **Positive:** Maximum utilization of asymmetric connections (e.g. 100 Mbps Wi-Fi + 20 Mbps LTE). Eliminates tail latency stalls.
- **Negative:** Increased scheduling logic complexity and requires careful segment boundary synchronization.

## 6. Risks & Mitigation
- **Risk:** Rapid path switching / flapping due to temporary network jitter.
- **Mitigation:** Hysteresis thresholds and minimum segment chunk sizes (e.g., minimum 1 MB per segment).

## 7. Validation Strategy
Simulation tests with variable artificial latency and bandwidth constraints verifying throughput convergence.
