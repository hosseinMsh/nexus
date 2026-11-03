# Performance & Throughput Benchmarks

This directory contains Criterion benchmarks measuring:
- Multi-path scheduling throughput under asymmetric bandwidth and artificial latency.
- Non-overlapping segment partitioning and work-stealing split overhead.
- Checksum hashing throughput (SHA-256 vs BLAKE3) on large files (> 10 GB).
- Memory consumption profiling (keeping idle memory < 100 MB and active < 300 MB).
