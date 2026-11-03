# Crash Resilience & Recovery Tests

This directory contains failure injection and recovery verification:
- Unexpected process termination (`kill -9`) mid-transfer.
- State reconstruction from SQLite WAL logs on restart.
- Verification that committed byte offsets match disk content with zero corruption.
