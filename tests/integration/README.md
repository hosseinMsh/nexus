# End-to-End Integration Tests

This directory houses cross-crate integration tests exercising end-to-end download flows:
- HTTP range request validation and Content-Range edge cases.
- Multi-connection segment assembly without byte corruption.
- Real-time progress updates across the event bus.
