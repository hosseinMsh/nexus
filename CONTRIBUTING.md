# Contributing to NEXUS

Thank you for your interest in contributing to NEXUS! As an open-source, high-reliability download and transfer management platform, we hold code quality, security, and correctness to the highest standard.

---

## 1. Non-Negotiable Development Rules

Before contributing, please review our foundational development rules:

1. **No features without defined requirements:** Every functional change must map to an issue or PRD section.
2. **Strict separation of concerns:** Domain logic must never depend on UI frameworks or OS-specific APIs.
3. **Rust First & Native First:** No Electron, CEF, or WebView wrappers.
4. **No plain-text credentials:** Proxy and authentication credentials must integrate with OS keychains or secure memory buffers.
5. **No silent fallbacks:** Network paths and proxies must never silently fallback to default routes without explicit scheduler decisions.
6. **Zero unsafe Rust:** Unsafe Rust is strictly prohibited unless explicitly justified, reviewed, documented, and approved via an ADR.
7. **Comprehensive tests:** No code is merged without unit and integration tests covering both happy and failure paths.
8. **Never compromise correctness for benchmarks:** Flawless file recovery and integrity supersede raw transfer numbers.

---

## 2. Development Workflow

1. **Check existing issues or open an Architecture Discussion.**
2. **Follow the ADR process:** If your proposal introduces or modifies an architectural boundary, draft an ADR in `docs/decisions/`.
3. **Branch from `main`:** Use descriptive branch names (`feat/download-ranges`, `fix/macos-interface-enumeration`).
4. **Adhere to the Rust formatting and lint guidelines:**
   ```bash
   cargo fmt --check
   cargo clippy --workspace --all-targets -- -D warnings
   ```
5. **Run all tests:**
   ```bash
   cargo test --workspace
   ```
6. **Commit with Conventional Commits:** e.g., `feat(download): support Range header validation`, `fix(storage): correct wal checkpoint concurrency`.
7. **Submit a Pull Request:** Provide a clear description, rationale, affected crates, and verification evidence.

---

## 3. Workspace Layout

NEXUS is organized as a modular monolith in a Cargo workspace:
- `apps/`: Executables (`nexus-desktop`, `nexus-cli`).
- `crates/`: Decoupled, single-responsibility library crates.
- `tests/`: End-to-end and cross-cutting integration test suites.
- `docs/`: Comprehensive architecture specifications and ADRs.

Refer to `docs/development/SETUP.md` for a comprehensive local setup guide.
