# ADR-002: Rust Workspace Design

- **Status:** Approved
- **Date:** 2026-11-03
- **Deciders:** NEXUS Architecture Team

---

## 1. Context
NEXUS requires fine-grained crate boundaries to isolate operating system platform calls, network transports, core business models, and user presentation.

## 2. Problem
How should the Cargo workspace be organized to optimize compilation times, enforce strict architectural boundaries, and allow seamless integration across internal components?

## 3. Considered Alternatives
1. **Flat crates directory:** Placing all crates and binaries in a single `crates/` folder without distinction.
2. **Layered Directory Structure (`apps/`, `crates/`, `platforms/`, `tests/`):** Grouping user-facing entry points in `apps/`, internal reusable logic in `crates/`, platform bindings in `platforms/`, and cross-crate verification in `tests/`.

## 4. Decision
We adopt the **Layered Workspace Structure** utilizing Cargo Workspace resolver `"2"` and centralizing shared dependencies in `[workspace.dependencies]`.
The layout consists of:
- `apps/nexus-desktop`, `apps/nexus-cli`
- 19 specialized crates under `crates/`
- Integration and regression suites under `tests/`

## 5. Consequences
- **Positive:** Dependency version drift is completely eliminated. Compilation units can be cached and checked in parallel by `cargo`. Clean boundaries encourage test-driven development.
- **Negative:** Adding a new shared dependency requires declaring it in root `Cargo.toml`.

## 6. Risks & Mitigation
- **Risk:** Too many crates could increase linking overhead.
- **Mitigation:** Crates are structured logically by domain responsibility, avoiding micro-crates that contain only trivial helpers.

## 7. Validation Strategy
Verified using `cargo check --workspace` and `cargo test --workspace`.
