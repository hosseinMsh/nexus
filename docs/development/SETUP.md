# Developer Setup Guide

This guide describes how to configure your development environment to build, test, and contribute to NEXUS.

---

## 1. Prerequisites

### Rust Toolchain
NEXUS requires Rust 1.80 or later. We recommend using `rustup`:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable
rustup component add rustfmt clippy
```

### OS Dependencies
- **macOS:** Xcode command-line tools (`xcode-select --install`).
- **Linux (Debian/Ubuntu):**
  ```bash
  sudo apt update
  sudo apt install build-essential pkg-config libssl-dev
  ```
- **Windows:** Visual Studio C++ Build Tools.

---

## 2. Repository Setup

Clone the repository and inspect the workspace:
```bash
git clone https://github.com/nexus-transfer/nexus.git
cd nexus
```

Verify your workspace builds cleanly:
```bash
cargo build --workspace
```

---

## 3. Quality & Lint Verification

Before submitting code, always run the full verification pipeline:

```bash
# 1. Format check
cargo fmt --all --check

# 2. Clippy check with zero warnings allowed
cargo clippy --workspace --all-targets --all-features -- -D warnings

# 3. Test suite
cargo test --workspace --all-features
```

---

## 4. Running the CLI & Desktop Binaries

To test the CLI application locally:
```bash
cargo run -p nexus-cli -- --help
cargo run -p nexus-cli -- network interfaces
cargo run -p nexus-cli -- network paths
```

To run the native desktop shell:
```bash
cargo run -p nexus-desktop
```
