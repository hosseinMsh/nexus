#!/usr/bin/env bash
set -euo pipefail

echo "========================================="
echo "   NEXUS Quality Verification Suite     "
echo "========================================="

echo "[1/4] Checking code formatting..."
cargo fmt --all --check

echo "[2/4] Running Clippy linter..."
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "[3/4] Running all workspace tests..."
cargo test --workspace --all-features

echo "[4/4] Verifying documentation builds cleanly..."
cargo doc --workspace --no-deps

echo "========================================="
echo "   All verification checks passed!       "
echo "========================================="
