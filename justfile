# Justfile for jsonldag — single-crate publishable library.
# Mirrors the rust_v1.0_mono template's quality gates, adapted for one crate.

set shell := ["bash", "-c"]
COVERAGE_THRESHOLD := "80"

# Default: show available commands.
default:
    @just --list

# === TIER 1: fast quality gates ===

# Format check.
fmt:
    cargo fmt --all -- --check

# Apply formatting.
fmt-fix:
    cargo fmt --all

# Strict clippy (all targets + features, -D warnings — the `cargo lint` alias).
lint:
    cargo clippy --all-targets --all-features --locked -- -D warnings

# Quick compile check.
check:
    cargo check --all-features

# === TIER 2: tests ===

# Run the unit + doc test suite.
test:
    cargo test --all-features

# Run tests with cargo-nextest if available, else fall back to libtest.
test-nextest:
    @command -v cargo-nextest >/dev/null 2>&1 && cargo nextest run --all-features || cargo test --all-features

# Doc tests only.
test-doc:
    cargo test --doc --all-features

# Verify the crate compiles WITHOUT default features (no `uuid`).
check-no-default:
    cargo check --no-default-features

# === TIER 3: build artifacts ===

# Build the library.
build:
    cargo build --all-features

# Release build.
build-release:
    cargo build --release --all-features

# Build the docs.
docs:
    cargo doc --no-deps --all-features

# === PUBLISH ===

# Dry-run packaging (verifies the published artifact).
package:
    cargo package --no-verify

# Full package verification (builds + tests inside the package).
package-verify:
    cargo package

# === COMPOSITE GATES ===

# Everything CI runs locally before a push.
qa: fmt lint test check-no-default
    @echo "✓ qa: fmt + lint + test + no-default all green"

# One-shot local verification mirroring CI.
ci-local: qa build docs
    @echo "✓ ci-local: full local CI gate green"

# === SECURITY (optional, if the tools are installed) ===

# Dependency advisories (requires cargo-audit).
audit:
    @command -v cargo-audit >/dev/null 2>&1 && cargo audit || echo "cargo-audit not installed; skipping"

# License/ban/source policy (requires cargo-deny).
deny:
    @command -v cargo-deny >/dev/null 2>&1 && cargo deny check || echo "cargo-deny not installed; skipping"
