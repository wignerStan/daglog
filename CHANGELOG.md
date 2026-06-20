# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial Rust workspace template structure
- Tiered verification system (5 tiers: qa → qa-lint → qa-full → qa-security → qa-mutation)
- Lefthook git hooks (pre-commit: fmt + check, pre-push: clippy + unit tests)
- Nextest test runner with CI profile
- Coverage gate at 80% via cargo-llvm-cov
- Property-based testing scaffolding (proptest)
- Snapshot testing scaffolding (insta)
- Fuzzing scaffolding (cargo-fuzz)
- Mutation testing scaffolding (cargo-mutants)
- Benchmarking scaffolding (criterion)
- Clippy configuration with cognitive complexity threshold
- Cargo deny for license/advisory/ban/source policy
- Multi-tier CI pipeline (fmt → lint → test+coverage → build → docs → mutation)
- Security pipeline (gitleaks, trufflehog, cargo-audit, cargo-deny, CodeQL)
- Concurrency safety testing scaffolding (miri, tsan — commented)

---

## [1.0.0] - YYYY-MM-DD

### Initial Release
- Rust workspace architecture with apps/ and packages/
- Production-ready CI/CD pipeline with tiered verification
- Comprehensive testing infrastructure
- Security-first configuration
