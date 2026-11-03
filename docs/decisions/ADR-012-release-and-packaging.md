# ADR-012: Release and Packaging

- **Status:** Approved
- **Date:** 2026-11-03
- **Deciders:** NEXUS Architecture Team

---

## 1. Context
NEXUS targets macOS, Windows, and Linux. End users require simple, native installation packages, while advanced users require single-binary portable CLI and desktop packages.

## 2. Problem
How should release artifacts be built, signed, and distributed across platforms?

## 3. Considered Alternatives
1. **Container images only (Docker):** Unsuitable for a native desktop application managing native network interfaces.
2. **Raw archive bundles (`.tar.gz` / `.zip`):** Functional for CLI, but lacks native OS installer workflows for desktop users.
3. **Native Platform Packages + Standalone Binaries:**
   - macOS: DMG / `.app` bundle, universal binary (Apple Silicon + Intel x86_64), notarization.
   - Windows: MSI installer / WiX / NSIS and portable `.zip`.
   - Linux: AppImage, `.deb`, `.rpm`, and portable tarball.

## 4. Decision
We adopt **Native Platform Packages + Standalone Binaries**:
- Release builds automated via GitHub Actions release workflows.
- Standalone CLI binaries distributed for all supported architectures (`x86_64` and `aarch64`).
- Checksums (SHA-256) published for every release artifact.

## 5. Consequences
- **Positive:** Frictionless end-user installation, native desktop integration, verified authenticity.
- **Negative:** Requires code signing certificates and notarization workflows for macOS and Windows.

## 6. Risks & Mitigation
- **Risk:** Cross-compilation complexities with SQLite and platform network APIs.
- **Mitigation:** Native runners for each OS in CI/CD pipeline rather than cross-compiling from a single Linux host.

## 7. Validation Strategy
Automated smoke tests on clean CI runner environments verifying package installation, launch, and uninstallation.
