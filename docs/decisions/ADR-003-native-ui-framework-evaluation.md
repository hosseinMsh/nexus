# ADR-003: Native UI Framework Evaluation

- **Status:** Approved
- **Date:** 2026-11-03
- **Deciders:** NEXUS Architecture Team

---

## 1. Context
NEXUS is committed to a **Native First** desktop experience. Using Electron, Chromium Embedded Framework (CEF), or Tauri/WebView is explicitly rejected due to memory bloat, high idle CPU utilization, and lack of fine-grained rendering control.

## 2. Problem
Which native Rust graphical user interface (GUI) framework should power `nexus-desktop`?

## 3. Considered Alternatives
1. **egui:** Immediate-mode GUI framework with high rendering performance, minimal dependencies, and mature cross-platform integration via `eframe` (WGPU/glow).
2. **iced:** Retained-mode, Elm-inspired GUI framework with strict reactive state management and native look-and-feel.
3. **Slint:** Declarative UI toolkit with a specialized markup DSL.
4. **WebView / Web-based shells:** Disqualified by core product principles.

## 4. Decision
We establish a structured prototype evaluation between **egui** and **iced** during Phase 0 and Phase 7. The evaluation criteria are:
- Memory footprint under high download queue loads (< 100 MB target).
- 60 FPS rendering of dynamic transfer graphs without dropped frames.
- System tray, native dialogs, and accessibility support.
- Keyboard navigation and shortcut handling.
- Maintainability of decoupled state updates from the async download engine.

The core desktop architecture in `apps/nexus-desktop` is designed with an engine-to-UI message channel interface so that the UI presentation layer is completely decoupled from download domain logic.

## 5. Consequences
- **Positive:** Extreme memory efficiency, near-instantaneous startup, zero web runtime overhead.
- **Negative:** Native Rust UI ecosystems are evolving; custom widgets (like complex segmented download progress bars) must be implemented directly.

## 6. Risks & Mitigation
- **Risk:** Framework limitations in platform accessibility APIs.
- **Mitigation:** Abstract UI event loops behind clean application service traits and evaluate accessibility primitives in Phase 7.

## 7. Validation Strategy
Benchmark prototypes on macOS, Windows, and Linux measuring memory consumption, render frame rates, and OS integration.
