//! Native Desktop Application controller managing asynchronous engine-to-UI message decoupling.

use crate::state::{DesktopLogEntry, DesktopState, DesktopView, DownloadFilter};
use anyhow::{Context, Result};
use nexus_application::NexusEngine;
use nexus_domain::{DownloadTask, InterfaceId, PathLimits, ProxyProfile, TaskId};
use nexus_events::SystemEvent;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;
use url::Url;

/// Asynchronous controller coordinating UI actions with the underlying NEXUS application engine.
pub struct DesktopController {
    engine: Arc<NexusEngine>,
    state: Arc<RwLock<DesktopState>>,
}

impl DesktopController {
    #[must_use]
    pub fn new(engine: Arc<NexusEngine>) -> Self {
        let state = Arc::new(RwLock::new(DesktopState::default()));
        let controller = Self { engine, state };
        controller.spawn_event_listener();
        controller
    }

    /// Spawns background event bus subscriber pushing live updates into desktop state.
    fn spawn_event_listener(&self) {
        let mut subscriber = self.engine.event_bus().subscribe();
        let state = self.state.clone();

        tokio::spawn(async move {
            while let Ok(event) = subscriber.recv().await {
                let mut st = state.write().await;
                match &event {
                    SystemEvent::TaskCreated { task_id, timestamp } => {
                        st.logs.push(DesktopLogEntry {
                            timestamp: *timestamp,
                            level: "INFO".to_string(),
                            message: format!("Download task created: {task_id}"),
                        });
                    }
                    SystemEvent::TaskStatusChanged {
                        task_id,
                        old_status,
                        new_status,
                        timestamp,
                    } => {
                        st.logs.push(DesktopLogEntry {
                            timestamp: *timestamp,
                            level: "INFO".to_string(),
                            message: format!("Task {task_id}: {old_status} -> {new_status}"),
                        });
                    }
                    SystemEvent::PathFailed {
                        path_id,
                        reason,
                        timestamp,
                    } => {
                        st.logs.push(DesktopLogEntry {
                            timestamp: *timestamp,
                            level: "WARN".to_string(),
                            message: format!("Path failure on {path_id}: {reason}"),
                        });
                    }
                    SystemEvent::TaskCompleted {
                        task_id,
                        final_bytes,
                        timestamp,
                    } => {
                        st.logs.push(DesktopLogEntry {
                            timestamp: *timestamp,
                            level: "INFO".to_string(),
                            message: format!(
                                "Task {task_id} completed successfully ({final_bytes} bytes)"
                            ),
                        });
                    }
                    _ => {}
                }
            }
        });
    }

    /// Pulls latest snapshot from the engine into the desktop state model.
    pub async fn refresh(&self) -> Result<()> {
        let downloads = self.engine.list_downloads(None).await?;
        let interfaces = self.engine.network_manager().list_interfaces()?;
        let paths = self.engine.network_manager().list_paths().await;
        let proxies = self.engine.proxy_manager().list_profiles().await;
        let stats = self.engine.stats().await;
        let queue_active = self.engine.queue_manager().active_count().await;
        let queue_pending = self.engine.queue_manager().pending_count().await;

        let mut st = self.state.write().await;
        st.downloads = downloads;
        st.interfaces = interfaces;
        st.paths = paths;
        st.proxies = proxies;
        st.stats = stats;
        st.queue_active_count = queue_active;
        st.queue_pending_count = queue_pending;

        Ok(())
    }

    /// Switches the active desktop view.
    pub async fn switch_view(&self, view: DesktopView) {
        let mut st = self.state.write().await;
        st.active_view = view;
        info!(view = ?view, "Switched desktop active view");
    }

    /// Updates download filter tab.
    pub async fn set_download_filter(&self, filter: DownloadFilter) {
        let mut st = self.state.write().await;
        st.download_filter = filter;
    }

    /// Selects a task for inspector pane.
    pub async fn select_task(&self, task_id: Option<TaskId>) {
        let mut st = self.state.write().await;
        st.selected_task_id = task_id;
    }

    /// Adds a new download and optionally begins execution immediately.
    pub async fn add_download(
        &self,
        url: Url,
        filename: String,
        destination: String,
        priority: Option<u32>,
        auto_start: bool,
    ) -> Result<DownloadTask> {
        let task = self
            .engine
            .create_download_with_options(url, filename, destination, priority, None)
            .await
            .context("Failed to create download")?;

        if auto_start {
            self.engine.start_download(&task.id).await?;
        }

        self.refresh().await?;
        Ok(task)
    }

    /// Pauses an active download.
    pub async fn pause_download(&self, task_id: &TaskId) -> Result<()> {
        self.engine.pause_download(task_id).await?;
        self.refresh().await?;
        Ok(())
    }

    /// Resumes a paused download.
    pub async fn resume_download(&self, task_id: &TaskId) -> Result<()> {
        self.engine.resume_download(task_id).await?;
        self.refresh().await?;
        Ok(())
    }

    /// Cancels a download.
    pub async fn cancel_download(&self, task_id: &TaskId) -> Result<()> {
        self.engine.cancel_download(task_id).await?;
        self.refresh().await?;
        Ok(())
    }

    /// Adds a network path.
    pub async fn add_path(
        &self,
        name: String,
        interface_id: InterfaceId,
        priority: u8,
    ) -> Result<()> {
        self.engine
            .network_manager()
            .create_path(name, interface_id, PathLimits::default(), priority)
            .await?;
        self.refresh().await?;
        Ok(())
    }

    /// Adds a proxy profile.
    pub async fn add_proxy(&self, profile: ProxyProfile) -> Result<()> {
        let _ = self.engine.proxy_manager().register_profile(profile).await;
        self.refresh().await?;
        Ok(())
    }

    /// Retrieves an immutable copy of the reactive desktop state.
    pub async fn state(&self) -> DesktopState {
        self.state.read().await.clone()
    }

    /// Generates a rendered terminal/native presentation of the current active view.
    pub async fn render(&self) -> String {
        let st = self.state.read().await;
        let mut out = String::new();

        out.push_str(
            "╔══════════════════════════════════════════════════════════════════════════════╗\n",
        );
        out.push_str(
            "║                   NEXUS NATIVE DESKTOP SHELL (v0.1.0)                        ║\n",
        );
        out.push_str(
            "╠══════════════════════════════════════════════════════════════════════════════╣\n",
        );
        out.push_str(&format!(
            "║ Active View: [{:?}] | Active Downloads: {} | Queue: {} pending             ║\n",
            st.active_view, st.stats.active_downloads, st.queue_pending_count
        ));
        out.push_str(&format!(
            "║ Aggregate Throughput: {:.2} KB/s | Active Paths: {} | Connections: {}        ║\n",
            st.stats.aggregate_throughput_bps as f64 / 1024.0,
            st.stats.active_network_paths,
            st.stats.active_connections
        ));
        out.push_str(
            "╠══════════════════════════════════════════════════════════════════════════════╣\n",
        );

        match st.active_view {
            DesktopView::Dashboard | DesktopView::Downloads => {
                out.push_str("║ DOWNLOADS                                                                    ║\n");
                out.push_str("║ ID                                   STATUS       PROGRESS   FILENAME        ║\n");
                out.push_str("║ ──────────────────────────────────── ─────────── ────────── ──────────────── ║\n");
                if st.downloads.is_empty() {
                    out.push_str("║ (No downloads active or queued)                                              ║\n");
                } else {
                    for d in &st.downloads {
                        let prog = d
                            .progress_fraction()
                            .map(|f| format!("{:>5.1}%", f * 100.0))
                            .unwrap_or_else(|| "  -   ".to_string());
                        let name = if d.filename.len() > 16 {
                            format!("{}...", &d.filename[..13])
                        } else {
                            format!("{:<16}", d.filename)
                        };
                        out.push_str(&format!(
                            "║ {:<36} {:<11} {} {}\n",
                            d.id.to_string(),
                            d.status.to_string(),
                            prog,
                            name
                        ));
                    }
                }
            }
            DesktopView::NetworkMonitor => {
                out.push_str("║ NETWORK INTERFACES & CONFIGURED PATHS                                        ║\n");
                for iface in &st.interfaces {
                    let status = if iface.is_up { "UP" } else { "DOWN" };
                    out.push_str(&format!(
                        "║ Interface: {:<8} ({:?}) [{}] - IPs: {:?}\n",
                        iface.system_name, iface.interface_type, status, iface.ip_addresses
                    ));
                }
                out.push_str("║ Configured Paths:\n");
                for p in &st.paths {
                    out.push_str(&format!(
                        "║   Path: {:<16} Priority: {} Enabled: {}\n",
                        p.name, p.priority, p.enabled
                    ));
                }
            }
            DesktopView::ProxyManager => {
                out.push_str("║ CONFIGURED PROXY PROFILES                                                    ║\n");
                if st.proxies.is_empty() {
                    out.push_str("║ (No proxies configured)                                                      ║\n");
                } else {
                    for p in &st.proxies {
                        out.push_str(&format!(
                            "║ Profile: {:<16} Type: {:<8?} Endpoint: {}:{}\n",
                            p.name, p.proxy_type, p.host, p.port
                        ));
                    }
                }
            }
            DesktopView::Queue => {
                out.push_str("║ DOWNLOAD QUEUE & CONCURRENCY CONTROLLER                                      ║\n");
                out.push_str(&format!(
                    "║ Active Tasks: {} | Pending Tasks: {}\n",
                    st.queue_active_count, st.queue_pending_count
                ));
            }
            DesktopView::Scheduler => {
                out.push_str("║ OFF-PEAK TRANSFER SCHEDULER                                                  ║\n");
                out.push_str("║ Configured Policy: Daytime & Overnight Off-Peak Bandwidth Allocation         ║\n");
            }
            DesktopView::Settings => {
                out.push_str("║ SETTINGS & PREFERENCES                                                       ║\n");
                out.push_str("║ Native First, Local First, Zero Plaintext Passwords                          ║\n");
            }
            DesktopView::Logs => {
                out.push_str("║ RECENT DIAGNOSTIC LOGS                                                       ║\n");
                for l in st.logs.iter().rev().take(5) {
                    out.push_str(&format!(
                        "║ [{}] {}: {}\n",
                        l.timestamp.format("%H:%M:%S"),
                        l.level,
                        l.message
                    ));
                }
            }
        }

        out.push_str(
            "╚══════════════════════════════════════════════════════════════════════════════╝\n",
        );
        out
    }
}
