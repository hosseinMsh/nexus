//! Desktop application reactive state and view models.

use nexus_application::EngineStats;
use nexus_domain::{DownloadTask, NetworkInterface, NetworkPath, ProxyProfile, TaskId};
use serde::{Deserialize, Serialize};

/// High-level navigation views in the NEXUS Desktop Application (PRD Section 4.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum DesktopView {
    #[default]
    Dashboard,
    Downloads,
    NetworkMonitor,
    ProxyManager,
    Queue,
    Scheduler,
    Settings,
    Logs,
}

/// Download category filter in the Downloads view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DownloadFilter {
    #[default]
    All,
    Active,
    Paused,
    Completed,
    Failed,
}

/// Log notification entry recorded by the desktop shell.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesktopLogEntry {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub level: String,
    pub message: String,
}

/// Reactive snapshot of the desktop UI state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesktopState {
    pub active_view: DesktopView,
    pub download_filter: DownloadFilter,
    pub selected_task_id: Option<TaskId>,
    pub downloads: Vec<DownloadTask>,
    pub interfaces: Vec<NetworkInterface>,
    pub paths: Vec<NetworkPath>,
    pub proxies: Vec<ProxyProfile>,
    pub stats: EngineStats,
    pub queue_active_count: usize,
    pub queue_pending_count: usize,
    pub logs: Vec<DesktopLogEntry>,
}

impl Default for DesktopState {
    fn default() -> Self {
        Self {
            active_view: DesktopView::Dashboard,
            download_filter: DownloadFilter::All,
            selected_task_id: None,
            downloads: Vec::new(),
            interfaces: Vec::new(),
            paths: Vec::new(),
            proxies: Vec::new(),
            stats: EngineStats {
                active_downloads: 0,
                queued_downloads: 0,
                paused_downloads: 0,
                completed_downloads: 0,
                failed_downloads: 0,
                total_downloaded_bytes: 0,
                aggregate_throughput_bps: 0,
                active_network_paths: 0,
                active_connections: 0,
            },
            queue_active_count: 0,
            queue_pending_count: 0,
            logs: Vec::new(),
        }
    }
}
