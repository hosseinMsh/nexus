//! Performance metrics and aggregated transfer statistics.

use serde::{Deserialize, Serialize};

/// High-level aggregated runtime metrics across all active transfers and network paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineStats {
    pub active_downloads: usize,
    pub queued_downloads: usize,
    pub paused_downloads: usize,
    pub completed_downloads: usize,
    pub failed_downloads: usize,
    pub total_downloaded_bytes: u64,
    pub aggregate_throughput_bps: u64,
    pub active_network_paths: usize,
    pub active_connections: usize,
}
