//! Core domain models and business rules for NEXUS.
//!
//! This crate contains pure domain entities, state enumerations, and validation logic.
//! It has zero dependencies on operating system APIs, file I/O, or presentation frameworks.

use chrono::{DateTime, Utc};
use nexus_core::{ByteSize, Id};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::IpAddr;
use std::time::Duration;
use thiserror::Error;
use url::Url;

// Type-safe ID aliases
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TaskTag;
pub type TaskId = Id<TaskTag>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SegmentTag;
pub type SegmentId = Id<SegmentTag>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct InterfaceTag;
pub type InterfaceId = Id<InterfaceTag>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct PathTag;
pub type PathId = Id<PathTag>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ProxyTag;
pub type ProxyId = Id<ProxyTag>;

/// The lifecycle state of a download task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DownloadStatus {
    Created,
    Queued,
    Preparing,
    Downloading,
    Paused,
    Recovering,
    Verifying,
    Completed,
    Failed,
    Cancelled,
}

impl DownloadStatus {
    /// Returns true if the status is active and progressing.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Preparing | Self::Downloading | Self::Recovering | Self::Verifying
        )
    }

    /// Returns true if the status is in a final, terminal state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for DownloadStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Created => write!(f, "Created"),
            Self::Queued => write!(f, "Queued"),
            Self::Preparing => write!(f, "Preparing"),
            Self::Downloading => write!(f, "Downloading"),
            Self::Paused => write!(f, "Paused"),
            Self::Recovering => write!(f, "Recovering"),
            Self::Verifying => write!(f, "Verifying"),
            Self::Completed => write!(f, "Completed"),
            Self::Failed => write!(f, "Failed"),
            Self::Cancelled => write!(f, "Cancelled"),
        }
    }
}

/// Core domain entity representing a download task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadTask {
    pub id: TaskId,
    pub url: Url,
    pub filename: String,
    pub destination_dir: String,
    pub total_size: Option<ByteSize>,
    pub downloaded_size: ByteSize,
    pub status: DownloadStatus,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub category: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub policy: DownloadPolicy,
}

impl DownloadTask {
    /// Creates a new download task in the `Created` state.
    #[must_use]
    pub fn new(url: Url, filename: String, destination_dir: String) -> Self {
        let now = Utc::now();
        Self {
            id: TaskId::new(),
            url,
            filename,
            destination_dir,
            total_size: None,
            downloaded_size: ByteSize::ZERO,
            status: DownloadStatus::Created,
            etag: None,
            last_modified: None,
            category: None,
            created_at: now,
            updated_at: now,
            policy: DownloadPolicy::default(),
        }
    }

    /// Calculates current progress as a fraction between 0.0 and 1.0 if total size is known.
    #[must_use]
    pub fn progress_fraction(&self) -> Option<f64> {
        self.total_size.and_then(|tot| {
            if tot.bytes() == 0 {
                None
            } else {
                Some((self.downloaded_size.bytes() as f64) / (tot.bytes() as f64))
            }
        })
    }
}

/// Download execution policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadPolicy {
    pub max_connections: u32,
    pub max_speed_bps: Option<u64>,
    pub retry_limit: u32,
    pub auto_resume: bool,
}

impl Default for DownloadPolicy {
    fn default() -> Self {
        Self {
            max_connections: 8,
            max_speed_bps: None,
            retry_limit: 5,
            auto_resume: true,
        }
    }
}

/// Status of an individual byte range segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SegmentStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

/// A specific non-overlapping byte range within a download task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadSegment {
    pub id: SegmentId,
    pub download_id: TaskId,
    pub start_offset: u64,
    pub end_offset: u64,
    pub downloaded_bytes: u64,
    pub assigned_path_id: Option<PathId>,
    pub status: SegmentStatus,
    pub retry_count: u32,
}

impl DownloadSegment {
    #[must_use]
    pub fn new(download_id: TaskId, start_offset: u64, end_offset: u64) -> Self {
        Self {
            id: SegmentId::new(),
            download_id,
            start_offset,
            end_offset,
            downloaded_bytes: 0,
            assigned_path_id: None,
            status: SegmentStatus::Pending,
            retry_count: 0,
        }
    }

    /// Returns the total byte size of this segment (inclusive: [start, end]).
    #[must_use]
    pub const fn total_bytes(&self) -> u64 {
        if self.end_offset >= self.start_offset {
            (self.end_offset - self.start_offset) + 1
        } else {
            0
        }
    }

    /// Returns the remaining uncompleted bytes for this segment.
    #[must_use]
    pub const fn remaining_bytes(&self) -> u64 {
        let tot = self.total_bytes();
        tot.saturating_sub(self.downloaded_bytes)
    }
}

/// Classification of physical and logical network interfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InterfaceType {
    Wifi,
    Ethernet,
    Cellular,
    Loopback,
    Other,
}

/// Physical or logical network interface detected on the host OS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkInterface {
    pub id: InterfaceId,
    pub system_name: String,
    pub display_name: String,
    pub interface_type: InterfaceType,
    pub ip_addresses: Vec<IpAddr>,
    pub is_up: bool,
    pub is_default_route: bool,
    pub mac_address: Option<String>,
}

/// Supported proxy types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProxyType {
    Http,
    Https,
    Socks4,
    Socks4a,
    Socks5,
}

/// DNS resolution behavior for proxy connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DnsPolicy {
    Local,
    Remote,
    Auto,
}

/// Proxy profile entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyProfile {
    pub id: ProxyId,
    pub name: String,
    pub proxy_type: ProxyType,
    pub host: String,
    pub port: u16,
    pub credential_reference: Option<String>,
    pub enabled: bool,
    pub dns_policy: DnsPolicy,
}

/// Network path limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PathLimits {
    pub max_connections: Option<u32>,
    pub max_bandwidth_bps: Option<u64>,
}

/// A configured transfer route combining an interface and optional proxy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkPath {
    pub id: PathId,
    pub name: String,
    pub interface_id: InterfaceId,
    pub proxy_id: Option<ProxyId>,
    pub priority: u8,
    pub enabled: bool,
    pub limits: PathLimits,
}

/// Aggregated transfer performance metrics.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
pub struct TransferStatistics {
    pub instant_throughput_bps: f64,
    pub average_throughput_bps: f64,
    pub total_bytes_transferred: u64,
    pub elapsed: Duration,
    pub active_connections: usize,
}

/// Domain errors.
#[derive(Error, Debug)]
pub enum DomainError {
    #[error("Invalid URL: {0}")]
    Url(String),

    #[error("Invalid filename: {0}")]
    Filename(String),

    #[error("Invalid segment range: start {start} > end {end}")]
    SegmentRange { start: u64, end: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_download_task_creation_and_progress() {
        let url = Url::parse("https://example.com/file.zip").unwrap();
        let mut task = DownloadTask::new(url, "file.zip".to_string(), "/tmp".to_string());
        assert_eq!(task.status, DownloadStatus::Created);
        assert_eq!(task.progress_fraction(), None);

        task.total_size = Some(ByteSize(1000));
        task.downloaded_size = ByteSize(250);
        assert_eq!(task.progress_fraction(), Some(0.25));
    }

    #[test]
    fn test_segment_byte_calculations() {
        let task_id = TaskId::new();
        let seg = DownloadSegment::new(task_id, 100, 199);
        assert_eq!(seg.total_bytes(), 100);
        assert_eq!(seg.remaining_bytes(), 100);
    }
}
