//! Low-level socket transport abstractions, interface socket binding, and connection traits.

use async_trait::async_trait;
use nexus_domain::NetworkPath;
use std::net::SocketAddr;
use std::time::Duration;
use thiserror::Error;
use url::Url;

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("Connection timeout after {0:?}")]
    ConnectionTimeout(Duration),

    #[error("Failed to bind socket to local interface address: {0}")]
    BindError(String),

    #[error("DNS resolution failed for {0}: {1}")]
    DnsError(String, String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Transport protocol error: {0}")]
    Protocol(String),
}

/// Metadata obtained from probing a remote resource before starting a download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteMetadata {
    pub total_size: Option<u64>,
    pub supports_ranges: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub redirected_url: Option<Url>,
    pub suggested_filename: Option<String>,
}

/// Abstract network transport client capable of executing range requests across designated paths.
#[async_trait]
pub trait TransportClient: Send + Sync {
    /// Inspects remote metadata (via HEAD or ranged GET fallback) without downloading the body.
    async fn probe_metadata(
        &self,
        path: &NetworkPath,
        url: &Url,
    ) -> Result<RemoteMetadata, TransportError>;

    /// Initiates a ranged stream transfer for the byte interval `[start, end]`.
    async fn fetch_range(
        &self,
        path: &NetworkPath,
        url: &Url,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Box<dyn tokio::io::AsyncRead + Unpin + Send>, TransportError>;
}

/// Connection options configured per transport stream.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub bind_local_addr: Option<SocketAddr>,
    pub user_agent: String,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(15),
            read_timeout: Duration::from_secs(30),
            bind_local_addr: None,
            user_agent: "NEXUS/1.0 (Native; Multi-Path)".to_string(),
        }
    }
}
