//! Dynamic Protocol Manager dispatching transfers to registered protocol adapters.

use nexus_domain::NetworkPath;
use nexus_transport::RemoteMetadata;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio::sync::RwLock;
use tracing::debug;
use url::Url;

use crate::{ProtocolAdapter, ProtocolError};

/// Central registry and dispatcher for protocol adapters.
#[derive(Clone, Default)]
pub struct ProtocolManager {
    adapters: Arc<RwLock<Vec<Arc<dyn ProtocolAdapter>>>>,
}

impl ProtocolManager {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a new protocol adapter.
    pub async fn register_adapter(&self, adapter: Arc<dyn ProtocolAdapter>) {
        let mut list = self.adapters.write().await;
        list.push(adapter);
    }

    /// Finds an adapter capable of handling the specified URL.
    pub async fn get_adapter(&self, url: &Url) -> Result<Arc<dyn ProtocolAdapter>, ProtocolError> {
        let list = self.adapters.read().await;
        for adapter in list.iter() {
            if adapter.can_handle(url) {
                return Ok(Arc::clone(adapter));
            }
        }

        Err(ProtocolError::UnsupportedScheme(url.scheme().to_string()))
    }

    /// Probes metadata using the matching protocol adapter.
    pub async fn probe_metadata(
        &self,
        path: &NetworkPath,
        url: &Url,
    ) -> Result<RemoteMetadata, ProtocolError> {
        let adapter = self.get_adapter(url).await?;
        debug!(url = %url, kind = ?adapter.kind(), "Dispatching metadata probe to adapter");
        adapter.probe_metadata(path, url).await
    }

    /// Fetches a byte range using the matching protocol adapter.
    pub async fn fetch_range(
        &self,
        path: &NetworkPath,
        url: &Url,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send>, ProtocolError> {
        let adapter = self.get_adapter(url).await?;
        debug!(
            url = %url,
            kind = ?adapter.kind(),
            start = start_offset,
            end = end_offset,
            "Dispatching range fetch to adapter"
        );
        adapter
            .fetch_range(path, url, start_offset, end_offset)
            .await
    }
}
