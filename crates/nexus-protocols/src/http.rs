//! HTTP / HTTPS protocol adapter wrapping the core HTTP transport client.

use async_trait::async_trait;
use nexus_domain::NetworkPath;
use nexus_transport::{RemoteMetadata, TransportClient};
use std::sync::Arc;
use tokio::io::AsyncRead;
use url::Url;

use crate::{ProtocolAdapter, ProtocolError, ProtocolKind};

/// Protocol adapter for HTTP and HTTPS transfers.
#[derive(Clone)]
pub struct HttpProtocolAdapter {
    transport: Arc<dyn TransportClient>,
}

impl HttpProtocolAdapter {
    #[must_use]
    pub fn new(transport: Arc<dyn TransportClient>) -> Self {
        Self { transport }
    }
}

#[async_trait]
impl ProtocolAdapter for HttpProtocolAdapter {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::Http
    }

    fn can_handle(&self, url: &Url) -> bool {
        url.scheme() == "http" || url.scheme() == "https"
    }

    async fn probe_metadata(
        &self,
        path: &NetworkPath,
        url: &Url,
    ) -> Result<RemoteMetadata, ProtocolError> {
        self.transport
            .probe_metadata(path, url)
            .await
            .map_err(|e| ProtocolError::Protocol(format!("HTTP probe failed: {e}")))
    }

    async fn fetch_range(
        &self,
        path: &NetworkPath,
        url: &Url,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send>, ProtocolError> {
        self.transport
            .fetch_range(path, url, start_offset, end_offset)
            .await
            .map_err(|e| ProtocolError::Protocol(format!("HTTP fetch range failed: {e}")))
    }
}
