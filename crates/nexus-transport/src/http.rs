//! HTTP/HTTPS transport client implementation using Reqwest and Rustls.

use async_trait::async_trait;
use futures::StreamExt;
use nexus_domain::{InterfaceId, NetworkPath, PathId};
use nexus_proxy::{format_authenticated_proxy_url, ProxyManager};
use nexus_security::CredentialStore;
use reqwest::header::{
    ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, ETAG, LAST_MODIFIED, RANGE,
};
use reqwest::{Client, Response, StatusCode};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use url::Url;

use crate::{ConnectionOptions, RemoteMetadata, TransportClient, TransportError};

#[derive(Error, Debug)]
pub enum HttpError {
    #[error("Reqwest network error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Server returned non-success HTTP status: {0}")]
    HttpStatus(StatusCode),

    #[error("Server rejected Range request with status {0} (expected 206 Partial Content)")]
    InvalidRangeStatus(StatusCode),

    #[error("Invalid Content-Range header value: '{0}'")]
    InvalidContentRange(String),
}

impl From<HttpError> for TransportError {
    fn from(err: HttpError) -> Self {
        match err {
            HttpError::Reqwest(e) => {
                if e.is_timeout() {
                    Self::ConnectionTimeout(Duration::from_secs(30))
                } else {
                    Self::Protocol(e.to_string())
                }
            }
            other => Self::Protocol(other.to_string()),
        }
    }
}

/// Production HTTP/HTTPS transport client with per-path proxy routing and interface IP binding.
#[derive(Clone)]
pub struct HttpTransportClient {
    default_client: Client,
    options: ConnectionOptions,
    proxy_manager: Option<Arc<ProxyManager>>,
    credential_store: Option<Arc<dyn CredentialStore>>,
    path_clients: Arc<RwLock<HashMap<PathId, Client>>>,
    interface_ips: Arc<RwLock<HashMap<InterfaceId, std::net::IpAddr>>>,
}

impl std::fmt::Debug for HttpTransportClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTransportClient")
            .field("options", &self.options)
            .finish()
    }
}

impl HttpTransportClient {
    /// Creates a new HTTP transport client with the specified connection options.
    pub fn new(options: &ConnectionOptions) -> Result<Self, TransportError> {
        let client = Self::build_reqwest_client(options, None)?;
        Ok(Self {
            default_client: client,
            options: options.clone(),
            proxy_manager: None,
            credential_store: None,
            path_clients: Arc::new(RwLock::new(HashMap::new())),
            interface_ips: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Creates an HTTP transport client integrated with a ProxyManager and optional CredentialStore.
    pub fn with_proxy_manager(
        options: &ConnectionOptions,
        proxy_manager: Arc<ProxyManager>,
        credential_store: Option<Arc<dyn CredentialStore>>,
    ) -> Result<Self, TransportError> {
        let client = Self::build_reqwest_client(options, None)?;
        Ok(Self {
            default_client: client,
            options: options.clone(),
            proxy_manager: Some(proxy_manager),
            credential_store,
            path_clients: Arc::new(RwLock::new(HashMap::new())),
            interface_ips: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Fluent builder method to attach proxy manager and optional credential store.
    #[must_use]
    pub fn with_proxy_config(
        mut self,
        proxy_manager: Arc<ProxyManager>,
        credential_store: Option<Arc<dyn CredentialStore>>,
    ) -> Self {
        self.proxy_manager = Some(proxy_manager);
        self.credential_store = credential_store;
        self
    }

    /// Registers a local IP address to bind sockets on a specific network interface.
    pub async fn register_interface_ip(&self, interface_id: InterfaceId, ip: std::net::IpAddr) {
        let mut ips = self.interface_ips.write().await;
        ips.insert(interface_id, ip);
    }

    fn build_reqwest_client(
        options: &ConnectionOptions,
        proxy: Option<reqwest::Proxy>,
    ) -> Result<Client, TransportError> {
        let mut builder = Client::builder()
            .connect_timeout(options.connect_timeout)
            .timeout(options.read_timeout)
            .user_agent(&options.user_agent)
            .redirect(reqwest::redirect::Policy::limited(10));

        if let Some(local_addr) = options.bind_local_addr {
            builder = builder.local_address(local_addr.ip());
        }

        if let Some(p) = proxy {
            builder = builder.proxy(p);
        }

        builder
            .build()
            .map_err(|e| TransportError::Protocol(format!("Failed to build HTTP client: {e}")))
    }

    /// Resolves the dedicated HTTP client configured for the specific NetworkPath.
    ///
    /// Guarantees that if a path specifies a proxy or interface IP binding, traffic strictly adheres to that path.
    async fn get_client_for_path(&self, path: &NetworkPath) -> Result<Client, TransportError> {
        // Check cached client for this PathId
        {
            let cache = self.path_clients.read().await;
            if let Some(client) = cache.get(&path.id) {
                return Ok(client.clone());
            }
        }

        let local_ip = {
            let ips = self.interface_ips.read().await;
            ips.get(&path.interface_id).copied()
        };

        if path.proxy_id.is_none() && local_ip.is_none() {
            return Ok(self.default_client.clone());
        }

        let reqwest_proxy = if let Some(ref proxy_id) = path.proxy_id {
            let mgr = self.proxy_manager.as_ref().ok_or_else(|| {
                TransportError::Protocol(format!(
                    "NetworkPath specifies proxy '{proxy_id}', but no ProxyManager is configured"
                ))
            })?;

            let profile = mgr.get_profile(proxy_id).await.ok_or_else(|| {
                TransportError::Protocol(format!("Configured proxy '{proxy_id}' not found"))
            })?;

            if !profile.enabled {
                return Err(TransportError::Protocol(format!(
                    "Configured proxy '{}' is disabled",
                    profile.name
                )));
            }

            let creds = if let (Some(ref cred_store), Some(ref cred_ref)) =
                (&self.credential_store, &profile.credential_reference)
            {
                cred_store.get_credential(cred_ref).await.map_err(|e| {
                    TransportError::Protocol(format!("Failed to retrieve proxy credentials: {e}"))
                })?
            } else {
                None
            };

            let cred_tuple = creds.as_ref().map(|(u, p)| (u.as_str(), p.as_str()));
            let proxy_url_str =
                format_authenticated_proxy_url(&profile, cred_tuple).map_err(|e| {
                    TransportError::Protocol(format!("Failed to format proxy URL: {e}"))
                })?;

            let p = reqwest::Proxy::all(&proxy_url_str).map_err(|e| {
                TransportError::Protocol(format!("Invalid proxy URL '{proxy_url_str}': {e}"))
            })?;
            Some(p)
        } else {
            None
        };

        let mut options = self.options.clone();
        if let Some(ip) = local_ip {
            options.bind_local_addr = Some(std::net::SocketAddr::new(ip, 0));
        }

        let client = Self::build_reqwest_client(&options, reqwest_proxy)?;

        let mut cache = self.path_clients.write().await;
        cache.insert(path.id, client.clone());

        Ok(client)
    }

    /// Helper to parse filename from Content-Disposition header.
    fn extract_filename_from_headers(res: &Response) -> Option<String> {
        if let Some(cd) = res.headers().get(CONTENT_DISPOSITION) {
            if let Ok(cd_str) = cd.to_str() {
                // Parse: filename="name.ext" or filename=name.ext
                for part in cd_str.split(';') {
                    let trimmed = part.trim();
                    if let Some(rest) = trimmed.strip_prefix("filename=") {
                        let name = rest.trim_matches('"').trim();
                        if !name.is_empty() {
                            return Some(name.to_string());
                        }
                    } else if let Some(rest) = trimmed.strip_prefix("filename*=") {
                        // RFC 5987: filename*=UTF-8''encoded_name
                        if let Some((_, encoded)) = rest.split_once("''") {
                            if let Some(decoded) = Self::decode_percent_encoded(encoded) {
                                return Some(decoded);
                            }
                        }
                    }
                }
            }
        }
        None
    }

    fn decode_percent_encoded(input: &str) -> Option<String> {
        let mut bytes = Vec::new();
        let mut chars = input.chars();
        while let Some(c) = chars.next() {
            if c == '%' {
                let h1 = chars.next()?;
                let h2 = chars.next()?;
                let hex_str = format!("{h1}{h2}");
                let b = u8::from_str_radix(&hex_str, 16).ok()?;
                bytes.push(b);
            } else {
                bytes.push(c as u8);
            }
        }
        String::from_utf8(bytes).ok()
    }

    /// Helper to parse total size from Content-Range header: `bytes start-end/total`
    fn parse_content_range_total(header_val: &str) -> Option<u64> {
        if let Some((_, total_str)) = header_val.split_once('/') {
            let total_str = total_str.trim();
            if total_str != "*" {
                return total_str.parse::<u64>().ok();
            }
        }
        None
    }
}

impl Default for HttpTransportClient {
    fn default() -> Self {
        Self::new(&ConnectionOptions::default()).expect("Default HttpTransportClient build failed")
    }
}

#[async_trait]
impl TransportClient for HttpTransportClient {
    async fn probe_metadata(
        &self,
        path: &NetworkPath,
        url: &Url,
    ) -> Result<RemoteMetadata, TransportError> {
        let client = self.get_client_for_path(path).await?;
        debug!(url = %url, path_id = %path.id, "Probing remote resource metadata via HEAD");

        // Attempt 1: Standard HEAD request
        let head_res = client.head(url.as_str()).send().await;

        let res = match head_res {
            Ok(r) if r.status().is_success() => r,
            Ok(r)
                if r.status() == StatusCode::METHOD_NOT_ALLOWED
                    || r.status() == StatusCode::NOT_IMPLEMENTED =>
            {
                debug!(status = %r.status(), "HEAD not allowed by remote server; falling back to Range GET probe");
                // Attempt 2: GET with Range: bytes=0-0 probe
                client
                    .get(url.as_str())
                    .header(RANGE, "bytes=0-0")
                    .send()
                    .await
                    .map_err(|e| TransportError::Protocol(format!("GET range probe failed: {e}")))?
            }
            Ok(r) => {
                warn!(status = %r.status(), "HEAD returned non-success; attempting GET range probe fallback");
                client
                    .get(url.as_str())
                    .header(RANGE, "bytes=0-0")
                    .send()
                    .await
                    .map_err(|e| {
                        TransportError::Protocol(format!("GET range probe fallback failed: {e}"))
                    })?
            }
            Err(e) => {
                warn!(error = %e, "HEAD request error; attempting GET range probe fallback");
                client
                    .get(url.as_str())
                    .header(RANGE, "bytes=0-0")
                    .send()
                    .await
                    .map_err(|e| {
                        TransportError::Protocol(format!("GET range probe fallback failed: {e}"))
                    })?
            }
        };

        let status = res.status();
        let redirected_url = if res.url() != url {
            Some(res.url().clone())
        } else {
            None
        };

        let headers = res.headers();

        // Check Range support
        let mut supports_ranges = false;
        if let Some(accept_ranges) = headers.get(ACCEPT_RANGES) {
            if let Ok(val) = accept_ranges.to_str() {
                supports_ranges = val.eq_ignore_ascii_case("bytes");
            }
        }
        if status == StatusCode::PARTIAL_CONTENT {
            supports_ranges = true;
        }

        // Determine total size
        let mut total_size = None;
        if status == StatusCode::PARTIAL_CONTENT {
            if let Some(cr) = headers.get(CONTENT_RANGE) {
                if let Ok(cr_str) = cr.to_str() {
                    total_size = Self::parse_content_range_total(cr_str);
                }
            }
        }
        if total_size.is_none() {
            if let Some(cl) = headers.get(CONTENT_LENGTH) {
                if let Ok(cl_str) = cl.to_str() {
                    total_size = cl_str.parse::<u64>().ok();
                }
            }
        }

        let etag = headers
            .get(ETAG)
            .and_then(|h| h.to_str().ok())
            .map(ToString::to_string);

        let last_modified = headers
            .get(LAST_MODIFIED)
            .and_then(|h| h.to_str().ok())
            .map(ToString::to_string);

        let suggested_filename = Self::extract_filename_from_headers(&res);

        debug!(
            supports_ranges = supports_ranges,
            total_size = ?total_size,
            etag = ?etag,
            "Probed metadata completed"
        );

        Ok(RemoteMetadata {
            total_size,
            supports_ranges,
            etag,
            last_modified,
            redirected_url,
            suggested_filename,
        })
    }

    async fn fetch_range(
        &self,
        path: &NetworkPath,
        url: &Url,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send>, TransportError> {
        let client = self.get_client_for_path(path).await?;
        let range_header = format!("bytes={start_offset}-{end_offset}");
        debug!(url = %url, path_id = %path.id, range = %range_header, "Executing HTTP range request");

        let response = client
            .get(url.as_str())
            .header(RANGE, range_header)
            .send()
            .await
            .map_err(|e| TransportError::Protocol(format!("HTTP Range request failed: {e}")))?;

        let status = response.status();
        if status != StatusCode::PARTIAL_CONTENT && !(status.is_success() && start_offset == 0) {
            return Err(TransportError::Protocol(format!(
                "Unexpected status for range request: {status}"
            )));
        }

        let byte_stream = response
            .bytes_stream()
            .map(|item| item.map_err(|e| std::io::Error::other(e.to_string())));

        let reader = StreamAsyncReader::new(byte_stream);
        Ok(Box::new(reader))
    }
}

/// Adapter converting a Stream of `Result<Bytes, io::Error>` into an `AsyncRead`.
pub struct StreamAsyncReader<S> {
    stream: S,
    current_chunk: Option<bytes::Bytes>,
}

impl<S> StreamAsyncReader<S> {
    #[must_use]
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            current_chunk: None,
        }
    }
}

impl<S> AsyncRead for StreamAsyncReader<S>
where
    S: futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        use bytes::Buf;

        loop {
            if let Some(mut chunk) = self.current_chunk.take() {
                if !chunk.is_empty() {
                    let to_write = chunk.len().min(buf.remaining());
                    buf.put_slice(&chunk[..to_write]);
                    chunk.advance(to_write);
                    if !chunk.is_empty() {
                        self.current_chunk = Some(chunk);
                    }
                    return Poll::Ready(Ok(()));
                }
            }

            match self.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    self.current_chunk = Some(bytes);
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Ready(None) => return Poll::Ready(Ok(())), // EOF
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_content_range_total() {
        assert_eq!(
            HttpTransportClient::parse_content_range_total("bytes 0-499/1234"),
            Some(1234)
        );
        assert_eq!(
            HttpTransportClient::parse_content_range_total("bytes 100-200/*"),
            None
        );
    }
}
