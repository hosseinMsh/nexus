//! WebDAV (RFC 4918) protocol adapter with PROPFIND metadata querying and range downloads.

use async_trait::async_trait;
use nexus_domain::NetworkPath;
use nexus_transport::{RemoteMetadata, TransportClient};
use std::sync::Arc;
use tokio::io::AsyncRead;
use url::Url;

use crate::{ProtocolAdapter, ProtocolError, ProtocolKind};

/// Parsed WebDAV resource properties from a PROPFIND response.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WebDavProperties {
    pub content_length: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub is_collection: bool,
}

/// Parses standard WebDAV XML multistatus response.
#[must_use]
pub fn parse_propfind_xml(xml: &str) -> WebDavProperties {
    let mut props = WebDavProperties::default();

    // Check for collection: <D:collection/> or <collection/>
    if xml.contains("<D:collection") || xml.contains("<collection") {
        props.is_collection = true;
    }

    // Extract content length: <D:getcontentlength>12345</D:getcontentlength>
    if let Some(len_str) = extract_tag_value(xml, "getcontentlength") {
        props.content_length = len_str.parse::<u64>().ok();
    }

    // Extract etag: <D:getetag>"abcd"</D:getetag>
    if let Some(etag_str) = extract_tag_value(xml, "getetag") {
        props.etag = Some(etag_str.trim_matches('"').to_string());
    }

    // Extract last modified: <D:getlastmodified>...</D:getlastmodified>
    if let Some(lm_str) = extract_tag_value(xml, "getlastmodified") {
        props.last_modified = Some(lm_str.to_string());
    }

    props
}

fn extract_tag_value<'a>(xml: &'a str, tag_name: &str) -> Option<&'a str> {
    // Look for both <D:tag> and <tag>
    let patterns = [format!("<D:{tag_name}>"), format!("<{tag_name}>")];
    let close_patterns = [format!("</D:{tag_name}>"), format!("</{tag_name}>")];

    for (open, close) in patterns.iter().zip(close_patterns.iter()) {
        if let Some(start) = xml.find(open) {
            let content_start = start + open.len();
            if let Some(end) = xml[content_start..].find(close) {
                return Some(xml[content_start..content_start + end].trim());
            }
        }
    }

    None
}

/// WebDAV protocol adapter.
#[derive(Clone)]
pub struct WebDavProtocolAdapter {
    transport: Arc<dyn TransportClient>,
}

impl WebDavProtocolAdapter {
    #[must_use]
    pub fn new(transport: Arc<dyn TransportClient>) -> Self {
        Self { transport }
    }

    /// Converts webdav:// or webdavs:// URL into standard http:// or https:// for underlying transport.
    pub fn to_http_url(url: &Url) -> Result<Url, ProtocolError> {
        let scheme = match url.scheme() {
            "webdav" => "http",
            "webdavs" => "https",
            "http" | "https" => return Ok(url.clone()),
            other => return Err(ProtocolError::UnsupportedScheme(other.to_string())),
        };

        let raw = url.as_str();
        if let Some((_, rest)) = raw.split_once("://") {
            let replaced = format!("{scheme}://{rest}");
            Url::parse(&replaced)
                .map_err(|e| ProtocolError::Protocol(format!("Invalid mapped URL: {e}")))
        } else {
            Err(ProtocolError::Protocol(
                "Malformed URL structure".to_string(),
            ))
        }
    }
}

#[async_trait]
impl ProtocolAdapter for WebDavProtocolAdapter {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::WebDav
    }

    fn can_handle(&self, url: &Url) -> bool {
        url.scheme() == "webdav" || url.scheme() == "webdavs"
    }

    async fn probe_metadata(
        &self,
        path: &NetworkPath,
        url: &Url,
    ) -> Result<RemoteMetadata, ProtocolError> {
        let http_url = Self::to_http_url(url)?;

        // Probe via HTTP transport
        self.transport
            .probe_metadata(path, &http_url)
            .await
            .map_err(|e| ProtocolError::Protocol(format!("WebDAV probe failed: {e}")))
    }

    async fn fetch_range(
        &self,
        path: &NetworkPath,
        url: &Url,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send>, ProtocolError> {
        let http_url = Self::to_http_url(url)?;

        self.transport
            .fetch_range(path, &http_url, start_offset, end_offset)
            .await
            .map_err(|e| ProtocolError::Protocol(format!("WebDAV range fetch failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_webdav_propfind_xml() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/remote.php/webdav/test.pdf</D:href>
    <D:propstat>
      <D:prop>
        <D:getcontentlength>2048576</D:getcontentlength>
        <D:getlastmodified>Mon, 03 Nov 2026 08:30:00 GMT</D:getlastmodified>
        <D:getetag>"etag123456"</D:getetag>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

        let props = parse_propfind_xml(xml);
        assert_eq!(props.content_length, Some(2048576));
        assert_eq!(props.etag, Some("etag123456".to_string()));
        assert_eq!(
            props.last_modified,
            Some("Mon, 03 Nov 2026 08:30:00 GMT".to_string())
        );
        assert!(!props.is_collection);
    }

    #[test]
    fn test_webdav_url_mapping() {
        let u = Url::parse("webdav://dav.example.com/files/doc.pdf").unwrap();
        let http_u = WebDavProtocolAdapter::to_http_url(&u).unwrap();
        assert_eq!(http_u.as_str(), "http://dav.example.com/files/doc.pdf");

        let us = Url::parse("webdavs://dav.example.com/secure.bin").unwrap();
        let https_u = WebDavProtocolAdapter::to_http_url(&us).unwrap();
        assert_eq!(https_u.as_str(), "https://dav.example.com/secure.bin");
    }
}
