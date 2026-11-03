//! Protocol adapters and protocol dispatching framework.

use async_trait::async_trait;
use nexus_domain::NetworkPath;
use nexus_transport::RemoteMetadata;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncRead;
use url::Url;

pub mod ftp;
pub mod http;
pub mod manager;
pub mod metalink;
pub mod torrent;
pub mod webdav;

pub use ftp::FtpProtocolAdapter;
pub use http::HttpProtocolAdapter;
pub use manager::ProtocolManager;
pub use metalink::{MetalinkDocument, MetalinkFile, MetalinkHash, MetalinkParser, MetalinkUrl};
pub use torrent::{BencodeValue, MagnetProtocolAdapter, MagnetUri};
pub use webdav::{parse_propfind_xml, WebDavProperties, WebDavProtocolAdapter};

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Unsupported URL scheme: {0}")]
    UnsupportedScheme(String),

    #[error("Protocol error: {0}")]
    Protocol(String),
}

/// Supported transfer protocols defined in PRD Section 4.6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtocolKind {
    Http,
    Https,
    Ftp,
    Ftps,
    Sftp,
    BitTorrent,
    Magnet,
    Metalink,
    Hls,
    Dash,
    WebDav,
}

impl ProtocolKind {
    /// Detects the protocol type from a URL scheme.
    pub fn from_url(url: &Url) -> Result<Self, ProtocolError> {
        match url.scheme() {
            "http" => Ok(Self::Http),
            "https" => Ok(Self::Https),
            "ftp" => Ok(Self::Ftp),
            "ftps" => Ok(Self::Ftps),
            "sftp" => Ok(Self::Sftp),
            "magnet" => Ok(Self::Magnet),
            "webdav" | "webdavs" => Ok(Self::WebDav),
            other => Err(ProtocolError::UnsupportedScheme(other.to_string())),
        }
    }
}

/// Abstract protocol handler interface.
#[async_trait]
pub trait ProtocolAdapter: Send + Sync {
    /// Returns the protocol kind implemented by this adapter.
    fn kind(&self) -> ProtocolKind;

    /// Validates whether the given URL is supported.
    fn can_handle(&self, url: &Url) -> bool {
        ProtocolKind::from_url(url).is_ok_and(|k| k == self.kind())
    }

    /// Probes remote resource metadata.
    async fn probe_metadata(
        &self,
        path: &NetworkPath,
        url: &Url,
    ) -> Result<RemoteMetadata, ProtocolError>;

    /// Fetches a specific byte range.
    async fn fetch_range(
        &self,
        path: &NetworkPath,
        url: &Url,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send>, ProtocolError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_detection() {
        assert_eq!(
            ProtocolKind::from_url(&Url::parse("https://example.com/file").unwrap()).unwrap(),
            ProtocolKind::Https
        );
        assert_eq!(
            ProtocolKind::from_url(&Url::parse("http://example.com/file").unwrap()).unwrap(),
            ProtocolKind::Http
        );
        assert_eq!(
            ProtocolKind::from_url(&Url::parse("ftp://ftp.example.com/file").unwrap()).unwrap(),
            ProtocolKind::Ftp
        );
        assert_eq!(
            ProtocolKind::from_url(&Url::parse("magnet:?xt=urn:btih:1234").unwrap()).unwrap(),
            ProtocolKind::Magnet
        );
        assert_eq!(
            ProtocolKind::from_url(&Url::parse("webdav://dav.box.com/file").unwrap()).unwrap(),
            ProtocolKind::WebDav
        );
        assert!(ProtocolKind::from_url(&Url::parse("gopher://example.com").unwrap()).is_err());
    }
}
