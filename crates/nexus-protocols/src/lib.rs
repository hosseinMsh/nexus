//! Protocol adapters and protocol dispatching framework.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

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
            "webdav" => Ok(Self::WebDav),
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
        assert!(ProtocolKind::from_url(&Url::parse("gopher://example.com").unwrap()).is_err());
    }
}
