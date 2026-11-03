//! Security guards: SSRF validation, path traversal prevention, and filename sanitization.

use std::net::IpAddr;
use std::path::{Component, Path};
use thiserror::Error;
use url::Url;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum SecurityError {
    #[error("SSRF Protection: Blocked prohibited destination address: {0}")]
    SsrfBlocked(String),

    #[error("Path traversal detected in filename: '{0}'")]
    PathTraversalDetected(String),

    #[error("Invalid filename characters: '{0}'")]
    InvalidFilename(String),

    #[error("URL scheme '{0}' is not permitted")]
    DisallowedScheme(String),
}

/// Checks whether an IP address belongs to loopback, private, link-local, or broadcast ranges.
#[must_use]
pub fn is_private_or_restricted_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => {
            ipv4.is_loopback()
                || ipv4.is_private()
                || ipv4.is_link_local()
                || ipv4.is_broadcast()
                || ipv4.is_unspecified()
                // Carrier-grade NAT (100.64.0.0/10)
                || (ipv4.octets()[0] == 100 && (ipv4.octets()[1] & 0xc0) == 64)
        }
        IpAddr::V6(ipv6) => ipv6.is_loopback() || ipv6.is_unspecified(),
    }
}

/// Validates that a target URL does not directly target restricted local addresses unless allowed.
pub fn validate_url_ssrf(url: &Url, allow_private_networks: bool) -> Result<(), SecurityError> {
    if !allow_private_networks {
        if let Some(host_str) = url.host_str() {
            if host_str.eq_ignore_ascii_case("localhost") {
                return Err(SecurityError::SsrfBlocked("localhost".to_string()));
            }

            if let Ok(ip) = host_str.parse::<IpAddr>() {
                if is_private_or_restricted_ip(&ip) {
                    return Err(SecurityError::SsrfBlocked(ip.to_string()));
                }
            }
        }
    }

    match url.scheme() {
        "http" | "https" | "ftp" | "ftps" | "sftp" => Ok(()),
        other => Err(SecurityError::DisallowedScheme(other.to_string())),
    }
}

/// Sanitizes untrusted filenames from Content-Disposition headers or URL paths,
/// completely stripping path components, directory separators, null bytes, and reserved characters.
pub fn sanitize_filename(raw_filename: &str) -> Result<String, SecurityError> {
    let trimmed = raw_filename.trim();
    if trimmed.is_empty() {
        return Err(SecurityError::InvalidFilename(
            "Filename is empty".to_string(),
        ));
    }

    // Check for directory separators
    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains('\0') {
        return Err(SecurityError::PathTraversalDetected(trimmed.to_string()));
    }

    // Verify path components
    let path = Path::new(trimmed);
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => return Err(SecurityError::PathTraversalDetected(trimmed.to_string())),
        }
    }

    // Filter reserved characters for Windows & cross-platform safety: < > : " / \ | ? *
    let cleaned: String = trimmed
        .chars()
        .filter(|&c| !matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
        .collect();

    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        return Err(SecurityError::InvalidFilename(trimmed.to_string()));
    }

    Ok(cleaned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssrf_blocking() {
        let localhost_url = Url::parse("http://127.0.0.1:8080/secret").unwrap();
        assert!(validate_url_ssrf(&localhost_url, false).is_err());
        assert!(validate_url_ssrf(&localhost_url, true).is_ok());

        let public_url = Url::parse("https://releases.ubuntu.com/jammy.iso").unwrap();
        assert!(validate_url_ssrf(&public_url, false).is_ok());
    }

    #[test]
    fn test_sanitize_filename_traversal_detection() {
        assert!(sanitize_filename("../../etc/passwd").is_err());
        assert!(sanitize_filename("..\\..\\Windows\\cmd.exe").is_err());
        assert_eq!(
            sanitize_filename("valid_archive.tar.gz").unwrap(),
            "valid_archive.tar.gz"
        );
        assert_eq!(
            sanitize_filename("report:2026.pdf").unwrap(),
            "report2026.pdf"
        );
    }
}
