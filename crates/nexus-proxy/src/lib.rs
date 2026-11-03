//! Proxy profile management, protocol abstractions, and routing configuration.

use nexus_domain::{DnsPolicy, ProxyId, ProxyProfile, ProxyType};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("Invalid proxy host: {0}")]
    InvalidHost(String),

    #[error("Invalid proxy port: {0}")]
    InvalidPort(u16),

    #[error("Proxy profile '{0}' not found")]
    NotFound(String),

    #[error("Proxy profile named '{0}' already exists")]
    AlreadyExists(String),
}

/// Formats a proxy URI according to the profile parameters.
pub fn format_proxy_url(profile: &ProxyProfile) -> Result<String, ProxyError> {
    if profile.host.is_empty() {
        return Err(ProxyError::InvalidHost(profile.host.clone()));
    }
    if profile.port == 0 {
        return Err(ProxyError::InvalidPort(profile.port));
    }

    let scheme = match profile.proxy_type {
        ProxyType::Http => "http",
        ProxyType::Https => "https",
        ProxyType::Socks4 => "socks4",
        ProxyType::Socks4a => "socks4a",
        ProxyType::Socks5 => match profile.dns_policy {
            DnsPolicy::Remote => "socks5h",
            _ => "socks5",
        },
    };

    Ok(format!("{}://{}:{}", scheme, profile.host, profile.port))
}

/// Registry and coordinator for proxy profiles.
#[derive(Clone, Default)]
pub struct ProxyManager {
    profiles: Arc<RwLock<HashMap<ProxyId, ProxyProfile>>>,
}

impl ProxyManager {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates and registers a new proxy profile.
    pub async fn create_profile(
        &self,
        name: String,
        proxy_type: ProxyType,
        host: String,
        port: u16,
        credential_reference: Option<String>,
        dns_policy: DnsPolicy,
    ) -> Result<ProxyProfile, ProxyError> {
        let mut profiles = self.profiles.write().await;
        if profiles.values().any(|p| p.name == name) {
            return Err(ProxyError::AlreadyExists(name));
        }

        let profile = ProxyProfile {
            id: ProxyId::new(),
            name,
            proxy_type,
            host,
            port,
            credential_reference,
            enabled: true,
            dns_policy,
        };

        // Validate formatting
        let _ = format_proxy_url(&profile)?;

        info!(proxy_id = %profile.id, proxy_name = %profile.name, "Created new ProxyProfile");
        profiles.insert(profile.id, profile.clone());
        Ok(profile)
    }

    /// Returns a list of all proxy profiles.
    pub async fn list_profiles(&self) -> Vec<ProxyProfile> {
        self.profiles.read().await.values().cloned().collect()
    }

    /// Finds a proxy profile by ID.
    pub async fn get_profile(&self, id: &ProxyId) -> Option<ProxyProfile> {
        self.profiles.read().await.get(id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_proxy_url() {
        let profile = ProxyProfile {
            id: ProxyId::new(),
            name: "test-socks".to_string(),
            proxy_type: ProxyType::Socks5,
            host: "127.0.0.1".to_string(),
            port: 1080,
            credential_reference: None,
            enabled: true,
            dns_policy: DnsPolicy::Remote,
        };

        assert_eq!(
            format_proxy_url(&profile).unwrap(),
            "socks5h://127.0.0.1:1080"
        );
    }

    #[tokio::test]
    async fn test_proxy_manager_registration() {
        let mgr = ProxyManager::new();
        let prof = mgr
            .create_profile(
                "corporate-http".to_string(),
                ProxyType::Http,
                "proxy.corp.internal".to_string(),
                8080,
                None,
                DnsPolicy::Auto,
            )
            .await
            .unwrap();

        assert_eq!(prof.name, "corporate-http");
        assert_eq!(mgr.list_profiles().await.len(), 1);
    }
}
