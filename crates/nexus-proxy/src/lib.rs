//! Proxy profile management, protocol abstractions, health checks, and routing configuration.

use nexus_domain::{DnsPolicy, ProxyId, ProxyProfile, ProxyType};
use nexus_security::CredentialStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
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

    #[error("Security / credential error: {0}")]
    Credential(#[from] nexus_security::CredentialError),
}

/// Dynamic health, latency, and operational status of a proxy profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyHealth {
    pub is_alive: bool,
    pub latency_ms: u64,
    pub error: Option<String>,
}

fn percent_encode(s: &str) -> String {
    s.chars().fold(String::new(), |mut acc, c| {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => acc.push(c),
            _ => {
                for b in c.to_string().as_bytes() {
                    acc.push_str(&format!("%{b:02X}"));
                }
            }
        }
        acc
    })
}

/// Formats a proxy URI according to the profile parameters without credentials.
pub fn format_proxy_url(profile: &ProxyProfile) -> Result<String, ProxyError> {
    format_authenticated_proxy_url(profile, None)
}

/// Formats an authenticated proxy URI according to the profile parameters and optional credentials.
pub fn format_authenticated_proxy_url(
    profile: &ProxyProfile,
    creds: Option<(&str, &str)>,
) -> Result<String, ProxyError> {
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

    if let Some((user, pass)) = creds {
        let enc_user = percent_encode(user);
        let enc_pass = percent_encode(pass);
        Ok(format!(
            "{scheme}://{enc_user}:{enc_pass}@{}:{}",
            profile.host, profile.port
        ))
    } else {
        Ok(format!("{scheme}://{}:{}", profile.host, profile.port))
    }
}

/// Performs a live connectivity and protocol negotiation check on a proxy endpoint.
pub async fn check_proxy_health(
    profile: &ProxyProfile,
    creds: Option<(&str, &str)>,
    timeout: Duration,
) -> ProxyHealth {
    let start = std::time::Instant::now();
    let addr = format!("{}:{}", profile.host, profile.port);

    let connect_res = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await;
    let mut stream = match connect_res {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            return ProxyHealth {
                is_alive: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(format!("TCP connection error: {e}")),
            }
        }
        Err(_) => {
            return ProxyHealth {
                is_alive: false,
                latency_ms: timeout.as_millis() as u64,
                error: Some("Connection timed out".to_string()),
            }
        }
    };

    if profile.proxy_type == ProxyType::Socks5 {
        let mut methods = vec![0x00u8]; // No authentication required
        if creds.is_some() {
            methods.push(0x02u8); // Username/password authentication
        }

        let mut greeting = vec![0x05u8, methods.len() as u8];
        greeting.extend(&methods);

        if let Err(e) = stream.write_all(&greeting).await {
            return ProxyHealth {
                is_alive: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(format!("Failed to write SOCKS5 greeting: {e}")),
            };
        }

        let mut resp = [0u8; 2];
        if let Err(e) = stream.read_exact(&mut resp).await {
            return ProxyHealth {
                is_alive: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(format!("Failed to read SOCKS5 greeting response: {e}")),
            };
        }

        if resp[0] != 0x05 {
            return ProxyHealth {
                is_alive: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(format!("Invalid SOCKS version: {}", resp[0])),
            };
        }

        if resp[1] == 0x02 {
            // SOCKS5 Username/Password authentication (RFC 1929)
            if let Some((user, pass)) = creds {
                let mut auth_req = vec![0x01u8];
                auth_req.push(user.len() as u8);
                auth_req.extend_from_slice(user.as_bytes());
                auth_req.push(pass.len() as u8);
                auth_req.extend_from_slice(pass.as_bytes());

                if let Err(e) = stream.write_all(&auth_req).await {
                    return ProxyHealth {
                        is_alive: false,
                        latency_ms: start.elapsed().as_millis() as u64,
                        error: Some(format!("Failed to write SOCKS5 auth: {e}")),
                    };
                }

                let mut auth_resp = [0u8; 2];
                if let Err(e) = stream.read_exact(&mut auth_resp).await {
                    return ProxyHealth {
                        is_alive: false,
                        latency_ms: start.elapsed().as_millis() as u64,
                        error: Some(format!("Failed to read SOCKS5 auth reply: {e}")),
                    };
                }

                if auth_resp[1] != 0x00 {
                    return ProxyHealth {
                        is_alive: false,
                        latency_ms: start.elapsed().as_millis() as u64,
                        error: Some("SOCKS5 authentication failed".to_string()),
                    };
                }
            } else {
                return ProxyHealth {
                    is_alive: false,
                    latency_ms: start.elapsed().as_millis() as u64,
                    error: Some("Proxy requires authentication but none provided".to_string()),
                };
            }
        } else if resp[1] == 0xFF {
            return ProxyHealth {
                is_alive: false,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some("No acceptable authentication methods".to_string()),
            };
        }
    }

    ProxyHealth {
        is_alive: true,
        latency_ms: start.elapsed().as_millis() as u64,
        error: None,
    }
}

/// Registry and coordinator for proxy profiles.
#[derive(Clone, Default)]
pub struct ProxyManager {
    profiles: Arc<RwLock<HashMap<ProxyId, ProxyProfile>>>,
    credential_store: Option<Arc<dyn CredentialStore>>,
}

impl ProxyManager {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_credentials(credential_store: Arc<dyn CredentialStore>) -> Self {
        Self {
            profiles: Arc::new(RwLock::new(HashMap::new())),
            credential_store: Some(credential_store),
        }
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

    /// Registers an existing proxy profile.
    pub async fn register_profile(&self, profile: ProxyProfile) -> Result<(), ProxyError> {
        let mut profiles = self.profiles.write().await;
        if profiles.values().any(|p| p.name == profile.name) {
            return Err(ProxyError::AlreadyExists(profile.name));
        }
        let _ = format_proxy_url(&profile)?;
        profiles.insert(profile.id, profile);
        Ok(())
    }

    /// Returns a list of all proxy profiles.
    pub async fn list_profiles(&self) -> Vec<ProxyProfile> {
        self.profiles.read().await.values().cloned().collect()
    }

    /// Finds a proxy profile by ID.
    pub async fn get_profile(&self, id: &ProxyId) -> Option<ProxyProfile> {
        self.profiles.read().await.get(id).cloned()
    }

    /// Deletes a proxy profile by ID.
    pub async fn delete_profile(&self, id: &ProxyId) -> bool {
        let mut profiles = self.profiles.write().await;
        profiles.remove(id).is_some()
    }

    /// Checks the health of a specific proxy profile.
    pub async fn check_health(
        &self,
        id: &ProxyId,
        timeout: Duration,
    ) -> Result<ProxyHealth, ProxyError> {
        let profile = self
            .get_profile(id)
            .await
            .ok_or_else(|| ProxyError::NotFound(id.to_string()))?;

        let creds = if let (Some(ref cred_store), Some(ref cred_ref)) =
            (&self.credential_store, &profile.credential_reference)
        {
            cred_store.get_credential(cred_ref).await?
        } else {
            None
        };

        let cred_tuple = creds.as_ref().map(|(u, p)| (u.as_str(), p.as_str()));
        Ok(check_proxy_health(&profile, cred_tuple, timeout).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_security::InMemoryCredentialStore;

    #[test]
    fn test_format_authenticated_proxy_urls() {
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

        // Without auth
        assert_eq!(
            format_proxy_url(&profile).unwrap(),
            "socks5h://127.0.0.1:1080"
        );

        // With credentials
        assert_eq!(
            format_authenticated_proxy_url(&profile, Some(("user@admin", "secret:pass"))).unwrap(),
            "socks5h://user%40admin:secret%3Apass@127.0.0.1:1080"
        );
    }

    #[tokio::test]
    async fn test_proxy_manager_with_credentials_and_profiles() {
        let cred_store = Arc::new(InMemoryCredentialStore::new());
        cred_store
            .set_credential("proxy:corp", "agent", "pass123")
            .await
            .unwrap();

        let mgr = ProxyManager::with_credentials(cred_store);
        let prof = mgr
            .create_profile(
                "corporate-http".to_string(),
                ProxyType::Http,
                "127.0.0.1".to_string(),
                8080,
                Some("proxy:corp".to_string()),
                DnsPolicy::Auto,
            )
            .await
            .unwrap();

        assert_eq!(prof.name, "corporate-http");
        assert_eq!(mgr.list_profiles().await.len(), 1);

        assert!(mgr.delete_profile(&prof.id).await);
        assert_eq!(mgr.list_profiles().await.len(), 0);
    }
}
