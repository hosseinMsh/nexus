//! Secure credential storage abstraction and in-memory test implementation.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum CredentialError {
    #[error("Credential reference '{0}' not found")]
    NotFound(String),

    #[error("Credential storage backend error: {0}")]
    BackendError(String),
}

/// Abstract contract for secure credential storage (OS Keychain, DPAPI, or Secret Service).
#[async_trait]
pub trait CredentialStore: Send + Sync {
    /// Securely stores a username and secret associated with an opaque reference key.
    async fn set_credential(
        &self,
        reference: &str,
        username: &str,
        secret: &str,
    ) -> Result<(), CredentialError>;

    /// Retrieves credentials for a given reference key.
    async fn get_credential(
        &self,
        reference: &str,
    ) -> Result<Option<(String, String)>, CredentialError>;

    /// Deletes a stored credential entry.
    async fn delete_credential(&self, reference: &str) -> Result<bool, CredentialError>;
}

/// In-memory thread-safe credential store for hermetic testing and headless sessions.
#[derive(Debug, Default, Clone)]
pub struct InMemoryCredentialStore {
    store: Arc<RwLock<HashMap<String, (String, String)>>>,
}

impl InMemoryCredentialStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CredentialStore for InMemoryCredentialStore {
    async fn set_credential(
        &self,
        reference: &str,
        username: &str,
        secret: &str,
    ) -> Result<(), CredentialError> {
        let mut map = self.store.write().await;
        map.insert(
            reference.to_string(),
            (username.to_string(), secret.to_string()),
        );
        Ok(())
    }

    async fn get_credential(
        &self,
        reference: &str,
    ) -> Result<Option<(String, String)>, CredentialError> {
        let map = self.store.read().await;
        Ok(map.get(reference).cloned())
    }

    async fn delete_credential(&self, reference: &str) -> Result<bool, CredentialError> {
        let mut map = self.store.write().await;
        Ok(map.remove(reference).is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_credential_operations() {
        let store = InMemoryCredentialStore::new();

        assert_eq!(store.get_credential("proxy:socks").await.unwrap(), None);

        store
            .set_credential("proxy:socks", "nexus_user", "s3cr3t_p@ss")
            .await
            .unwrap();

        let cred = store.get_credential("proxy:socks").await.unwrap().unwrap();
        assert_eq!(cred.0, "nexus_user");
        assert_eq!(cred.1, "s3cr3t_p@ss");

        assert!(store.delete_credential("proxy:socks").await.unwrap());
        assert_eq!(store.get_credential("proxy:socks").await.unwrap(), None);
    }
}
