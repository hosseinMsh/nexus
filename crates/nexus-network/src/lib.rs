//! High-level network interface discovery and NetworkPath management.

use nexus_domain::{InterfaceId, InterfaceType, NetworkInterface, NetworkPath, PathId, PathLimits};
use nexus_network_platform::{NetworkPlatform, NetworkPlatformError};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Platform error: {0}")]
    Platform(#[from] NetworkPlatformError),

    #[error("Path '{0}' not found")]
    PathNotFound(String),

    #[error("Interface '{0}' not found")]
    InterfaceNotFound(String),

    #[error("Path with name '{0}' already exists")]
    PathAlreadyExists(String),
}

/// Coordinates network interfaces and configured network paths.
#[derive(Clone)]
pub struct NetworkManager {
    platform: Arc<dyn NetworkPlatform>,
    paths: Arc<RwLock<HashMap<PathId, NetworkPath>>>,
}

impl NetworkManager {
    #[must_use]
    pub fn new(platform: Arc<dyn NetworkPlatform>) -> Self {
        Self {
            platform,
            paths: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Discovers all network interfaces currently available.
    pub fn list_interfaces(&self) -> Result<Vec<NetworkInterface>, NetworkError> {
        Ok(self.platform.list_interfaces()?)
    }

    /// Returns interfaces filtered by specific type (e.g. Wifi or Ethernet).
    pub fn filter_interfaces_by_type(
        &self,
        kind: InterfaceType,
    ) -> Result<Vec<NetworkInterface>, NetworkError> {
        let all = self.list_interfaces()?;
        Ok(all
            .into_iter()
            .filter(|i| i.interface_type == kind)
            .collect())
    }

    /// Registers a new NetworkPath.
    pub async fn create_path(
        &self,
        name: String,
        interface_id: InterfaceId,
        limits: PathLimits,
        priority: u8,
    ) -> Result<NetworkPath, NetworkError> {
        // Validate interface exists
        let ifaces = self.list_interfaces()?;
        if !ifaces.iter().any(|i| i.id == interface_id) {
            return Err(NetworkError::InterfaceNotFound(interface_id.to_string()));
        }

        let mut paths = self.paths.write().await;
        if paths.values().any(|p| p.name == name) {
            return Err(NetworkError::PathAlreadyExists(name));
        }

        let path = NetworkPath {
            id: PathId::new(),
            name,
            interface_id,
            proxy_id: None,
            priority,
            enabled: true,
            limits,
        };

        info!(path_id = %path.id, path_name = %path.name, "Created new NetworkPath");
        paths.insert(path.id, path.clone());
        Ok(path)
    }

    /// Lists all configured network paths.
    pub async fn list_paths(&self) -> Vec<NetworkPath> {
        self.paths.read().await.values().cloned().collect()
    }

    /// Toggles the enabled state of a network path.
    pub async fn set_path_enabled(
        &self,
        path_id: &PathId,
        enabled: bool,
    ) -> Result<(), NetworkError> {
        let mut paths = self.paths.write().await;
        let path = paths
            .get_mut(path_id)
            .ok_or_else(|| NetworkError::PathNotFound(path_id.to_string()))?;
        path.enabled = enabled;
        info!(path_id = %path_id, enabled = enabled, "Updated path enabled status");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_network_platform::SystemNetworkPlatform;

    #[tokio::test]
    async fn test_create_and_list_network_path() {
        let platform = Arc::new(SystemNetworkPlatform);
        let manager = NetworkManager::new(platform);

        let ifaces = manager.list_interfaces().unwrap();
        let wifi = ifaces
            .iter()
            .find(|i| i.interface_type == InterfaceType::Wifi)
            .unwrap();

        let path = manager
            .create_path("fast-wifi".to_string(), wifi.id, PathLimits::default(), 10)
            .await
            .unwrap();

        assert_eq!(path.name, "fast-wifi");
        assert!(path.enabled);

        let all_paths = manager.list_paths().await;
        assert_eq!(all_paths.len(), 1);
        assert_eq!(all_paths[0].id, path.id);
    }
}
