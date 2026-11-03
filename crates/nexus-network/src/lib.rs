//! High-level network interface discovery, NetworkPath management, and health auditing.

use nexus_domain::{InterfaceId, InterfaceType, NetworkInterface, NetworkPath, PathId, PathLimits};
use nexus_network_platform::{NetworkPlatform, NetworkPlatformError};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::info;

pub mod monitor;

pub use monitor::{NetworkChangeEvent, NetworkMonitor};

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

/// Dynamic health, operational status, and reachability details of a configured NetworkPath.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathHealth {
    pub path_id: PathId,
    pub interface_id: InterfaceId,
    pub is_available: bool,
    pub is_up: bool,
    pub has_carrier: bool,
    pub ip_addresses: Vec<IpAddr>,
}

/// Coordinates network interfaces, path configurations, and live monitoring.
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

    /// Returns interfaces filtered by specific type (e.g. Wifi, Ethernet, UsbTethering).
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

    /// Retrieves a specific network path by ID.
    pub async fn get_path(&self, path_id: &PathId) -> Result<NetworkPath, NetworkError> {
        self.paths
            .read()
            .await
            .get(path_id)
            .cloned()
            .ok_or_else(|| NetworkError::PathNotFound(path_id.to_string()))
    }

    /// Deletes a configured network path.
    pub async fn remove_path(&self, path_id: &PathId) -> Result<(), NetworkError> {
        let mut paths = self.paths.write().await;
        paths
            .remove(path_id)
            .ok_or_else(|| NetworkError::PathNotFound(path_id.to_string()))?;
        info!(path_id = %path_id, "Removed NetworkPath");
        Ok(())
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

    /// Inspects the live health, operational state, and IP connectivity of a NetworkPath.
    pub async fn inspect_path_health(&self, path_id: &PathId) -> Result<PathHealth, NetworkError> {
        let path = self.get_path(path_id).await?;

        match self.platform.inspect_interface(&path.interface_id).await {
            Ok(status) => Ok(PathHealth {
                path_id: *path_id,
                interface_id: path.interface_id,
                is_available: path.enabled && status.is_up && status.has_carrier,
                is_up: status.is_up,
                has_carrier: status.has_carrier,
                ip_addresses: status.ip_addresses,
            }),
            Err(NetworkPlatformError::InterfaceNotFound(_)) => Ok(PathHealth {
                path_id: *path_id,
                interface_id: path.interface_id,
                is_available: false,
                is_up: false,
                has_carrier: false,
                ip_addresses: Vec::new(),
            }),
            Err(e) => Err(NetworkError::Platform(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_network_platform::SimulatedNetworkPlatform;

    #[tokio::test]
    async fn test_create_and_inspect_path_health() {
        let platform = Arc::new(SimulatedNetworkPlatform::new());
        let manager = NetworkManager::new(platform.clone());

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

        // Path is healthy initially
        let health = manager.inspect_path_health(&path.id).await.unwrap();
        assert!(health.is_available);
        assert!(health.is_up);
        assert!(health.has_carrier);

        // Disconnect interface
        platform.set_interface_up(&wifi.id, false);
        let health_after_down = manager.inspect_path_health(&path.id).await.unwrap();
        assert!(!health_after_down.is_available);
        assert!(!health_after_down.is_up);

        // Reconnect
        platform.set_interface_up(&wifi.id, true);
        let health_restored = manager.inspect_path_health(&path.id).await.unwrap();
        assert!(health_restored.is_available);
    }
}
