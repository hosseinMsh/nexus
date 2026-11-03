//! Cross-platform network interface enumeration and socket binding abstraction.

use async_trait::async_trait;
use nexus_domain::{InterfaceId, InterfaceType, NetworkInterface};
use std::net::IpAddr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NetworkPlatformError {
    #[error("Failed to enumerate network interfaces: {0}")]
    InterfaceEnumerationFailed(String),

    #[error("Interface with ID '{0}' not found")]
    InterfaceNotFound(String),

    #[error("Platform network binding unsupported or permission denied: {0}")]
    BindingFailed(String),

    #[error("Network monitor error: {0}")]
    MonitorError(String),
}

/// Interface health and connectivity status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterfaceStatus {
    pub is_up: bool,
    pub has_carrier: bool,
    pub ip_addresses: Vec<IpAddr>,
}

/// Abstract contract for operating system network interface discovery and binding.
#[async_trait]
pub trait NetworkPlatform: Send + Sync {
    /// Discovers all network interfaces exposed by the operating system.
    fn list_interfaces(&self) -> Result<Vec<NetworkInterface>, NetworkPlatformError>;

    /// Inspects the live health and status of a specific interface.
    async fn inspect_interface(
        &self,
        interface_id: &InterfaceId,
    ) -> Result<InterfaceStatus, NetworkPlatformError>;

    /// Binds an outbound socket to a specific interface or IP address.
    fn bind_socket_to_interface(
        &self,
        interface: &NetworkInterface,
        socket_fd: std::os::raw::c_int,
    ) -> Result<(), NetworkPlatformError>;
}

/// Default/fallback implementation providing local discovery.
#[derive(Debug, Default)]
pub struct SystemNetworkPlatform;

#[async_trait]
impl NetworkPlatform for SystemNetworkPlatform {
    fn list_interfaces(&self) -> Result<Vec<NetworkInterface>, NetworkPlatformError> {
        // Fallback discovery: standard loopback and placeholder interfaces.
        // Platform-specific crates (macos, windows, linux) implement native discovery in Phase 4.
        let mut interfaces = Vec::new();

        let lo0_uuid = uuid::Uuid::from_u128(1);
        let en0_uuid = uuid::Uuid::from_u128(2);

        interfaces.push(NetworkInterface {
            id: InterfaceId::from_uuid(lo0_uuid),
            system_name: "lo0".to_string(),
            display_name: "Loopback Interface".to_string(),
            interface_type: InterfaceType::Loopback,
            ip_addresses: vec!["127.0.0.1".parse().unwrap(), "::1".parse().unwrap()],
            is_up: true,
            is_default_route: false,
            mac_address: None,
        });

        interfaces.push(NetworkInterface {
            id: InterfaceId::from_uuid(en0_uuid),
            system_name: "en0".to_string(),
            display_name: "Primary Wi-Fi".to_string(),
            interface_type: InterfaceType::Wifi,
            ip_addresses: vec!["192.168.1.100".parse().unwrap()],
            is_up: true,
            is_default_route: true,
            mac_address: Some("00:11:22:33:44:55".to_string()),
        });

        Ok(interfaces)
    }

    async fn inspect_interface(
        &self,
        interface_id: &InterfaceId,
    ) -> Result<InterfaceStatus, NetworkPlatformError> {
        let list = self.list_interfaces()?;
        let iface = list
            .into_iter()
            .find(|i| &i.id == interface_id)
            .ok_or_else(|| NetworkPlatformError::InterfaceNotFound(interface_id.to_string()))?;

        Ok(InterfaceStatus {
            is_up: iface.is_up,
            has_carrier: true,
            ip_addresses: iface.ip_addresses,
        })
    }

    fn bind_socket_to_interface(
        &self,
        _interface: &NetworkInterface,
        _socket_fd: std::os::raw::c_int,
    ) -> Result<(), NetworkPlatformError> {
        // Mock / placeholder socket binding
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_system_platform_interface_enumeration() {
        let platform = SystemNetworkPlatform;
        let ifaces = platform.list_interfaces().unwrap();
        assert!(!ifaces.is_empty());
        assert!(ifaces
            .iter()
            .any(|i| i.interface_type == InterfaceType::Wifi));
    }
}
