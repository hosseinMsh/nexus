//! Cross-platform network interface enumeration, status inspection, and socket binding abstraction.

use async_trait::async_trait;
use nexus_domain::{InterfaceId, InterfaceType, NetworkInterface};
use std::net::IpAddr;
use thiserror::Error;
use uuid::Uuid;

pub mod linux;
pub mod macos;
pub mod simulated;
pub mod windows;

pub use linux::LinuxNetworkPlatform;
pub use macos::MacOsNetworkPlatform;
pub use simulated::SimulatedNetworkPlatform;
pub use windows::WindowsNetworkPlatform;

#[cfg(target_os = "macos")]
pub type NativeNetworkPlatform = macos::MacOsNetworkPlatform;

#[cfg(target_os = "linux")]
pub type NativeNetworkPlatform = linux::LinuxNetworkPlatform;

#[cfg(target_os = "windows")]
pub type NativeNetworkPlatform = windows::WindowsNetworkPlatform;

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
pub type NativeNetworkPlatform = SystemNetworkPlatform;

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

/// Generates a deterministic, reproducible `InterfaceId` from platform prefix and system name.
#[must_use]
pub fn deterministic_interface_id(platform_prefix: &str, system_name: &str) -> InterfaceId {
    let namespace = Uuid::from_u128(0x4e455855535f4e4554574f524b000000); // "NEXUS_NETWORK"
    let name = format!("{platform_prefix}:{system_name}");
    InterfaceId::from_uuid(Uuid::new_v5(&namespace, name.as_bytes()))
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

/// Fallback platform implementation.
#[derive(Debug, Default)]
pub struct SystemNetworkPlatform;

#[async_trait]
impl NetworkPlatform for SystemNetworkPlatform {
    fn list_interfaces(&self) -> Result<Vec<NetworkInterface>, NetworkPlatformError> {
        let mut interfaces = Vec::new();

        let lo0_id = deterministic_interface_id("system", "lo0");
        let en0_id = deterministic_interface_id("system", "en0");

        interfaces.push(NetworkInterface {
            id: lo0_id,
            system_name: "lo0".to_string(),
            display_name: "Loopback Interface".to_string(),
            interface_type: InterfaceType::Loopback,
            ip_addresses: vec!["127.0.0.1".parse().unwrap(), "::1".parse().unwrap()],
            is_up: true,
            is_default_route: false,
            mac_address: None,
        });

        interfaces.push(NetworkInterface {
            id: en0_id,
            system_name: "en0".to_string(),
            display_name: "Primary Network Adapter".to_string(),
            interface_type: InterfaceType::Ethernet,
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
            .any(|i| i.interface_type == InterfaceType::Ethernet));
    }

    #[test]
    fn test_deterministic_id_reproducibility() {
        let id1 = deterministic_interface_id("macos", "en0");
        let id2 = deterministic_interface_id("macos", "en0");
        let id3 = deterministic_interface_id("macos", "en1");
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }
}
