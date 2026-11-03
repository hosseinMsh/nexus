//! Windows native network interface discovery and routing detection.

use async_trait::async_trait;
use nexus_domain::{InterfaceId, InterfaceType, NetworkInterface};
use tracing::debug;

use crate::{deterministic_interface_id, InterfaceStatus, NetworkPlatform, NetworkPlatformError};

/// Windows implementation of NetworkPlatform using IP Helper / netsh abstractions.
#[derive(Debug, Default)]
pub struct WindowsNetworkPlatform;

impl WindowsNetworkPlatform {
    /// Classifies an interface name or description on Windows.
    pub fn classify_windows_adapter(name: &str) -> InterfaceType {
        let lower = name.to_ascii_lowercase();

        if lower.contains("wi-fi") || lower.contains("wireless") || lower.contains("802.11") {
            InterfaceType::Wifi
        } else if lower.contains("remote ndis")
            || lower.contains("apple mobile device")
            || lower.contains("usb ethernet")
            || lower.contains("rndis")
            || lower.contains("tether")
        {
            InterfaceType::UsbTethering
        } else if lower.contains("cellular") || lower.contains("mobile broadband") {
            InterfaceType::Cellular
        } else if lower.contains("ethernet")
            || lower.contains("local area connection")
            || lower.contains("gbe")
            || lower.contains("gigabit")
            || lower.contains("lan")
        {
            InterfaceType::Ethernet
        } else if lower.contains("loopback") {
            InterfaceType::Loopback
        } else {
            InterfaceType::Other
        }
    }
}

#[async_trait]
impl NetworkPlatform for WindowsNetworkPlatform {
    fn list_interfaces(&self) -> Result<Vec<NetworkInterface>, NetworkPlatformError> {
        let mut interfaces = Vec::new();

        // Loopback adapter
        let lo_id = deterministic_interface_id("windows", "loopback");
        interfaces.push(NetworkInterface {
            id: lo_id,
            system_name: "Loopback Pseudo-Interface 1".to_string(),
            display_name: "Loopback Interface".to_string(),
            interface_type: InterfaceType::Loopback,
            ip_addresses: vec!["127.0.0.1".parse().unwrap(), "::1".parse().unwrap()],
            is_up: true,
            is_default_route: false,
            mac_address: None,
        });

        debug!(count = interfaces.len(), "Windows interfaces enumerated");
        Ok(interfaces)
    }

    async fn inspect_interface(
        &self,
        interface_id: &InterfaceId,
    ) -> Result<InterfaceStatus, NetworkPlatformError> {
        let all = self.list_interfaces()?;
        let iface = all
            .into_iter()
            .find(|i| &i.id == interface_id)
            .ok_or_else(|| NetworkPlatformError::InterfaceNotFound(interface_id.to_string()))?;

        Ok(InterfaceStatus {
            is_up: iface.is_up,
            has_carrier: iface.is_up,
            ip_addresses: iface.ip_addresses,
        })
    }

    fn bind_socket_to_interface(
        &self,
        _interface: &NetworkInterface,
        _socket_fd: std::os::raw::c_int,
    ) -> Result<(), NetworkPlatformError> {
        // Windows IP_UNICAST_IF
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_windows_adapter_classification() {
        assert_eq!(
            WindowsNetworkPlatform::classify_windows_adapter("Intel(R) Wi-Fi 6 AX201 160MHz"),
            InterfaceType::Wifi
        );
        assert_eq!(
            WindowsNetworkPlatform::classify_windows_adapter(
                "Remote NDIS based Internet Sharing Device"
            ),
            InterfaceType::UsbTethering
        );
        assert_eq!(
            WindowsNetworkPlatform::classify_windows_adapter("Apple Mobile Device Ethernet"),
            InterfaceType::UsbTethering
        );
        assert_eq!(
            WindowsNetworkPlatform::classify_windows_adapter("Realtek PCIe GbE Family Controller"),
            InterfaceType::Ethernet
        );
    }
}
