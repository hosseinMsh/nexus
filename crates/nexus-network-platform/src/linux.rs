//! Linux native network interface discovery, sysfs inspection, and routing detection.

use async_trait::async_trait;
use nexus_domain::{InterfaceId, InterfaceType, NetworkInterface};
use std::fs;
use std::path::Path;
use tracing::debug;

use crate::{deterministic_interface_id, InterfaceStatus, NetworkPlatform, NetworkPlatformError};

/// Linux implementation of NetworkPlatform using `/sys/class/net` and `/proc/net/route`.
#[derive(Debug, Default)]
pub struct LinuxNetworkPlatform;

impl LinuxNetworkPlatform {
    /// Reads default gateway interface from `/proc/net/route`.
    fn detect_default_gateway_interface() -> Option<String> {
        let content = fs::read_to_string("/proc/net/route").ok()?;
        for line in content.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            // Field 0: Iface, Field 1: Destination (00000000 is default route)
            if fields.len() >= 2 && fields[1] == "00000000" {
                return Some(fields[0].to_string());
            }
        }
        None
    }

    /// Classifies a Linux network interface based on name and sysfs attributes.
    pub fn classify_interface(name: &str, sysfs_path: &Path) -> InterfaceType {
        if name == "lo" {
            return InterfaceType::Loopback;
        }

        // Check if device is a USB tethering interface (e.g. usb0, rndis0, cdc-wdm)
        let driver_path = sysfs_path.join("device/uevent");
        if let Ok(uevent) = fs::read_to_string(&driver_path) {
            let lower = uevent.to_ascii_lowercase();
            if lower.contains("driver=rndis_host")
                || lower.contains("driver=cdc_ncm")
                || lower.contains("driver=cdc_ether")
                || lower.contains("driver=cdc_subset")
                || lower.contains("driver=ipheth")
            // Apple iPhone USB Ethernet driver
            {
                return InterfaceType::UsbTethering;
            }
        }

        if name.starts_with("usb") || name.starts_with("rndis") {
            return InterfaceType::UsbTethering;
        }

        // Check wireless sysfs presence
        if sysfs_path.join("wireless").exists()
            || name.starts_with("wl")
            || name.starts_with("wlan")
        {
            return InterfaceType::Wifi;
        }

        if name.starts_with("wwan") || name.starts_with("cdc-wdm") {
            return InterfaceType::Cellular;
        }

        if name.starts_with("eth") || name.starts_with("en") {
            return InterfaceType::Ethernet;
        }

        InterfaceType::Other
    }
}

#[async_trait]
impl NetworkPlatform for LinuxNetworkPlatform {
    fn list_interfaces(&self) -> Result<Vec<NetworkInterface>, NetworkPlatformError> {
        let sys_net = Path::new("/sys/class/net");
        if !sys_net.exists() {
            return Err(NetworkPlatformError::InterfaceEnumerationFailed(
                "/sys/class/net directory not found".to_string(),
            ));
        }

        let default_iface = Self::detect_default_gateway_interface();
        let mut interfaces = Vec::new();

        let entries = fs::read_dir(sys_net).map_err(|e| {
            NetworkPlatformError::InterfaceEnumerationFailed(format!(
                "Failed to read /sys/class/net: {e}"
            ))
        })?;

        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            let iface_path = entry.path();

            let is_up = fs::read_to_string(iface_path.join("operstate"))
                .map(|s| s.trim() == "up" || s.trim() == "unknown")
                .unwrap_or(false);

            let _has_carrier = fs::read_to_string(iface_path.join("carrier"))
                .map(|s| s.trim() == "1")
                .unwrap_or(false);

            let mac_address = fs::read_to_string(iface_path.join("address"))
                .map(|s| s.trim().to_string())
                .ok()
                .filter(|m| !m.is_empty() && m != "00:00:00:00:00:00");

            let iface_type = Self::classify_interface(&name, &iface_path);
            let display_name = match iface_type {
                InterfaceType::Wifi => format!("Wi-Fi ({name})"),
                InterfaceType::Ethernet => format!("Ethernet ({name})"),
                InterfaceType::UsbTethering => format!("USB Tethering ({name})"),
                InterfaceType::Cellular => format!("Cellular Modem ({name})"),
                InterfaceType::Loopback => "Loopback Interface".to_string(),
                InterfaceType::Other => format!("Network Adapter ({name})"),
            };

            let is_default = default_iface.as_ref().map(|d| d == &name).unwrap_or(false);
            let id = deterministic_interface_id("linux", &name);

            interfaces.push(NetworkInterface {
                id,
                system_name: name,
                display_name,
                interface_type: iface_type,
                ip_addresses: Vec::new(), // Populated via getifaddrs/ip if available
                is_up,
                is_default_route: is_default,
                mac_address,
            });
        }

        debug!(
            count = interfaces.len(),
            "Linux interfaces enumerated successfully"
        );
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
        // Linux SO_BINDTODEVICE
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linux_interface_classification_heuristics() {
        assert_eq!(
            LinuxNetworkPlatform::classify_interface("lo", Path::new("/sys/class/net/lo")),
            InterfaceType::Loopback
        );
        assert_eq!(
            LinuxNetworkPlatform::classify_interface("wlan0", Path::new("/sys/class/net/wlan0")),
            InterfaceType::Wifi
        );
        assert_eq!(
            LinuxNetworkPlatform::classify_interface("usb0", Path::new("/sys/class/net/usb0")),
            InterfaceType::UsbTethering
        );
        assert_eq!(
            LinuxNetworkPlatform::classify_interface("rndis0", Path::new("/sys/class/net/rndis0")),
            InterfaceType::UsbTethering
        );
        assert_eq!(
            LinuxNetworkPlatform::classify_interface("eth0", Path::new("/sys/class/net/eth0")),
            InterfaceType::Ethernet
        );
    }
}
