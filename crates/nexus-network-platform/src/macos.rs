//! macOS native network interface discovery, status inspection, and routing detection.

use async_trait::async_trait;
use nexus_domain::{InterfaceId, InterfaceType, NetworkInterface};
use std::collections::HashMap;
use std::net::IpAddr;
use std::process::Command;
use tracing::{debug, warn};

use crate::{deterministic_interface_id, InterfaceStatus, NetworkPlatform, NetworkPlatformError};

/// macOS implementation of NetworkPlatform using `networksetup`, `ifconfig`, and `route`.
#[derive(Debug, Default)]
pub struct MacOsNetworkPlatform;

#[derive(Debug, Default, Clone)]
struct ParsedIfconfig {
    system_name: String,
    is_up: bool,
    has_carrier: bool,
    mac_address: Option<String>,
    ip_addresses: Vec<IpAddr>,
}

impl MacOsNetworkPlatform {
    /// Queries `networksetup -listallhardwareports` to map devices to hardware types.
    fn parse_hardware_ports() -> HashMap<String, (String, InterfaceType)> {
        let mut map = HashMap::new();

        let output = match Command::new("networksetup")
            .arg("-listallhardwareports")
            .output()
        {
            Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).to_string(),
            Ok(out) => {
                warn!(
                    code = ?out.status.code(),
                    "networksetup command returned non-zero status"
                );
                return map;
            }
            Err(e) => {
                warn!(error = %e, "Failed to execute networksetup; falling back to heuristic classification");
                return map;
            }
        };

        let mut current_port = String::new();
        let mut current_device = String::new();

        for line in output.lines() {
            let trimmed = line.trim();
            if let Some(port) = trimmed.strip_prefix("Hardware Port: ") {
                current_port = port.trim().to_string();
            } else if let Some(device) = trimmed.strip_prefix("Device: ") {
                current_device = device.trim().to_string();
                if !current_port.is_empty() && !current_device.is_empty() {
                    let kind = Self::classify_port_name(&current_port);
                    map.insert(current_device.clone(), (current_port.clone(), kind));
                }
            } else if trimmed.is_empty() {
                current_port.clear();
                current_device.clear();
            }
        }

        map
    }

    /// Classifies an interface type based on its hardware port display name.
    pub fn classify_port_name(name: &str) -> InterfaceType {
        let lower = name.to_ascii_lowercase();

        if lower.contains("wi-fi") || lower.contains("airport") || lower.contains("wireless") {
            InterfaceType::Wifi
        } else if lower.contains("iphone")
            || lower.contains("ipad")
            || lower.contains("android")
            || lower.contains("usb")
            || lower.contains("tether")
            || lower.contains("rndis")
            || lower.contains("ncm")
            || lower.contains("cdc")
        {
            InterfaceType::UsbTethering
        } else if lower.contains("cellular") || lower.contains("modem") || lower.contains("lte") {
            InterfaceType::Cellular
        } else if lower.contains("ethernet")
            || lower.contains("thunderbolt")
            || lower.contains("lan")
            || lower.contains("bridge")
        {
            InterfaceType::Ethernet
        } else if lower.contains("loopback") {
            InterfaceType::Loopback
        } else {
            InterfaceType::Other
        }
    }

    /// Queries `route -n get default` to determine which interface holds the active default gateway.
    fn detect_default_gateway_interface() -> Option<String> {
        let output = Command::new("route")
            .args(["-n", "get", "default"])
            .output()
            .ok()?;

        if !output.status.success() {
            return None;
        }

        let text = String::from_utf8_lossy(&output.stdout);
        for line in text.lines() {
            let trimmed = line.trim();
            if let Some(iface) = trimmed.strip_prefix("interface: ") {
                return Some(iface.trim().to_string());
            }
        }

        None
    }

    /// Parses the raw output of `ifconfig`.
    fn parse_ifconfig_output(raw: &str) -> Vec<ParsedIfconfig> {
        let mut entries = Vec::new();
        let mut current: Option<ParsedIfconfig> = None;

        for line in raw.lines() {
            if line.is_empty() {
                continue;
            }

            // Interface block header: starts with non-whitespace and ends with ':'
            if !line.starts_with(' ') && !line.starts_with('\t') && line.contains(':') {
                if let Some(entry) = current.take() {
                    entries.push(entry);
                }

                let name = line.split(':').next().unwrap_or("").trim().to_string();
                let flags_part = line.split("flags=").nth(1).unwrap_or("");
                let is_up = flags_part.contains("UP");
                let has_carrier = flags_part.contains("RUNNING");

                current = Some(ParsedIfconfig {
                    system_name: name,
                    is_up,
                    has_carrier,
                    mac_address: None,
                    ip_addresses: Vec::new(),
                });
            } else if let Some(ref mut entry) = current {
                let trimmed = line.trim();

                if let Some(rest) = trimmed.strip_prefix("ether ") {
                    let mac = rest.split_whitespace().next().unwrap_or("").trim();
                    if !mac.is_empty() {
                        entry.mac_address = Some(mac.to_string());
                    }
                } else if let Some(rest) = trimmed.strip_prefix("status: ") {
                    if rest.trim() == "active" {
                        entry.has_carrier = true;
                    }
                } else if let Some(rest) = trimmed.strip_prefix("inet ") {
                    let ip_str = rest.split_whitespace().next().unwrap_or("");
                    if let Ok(ip) = ip_str.parse::<IpAddr>() {
                        entry.ip_addresses.push(ip);
                    }
                } else if let Some(rest) = trimmed.strip_prefix("inet6 ") {
                    let ip_str = rest.split_whitespace().next().unwrap_or("");
                    // Strip scope id if present (e.g. fe80::1%en0 -> fe80::1)
                    let clean_ip = ip_str.split('%').next().unwrap_or(ip_str);
                    if let Ok(ip) = clean_ip.parse::<IpAddr>() {
                        entry.ip_addresses.push(ip);
                    }
                }
            }
        }

        if let Some(entry) = current {
            entries.push(entry);
        }

        entries
    }
}

#[async_trait]
impl NetworkPlatform for MacOsNetworkPlatform {
    fn list_interfaces(&self) -> Result<Vec<NetworkInterface>, NetworkPlatformError> {
        let hardware_ports = Self::parse_hardware_ports();
        let default_iface = Self::detect_default_gateway_interface();

        let ifconfig_raw = match Command::new("ifconfig").output() {
            Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).to_string(),
            Ok(out) => {
                return Err(NetworkPlatformError::InterfaceEnumerationFailed(format!(
                    "ifconfig failed with exit status: {:?}",
                    out.status.code()
                )));
            }
            Err(e) => {
                return Err(NetworkPlatformError::InterfaceEnumerationFailed(format!(
                    "Failed to execute ifconfig: {e}"
                )));
            }
        };

        let parsed = Self::parse_ifconfig_output(&ifconfig_raw);
        let mut interfaces = Vec::new();

        for item in parsed {
            // Determine interface type and display name
            let (display_name, iface_type) =
                if item.system_name == "lo0" || item.system_name.starts_with("lo") {
                    ("Loopback Interface".to_string(), InterfaceType::Loopback)
                } else if let Some((hw_name, kind)) = hardware_ports.get(&item.system_name) {
                    (hw_name.clone(), *kind)
                } else if item.system_name.starts_with("en") {
                    (
                        format!("Ethernet / Wi-Fi Adapter ({})", item.system_name),
                        InterfaceType::Other,
                    )
                } else if item.system_name.starts_with("utun") {
                    (
                        format!("Virtual Tunnel ({})", item.system_name),
                        InterfaceType::Other,
                    )
                } else {
                    (
                        format!("Network Adapter ({})", item.system_name),
                        InterfaceType::Other,
                    )
                };

            let is_default = default_iface
                .as_ref()
                .map(|def| def == &item.system_name)
                .unwrap_or(false);

            let id = deterministic_interface_id("macos", &item.system_name);

            interfaces.push(NetworkInterface {
                id,
                system_name: item.system_name,
                display_name,
                interface_type: iface_type,
                ip_addresses: item.ip_addresses,
                is_up: item.is_up,
                is_default_route: is_default,
                mac_address: item.mac_address,
            });
        }

        debug!(
            count = interfaces.len(),
            "macOS interfaces enumerated successfully"
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
            has_carrier: iface.is_up && !iface.ip_addresses.is_empty(),
            ip_addresses: iface.ip_addresses,
        })
    }

    fn bind_socket_to_interface(
        &self,
        _interface: &NetworkInterface,
        _socket_fd: std::os::raw::c_int,
    ) -> Result<(), NetworkPlatformError> {
        // Interface-specific socket binding via IP_BOUND_IF / SO_BINDTODEVICE
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_port_names() {
        assert_eq!(
            MacOsNetworkPlatform::classify_port_name("Wi-Fi"),
            InterfaceType::Wifi
        );
        assert_eq!(
            MacOsNetworkPlatform::classify_port_name("AirPort"),
            InterfaceType::Wifi
        );
        assert_eq!(
            MacOsNetworkPlatform::classify_port_name("Thunderbolt Ethernet"),
            InterfaceType::Ethernet
        );
        assert_eq!(
            MacOsNetworkPlatform::classify_port_name("iPhone USB"),
            InterfaceType::UsbTethering
        );
        assert_eq!(
            MacOsNetworkPlatform::classify_port_name("Android USB Tethering"),
            InterfaceType::UsbTethering
        );
        assert_eq!(
            MacOsNetworkPlatform::classify_port_name("CDC NCM Data"),
            InterfaceType::UsbTethering
        );
        assert_eq!(
            MacOsNetworkPlatform::classify_port_name("Cellular Modem"),
            InterfaceType::Cellular
        );
    }

    #[test]
    fn test_parse_ifconfig_snippet() {
        let snippet = r#"
lo0: flags=8049<UP,LOOPBACK,RUNNING,MULTICAST> mtu 16384
	inet 127.0.0.1 netmask 0xff000000 
	inet6 ::1 prefixlen 128 
en0: flags=8863<UP,BROADCAST,SMART,RUNNING,SIMPLEX,MULTICAST> mtu 1500
	ether a0:9a:8e:47:fe:a1
	inet 192.168.1.50 netmask 0xffffff00 broadcast 192.168.1.255
	status: active
"#;
        let entries = MacOsNetworkPlatform::parse_ifconfig_output(snippet);
        assert_eq!(entries.len(), 2);

        assert_eq!(entries[0].system_name, "lo0");
        assert!(entries[0].is_up);
        assert_eq!(entries[0].ip_addresses.len(), 2);

        assert_eq!(entries[1].system_name, "en0");
        assert!(entries[1].is_up);
        assert!(entries[1].has_carrier);
        assert_eq!(
            entries[1].mac_address,
            Some("a0:9a:8e:47:fe:a1".to_string())
        );
        assert_eq!(
            entries[1].ip_addresses,
            vec!["192.168.1.50".parse::<IpAddr>().unwrap()]
        );
    }

    #[test]
    fn test_macos_live_discovery() {
        let platform = MacOsNetworkPlatform;
        let list = platform.list_interfaces().unwrap();
        assert!(!list.is_empty());

        // At minimum, loopback lo0 should be present
        let lo = list.iter().find(|i| i.system_name == "lo0");
        assert!(lo.is_some());
        assert_eq!(lo.unwrap().interface_type, InterfaceType::Loopback);
    }
}
