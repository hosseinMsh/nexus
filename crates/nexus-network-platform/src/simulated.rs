//! Simulated network platform for hermetic testing of disconnections,
//! USB tethering attach/detach, and sleep/wake events.

use async_trait::async_trait;
use nexus_domain::{InterfaceId, InterfaceType, NetworkInterface};
use std::sync::{Arc, RwLock};

use crate::{deterministic_interface_id, InterfaceStatus, NetworkPlatform, NetworkPlatformError};

#[derive(Debug, Clone)]
pub struct SimulatedNetworkPlatform {
    interfaces: Arc<RwLock<Vec<NetworkInterface>>>,
    is_sleeping: Arc<RwLock<bool>>,
}

impl Default for SimulatedNetworkPlatform {
    fn default() -> Self {
        Self::new()
    }
}

impl SimulatedNetworkPlatform {
    #[must_use]
    pub fn new() -> Self {
        let lo_id = deterministic_interface_id("simulated", "lo0");
        let wifi_id = deterministic_interface_id("simulated", "en0");

        let default_interfaces = vec![
            NetworkInterface {
                id: lo_id,
                system_name: "lo0".to_string(),
                display_name: "Loopback Interface".to_string(),
                interface_type: InterfaceType::Loopback,
                ip_addresses: vec!["127.0.0.1".parse().unwrap(), "::1".parse().unwrap()],
                is_up: true,
                is_default_route: false,
                mac_address: None,
            },
            NetworkInterface {
                id: wifi_id,
                system_name: "en0".to_string(),
                display_name: "Wi-Fi (Primary)".to_string(),
                interface_type: InterfaceType::Wifi,
                ip_addresses: vec!["192.168.1.100".parse().unwrap()],
                is_up: true,
                is_default_route: true,
                mac_address: Some("00:11:22:33:44:55".to_string()),
            },
        ];

        Self {
            interfaces: Arc::new(RwLock::new(default_interfaces)),
            is_sleeping: Arc::new(RwLock::new(false)),
        }
    }

    /// Adds a new interface to the simulated environment.
    pub fn add_interface(&self, interface: NetworkInterface) {
        let mut list = self.interfaces.write().unwrap();
        list.retain(|i| i.id != interface.id && i.system_name != interface.system_name);
        list.push(interface);
    }

    /// Removes an interface by ID.
    pub fn remove_interface(&self, interface_id: &InterfaceId) {
        let mut list = self.interfaces.write().unwrap();
        list.retain(|i| &i.id != interface_id);
    }

    /// Sets the operational (UP/DOWN) state of an interface.
    pub fn set_interface_up(&self, interface_id: &InterfaceId, is_up: bool) {
        let mut list = self.interfaces.write().unwrap();
        if let Some(iface) = list.iter_mut().find(|i| &i.id == interface_id) {
            iface.is_up = is_up;
        }
    }

    /// Sets the default route interface.
    pub fn set_default_route(&self, interface_id: &InterfaceId) {
        let mut list = self.interfaces.write().unwrap();
        for iface in list.iter_mut() {
            iface.is_default_route = &iface.id == interface_id;
        }
    }

    /// Simulates plugging in a USB tethered phone.
    pub fn simulate_usb_tethering_plug(&self) -> InterfaceId {
        let id = deterministic_interface_id("simulated", "en5");
        let tether_iface = NetworkInterface {
            id,
            system_name: "en5".to_string(),
            display_name: "iPhone USB Tethering".to_string(),
            interface_type: InterfaceType::UsbTethering,
            ip_addresses: vec!["172.20.10.2".parse().unwrap()],
            is_up: true,
            is_default_route: false,
            mac_address: Some("aa:bb:cc:dd:ee:ff".to_string()),
        };
        self.add_interface(tether_iface);
        id
    }

    /// Simulates unplugging the USB tethered phone.
    pub fn simulate_usb_tethering_unplug(&self) {
        let id = deterministic_interface_id("simulated", "en5");
        self.remove_interface(&id);
    }

    /// Simulates system sleep (marks all non-loopback interfaces as DOWN).
    pub fn simulate_sleep(&self) {
        *self.is_sleeping.write().unwrap() = true;
        let mut list = self.interfaces.write().unwrap();
        for iface in list.iter_mut() {
            if iface.interface_type != InterfaceType::Loopback {
                iface.is_up = false;
            }
        }
    }

    /// Simulates system wake (restores interfaces to UP).
    pub fn simulate_wake(&self) {
        *self.is_sleeping.write().unwrap() = false;
        let mut list = self.interfaces.write().unwrap();
        for iface in list.iter_mut() {
            iface.is_up = true;
        }
    }

    pub fn is_sleeping(&self) -> bool {
        *self.is_sleeping.read().unwrap()
    }
}

#[async_trait]
impl NetworkPlatform for SimulatedNetworkPlatform {
    fn list_interfaces(&self) -> Result<Vec<NetworkInterface>, NetworkPlatformError> {
        let list = self.interfaces.read().unwrap();
        Ok(list.clone())
    }

    async fn inspect_interface(
        &self,
        interface_id: &InterfaceId,
    ) -> Result<InterfaceStatus, NetworkPlatformError> {
        let list = self.interfaces.read().unwrap();
        let iface = list
            .iter()
            .find(|i| &i.id == interface_id)
            .ok_or_else(|| NetworkPlatformError::InterfaceNotFound(interface_id.to_string()))?;

        Ok(InterfaceStatus {
            is_up: iface.is_up,
            has_carrier: iface.is_up && !iface.ip_addresses.is_empty(),
            ip_addresses: iface.ip_addresses.clone(),
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

    #[test]
    fn test_simulated_platform_operations() {
        let platform = SimulatedNetworkPlatform::new();

        let initial = platform.list_interfaces().unwrap();
        assert_eq!(initial.len(), 2);

        // Plug USB tethering
        let tether_id = platform.simulate_usb_tethering_plug();
        let with_tether = platform.list_interfaces().unwrap();
        assert_eq!(with_tether.len(), 3);
        assert!(with_tether
            .iter()
            .any(|i| i.interface_type == InterfaceType::UsbTethering));

        // Sleep
        platform.simulate_sleep();
        assert!(platform.is_sleeping());
        let sleeping = platform.list_interfaces().unwrap();
        let wifi = sleeping.iter().find(|i| i.system_name == "en0").unwrap();
        assert!(!wifi.is_up);

        // Wake
        platform.simulate_wake();
        let awake = platform.list_interfaces().unwrap();
        let wifi_awake = awake.iter().find(|i| i.system_name == "en0").unwrap();
        assert!(wifi_awake.is_up);

        // Unplug USB tethering
        platform.simulate_usb_tethering_unplug();
        let final_list = platform.list_interfaces().unwrap();
        assert_eq!(final_list.len(), 2);
        assert!(!final_list.iter().any(|i| i.id == tether_id));
    }
}
