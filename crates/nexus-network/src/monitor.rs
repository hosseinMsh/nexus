//! Real-time network interface and routing state change monitor.

use nexus_domain::{InterfaceId, InterfaceType, NetworkInterface};
use nexus_network_platform::NetworkPlatform;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// Network state change events emitted by `NetworkMonitor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkChangeEvent {
    InterfaceDiscovered(NetworkInterface),
    InterfaceRemoved(InterfaceId),
    InterfaceStatusChanged {
        id: InterfaceId,
        is_up: bool,
        has_carrier: bool,
    },
    DefaultRouteChanged {
        previous: Option<InterfaceId>,
        current: Option<InterfaceId>,
    },
    SleepDetected,
    WakeDetected,
}

/// Continuous monitor polling and tracking host network state transitions.
pub struct NetworkMonitor {
    platform: Arc<dyn NetworkPlatform>,
    known_interfaces: Arc<RwLock<HashMap<InterfaceId, NetworkInterface>>>,
    current_default_route: Arc<RwLock<Option<InterfaceId>>>,
    was_sleeping: Arc<RwLock<bool>>,
    sender: broadcast::Sender<NetworkChangeEvent>,
}

impl NetworkMonitor {
    #[must_use]
    pub fn new(platform: Arc<dyn NetworkPlatform>, channel_capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(channel_capacity);
        Self {
            platform,
            known_interfaces: Arc::new(RwLock::new(HashMap::new())),
            current_default_route: Arc::new(RwLock::new(None)),
            was_sleeping: Arc::new(RwLock::new(false)),
            sender: tx,
        }
    }

    /// Subscribes to the broadcast stream of network change events.
    pub fn subscribe(&self) -> broadcast::Receiver<NetworkChangeEvent> {
        self.sender.subscribe()
    }

    /// Performs a single comparison sweep between current and previous interface states.
    pub async fn poll_once(&self) -> Vec<NetworkChangeEvent> {
        let mut events = Vec::new();

        let current_list = match self.platform.list_interfaces() {
            Ok(list) => list,
            Err(e) => {
                debug!(error = %e, "Failed to poll network interfaces");
                return events;
            }
        };

        let mut current_map: HashMap<InterfaceId, NetworkInterface> = HashMap::new();
        let mut new_default_route = None;

        for iface in current_list {
            if iface.is_default_route {
                new_default_route = Some(iface.id);
            }
            current_map.insert(iface.id, iface);
        }

        let mut known = self.known_interfaces.write().await;
        let mut def_route = self.current_default_route.write().await;
        let mut sleeping_guard = self.was_sleeping.write().await;

        // 1. Detect sleep / wake conditions
        let active_physical_count = current_map
            .values()
            .filter(|i| i.interface_type != InterfaceType::Loopback && i.is_up)
            .count();

        if !*sleeping_guard && !known.is_empty() && active_physical_count == 0 {
            *sleeping_guard = true;
            info!("System sleep detected (all physical interfaces down)");
            events.push(NetworkChangeEvent::SleepDetected);
        } else if *sleeping_guard && active_physical_count > 0 {
            *sleeping_guard = false;
            info!("System wake detected (interfaces restored)");
            events.push(NetworkChangeEvent::WakeDetected);
        }

        // 2. Detect added or updated interfaces
        for (id, iface) in &current_map {
            if let Some(prev) = known.get(id) {
                // Check status change
                if prev.is_up != iface.is_up || prev.ip_addresses != iface.ip_addresses {
                    let has_carrier = iface.is_up && !iface.ip_addresses.is_empty();
                    events.push(NetworkChangeEvent::InterfaceStatusChanged {
                        id: *id,
                        is_up: iface.is_up,
                        has_carrier,
                    });
                }
            } else if !known.is_empty() {
                // Newly added interface
                info!(id = %id, name = %iface.display_name, "New network interface discovered");
                events.push(NetworkChangeEvent::InterfaceDiscovered(iface.clone()));
            }
        }

        // 3. Detect removed interfaces
        for id in known.keys() {
            if !current_map.contains_key(id) {
                info!(id = %id, "Network interface removed");
                events.push(NetworkChangeEvent::InterfaceRemoved(*id));
            }
        }

        // 4. Detect default route changes
        if *def_route != new_default_route {
            events.push(NetworkChangeEvent::DefaultRouteChanged {
                previous: *def_route,
                current: new_default_route,
            });
            *def_route = new_default_route;
        }

        // Update known interfaces
        *known = current_map;

        // Broadcast all events
        for event in &events {
            let _ = self.sender.send(event.clone());
        }

        events
    }

    /// Spawns continuous polling in a background Tokio task.
    pub fn spawn_monitoring_task(
        self: Arc<Self>,
        interval: Duration,
        cancel_token: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while !cancel_token.is_cancelled() {
                self.poll_once().await;
                tokio::select! {
                    () = tokio::time::sleep(interval) => {}
                    () = cancel_token.cancelled() => break,
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_network_platform::SimulatedNetworkPlatform;

    #[tokio::test]
    async fn test_monitor_detects_interface_lifecycle() {
        let platform = Arc::new(SimulatedNetworkPlatform::new());
        let monitor = Arc::new(NetworkMonitor::new(platform.clone(), 64));
        let rx = monitor.subscribe();

        // Initial sweep populates baseline without interface add/remove noise
        let _baseline = monitor.poll_once().await;

        // Step 1: Simulate USB tethering attached
        let tether_id = platform.simulate_usb_tethering_plug();
        let events = monitor.poll_once().await;
        assert_eq!(events.len(), 1);
        match &events[0] {
            NetworkChangeEvent::InterfaceDiscovered(iface) => {
                assert_eq!(iface.id, tether_id);
                assert_eq!(iface.interface_type, InterfaceType::UsbTethering);
            }
            other => panic!("Expected InterfaceDiscovered, got {:?}", other),
        }

        // Step 2: Simulate tethering disconnection
        platform.simulate_usb_tethering_unplug();
        let events2 = monitor.poll_once().await;
        assert_eq!(events2.len(), 1);
        match &events2[0] {
            NetworkChangeEvent::InterfaceRemoved(id) => {
                assert_eq!(*id, tether_id);
            }
            other => panic!("Expected InterfaceRemoved, got {:?}", other),
        }

        // Step 3: Simulate Sleep and Wake
        platform.simulate_sleep();
        let sleep_events = monitor.poll_once().await;
        assert!(sleep_events.contains(&NetworkChangeEvent::SleepDetected));

        platform.simulate_wake();
        let wake_events = monitor.poll_once().await;
        assert!(wake_events.contains(&NetworkChangeEvent::WakeDetected));

        // Verify receiver caught broadcast events
        assert!(!rx.is_empty());
    }
}
