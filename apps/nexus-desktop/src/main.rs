//! NEXUS Native Desktop Application Shell

use anyhow::Result;
use nexus_application::NexusEngine;
use nexus_events::EventBus;
use nexus_network::NetworkManager;
use nexus_network_platform::SystemNetworkPlatform;
use nexus_proxy::ProxyManager;
use nexus_storage::InMemoryStorage;
use nexus_telemetry::init_telemetry;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = init_telemetry("info");

    info!("Starting NEXUS Desktop Application (Native First)");

    // Initialize core application engine
    let storage = Arc::new(InMemoryStorage::new());
    let platform = Arc::new(SystemNetworkPlatform);
    let network_manager = Arc::new(NetworkManager::new(platform));
    let proxy_manager = Arc::new(ProxyManager::new());
    let event_bus = Arc::new(EventBus::default());

    let _engine = NexusEngine::new(storage, network_manager, proxy_manager, event_bus);

    info!("NEXUS Desktop Engine initialized successfully.");
    println!("NEXUS Desktop Application initialized.");
    println!("Desktop UI Framework Evaluation (ADR-003: egui / iced) initialized in Phase 0.");

    Ok(())
}
