//! NEXUS Native Desktop Application Shell

pub mod controller;
pub mod state;

use anyhow::{Context, Result};
use controller::DesktopController;
use nexus_application::NexusEngine;
use nexus_automation::{QueueManager, RuleEngine};
use nexus_download::DownloadEngine;
use nexus_events::EventBus;
use nexus_network::NetworkManager;
use nexus_network_platform::SystemNetworkPlatform;
use nexus_path_scheduler::{AdaptivePathScheduler, PathMetricsTracker};
use nexus_protocols::ProtocolManager;
use nexus_proxy::ProxyManager;
use nexus_storage::InMemoryStorage;
use nexus_telemetry::init_telemetry;
use nexus_transport::{ConnectionOptions, HttpTransportClient};
use state::DesktopView;
use std::sync::Arc;
use tracing::info;

fn build_desktop_engine() -> Result<Arc<NexusEngine>> {
    let storage = Arc::new(InMemoryStorage::new());
    let platform = Arc::new(SystemNetworkPlatform);
    let network_manager = Arc::new(NetworkManager::new(platform));
    let proxy_manager = Arc::new(ProxyManager::new());
    let event_bus = Arc::new(EventBus::default());
    let protocol_manager = Arc::new(ProtocolManager::new());
    let queue_manager = Arc::new(QueueManager::new(4));
    let rule_engine = Arc::new(RuleEngine::new());
    let path_scheduler = Arc::new(AdaptivePathScheduler::default());
    let metrics_tracker = Arc::new(PathMetricsTracker::new(3));

    let transport = Arc::new(
        HttpTransportClient::new(&ConnectionOptions::default())
            .context("Failed to initialize HTTP transport")?,
    );
    let download_engine = Arc::new(DownloadEngine::new(
        storage.clone(),
        storage.clone(),
        transport,
        event_bus.clone(),
    ));

    Ok(Arc::new(NexusEngine::new(
        storage.clone(),
        storage.clone(),
        network_manager,
        proxy_manager,
        protocol_manager,
        queue_manager,
        rule_engine,
        path_scheduler,
        metrics_tracker,
        download_engine,
        event_bus,
    )))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = init_telemetry("info");

    info!("Starting NEXUS Desktop Application (Native First)");

    let engine = build_desktop_engine()?;
    let controller = DesktopController::new(engine);

    // Initial state refresh
    controller.refresh().await?;

    println!("{}", controller.render().await);

    // Switch view demo
    controller.switch_view(DesktopView::NetworkMonitor).await;
    println!("{}", controller.render().await);

    info!("NEXUS Desktop Application shell is ready.");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[tokio::test]
    async fn test_desktop_controller_lifecycle_and_views() {
        let engine = build_desktop_engine().unwrap();
        let controller = DesktopController::new(engine);

        // 1. Initial State
        controller.refresh().await.unwrap();
        let st = controller.state().await;
        assert_eq!(st.active_view, DesktopView::Dashboard);
        assert_eq!(st.downloads.len(), 0);

        // 2. Add Download via UI action
        let task = controller
            .add_download(
                Url::parse("https://example.com/testfile.bin").unwrap(),
                "testfile.bin".to_string(),
                "/tmp".to_string(),
                Some(1),
                false,
            )
            .await
            .unwrap();

        let st_after_add = controller.state().await;
        assert_eq!(st_after_add.downloads.len(), 1);
        assert_eq!(st_after_add.downloads[0].id, task.id);

        // 3. Switch views
        controller.switch_view(DesktopView::NetworkMonitor).await;
        let st_net = controller.state().await;
        assert_eq!(st_net.active_view, DesktopView::NetworkMonitor);

        // 4. Render check
        let rendered = controller.render().await;
        assert!(rendered.contains("NEXUS NATIVE DESKTOP SHELL"));
        assert!(rendered.contains("NETWORK INTERFACES & CONFIGURED PATHS"));

        // 5. Pause and resume via UI controller
        controller.pause_download(&task.id).await.unwrap();
        let st_paused = controller.state().await;
        assert_eq!(
            st_paused.downloads[0].status,
            nexus_domain::DownloadStatus::Paused
        );

        controller.cancel_download(&task.id).await.unwrap();
        let st_cancelled = controller.state().await;
        assert_eq!(
            st_cancelled.downloads[0].status,
            nexus_domain::DownloadStatus::Cancelled
        );
    }
}
