//! Application use cases and transfer engine orchestration.

pub mod engine;
pub mod rpc;
pub mod stats;

pub use engine::{ApplicationError, NexusEngine};
pub use stats::EngineStats;

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_automation::{AutomationRule, QueueManager, RuleActions, RuleConditions, RuleEngine};
    use nexus_domain::DownloadStatus;
    use nexus_download::DownloadEngine;
    use nexus_events::EventBus;
    use nexus_ipc::{JsonRpcError, RpcHandler};
    use nexus_network::NetworkManager;
    use nexus_network_platform::SystemNetworkPlatform;
    use nexus_path_scheduler::{AdaptivePathScheduler, PathMetricsTracker};
    use nexus_protocols::ProtocolManager;
    use nexus_proxy::ProxyManager;
    use nexus_storage::InMemoryStorage;
    use nexus_transport::{ConnectionOptions, HttpTransportClient};
    use serde_json::json;
    use std::sync::Arc;
    use url::Url;

    fn build_test_engine() -> NexusEngine {
        let storage = Arc::new(InMemoryStorage::new());
        let platform = Arc::new(SystemNetworkPlatform);
        let network_mgr = Arc::new(NetworkManager::new(platform));
        let proxy_mgr = Arc::new(ProxyManager::new());
        let event_bus = Arc::new(EventBus::default());
        let protocol_mgr = Arc::new(ProtocolManager::new());
        let queue_mgr = Arc::new(QueueManager::new(4));
        let rule_engine = Arc::new(RuleEngine::new());
        let scheduler = Arc::new(AdaptivePathScheduler::default());
        let tracker = Arc::new(PathMetricsTracker::new(3));

        let transport = Arc::new(HttpTransportClient::new(&ConnectionOptions::default()).unwrap());
        let download_engine = Arc::new(DownloadEngine::new(
            storage.clone(),
            storage.clone(),
            transport,
            event_bus.clone(),
        ));

        NexusEngine::new(
            storage.clone(),
            storage.clone(),
            network_mgr,
            proxy_mgr,
            protocol_mgr,
            queue_mgr,
            rule_engine,
            scheduler,
            tracker,
            download_engine,
            event_bus,
        )
    }

    #[tokio::test]
    async fn test_nexus_engine_create_and_lifecycle() {
        let engine = build_test_engine();

        // 1. Add rule
        engine.rule_engine().add_rule(AutomationRule {
            id: nexus_automation::RuleId::new(),
            name: "ISO Images".to_string(),
            priority: 10,
            enabled: true,
            conditions: RuleConditions {
                url_contains: None,
                file_extensions: vec!["iso".to_string()],
                domains: vec![],
                min_size: None,
                max_size: None,
            },
            actions: RuleActions {
                destination_dir: Some("/storage/iso".to_string()),
                category: Some("OS Images".to_string()),
                max_connections: Some(16),
                speed_limit_bps: None,
                priority: None,
            },
        });

        // 2. Create download matching rule
        let task = engine
            .create_download(
                Url::parse("https://releases.ubuntu.com/noble.iso").unwrap(),
                "noble.iso".to_string(),
                "/default/dir".to_string(),
            )
            .await
            .unwrap();

        assert_eq!(task.status, DownloadStatus::Created);
        assert_eq!(task.destination_dir, "/storage/iso");
        assert_eq!(task.category, Some("OS Images".to_string()));
        assert_eq!(task.policy.max_connections, 16);

        // 3. Pause and Resume transitions
        engine.pause_download(&task.id).await.unwrap();
        let paused_task = engine.get_download(&task.id).await.unwrap().unwrap();
        assert_eq!(paused_task.status, DownloadStatus::Paused);

        // 4. Cancel
        engine.cancel_download(&task.id).await.unwrap();
        let cancelled = engine.get_download(&task.id).await.unwrap().unwrap();
        assert_eq!(cancelled.status, DownloadStatus::Cancelled);

        // 5. Aggregate stats
        let stats = engine.stats().await;
        assert_eq!(stats.queued_downloads, 0);
    }

    #[tokio::test]
    async fn test_nexus_engine_rpc_handler() {
        let engine = build_test_engine();

        // Test RPC tasks.add
        let add_res = engine
            .handle(
                "tasks.add",
                Some(json!({
                    "url": "https://example.com/archive.zip",
                    "destination": "/tmp/test",
                    "priority": 1
                })),
            )
            .await
            .unwrap();

        let tid = add_res.get("id").and_then(|v| v.as_str()).unwrap();

        // Test RPC tasks.list
        let list_res = engine.handle("tasks.list", None).await.unwrap();
        assert_eq!(list_res.as_array().unwrap().len(), 1);

        // Test RPC tasks.pause
        let pause_res = engine
            .handle("tasks.pause", Some(json!({ "id": tid })))
            .await
            .unwrap();
        assert_eq!(
            pause_res.get("status").and_then(|v| v.as_str()),
            Some("Paused")
        );

        // Test RPC system.stats
        let stats_res = engine.handle("system.stats", None).await.unwrap();
        assert!(stats_res.get("active_downloads").is_some());

        // Test RPC unknown method
        let bad_res = engine.handle("unknown.method", None).await;
        assert_eq!(bad_res.unwrap_err().code, JsonRpcError::METHOD_NOT_FOUND);
    }
}
