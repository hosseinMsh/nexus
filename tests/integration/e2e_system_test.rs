//! End-to-End System Smoke and Verification Suite (Release 1.0 Milestone).
//!
//! Validates the entire NEXUS transfer workflow:
//! 1. Automation rule matching and policy derivation
//! 2. Multi-path network routing and segment distribution
//! 3. Resilient transfer execution with HTTP Range requests
//! 4. Atomic file promotion and part-file cleanup
//! 5. Cryptographic integrity verification (SHA-256 and BLAKE3)
//! 6. JSON-RPC control plane query and statistics aggregation

use nexus_application::NexusEngine;
use nexus_automation::{AutomationRule, QueueManager, RuleActions, RuleConditions, RuleEngine};
use nexus_domain::{
    DownloadStatus, InterfaceType, NetworkInterface, NetworkPath, PathId, PathLimits,
};
use nexus_download::DownloadEngine;
use nexus_events::EventBus;
use nexus_integrity::{compute_hash, verify_checksum, HashAlgorithm};
use nexus_ipc::RpcHandler;
use nexus_network::NetworkManager;
use nexus_network_platform::SystemNetworkPlatform;
use nexus_path_scheduler::{AdaptivePathScheduler, PathMetricsTracker};
use nexus_protocols::ProtocolManager;
use nexus_proxy::ProxyManager;
use nexus_storage::{InMemoryStorage, TaskRepository};
use nexus_transport::{ConnectionOptions, HttpTransportClient};
use serde_json::json;
use std::fs;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use url::Url;

/// Spawns a hermetic mock HTTP server supporting HTTP Range requests.
async fn spawn_e2e_mock_server(payload: Vec<u8>) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let payload = Arc::new(payload);

    let handle = tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let payload = payload.clone();
            tokio::spawn(async move {
                let mut buffer = [0u8; 4096];
                let n = match socket.read(&mut buffer).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };

                let request_str = String::from_utf8_lossy(&buffer[..n]);
                let first_line = request_str.lines().next().unwrap_or("");

                if first_line.starts_with("HEAD") {
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"e2e-hash-tag\"\r\n\r\n",
                        payload.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }

                if first_line.starts_with("GET") {
                    let mut range_header = None;
                    for line in request_str.lines() {
                        if line.to_ascii_lowercase().starts_with("range:") {
                            if let Some((_, r)) = line.split_once(':') {
                                range_header = Some(r.trim().to_string());
                            }
                        }
                    }

                    if let Some(r) = range_header {
                        if let Some(spec) = r.strip_prefix("bytes=") {
                            if let Some((start_s, end_s)) = spec.split_once('-') {
                                let start: usize = start_s.parse().unwrap_or(0);
                                let end: usize =
                                    end_s.parse().unwrap_or(payload.len().saturating_sub(1));
                                let end = end.min(payload.len().saturating_sub(1));

                                if start <= end && start < payload.len() {
                                    let chunk = &payload[start..=end];
                                    let resp = format!(
                                        "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\n\r\n",
                                        start,
                                        end,
                                        payload.len(),
                                        chunk.len()
                                    );
                                    let _ = socket.write_all(resp.as_bytes()).await;
                                    let _ = socket.write_all(chunk).await;
                                    return;
                                }
                            }
                        }
                    }

                    // Full payload fallback
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
                        payload.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    let _ = socket.write_all(&payload).await;
                }
            });
        }
    });

    (port, handle)
}

#[tokio::test]
async fn test_end_to_end_transfer_lifecycle_and_verification() {
    // 1. Prepare deterministic test payload (128 KB)
    let payload_size = 131_072;
    let ground_truth_data: Vec<u8> = (0..payload_size)
        .map(|i| ((i * 37 + 13) % 251) as u8)
        .collect();

    // Ground truth cryptographic hashes
    let expected_sha256 = compute_hash(&ground_truth_data, HashAlgorithm::Sha256);
    let expected_blake3 = compute_hash(&ground_truth_data, HashAlgorithm::Blake3);

    let (server_port, _server_handle) = spawn_e2e_mock_server(ground_truth_data.clone()).await;
    let target_url = Url::parse(&format!("http://127.0.0.1:{server_port}/dataset.dat")).unwrap();

    let temp_workspace = tempdir().unwrap();
    let custom_dest_dir = temp_workspace.path().join("automated_downloads");
    fs::create_dir_all(&custom_dest_dir).unwrap();

    // 2. Initialize application subsystems
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

    let engine = Arc::new(NexusEngine::new(
        storage.clone(),
        storage.clone(),
        network_mgr.clone(),
        proxy_mgr,
        protocol_mgr,
        queue_mgr,
        rule_engine.clone(),
        scheduler,
        tracker,
        download_engine.clone(),
        event_bus.clone(),
    ));

    // 3. Configure automated rule targeting .dat datasets
    rule_engine.add_rule(AutomationRule {
        id: nexus_automation::RuleId::new(),
        name: "Scientific Datasets".to_string(),
        priority: 10,
        enabled: true,
        conditions: RuleConditions {
            url_contains: None,
            file_extensions: vec!["dat".to_string()],
            domains: vec![],
            min_size: None,
            max_size: None,
        },
        actions: RuleActions {
            destination_dir: Some(custom_dest_dir.to_string_lossy().to_string()),
            category: Some("Scientific Computing".to_string()),
            max_connections: Some(4),
            speed_limit_bps: None,
            priority: Some(0), // Urgent priority
        },
    });

    // 4. Create and enqueue task through engine
    let task = engine
        .create_download(
            target_url.clone(),
            "dataset.dat".to_string(),
            temp_workspace.path().to_string_lossy().to_string(),
        )
        .await
        .unwrap();

    // Verify automated rule application
    assert_eq!(task.destination_dir, custom_dest_dir.to_string_lossy());
    assert_eq!(task.category, Some("Scientific Computing".to_string()));
    assert_eq!(task.policy.max_connections, 4);

    // 5. Configure two independent network paths for multi-path execution
    let iface1 = NetworkInterface {
        id: nexus_domain::InterfaceId::new(),
        system_name: "en0".to_string(),
        display_name: "Wi-Fi (Primary)".to_string(),
        interface_type: InterfaceType::Wifi,
        ip_addresses: vec!["192.168.1.50".parse().unwrap()],
        is_up: true,
        is_default_route: true,
        mac_address: None,
    };
    let iface2 = NetworkInterface {
        id: nexus_domain::InterfaceId::new(),
        system_name: "en1".to_string(),
        display_name: "Ethernet (Secondary)".to_string(),
        interface_type: InterfaceType::Ethernet,
        ip_addresses: vec!["10.0.0.10".parse().unwrap()],
        is_up: true,
        is_default_route: false,
        mac_address: None,
    };

    let path1 = NetworkPath {
        id: PathId::new(),
        name: "Route-WiFi".to_string(),
        interface_id: iface1.id,
        proxy_id: None,
        priority: 10,
        enabled: true,
        limits: PathLimits::default(),
    };
    let path2 = NetworkPath {
        id: PathId::new(),
        name: "Route-Ethernet".to_string(),
        interface_id: iface2.id,
        proxy_id: None,
        priority: 5,
        enabled: true,
        limits: PathLimits::default(),
    };

    let paths = vec![path1, path2];

    // 6. Execute multi-path transfer
    let cancel_token = CancellationToken::new();
    download_engine
        .download_multi_path(&task.id, &paths, cancel_token)
        .await
        .unwrap();

    // 7. Verify persisted state
    let completed_task = storage.get_task(&task.id).await.unwrap().unwrap();
    assert_eq!(completed_task.status, DownloadStatus::Completed);
    assert_eq!(completed_task.downloaded_size.bytes(), payload_size as u64);

    // 8. Verify disk file promotion and clean up of part file
    let final_path = custom_dest_dir.join("dataset.dat");
    let part_path = custom_dest_dir.join("dataset.dat.nexuspart");

    assert!(
        final_path.exists(),
        "Final promoted file must exist on disk"
    );
    assert!(
        !part_path.exists(),
        "Part file must be cleanly promoted and removed"
    );

    let on_disk_bytes = fs::read(&final_path).unwrap();
    assert_eq!(on_disk_bytes.len(), payload_size);
    assert_eq!(on_disk_bytes, ground_truth_data);

    // 9. Cryptographic Hash Verification
    verify_checksum(&on_disk_bytes, HashAlgorithm::Sha256, &expected_sha256)
        .expect("SHA-256 cryptographic verification failed for on-disk file");

    verify_checksum(&on_disk_bytes, HashAlgorithm::Blake3, &expected_blake3)
        .expect("BLAKE3 cryptographic verification failed for on-disk file");

    // 10. Query JSON-RPC API to verify task status and stats
    let rpc_task = engine
        .handle("tasks.get", Some(json!({ "id": task.id.to_string() })))
        .await
        .unwrap();

    assert_eq!(
        rpc_task.get("status").and_then(serde_json::Value::as_str),
        Some("Completed")
    );
    assert_eq!(
        rpc_task
            .get("downloaded_bytes")
            .and_then(serde_json::Value::as_u64),
        Some(payload_size as u64)
    );

    let rpc_stats = engine.handle("system.stats", None).await.unwrap();
    assert_eq!(
        rpc_stats
            .get("completed_downloads")
            .and_then(serde_json::Value::as_u64),
        Some(1)
    );
    assert!(
        rpc_stats
            .get("total_downloaded_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap()
            >= payload_size as u64
    );
}
