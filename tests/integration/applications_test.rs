//! Integration tests for Desktop & CLI applications, local JSON-RPC daemon, and engine orchestration.

use chrono::Utc;
use nexus_application::NexusEngine;
use nexus_automation::{AutomationRule, QueueManager, RuleActions, RuleConditions, RuleEngine};
use nexus_domain::{DownloadStatus, TaskId};
use nexus_download::DownloadEngine;
use nexus_events::{EventBus, SystemEvent};
use nexus_ipc::{JsonRpcDispatcher, JsonRpcRequest, JsonRpcResponse};
use nexus_network::NetworkManager;
use nexus_network_platform::SystemNetworkPlatform;
use nexus_path_scheduler::{AdaptivePathScheduler, PathMetricsTracker};
use nexus_protocols::ProtocolManager;
use nexus_proxy::ProxyManager;
use nexus_storage::InMemoryStorage;
use nexus_transport::{ConnectionOptions, HttpTransportClient};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use url::Url;

fn build_test_engine() -> Arc<NexusEngine> {
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

    let transport = Arc::new(HttpTransportClient::new(&ConnectionOptions::default()).unwrap());
    let download_engine = Arc::new(DownloadEngine::new(
        storage.clone(),
        storage.clone(),
        transport,
        event_bus.clone(),
    ));

    Arc::new(NexusEngine::new(
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
    ))
}

#[tokio::test]
async fn test_cli_engine_orchestration_flow() {
    let engine = build_test_engine();

    // 1. Add automated rule for datasets
    engine.rule_engine().add_rule(AutomationRule {
        id: nexus_automation::RuleId::new(),
        name: "Datasets".to_string(),
        priority: 5,
        enabled: true,
        conditions: RuleConditions {
            url_contains: Some("datasets".to_string()),
            file_extensions: vec!["parquet".to_string()],
            domains: vec![],
            min_size: None,
            max_size: None,
        },
        actions: RuleActions {
            destination_dir: Some("/data/lake".to_string()),
            category: Some("Scientific Data".to_string()),
            max_connections: Some(12),
            speed_limit_bps: Some(50_000_000),
            priority: Some(1),
        },
    });

    // 2. Create download through engine
    let task = engine
        .create_download(
            Url::parse("https://huggingface.co/datasets/climate.parquet").unwrap(),
            "climate.parquet".to_string(),
            "/default".to_string(),
        )
        .await
        .unwrap();

    assert_eq!(task.destination_dir, "/data/lake");
    assert_eq!(task.category, Some("Scientific Data".to_string()));
    assert_eq!(task.policy.max_connections, 12);
    assert_eq!(task.policy.max_speed_bps, Some(50_000_000));

    // 3. Queue manager verification
    assert_eq!(engine.queue_manager().pending_count().await, 1);

    // 4. Lifecycle transitions
    engine.pause_download(&task.id).await.unwrap();
    let paused = engine.get_download(&task.id).await.unwrap().unwrap();
    assert_eq!(paused.status, DownloadStatus::Paused);

    engine.cancel_download(&task.id).await.unwrap();
    let cancelled = engine.get_download(&task.id).await.unwrap().unwrap();
    assert_eq!(cancelled.status, DownloadStatus::Cancelled);

    // 5. Aggregated stats
    let stats = engine.stats().await;
    assert_eq!(stats.failed_downloads, 0);
    assert_eq!(stats.completed_downloads, 0);
}

#[tokio::test]
async fn test_json_rpc_daemon_socket_roundtrip() {
    let engine = build_test_engine();
    let token = "nexus_secret_test_token_12345".to_string();

    // Bind real TCP listener on ephemeral port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let dispatcher = Arc::new(JsonRpcDispatcher::new(token.clone(), engine));

    // Spawn headless daemon background task
    tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let disp = dispatcher.clone();
            tokio::spawn(async move {
                let (reader, mut writer) = socket.into_split();
                let mut lines = BufReader::new(reader).lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    if line.trim().is_empty() {
                        continue;
                    }

                    // Check auth header if wrapped, or validate via dispatcher
                    let req: Result<JsonRpcRequest, _> = serde_json::from_str(&line);
                    if let Ok(r) = req {
                        let auth_header = Some("Bearer nexus_secret_test_token_12345");
                        let resp = disp.dispatch(auth_header, r).await;
                        let s = serde_json::to_string(&resp).unwrap();
                        let _ = writer.write_all(s.as_bytes()).await;
                        let _ = writer.write_all(b"\n").await;
                    }
                }
            });
        }
    });

    // Client connection
    let stream = TcpStream::connect(local_addr).await.unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    // Call 1: tasks.add
    let add_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tasks.add",
        "params": {
            "url": "https://example.com/file.bin",
            "destination": "/tmp/test",
            "priority": 1
        }
    });
    writer
        .write_all(add_req.to_string().as_bytes())
        .await
        .unwrap();
    writer.write_all(b"\n").await.unwrap();

    let resp_str = lines.next_line().await.unwrap().unwrap();
    let resp: JsonRpcResponse = serde_json::from_str(&resp_str).unwrap();
    assert!(resp.error.is_none());
    let _task_id = resp
        .result
        .unwrap()
        .get("id")
        .and_then(Value::as_str)
        .unwrap()
        .to_string();

    // Call 2: tasks.list
    let list_req = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tasks.list",
        "params": null
    });
    writer
        .write_all(list_req.to_string().as_bytes())
        .await
        .unwrap();
    writer.write_all(b"\n").await.unwrap();

    let list_resp_str = lines.next_line().await.unwrap().unwrap();
    let list_resp: JsonRpcResponse = serde_json::from_str(&list_resp_str).unwrap();
    assert_eq!(list_resp.result.unwrap().as_array().unwrap().len(), 1);

    // Call 3: system.status
    let status_req = json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "system.status",
        "params": null
    });
    writer
        .write_all(status_req.to_string().as_bytes())
        .await
        .unwrap();
    writer.write_all(b"\n").await.unwrap();

    let status_resp_str = lines.next_line().await.unwrap().unwrap();
    let status_resp: JsonRpcResponse = serde_json::from_str(&status_resp_str).unwrap();
    assert_eq!(
        status_resp
            .result
            .unwrap()
            .get("status")
            .and_then(Value::as_str),
        Some("operational")
    );
}

#[tokio::test]
async fn test_desktop_event_bus_notifications_and_state() {
    let engine = build_test_engine();
    let mut bus_sub = engine.event_bus().subscribe();

    // Emit various system events
    let tid = TaskId::new();
    engine.event_bus().publish(SystemEvent::TaskCreated {
        task_id: tid,
        timestamp: Utc::now(),
    });

    let ev = bus_sub.recv().await.unwrap();
    match ev {
        SystemEvent::TaskCreated { task_id, .. } => assert_eq!(task_id, tid),
        _ => panic!("Expected TaskCreated event"),
    }
}
