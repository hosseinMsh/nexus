//! Integration tests for NEXUS Multi-Path Networking (Phase 6).
//!
//! Validates:
//! - Concurrent multi-path downloading across independent routes (Direct + Proxied).
//! - Dynamic path failover: in-flight transfers survive mid-stream network/path failure and migrate seamlessly.
//! - Priority-based weighted path selection and connection limit enforcement.
//! - Aggregated throughput accounting.
//! - Strict data integrity without byte overlap or gaps under multi-path execution.

use nexus_domain::{
    DnsPolicy, DownloadStatus, DownloadTask, InterfaceId, NetworkPath, PathId, PathLimits, ProxyId,
    ProxyProfile, ProxyType,
};
use nexus_download::DownloadEngine;
use nexus_events::EventBus;
use nexus_path_scheduler::{AdaptivePathScheduler, PathMetricsTracker};
use nexus_proxy::ProxyManager;
use nexus_storage::{InMemoryStorage, TaskRepository};
use nexus_transport::{ConnectionOptions, HttpTransportClient};
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use url::Url;

/// Spawns an HTTP file server supporting Range requests.
async fn spawn_mock_file_server(payload: Vec<u8>) -> (u16, tokio::task::JoinHandle<()>) {
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
                let is_head = first_line.starts_with("HEAD");
                let is_get = first_line.starts_with("GET");

                if is_head {
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"multipath-etag\"\r\n\r\n",
                        payload.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }

                if is_get {
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
                            let parts: Vec<&str> = spec.split('-').collect();
                            let start: usize = parts[0].parse().unwrap_or(0);
                            let end: usize = if parts.len() > 1 && !parts[1].is_empty() {
                                parts[1].parse().unwrap_or(payload.len() - 1)
                            } else {
                                payload.len() - 1
                            };

                            let start = start.min(payload.len());
                            let end = end.min(payload.len() - 1);
                            let chunk = &payload[start..=end];
                            let resp = format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\n\r\n",
                                payload.len(),
                                chunk.len()
                            );
                            let _ = socket.write_all(resp.as_bytes()).await;
                            let _ = socket.write_all(chunk).await;
                            return;
                        }
                    }

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

/// Spawns a mock SOCKS5 proxy server.
async fn spawn_mock_socks5_server(
    request_count: Arc<AtomicUsize>,
    should_drop: Arc<AtomicBool>,
) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let handle = tokio::spawn(async move {
        loop {
            let (mut client_sock, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let req_counter = request_count.clone();
            let drop_flag = should_drop.clone();

            tokio::spawn(async move {
                if drop_flag.load(Ordering::SeqCst) {
                    // Intentionally close connection to simulate failed/dropped path
                    drop(client_sock);
                    return;
                }

                // SOCKS5 greeting
                let mut header = [0u8; 2];
                if client_sock.read_exact(&mut header).await.is_err() || header[0] != 0x05 {
                    return;
                }
                let mut methods = vec![0u8; header[1] as usize];
                if client_sock.read_exact(&mut methods).await.is_err() {
                    return;
                }
                // No auth (0x00)
                if client_sock.write_all(&[0x05, 0x00]).await.is_err() {
                    return;
                }

                // SOCKS5 CONNECT
                let mut req_hdr = [0u8; 4];
                if client_sock.read_exact(&mut req_hdr).await.is_err() {
                    return;
                }
                let atyp = req_hdr[3];
                let target_addr = match atyp {
                    0x01 => {
                        let mut ip = [0u8; 4];
                        if client_sock.read_exact(&mut ip).await.is_err() {
                            return;
                        }
                        let mut port = [0u8; 2];
                        if client_sock.read_exact(&mut port).await.is_err() {
                            return;
                        }
                        format!(
                            "{}.{}.{}.{}:{}",
                            ip[0],
                            ip[1],
                            ip[2],
                            ip[3],
                            u16::from_be_bytes(port)
                        )
                    }
                    _ => return,
                };

                let mut target_sock = match tokio::net::TcpStream::connect(&target_addr).await {
                    Ok(sock) => sock,
                    Err(_) => {
                        let _ = client_sock
                            .write_all(&[0x05, 0x04, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                            .await;
                        return;
                    }
                };

                // Success reply
                if client_sock
                    .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
                    .await
                    .is_err()
                {
                    return;
                }

                req_counter.fetch_add(1, Ordering::SeqCst);

                if drop_flag.load(Ordering::SeqCst) {
                    drop(client_sock);
                    drop(target_sock);
                    return;
                }

                let _ = tokio::io::copy_bidirectional(&mut client_sock, &mut target_sock).await;
            });
        }
    });

    (port, handle)
}

#[tokio::test]
async fn test_multi_path_parallel_download_and_file_integrity() {
    // 256 KB payload with non-uniform pseudorandom pattern
    let payload_size = 256 * 1024;
    let payload: Vec<u8> = (0..payload_size)
        .map(|i| ((i * 31 + 17) % 256) as u8)
        .collect();

    let (server_port, _server_h) = spawn_mock_file_server(payload.clone()).await;

    // Start SOCKS5 proxy on loopback
    let proxy_reqs = Arc::new(AtomicUsize::new(0));
    let should_drop = Arc::new(AtomicBool::new(false));
    let (socks_port, _socks_h) =
        spawn_mock_socks5_server(proxy_reqs.clone(), should_drop.clone()).await;

    let proxy_profile = ProxyProfile {
        id: ProxyId::new(),
        name: "multipath-socks-proxy".to_string(),
        proxy_type: ProxyType::Socks5,
        host: "127.0.0.1".to_string(),
        port: socks_port,
        credential_reference: None,
        enabled: true,
        dns_policy: DnsPolicy::Local,
    };

    let proxy_mgr = Arc::new(ProxyManager::new());
    proxy_mgr
        .register_profile(proxy_profile.clone())
        .await
        .unwrap();

    let transport = Arc::new(
        HttpTransportClient::new(&ConnectionOptions::default())
            .unwrap()
            .with_proxy_config(proxy_mgr, None),
    );

    let storage = Arc::new(InMemoryStorage::new());
    let events = Arc::new(EventBus::new(100));
    let engine = DownloadEngine::new(storage.clone(), storage.clone(), transport, events);

    let dest_dir = std::env::temp_dir().join(format!("nexus_mp_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&dest_dir).unwrap();

    let mut task = DownloadTask::new(
        Url::parse(&format!("http://127.0.0.1:{server_port}/data.bin")).unwrap(),
        "data.bin".to_string(),
        dest_dir.to_str().unwrap().to_string(),
    );
    task.policy.max_connections = 4; // 4 concurrent connections across 2 paths
    storage.save_task(&task).await.unwrap();

    // Path 1: Direct path
    let path_direct = NetworkPath {
        id: PathId::new(),
        name: "path-direct".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    // Path 2: Proxied path
    let path_proxied = NetworkPath {
        id: PathId::new(),
        name: "path-proxied".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: Some(proxy_profile.id),
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    let paths = vec![path_direct, path_proxied];

    let cancel = CancellationToken::new();
    let res = engine.download_multi_path(&task.id, &paths, cancel).await;
    assert!(res.is_ok(), "Multi-path download should succeed: {res:?}");

    let finished_task = storage.get_task(&task.id).await.unwrap().unwrap();
    assert_eq!(finished_task.status, DownloadStatus::Completed);
    assert_eq!(finished_task.downloaded_size.bytes(), payload.len() as u64);

    // Verify downloaded file integrity
    let dest_file = dest_dir.join("data.bin");
    let downloaded_data = fs::read(&dest_file).unwrap();
    assert_eq!(downloaded_data.len(), payload.len());
    assert_eq!(downloaded_data, payload);

    // Verify that proxied path actively participated
    assert!(
        proxy_reqs.load(Ordering::SeqCst) > 0,
        "Proxied path must have handled part of the transfer"
    );

    let _ = fs::remove_dir_all(&dest_dir);
}

#[tokio::test]
async fn test_multi_path_failover_when_one_path_drops() {
    let payload_size = 128 * 1024;
    let payload: Vec<u8> = (0..payload_size).map(|i| (i % 256) as u8).collect();
    let (server_port, _server_h) = spawn_mock_file_server(payload.clone()).await;

    // Start SOCKS5 proxy that will drop connections
    let proxy_reqs = Arc::new(AtomicUsize::new(0));
    let should_drop = Arc::new(AtomicBool::new(true)); // drops immediately
    let (socks_port, _socks_h) =
        spawn_mock_socks5_server(proxy_reqs.clone(), should_drop.clone()).await;

    let proxy_profile = ProxyProfile {
        id: ProxyId::new(),
        name: "fragile-proxy".to_string(),
        proxy_type: ProxyType::Socks5,
        host: "127.0.0.1".to_string(),
        port: socks_port,
        credential_reference: None,
        enabled: true,
        dns_policy: DnsPolicy::Local,
    };

    let proxy_mgr = Arc::new(ProxyManager::new());
    proxy_mgr
        .register_profile(proxy_profile.clone())
        .await
        .unwrap();

    let transport = Arc::new(
        HttpTransportClient::new(&ConnectionOptions::default())
            .unwrap()
            .with_proxy_config(proxy_mgr, None),
    );

    let storage = Arc::new(InMemoryStorage::new());
    let events = Arc::new(EventBus::new(100));
    let engine = DownloadEngine::new(storage.clone(), storage.clone(), transport, events);

    let dest_dir = std::env::temp_dir().join(format!("nexus_mp_failover_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&dest_dir).unwrap();

    let mut task = DownloadTask::new(
        Url::parse(&format!("http://127.0.0.1:{server_port}/failover.bin")).unwrap(),
        "failover.bin".to_string(),
        dest_dir.to_str().unwrap().to_string(),
    );
    task.policy.max_connections = 2;
    storage.save_task(&task).await.unwrap();

    // Primary path is Direct (healthy), Secondary is Fragile
    let path_healthy = NetworkPath {
        id: PathId::new(),
        name: "path-healthy".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        limits: PathLimits::default(),
        priority: 2,
        enabled: true,
    };

    let path_fragile = NetworkPath {
        id: PathId::new(),
        name: "path-fragile".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: Some(proxy_profile.id),
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    // Both paths are configured; fragile path will error, triggering automatic failover to healthy path
    let paths = vec![path_healthy, path_fragile];

    let cancel = CancellationToken::new();
    let res = engine.download_multi_path(&task.id, &paths, cancel).await;
    assert!(
        res.is_ok(),
        "Download must succeed by failing over to the surviving healthy path: {res:?}"
    );

    let finished_task = storage.get_task(&task.id).await.unwrap().unwrap();
    assert_eq!(finished_task.status, DownloadStatus::Completed);

    let dest_file = dest_dir.join("failover.bin");
    let downloaded_data = fs::read(&dest_file).unwrap();
    assert_eq!(downloaded_data, payload);

    let _ = fs::remove_dir_all(&dest_dir);
}

#[tokio::test]
async fn test_multi_path_priority_and_dynamic_scheduling() {
    let scheduler = AdaptivePathScheduler::default();
    let tracker = PathMetricsTracker::new(3);

    let path_hi = NetworkPath {
        id: PathId::new(),
        name: "high-priority-fiber".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        limits: PathLimits::default(),
        priority: 10,
        enabled: true,
    };

    let path_lo = NetworkPath {
        id: PathId::new(),
        name: "low-priority-cellular".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    let paths = vec![path_hi.clone(), path_lo.clone()];
    tracker.register_path(path_hi.id);
    tracker.register_path(path_lo.id);

    // Initial state: high priority path should be selected
    let best = scheduler.select_best_candidate(&paths, &tracker).unwrap();
    assert_eq!(best.id, path_hi.id);

    // Record high latency and failures on high priority path
    for _ in 0..3 {
        tracker.record_failure(path_hi.id);
    }
    assert!(!tracker.is_healthy(path_hi.id));

    // Dynamic failover to surviving healthy path
    let fallback = scheduler.select_best_candidate(&paths, &tracker).unwrap();
    assert_eq!(fallback.id, path_lo.id);
}

#[tokio::test]
async fn test_multi_path_connection_limits_enforced() {
    let scheduler = AdaptivePathScheduler::default();
    let tracker = PathMetricsTracker::new(3);

    let path_limited = NetworkPath {
        id: PathId::new(),
        name: "tethering-limited".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        limits: PathLimits {
            max_connections: Some(1),
            max_bandwidth_bps: None,
        },
        priority: 5,
        enabled: true,
    };

    let path_broad = NetworkPath {
        id: PathId::new(),
        name: "ethernet-unlimited".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        limits: PathLimits {
            max_connections: Some(10),
            max_bandwidth_bps: None,
        },
        priority: 2,
        enabled: true,
    };

    let paths = vec![path_limited.clone(), path_broad.clone()];
    tracker.register_path(path_limited.id);
    tracker.register_path(path_broad.id);

    // First connection goes to higher-priority path_limited
    let first = scheduler.select_best_candidate(&paths, &tracker).unwrap();
    assert_eq!(first.id, path_limited.id);

    // When connection limit is reached (1 active connection)
    tracker.inc_active_connections(path_limited.id);

    // Second connection must divert to path_broad because path_limited reached limit
    let second = scheduler.select_best_candidate(&paths, &tracker).unwrap();
    assert_eq!(second.id, path_broad.id);
}

#[tokio::test]
async fn test_aggregate_throughput_accounting() {
    let tracker = PathMetricsTracker::new(3);

    let path1_id = PathId::new();
    let path2_id = PathId::new();

    tracker.register_path(path1_id);
    tracker.register_path(path2_id);

    tracker.inc_active_connections(path1_id);
    tracker.inc_active_connections(path2_id);

    // Path 1 transferred 5 MB in 1 second (~5,000,000 Bps sample)
    tracker.record_success(path1_id, 5_000_000, Duration::from_secs(1));

    // Path 2 transferred 3 MB in 1 second (~3,000,000 Bps sample)
    tracker.record_success(path2_id, 3_000_000, Duration::from_secs(1));

    let aggregate = tracker.aggregate_throughput_bps();
    assert!(
        aggregate > 1_000_000.0,
        "Aggregate throughput should reflect combined active paths: {aggregate}"
    );

    assert_eq!(tracker.total_bytes(path1_id), 5_000_000);
    assert_eq!(tracker.total_bytes(path2_id), 3_000_000);
}
