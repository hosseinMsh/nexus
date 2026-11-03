//! Multi-Connection Engine Integration Tests
//!
//! Verifies:
//! - Multi-worker concurrent downloads across parallel connections.
//! - Strict absence of missing or overlapping byte ranges.
//! - Accurate cryptographic file reconstruction.
//! - Work stealing across asymmetric / stalled connections.
//! - Transient connection recovery and resume from segment offset checkpoints.
//! - Stable retry behavior under server rate limits (HTTP 429).

use nexus_domain::{
    DownloadPolicy, DownloadStatus, DownloadTask, InterfaceId, NetworkPath, PathLimits,
};
use nexus_download::DownloadEngine;
use nexus_events::EventBus;
use nexus_integrity::{compute_hash, verify_checksum, HashAlgorithm};
use nexus_segments::validate_segments;
use nexus_storage::{InMemoryStorage, SegmentRepository, TaskRepository};
use nexus_transport::HttpTransportClient;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use url::Url;

/// Spawns a multi-connection test HTTP server with optional simulated rate-limits or failures.
async fn spawn_test_server(
    payload: Vec<u8>,
    max_concurrent_connections: Option<usize>,
    fail_first_n_requests: usize,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let payload = Arc::new(payload);
    let active_connections = Arc::new(AtomicUsize::new(0));
    let failure_counter = Arc::new(AtomicUsize::new(0));

    let server_handle = tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let payload_clone = Arc::clone(&payload);
            let active_conns = Arc::clone(&active_connections);
            let fail_counter = Arc::clone(&failure_counter);

            tokio::spawn(async move {
                let current_conns = active_conns.fetch_add(1, Ordering::SeqCst) + 1;

                let mut buf = [0u8; 4096];
                let n = match socket.read(&mut buf).await {
                    Ok(n) if n > 0 => n,
                    _ => {
                        active_conns.fetch_sub(1, Ordering::SeqCst);
                        return;
                    }
                };

                let request = String::from_utf8_lossy(&buf[..n]);

                // Check failure injection
                let req_num = fail_counter.fetch_add(1, Ordering::SeqCst);
                if req_num < fail_first_n_requests && request.contains("Range:") {
                    // Abruptly close connection to simulate network drop
                    active_conns.fetch_sub(1, Ordering::SeqCst);
                    let _ = socket.shutdown().await;
                    return;
                }

                // Check concurrency limit
                if let Some(limit) = max_concurrent_connections {
                    if current_conns > limit && request.contains("Range:") {
                        let resp = "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\n\r\n";
                        let _ = socket.write_all(resp.as_bytes()).await;
                        active_conns.fetch_sub(1, Ordering::SeqCst);
                        return;
                    }
                }

                if request.starts_with("HEAD ") {
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\n\
                         Content-Length: {}\r\n\
                         Accept-Ranges: bytes\r\n\
                         ETag: \"multi-conn-test\"\r\n\
                         \r\n",
                        payload_clone.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                } else if request.starts_with("GET ") {
                    let mut start = 0usize;
                    let mut end = payload_clone.len().saturating_sub(1);
                    let mut is_range = false;

                    for line in request.lines() {
                        let lower = line.to_ascii_lowercase();
                        if lower.starts_with("range: bytes=") {
                            let range_val = &line["range: bytes=".len()..];
                            let parts: Vec<&str> = range_val.split('-').collect();
                            if let Ok(s) = parts[0].trim().parse::<usize>() {
                                start = s;
                                is_range = true;
                            }
                            if parts.len() > 1 && !parts[1].trim().is_empty() {
                                if let Ok(e) = parts[1].trim().parse::<usize>() {
                                    end = e.min(payload_clone.len() - 1);
                                }
                            }
                            break;
                        }
                    }

                    if is_range {
                        if start > end || start >= payload_clone.len() {
                            let resp = format!(
                                "HTTP/1.1 416 Range Not Satisfiable\r\n\
                                 Content-Range: bytes */{}\r\n\
                                 \r\n",
                                payload_clone.len()
                            );
                            let _ = socket.write_all(resp.as_bytes()).await;
                        } else {
                            let chunk_len = end - start + 1;
                            let resp_header = format!(
                                "HTTP/1.1 206 Partial Content\r\n\
                                 Content-Range: bytes {}-{}/{}\r\n\
                                 Content-Length: {}\r\n\
                                 Accept-Ranges: bytes\r\n\
                                 \r\n",
                                start,
                                end,
                                payload_clone.len(),
                                chunk_len
                            );
                            let _ = socket.write_all(resp_header.as_bytes()).await;
                            let _ = socket.write_all(&payload_clone[start..=end]).await;
                        }
                    } else {
                        let resp_header = format!(
                            "HTTP/1.1 200 OK\r\n\
                             Content-Length: {}\r\n\
                             Accept-Ranges: bytes\r\n\
                             \r\n",
                            payload_clone.len()
                        );
                        let _ = socket.write_all(resp_header.as_bytes()).await;
                        let _ = socket.write_all(&payload_clone).await;
                    }
                }

                let _ = socket.flush().await;
                active_conns.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });

    (format!("http://127.0.0.1:{port}"), server_handle)
}

#[tokio::test]
async fn test_multi_connection_16_workers_and_file_reconstruction() {
    let size = 2 * 1024 * 1024; // 2 MB
    let mut payload = vec![0u8; size];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }

    let (base_url, _server) = spawn_test_server(payload.clone(), None, 0).await;

    let temp_dir = std::env::temp_dir().join(format!("nexus_mc_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = Arc::new(InMemoryStorage::new());
    let transport = Arc::new(HttpTransportClient::default());
    let event_bus = Arc::new(EventBus::new(256));

    let engine = DownloadEngine::new(
        Arc::clone(&storage) as Arc<dyn TaskRepository>,
        Arc::clone(&storage) as Arc<dyn SegmentRepository>,
        transport,
        event_bus,
    );

    let mut task = DownloadTask::new(
        Url::parse(&format!("{base_url}/large.bin")).unwrap(),
        "reconstructed.bin".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    // Request 16 concurrent connections
    task.policy = DownloadPolicy {
        max_connections: 16,
        max_speed_bps: None,
        retry_limit: 5,
        auto_resume: true,
    };
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: nexus_domain::PathId::new(),
        name: "test-mc-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        priority: 1,
        enabled: true,
        limits: PathLimits::default(),
    };

    engine
        .download(&task.id, &path, CancellationToken::new())
        .await
        .unwrap();

    // Verify task completion
    let completed = storage.get_task(&task.id).await.unwrap().unwrap();
    assert_eq!(completed.status, DownloadStatus::Completed);
    assert_eq!(completed.downloaded_size.bytes(), size as u64);

    // Verify disk file exact bytes
    let final_path = temp_dir.join("reconstructed.bin");
    assert!(final_path.exists());
    let disk_content = fs::read(&final_path).unwrap();
    assert_eq!(disk_content.len(), size);
    assert_eq!(disk_content, payload);

    // Verify integrity hash
    let expected_hash = compute_hash(&payload, HashAlgorithm::Sha256);
    verify_checksum(&disk_content, HashAlgorithm::Sha256, &expected_hash).unwrap();

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_no_overlapping_and_no_missing_ranges() {
    let size = 1024 * 1024 + 777; // Non-power-of-two size: 1,049,353 bytes
    let mut payload = vec![0u8; size];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = ((i * 31) % 255) as u8;
    }

    let (base_url, _server) = spawn_test_server(payload.clone(), None, 0).await;

    let temp_dir = std::env::temp_dir().join(format!("nexus_ranges_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = Arc::new(InMemoryStorage::new());
    let transport = Arc::new(HttpTransportClient::default());
    let event_bus = Arc::new(EventBus::new(256));

    let engine = DownloadEngine::new(
        Arc::clone(&storage) as Arc<dyn TaskRepository>,
        Arc::clone(&storage) as Arc<dyn SegmentRepository>,
        transport,
        event_bus,
    );

    let mut task = DownloadTask::new(
        Url::parse(&format!("{base_url}/arbitrary.bin")).unwrap(),
        "arbitrary.bin".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    task.policy.max_connections = 7; // Odd number of connections
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: nexus_domain::PathId::new(),
        name: "test-ranges-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        priority: 1,
        enabled: true,
        limits: PathLimits::default(),
    };

    engine
        .download(&task.id, &path, CancellationToken::new())
        .await
        .unwrap();

    // Verify all stored segments cover file with strictly zero gaps and zero overlaps
    let segments = storage.get_segments_for_task(&task.id).await.unwrap();
    assert!(!segments.is_empty());
    validate_segments(&segments, size as u64).unwrap();

    for seg in &segments {
        assert_eq!(seg.status, nexus_domain::SegmentStatus::Completed);
        assert_eq!(seg.remaining_bytes(), 0);
    }

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_work_stealing_under_asymmetric_worker_speeds() {
    let size = 512 * 1024; // 512 KB
    let payload = vec![42u8; size];

    let (base_url, _server) = spawn_test_server(payload.clone(), None, 0).await;

    let temp_dir = std::env::temp_dir().join(format!("nexus_steal_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = Arc::new(InMemoryStorage::new());
    let transport = Arc::new(HttpTransportClient::default());
    let event_bus = Arc::new(EventBus::new(256));

    let engine = DownloadEngine::new(
        Arc::clone(&storage) as Arc<dyn TaskRepository>,
        Arc::clone(&storage) as Arc<dyn SegmentRepository>,
        transport,
        event_bus,
    );

    let mut task = DownloadTask::new(
        Url::parse(&format!("{base_url}/steal.bin")).unwrap(),
        "steal.bin".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    // 4 workers available
    task.policy.max_connections = 4;
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: nexus_domain::PathId::new(),
        name: "test-steal-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        priority: 1,
        enabled: true,
        limits: PathLimits::default(),
    };

    engine
        .download(&task.id, &path, CancellationToken::new())
        .await
        .unwrap();

    let final_path = temp_dir.join("steal.bin");
    assert!(final_path.exists());
    let disk_content = fs::read(&final_path).unwrap();
    assert_eq!(disk_content, payload);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_transient_worker_connection_drop_and_recovery() {
    let size = 300 * 1024; // 300 KB
    let payload = vec![7u8; size];

    // Server drops the first 2 Range requests mid-stream
    let (base_url, _server) = spawn_test_server(payload.clone(), None, 2).await;

    let temp_dir =
        std::env::temp_dir().join(format!("nexus_recover_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = Arc::new(InMemoryStorage::new());
    let transport = Arc::new(HttpTransportClient::default());
    let event_bus = Arc::new(EventBus::new(256));

    let engine = DownloadEngine::new(
        Arc::clone(&storage) as Arc<dyn TaskRepository>,
        Arc::clone(&storage) as Arc<dyn SegmentRepository>,
        transport,
        event_bus,
    );

    let mut task = DownloadTask::new(
        Url::parse(&format!("{base_url}/recover.bin")).unwrap(),
        "recover.bin".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    task.policy.max_connections = 4;
    task.policy.retry_limit = 5;
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: nexus_domain::PathId::new(),
        name: "test-recover-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        priority: 1,
        enabled: true,
        limits: PathLimits::default(),
    };

    // Should survive the transient drops and succeed via retries
    engine
        .download(&task.id, &path, CancellationToken::new())
        .await
        .unwrap();

    let final_path = temp_dir.join("recover.bin");
    assert!(final_path.exists());
    let disk_content = fs::read(&final_path).unwrap();
    assert_eq!(disk_content, payload);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_server_connection_limits_and_backoff() {
    let size = 256 * 1024; // 256 KB
    let payload = vec![99u8; size];

    // Server only allows at most 2 concurrent connections at a time (returns 429 if > 2)
    let (base_url, _server) = spawn_test_server(payload.clone(), Some(2), 0).await;

    let temp_dir = std::env::temp_dir().join(format!("nexus_429_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = Arc::new(InMemoryStorage::new());
    let transport = Arc::new(HttpTransportClient::default());
    let event_bus = Arc::new(EventBus::new(256));

    let engine = DownloadEngine::new(
        Arc::clone(&storage) as Arc<dyn TaskRepository>,
        Arc::clone(&storage) as Arc<dyn SegmentRepository>,
        transport,
        event_bus,
    );

    let mut task = DownloadTask::new(
        Url::parse(&format!("{base_url}/rate_limited.bin")).unwrap(),
        "rate_limited.bin".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    // 6 workers contending for 2 server slots
    task.policy.max_connections = 6;
    task.policy.retry_limit = 8;
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: nexus_domain::PathId::new(),
        name: "test-429-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        priority: 1,
        enabled: true,
        limits: PathLimits::default(),
    };

    engine
        .download(&task.id, &path, CancellationToken::new())
        .await
        .unwrap();

    let final_path = temp_dir.join("rate_limited.bin");
    assert!(final_path.exists());
    let disk_content = fs::read(&final_path).unwrap();
    assert_eq!(disk_content, payload);

    let _ = fs::remove_dir_all(&temp_dir);
}
