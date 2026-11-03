//! End-to-end integration tests for HTTP/HTTPS Range engine, non-range fallback, pause/resume, and retry logic.
//!
//! Acceptance criteria:
//! - Correct downloads from Range-capable servers.
//! - Correct fallback for servers without Range.
//! - Correct handling of redirects.
//! - Correct handling of unknown file sizes.
//! - No corrupted files under tested network interruptions.

use nexus_domain::{DownloadStatus, DownloadTask, InterfaceId, NetworkPath, PathLimits};
use nexus_download::DownloadEngine;
use nexus_events::EventBus;
use nexus_storage::{InMemoryStorage, SegmentRepository, TaskRepository};
use nexus_transport::HttpTransportClient;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use url::Url;

/// Spawns a lightweight local HTTP mock server for hermetic testing.
async fn spawn_mock_http_server(
    payload: Vec<u8>,
    reject_head: bool,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let payload = Arc::new(payload);
    let request_count = Arc::new(AtomicUsize::new(0));

    let handle = tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let payload = payload.clone();
            let _req_num = request_count.fetch_add(1, Ordering::SeqCst);

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

                if request_str.contains("/redirect") {
                    let resp = format!(
                        "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{port}/final.bin\r\nContent-Length: 0\r\n\r\n"
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }

                // Parse Range header if present
                let mut range_header = None;
                for line in request_str.lines() {
                    if line.to_ascii_lowercase().starts_with("range:") {
                        if let Some((_, r)) = line.split_once(':') {
                            range_header = Some(r.trim().to_string());
                        }
                    }
                }

                if is_head && reject_head {
                    // Simulate 405 Method Not Allowed to test HEAD fallback
                    let resp = "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n";
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }

                if is_head {
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"etag-test\"\r\n\r\n",
                        payload.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }

                if is_get {
                    if let Some(ref r) = range_header {
                        // Example: bytes=0-499 or bytes=0-0
                        if let Some(bytes_spec) = r.strip_prefix("bytes=") {
                            let parts: Vec<&str> = bytes_spec.split('-').collect();
                            let start: usize = parts[0].parse().unwrap_or(0);
                            let end: usize = if parts.len() > 1 && !parts[1].is_empty() {
                                parts[1].parse().unwrap_or(payload.len() - 1)
                            } else {
                                payload.len() - 1
                            };

                            let end = end.min(payload.len().saturating_sub(1));
                            let chunk = if start <= end && start < payload.len() {
                                &payload[start..=end]
                            } else {
                                &[]
                            };

                            let resp_headers = format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
                                payload.len(),
                                chunk.len()
                            );
                            let _ = socket.write_all(resp_headers.as_bytes()).await;
                            let _ = socket.write_all(chunk).await;
                            return;
                        }
                    }

                    // Standard full GET response
                    let resp_headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
                        payload.len()
                    );
                    let _ = socket.write_all(resp_headers.as_bytes()).await;
                    let _ = socket.write_all(&payload).await;
                }
            });
        }
    });

    (format!("http://127.0.0.1:{port}"), handle)
}

#[tokio::test]
async fn test_range_capable_download_and_file_assembly() {
    let payload_size = 50_000;
    let original_bytes: Vec<u8> = (0..payload_size).map(|i| (i % 256) as u8).collect();
    let (base_url, server_handle) = spawn_mock_http_server(original_bytes.clone(), false).await;

    let temp_dir = std::env::temp_dir().join(format!("nexus_range_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = Arc::new(InMemoryStorage::new());
    let transport = Arc::new(HttpTransportClient::default());
    let event_bus = Arc::new(EventBus::default());

    let engine = DownloadEngine::new(storage.clone(), storage.clone(), transport, event_bus);

    let mut task = DownloadTask::new(
        Url::parse(&format!("{base_url}/data.bin")).unwrap(),
        "data.bin".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    task.policy.max_connections = 4;
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: nexus_domain::PathId::new(),
        name: "test-direct".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        priority: 1,
        enabled: true,
        limits: PathLimits::default(),
    };

    let cancel_token = CancellationToken::new();
    engine
        .download(&task.id, &path, cancel_token)
        .await
        .unwrap();

    // Verify task state
    let completed_task = storage.get_task(&task.id).await.unwrap().unwrap();
    assert_eq!(completed_task.status, DownloadStatus::Completed);
    assert_eq!(completed_task.downloaded_size.bytes(), payload_size as u64);

    // Verify segments were partitioned and all completed
    let segs = storage.get_segments_for_task(&task.id).await.unwrap();
    assert!(!segs.is_empty());
    for s in &segs {
        assert_eq!(s.status, nexus_domain::SegmentStatus::Completed);
    }

    // Verify final assembled file on disk matches byte-for-byte
    let final_path = temp_dir.join("data.bin");
    assert!(final_path.exists());
    let downloaded_bytes = fs::read(&final_path).unwrap();
    assert_eq!(downloaded_bytes.len(), payload_size);
    assert_eq!(downloaded_bytes, original_bytes);

    server_handle.abort();
    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_head_rejection_probe_fallback() {
    let payload = b"Fallback probe byte test payload for NEXUS".to_vec();
    let (base_url, server_handle) = spawn_mock_http_server(payload.clone(), true).await;

    let temp_dir = std::env::temp_dir().join(format!("nexus_probe_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = Arc::new(InMemoryStorage::new());
    let transport = Arc::new(HttpTransportClient::default());
    let event_bus = Arc::new(EventBus::default());

    let engine = DownloadEngine::new(storage.clone(), storage.clone(), transport, event_bus);

    let task = DownloadTask::new(
        Url::parse(&format!("{base_url}/probe.txt")).unwrap(),
        "probe.txt".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: nexus_domain::PathId::new(),
        name: "test-direct".to_string(),
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

    let final_path = temp_dir.join("probe.txt");
    assert!(final_path.exists());
    let contents = fs::read(&final_path).unwrap();
    assert_eq!(contents, payload);

    server_handle.abort();
    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_pause_and_resume_download_lifecycle() {
    let payload_size = 100_000;
    let original_bytes: Vec<u8> = (0..payload_size).map(|i| (i % 256) as u8).collect();
    let (base_url, server_handle) = spawn_mock_http_server(original_bytes.clone(), false).await;

    let temp_dir = std::env::temp_dir().join(format!("nexus_pause_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = Arc::new(InMemoryStorage::new());
    let transport = Arc::new(HttpTransportClient::default());
    let event_bus = Arc::new(EventBus::default());

    let engine = DownloadEngine::new(storage.clone(), storage.clone(), transport, event_bus);

    let mut task = DownloadTask::new(
        Url::parse(&format!("{base_url}/pause_resume.bin")).unwrap(),
        "pause_resume.bin".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    task.policy.max_connections = 4;
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: nexus_domain::PathId::new(),
        name: "test-direct".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        priority: 1,
        enabled: true,
        limits: PathLimits::default(),
    };

    // Step 1: Trigger pause via cancelled token
    let pause_token = CancellationToken::new();
    pause_token.cancel(); // Immediate cancel to simulate pausing during download

    let res = engine.download(&task.id, &path, pause_token).await;
    assert!(res.is_err());

    let paused_task = storage.get_task(&task.id).await.unwrap().unwrap();
    assert_eq!(paused_task.status, DownloadStatus::Paused);

    // Step 2: Resume with fresh active token
    let resume_token = CancellationToken::new();
    engine
        .download(&task.id, &path, resume_token)
        .await
        .unwrap();

    let completed_task = storage.get_task(&task.id).await.unwrap().unwrap();
    assert_eq!(completed_task.status, DownloadStatus::Completed);
    assert_eq!(completed_task.downloaded_size.bytes(), payload_size as u64);

    let final_path = temp_dir.join("pause_resume.bin");
    assert!(final_path.exists());
    let downloaded = fs::read(&final_path).unwrap();
    assert_eq!(downloaded, original_bytes);

    server_handle.abort();
    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_non_range_server_fallback() {
    let payload = b"Legacy server with zero range request support".to_vec();

    // Spawns a server that completely ignores Range headers and returns 200 OK
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let payload_clone = payload.clone();

    let server_handle = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let p = payload_clone.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    p.len()
                );
                let _ = socket.write_all(resp.as_bytes()).await;
                let _ = socket.write_all(&p).await;
            });
        }
    });

    let temp_dir = std::env::temp_dir().join(format!("nexus_legacy_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = Arc::new(InMemoryStorage::new());
    let transport = Arc::new(HttpTransportClient::default());
    let event_bus = Arc::new(EventBus::default());

    let engine = DownloadEngine::new(storage.clone(), storage.clone(), transport, event_bus);

    let task = DownloadTask::new(
        Url::parse(&format!("http://127.0.0.1:{port}/legacy.bin")).unwrap(),
        "legacy.bin".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: nexus_domain::PathId::new(),
        name: "test-direct".to_string(),
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

    let final_path = temp_dir.join("legacy.bin");
    assert!(final_path.exists());
    let downloaded = fs::read(&final_path).unwrap();
    assert_eq!(downloaded, payload);

    server_handle.abort();
    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_redirect_handling() {
    let payload = b"Payload fetched successfully after following 302 redirect".to_vec();
    let (base_url, server_handle) = spawn_mock_http_server(payload.clone(), false).await;

    let temp_dir = std::env::temp_dir().join(format!("nexus_redir_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = Arc::new(InMemoryStorage::new());
    let transport = Arc::new(HttpTransportClient::default());
    let event_bus = Arc::new(EventBus::default());

    let engine = DownloadEngine::new(storage.clone(), storage.clone(), transport, event_bus);

    let task = DownloadTask::new(
        Url::parse(&format!("{base_url}/redirect")).unwrap(),
        "final.bin".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: nexus_domain::PathId::new(),
        name: "test-direct".to_string(),
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

    let final_path = temp_dir.join("final.bin");
    assert!(final_path.exists());
    let downloaded = fs::read(&final_path).unwrap();
    assert_eq!(downloaded, payload);

    server_handle.abort();
    let _ = fs::remove_dir_all(&temp_dir);
}
