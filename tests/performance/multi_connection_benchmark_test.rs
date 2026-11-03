//! Performance Benchmark: Single-Connection vs Multi-Connection Download
//!
//! Validates:
//! - Multi-connection parallel streaming provides significant throughput speedup
//!   over single-connection transfers on throttled or latency-bound networks.
//! - Accurate progress calculation and file reconstruction under both configurations.

use nexus_domain::{
    DownloadPolicy, DownloadStatus, DownloadTask, InterfaceId, NetworkPath, PathLimits,
};
use nexus_download::DownloadEngine;
use nexus_events::EventBus;
use nexus_storage::{InMemoryStorage, SegmentRepository, TaskRepository};
use nexus_transport::HttpTransportClient;
use std::fs;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use url::Url;

/// Spawns a mock HTTP server that simulates network latency (throttling per TCP connection).
async fn spawn_throttled_server(
    payload: Vec<u8>,
    chunk_delay_ms: u64,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let payload = Arc::new(payload);

    let handle = tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let payload_clone = Arc::clone(&payload);

            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let n = match socket.read(&mut buf).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };

                let request = String::from_utf8_lossy(&buf[..n]);

                if request.starts_with("HEAD ") {
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\n\
                         Content-Length: {}\r\n\
                         Accept-Ranges: bytes\r\n\
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

                    let (resp_header, slice) = if is_range {
                        let chunk_len = end - start + 1;
                        (
                            format!(
                                "HTTP/1.1 206 Partial Content\r\n\
                                 Content-Range: bytes {}-{}/{}\r\n\
                                 Content-Length: {}\r\n\
                                 Accept-Ranges: bytes\r\n\
                                 \r\n",
                                start,
                                end,
                                payload_clone.len(),
                                chunk_len
                            ),
                            &payload_clone[start..=end],
                        )
                    } else {
                        (
                            format!(
                                "HTTP/1.1 200 OK\r\n\
                                 Content-Length: {}\r\n\
                                 Accept-Ranges: bytes\r\n\
                                 \r\n",
                                payload_clone.len()
                            ),
                            &payload_clone[..],
                        )
                    };

                    let _ = socket.write_all(resp_header.as_bytes()).await;

                    // Stream with per-chunk artificial latency to simulate bandwidth throttling
                    let chunk_size = 32 * 1024; // 32 KB
                    for chunk in slice.chunks(chunk_size) {
                        if chunk_delay_ms > 0 {
                            tokio::time::sleep(tokio::time::Duration::from_millis(chunk_delay_ms))
                                .await;
                        }
                        if socket.write_all(chunk).await.is_err() {
                            return;
                        }
                    }
                }

                let _ = socket.flush().await;
            });
        }
    });

    (format!("http://127.0.0.1:{port}"), handle)
}

#[tokio::test]
async fn benchmark_single_vs_multi_connection_download() {
    // 1 MB payload with simulated 10ms per 32KB chunk latency
    let payload_size = 1024 * 1024; // 1 MB = 32 chunks of 32 KB
    let payload = vec![0xAB; payload_size];
    let chunk_delay_ms = 8; // 32 chunks * 8ms = ~256ms baseline for single connection

    let (base_url, _server) = spawn_throttled_server(payload.clone(), chunk_delay_ms).await;

    let temp_dir = std::env::temp_dir().join(format!("nexus_bench_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();

    let path = NetworkPath {
        id: nexus_domain::PathId::new(),
        name: "bench-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        priority: 1,
        enabled: true,
        limits: PathLimits::default(),
    };

    // --- Benchmark 1: Single Connection (1 worker) ---
    let storage_single = Arc::new(InMemoryStorage::new());
    let transport_single = Arc::new(HttpTransportClient::default());
    let engine_single = DownloadEngine::new(
        Arc::clone(&storage_single) as Arc<dyn TaskRepository>,
        Arc::clone(&storage_single) as Arc<dyn SegmentRepository>,
        transport_single,
        Arc::new(EventBus::new(64)),
    );

    let mut task_single = DownloadTask::new(
        Url::parse(&format!("{base_url}/bench.bin")).unwrap(),
        "single.bin".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    task_single.policy = DownloadPolicy {
        max_connections: 1,
        max_speed_bps: None,
        retry_limit: 3,
        auto_resume: false,
    };
    storage_single.save_task(&task_single).await.unwrap();

    let start_single = Instant::now();
    engine_single
        .download(&task_single.id, &path, CancellationToken::new())
        .await
        .unwrap();
    let duration_single = start_single.elapsed();

    let completed_single = storage_single
        .get_task(&task_single.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed_single.status, DownloadStatus::Completed);
    assert_eq!(
        completed_single.downloaded_size.bytes(),
        payload_size as u64
    );

    // --- Benchmark 2: Multi-Connection (4 workers) ---
    let storage_multi = Arc::new(InMemoryStorage::new());
    let transport_multi = Arc::new(HttpTransportClient::default());
    let engine_multi = DownloadEngine::new(
        Arc::clone(&storage_multi) as Arc<dyn TaskRepository>,
        Arc::clone(&storage_multi) as Arc<dyn SegmentRepository>,
        transport_multi,
        Arc::new(EventBus::new(64)),
    );

    let mut task_multi = DownloadTask::new(
        Url::parse(&format!("{base_url}/bench.bin")).unwrap(),
        "multi.bin".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    task_multi.policy = DownloadPolicy {
        max_connections: 4,
        max_speed_bps: None,
        retry_limit: 3,
        auto_resume: false,
    };
    storage_multi.save_task(&task_multi).await.unwrap();

    let start_multi = Instant::now();
    engine_multi
        .download(&task_multi.id, &path, CancellationToken::new())
        .await
        .unwrap();
    let duration_multi = start_multi.elapsed();

    let completed_multi = storage_multi
        .get_task(&task_multi.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed_multi.status, DownloadStatus::Completed);
    assert_eq!(completed_multi.downloaded_size.bytes(), payload_size as u64);

    // Verify both files are byte-for-byte identical to the original payload
    let single_bytes = fs::read(temp_dir.join("single.bin")).unwrap();
    let multi_bytes = fs::read(temp_dir.join("multi.bin")).unwrap();
    assert_eq!(single_bytes, payload);
    assert_eq!(multi_bytes, payload);

    let speedup = (duration_single.as_secs_f64()) / (duration_multi.as_secs_f64());

    println!("\n=== NEXUS Multi-Connection Download Benchmark ===");
    println!("Payload Size:           {payload_size} bytes (1 MB)");
    println!("Simulated Chunk Delay:  {chunk_delay_ms} ms per 32 KB chunk");
    println!("Single-Connection Time: {:.2?}", duration_single);
    println!(
        "Multi-Connection Time:  {:.2?} (4 connections)",
        duration_multi
    );
    println!("Measured Speedup:       {:.2}x", speedup);
    println!("=================================================\n");

    // Multi-connection should be significantly faster than single connection
    assert!(
        duration_multi < duration_single,
        "Multi-connection ({:?}) should be faster than single connection ({:?})",
        duration_multi,
        duration_single
    );
    assert!(
        speedup >= 1.5,
        "Expected at least 1.5x speedup with 4 connections on throttled connection, got {:.2}x",
        speedup
    );

    let _ = fs::remove_dir_all(&temp_dir);
}
