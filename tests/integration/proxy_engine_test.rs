//! Integration tests for the NEXUS Proxy Engine.
//!
//! Validates:
//! - SOCKS5 proxying with RFC 1928 / RFC 1929 authentication.
//! - HTTP proxying with Range headers.
//! - Remote DNS resolution (`socks5h://`, ATYP 0x03).
//! - Independent per-path proxy routing.
//! - Strict isolation: zero fallback to direct unproxied connections on proxy failure.
//! - Secure credential separation at rest.
//! - Live proxy health probing.

use nexus_domain::{
    DnsPolicy, DownloadStatus, DownloadTask, InterfaceId, NetworkPath, PathId, PathLimits, ProxyId,
    ProxyProfile, ProxyType,
};
use nexus_download::DownloadEngine;
use nexus_events::EventBus;
use nexus_proxy::{check_proxy_health, ProxyManager};
use nexus_security::{CredentialStore, InMemoryCredentialStore};
use nexus_storage::{InMemoryStorage, TaskRepository};
use nexus_transport::{ConnectionOptions, HttpTransportClient, TransportClient};
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use url::Url;

/// Spawns a lightweight HTTP file server returning known test payloads.
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
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"proxy-test-etag\"\r\n\r\n",
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

                    // Full content
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

/// Spawns a mock SOCKS5 proxy server supporting optional RFC 1929 username/password auth
/// and tracking remote domain resolution requests (ATYP 0x03).
async fn spawn_mock_socks5_server(
    auth: Option<(&'static str, &'static str)>,
    remote_dns_count: Arc<AtomicUsize>,
    request_count: Arc<AtomicUsize>,
) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let handle = tokio::spawn(async move {
        loop {
            let (mut client_sock, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let remote_dns = remote_dns_count.clone();
            let req_counter = request_count.clone();

            tokio::spawn(async move {
                // 1. Read SOCKS5 greeting
                let mut header = [0u8; 2];
                if client_sock.read_exact(&mut header).await.is_err() {
                    return;
                }
                if header[0] != 0x05 {
                    return;
                }
                let nmethods = header[1] as usize;
                let mut methods = vec![0u8; nmethods];
                if client_sock.read_exact(&mut methods).await.is_err() {
                    return;
                }

                // Select auth method
                if let Some((expected_user, expected_pass)) = auth {
                    if !methods.contains(&0x02) {
                        // Method 0xFF: No acceptable methods
                        let _ = client_sock.write_all(&[0x05, 0xFF]).await;
                        return;
                    }
                    // Select 0x02: Username/Password
                    if client_sock.write_all(&[0x05, 0x02]).await.is_err() {
                        return;
                    }

                    // Read RFC 1929 auth negotiation
                    let mut auth_ver = [0u8; 1];
                    if client_sock.read_exact(&mut auth_ver).await.is_err() || auth_ver[0] != 0x01 {
                        return;
                    }
                    let mut ulen = [0u8; 1];
                    if client_sock.read_exact(&mut ulen).await.is_err() {
                        return;
                    }
                    let mut user_bytes = vec![0u8; ulen[0] as usize];
                    if client_sock.read_exact(&mut user_bytes).await.is_err() {
                        return;
                    }
                    let mut plen = [0u8; 1];
                    if client_sock.read_exact(&mut plen).await.is_err() {
                        return;
                    }
                    let mut pass_bytes = vec![0u8; plen[0] as usize];
                    if client_sock.read_exact(&mut pass_bytes).await.is_err() {
                        return;
                    }

                    let username = String::from_utf8_lossy(&user_bytes);
                    let password = String::from_utf8_lossy(&pass_bytes);

                    if username != expected_user || password != expected_pass {
                        // Auth failure: 0x01, status non-zero
                        let _ = client_sock.write_all(&[0x01, 0x01]).await;
                        return;
                    }
                    // Auth success: 0x01, 0x00
                    if client_sock.write_all(&[0x01, 0x00]).await.is_err() {
                        return;
                    }
                } else {
                    // No authentication required (0x00)
                    if client_sock.write_all(&[0x05, 0x00]).await.is_err() {
                        return;
                    }
                }

                // 2. Read SOCKS5 connection request: [VER, CMD, RSV, ATYP, DST.ADDR, DST.PORT]
                let mut req_hdr = [0u8; 4];
                if client_sock.read_exact(&mut req_hdr).await.is_err() {
                    return;
                }
                if req_hdr[0] != 0x05 || req_hdr[1] != 0x01 {
                    // Only CONNECT (0x01) supported
                    return;
                }

                let atyp = req_hdr[3];
                let target_host = match atyp {
                    0x01 => {
                        // IPv4: 4 bytes
                        let mut ip = [0u8; 4];
                        if client_sock.read_exact(&mut ip).await.is_err() {
                            return;
                        }
                        format!("{}.{}.{}.{}", ip[0], ip[1], ip[2], ip[3])
                    }
                    0x03 => {
                        // Domain name: 1 byte len + domain bytes
                        remote_dns.fetch_add(1, Ordering::SeqCst);
                        let mut dlen = [0u8; 1];
                        if client_sock.read_exact(&mut dlen).await.is_err() {
                            return;
                        }
                        let mut domain = vec![0u8; dlen[0] as usize];
                        if client_sock.read_exact(&mut domain).await.is_err() {
                            return;
                        }
                        String::from_utf8_lossy(&domain).to_string()
                    }
                    0x04 => {
                        // IPv6: 16 bytes
                        let mut ip = [0u8; 16];
                        if client_sock.read_exact(&mut ip).await.is_err() {
                            return;
                        }
                        "127.0.0.1".to_string()
                    }
                    _ => return,
                };

                let mut port_bytes = [0u8; 2];
                if client_sock.read_exact(&mut port_bytes).await.is_err() {
                    return;
                }
                let target_port = u16::from_be_bytes(port_bytes);

                // Dials target (remap 'localhost' or domain to 127.0.0.1 for local hermetic testing)
                let dial_host = if target_host == "localhost" {
                    "127.0.0.1"
                } else {
                    &target_host
                };
                let target_addr = format!("{dial_host}:{target_port}");

                let mut target_sock = match tokio::net::TcpStream::connect(&target_addr).await {
                    Ok(sock) => sock,
                    Err(_) => {
                        // SOCKS error: Host unreachable (0x04)
                        let _ = client_sock
                            .write_all(&[0x05, 0x04, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                            .await;
                        return;
                    }
                };

                // Success reply: 0x05, 0x00 (succeeded), 0x00, 0x01, BND.ADDR, BND.PORT
                if client_sock
                    .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
                    .await
                    .is_err()
                {
                    return;
                }

                req_counter.fetch_add(1, Ordering::SeqCst);

                // Proxy stream bidirectionally
                let _ = tokio::io::copy_bidirectional(&mut client_sock, &mut target_sock).await;
            });
        }
    });

    (port, handle)
}

/// Spawns a mock HTTP forward/tunneling proxy server.
async fn spawn_mock_http_proxy(
    request_count: Arc<AtomicUsize>,
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

            tokio::spawn(async move {
                let mut buffer = [0u8; 4096];
                let n = match client_sock.read(&mut buffer).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };

                req_counter.fetch_add(1, Ordering::SeqCst);
                let request_str = String::from_utf8_lossy(&buffer[..n]);
                let first_line = request_str.lines().next().unwrap_or("");

                if first_line.starts_with("CONNECT ") {
                    // Tunneling proxy
                    let parts: Vec<&str> = first_line.split_whitespace().collect();
                    if parts.len() < 2 {
                        return;
                    }
                    let target_addr = parts[1];
                    let mut target_sock = match tokio::net::TcpStream::connect(target_addr).await {
                        Ok(s) => s,
                        Err(_) => {
                            let _ = client_sock
                                .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                                .await;
                            return;
                        }
                    };

                    let _ = client_sock
                        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                        .await;
                    let _ = tokio::io::copy_bidirectional(&mut client_sock, &mut target_sock).await;
                } else {
                    // Standard HTTP proxy request: e.g. "GET http://127.0.0.1:PORT/path HTTP/1.1"
                    let parts: Vec<&str> = first_line.split_whitespace().collect();
                    if parts.len() < 3 {
                        return;
                    }
                    let method = parts[0];
                    let raw_url = parts[1];
                    let proto = parts[2];

                    let parsed_url = match Url::parse(raw_url) {
                        Ok(u) => u,
                        Err(_) => return,
                    };

                    let host = parsed_url.host_str().unwrap_or("127.0.0.1");
                    let port = parsed_url.port().unwrap_or(80);
                    let target_addr = format!("{host}:{port}");

                    let mut target_sock = match tokio::net::TcpStream::connect(&target_addr).await {
                        Ok(s) => s,
                        Err(_) => {
                            let _ = client_sock
                                .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                                .await;
                            return;
                        }
                    };

                    // Reconstruct path and forward request
                    let path_and_query = match parsed_url.query() {
                        Some(q) => format!("{}?{}", parsed_url.path(), q),
                        None => parsed_url.path().to_string(),
                    };

                    // Strip absolute URI from first line: e.g. "GET /path HTTP/1.1\r\n"
                    let mut forwarded_req = format!("{method} {path_and_query} {proto}\r\n");
                    for line in request_str.lines().skip(1) {
                        forwarded_req.push_str(line);
                        forwarded_req.push_str("\r\n");
                    }

                    if target_sock
                        .write_all(forwarded_req.as_bytes())
                        .await
                        .is_err()
                    {
                        return;
                    }

                    let _ = tokio::io::copy_bidirectional(&mut client_sock, &mut target_sock).await;
                }
            });
        }
    });

    (port, handle)
}

#[tokio::test]
async fn test_socks5_authenticated_download_and_file_integrity() {
    let payload = vec![0xAB; 64 * 1024]; // 64 KB test payload
    let (server_port, _server_h) = spawn_mock_file_server(payload.clone()).await;

    let remote_dns = Arc::new(AtomicUsize::new(0));
    let socks_reqs = Arc::new(AtomicUsize::new(0));
    let (socks_port, _socks_h) = spawn_mock_socks5_server(
        Some(("admin_user", "super_secret_nexus_pass")),
        remote_dns,
        socks_reqs.clone(),
    )
    .await;

    let cred_store = Arc::new(InMemoryCredentialStore::new());
    let cred_ref = "cred:test:socks5".to_string();
    cred_store
        .set_credential(&cred_ref, "admin_user", "super_secret_nexus_pass")
        .await
        .unwrap();

    let proxy_profile = ProxyProfile {
        id: ProxyId::new(),
        name: "test-auth-socks5".to_string(),
        proxy_type: ProxyType::Socks5,
        host: "127.0.0.1".to_string(),
        port: socks_port,
        credential_reference: Some(cred_ref),
        enabled: true,
        dns_policy: DnsPolicy::Local,
    };

    let proxy_mgr = Arc::new(ProxyManager::with_credentials(cred_store.clone()));
    proxy_mgr
        .register_profile(proxy_profile.clone())
        .await
        .unwrap();

    let transport = Arc::new(
        HttpTransportClient::new(&ConnectionOptions::default())
            .unwrap()
            .with_proxy_config(proxy_mgr, Some(cred_store)),
    );

    let storage = Arc::new(InMemoryStorage::new());
    let events = Arc::new(EventBus::new(100));
    let engine = DownloadEngine::new(storage.clone(), storage.clone(), transport, events);

    let dest_dir = std::env::temp_dir().join(format!("nexus_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&dest_dir).unwrap();

    let mut task = DownloadTask::new(
        Url::parse(&format!("http://127.0.0.1:{server_port}/sample.bin")).unwrap(),
        "sample.bin".to_string(),
        dest_dir.to_str().unwrap().to_string(),
    );
    task.policy.max_connections = 1;
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: PathId::new(),
        name: "test-socks-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: Some(proxy_profile.id),
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    let cancel = CancellationToken::new();
    let res = engine.download(&task.id, &path, cancel).await;
    assert!(
        res.is_ok(),
        "Authenticated SOCKS5 download should succeed: {res:?}"
    );

    let finished_task = storage.get_task(&task.id).await.unwrap().unwrap();
    assert_eq!(finished_task.status, DownloadStatus::Completed);
    assert_eq!(finished_task.downloaded_size.bytes(), payload.len() as u64);

    // Verify downloaded file content
    let dest_file = dest_dir.join("sample.bin");
    let downloaded_data = fs::read(&dest_file).unwrap();
    assert_eq!(downloaded_data.len(), payload.len());
    assert_eq!(downloaded_data, payload);

    // Verify proxy server processed connections
    assert!(
        socks_reqs.load(Ordering::SeqCst) > 0,
        "SOCKS5 proxy should have routed the requests"
    );

    let _ = fs::remove_dir_all(&dest_dir);
}

#[tokio::test]
async fn test_http_proxy_download_with_range_support() {
    let payload = (0..32768).map(|i| (i % 256) as u8).collect::<Vec<u8>>();
    let (server_port, _server_h) = spawn_mock_file_server(payload.clone()).await;

    let http_proxy_reqs = Arc::new(AtomicUsize::new(0));
    let (proxy_port, _proxy_h) = spawn_mock_http_proxy(http_proxy_reqs.clone()).await;

    let proxy_profile = ProxyProfile {
        id: ProxyId::new(),
        name: "test-http-proxy".to_string(),
        proxy_type: ProxyType::Http,
        host: "127.0.0.1".to_string(),
        port: proxy_port,
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

    let dest_dir = std::env::temp_dir().join(format!("nexus_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&dest_dir).unwrap();

    let mut task = DownloadTask::new(
        Url::parse(&format!("http://127.0.0.1:{server_port}/data.bin")).unwrap(),
        "data.bin".to_string(),
        dest_dir.to_str().unwrap().to_string(),
    );
    task.policy.max_connections = 2;
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: PathId::new(),
        name: "test-http-proxy-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: Some(proxy_profile.id),
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    let cancel = CancellationToken::new();
    let res = engine.download(&task.id, &path, cancel).await;
    assert!(res.is_ok(), "HTTP proxy download should succeed: {res:?}");

    let finished_task = storage.get_task(&task.id).await.unwrap().unwrap();
    assert_eq!(finished_task.status, DownloadStatus::Completed);

    let dest_file = dest_dir.join("data.bin");
    let downloaded_data = fs::read(&dest_file).unwrap();
    assert_eq!(downloaded_data, payload);
    assert!(
        http_proxy_reqs.load(Ordering::SeqCst) > 0,
        "HTTP proxy must have handled the requests"
    );

    let _ = fs::remove_dir_all(&dest_dir);
}

#[tokio::test]
async fn test_socks5_remote_dns_resolution() {
    let payload = b"REMOTE_DNS_SOCKS5_DATA".to_vec();
    let (server_port, _server_h) = spawn_mock_file_server(payload).await;

    let remote_dns = Arc::new(AtomicUsize::new(0));
    let socks_reqs = Arc::new(AtomicUsize::new(0));
    let (socks_port, _socks_h) =
        spawn_mock_socks5_server(None, remote_dns.clone(), socks_reqs.clone()).await;

    // DnsPolicy::Remote triggers "socks5h://" scheme, instructing Reqwest to send domain in ATYP 0x03
    let proxy_profile = ProxyProfile {
        id: ProxyId::new(),
        name: "remote-dns-socks5".to_string(),
        proxy_type: ProxyType::Socks5,
        host: "127.0.0.1".to_string(),
        port: socks_port,
        credential_reference: None,
        enabled: true,
        dns_policy: DnsPolicy::Remote,
    };

    let proxy_mgr = Arc::new(ProxyManager::new());
    proxy_mgr
        .register_profile(proxy_profile.clone())
        .await
        .unwrap();

    let transport = HttpTransportClient::new(&ConnectionOptions::default())
        .unwrap()
        .with_proxy_config(proxy_mgr, None);

    let path = NetworkPath {
        id: PathId::new(),
        name: "test-remote-dns-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: Some(proxy_profile.id),
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    // Using "localhost" as host so that remote resolution preserves the domain name
    let url = Url::parse(&format!("http://localhost:{server_port}/info")).unwrap();
    let metadata_res = transport.probe_metadata(&path, &url).await;
    assert!(
        metadata_res.is_ok(),
        "Remote DNS probe should succeed: {metadata_res:?}"
    );

    assert!(
        remote_dns.load(Ordering::SeqCst) > 0,
        "SOCKS5 proxy should have received domain name in ATYP 0x03"
    );
}

#[tokio::test]
async fn test_independent_per_path_proxy_routing() {
    let payload = b"PER_PATH_ROUTING_TEST".to_vec();
    let (server_port, _server_h) = spawn_mock_file_server(payload).await;

    let remote_dns = Arc::new(AtomicUsize::new(0));
    let proxy_reqs = Arc::new(AtomicUsize::new(0));
    let (socks_port, _socks_h) =
        spawn_mock_socks5_server(None, remote_dns, proxy_reqs.clone()).await;

    let proxy_profile = ProxyProfile {
        id: ProxyId::new(),
        name: "path-proxy".to_string(),
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

    let transport = HttpTransportClient::new(&ConnectionOptions::default())
        .unwrap()
        .with_proxy_config(proxy_mgr, None);

    let url = Url::parse(&format!("http://127.0.0.1:{server_port}/item")).unwrap();

    // 1. Path A has proxy configured
    let path_a = NetworkPath {
        id: PathId::new(),
        name: "path-a-socks".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: Some(proxy_profile.id),
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    let meta_a = transport.probe_metadata(&path_a, &url).await;
    assert!(meta_a.is_ok());
    let reqs_after_a = proxy_reqs.load(Ordering::SeqCst);
    assert!(
        reqs_after_a > 0,
        "Path A request must route through the proxy"
    );

    // 2. Path B is direct (no proxy)
    let path_b = NetworkPath {
        id: PathId::new(),
        name: "path-b-direct".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    let meta_b = transport.probe_metadata(&path_b, &url).await;
    assert!(meta_b.is_ok());
    let reqs_after_b = proxy_reqs.load(Ordering::SeqCst);
    assert_eq!(
        reqs_after_b, reqs_after_a,
        "Path B must NOT route through proxy; proxy count should remain unchanged"
    );
}

#[tokio::test]
async fn test_strict_no_direct_fallback_on_proxy_failure() {
    let payload = b"STRICT_SECURITY_TEST".to_vec();
    let (server_port, _server_h) = spawn_mock_file_server(payload).await;

    // Use a closed port for proxy
    let closed_port = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
        // listener dropped immediately, port is closed
    };

    let proxy_profile = ProxyProfile {
        id: ProxyId::new(),
        name: "dead-proxy".to_string(),
        proxy_type: ProxyType::Socks5,
        host: "127.0.0.1".to_string(),
        port: closed_port,
        credential_reference: None,
        enabled: true,
        dns_policy: DnsPolicy::Local,
    };

    let proxy_mgr = Arc::new(ProxyManager::new());
    proxy_mgr
        .register_profile(proxy_profile.clone())
        .await
        .unwrap();

    let transport = HttpTransportClient::new(&ConnectionOptions::default())
        .unwrap()
        .with_proxy_config(proxy_mgr, None);

    let path = NetworkPath {
        id: PathId::new(),
        name: "test-dead-proxy-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: Some(proxy_profile.id),
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    let url = Url::parse(&format!("http://127.0.0.1:{server_port}/sensitive")).unwrap();

    // Probe should strictly fail and NEVER fall back to direct connection!
    let probe_res = transport.probe_metadata(&path, &url).await;
    assert!(
        probe_res.is_err(),
        "Must NOT fall back to direct network when proxy connection fails"
    );
}

#[tokio::test]
async fn test_credential_storage_at_rest_not_in_database() {
    let cred_store = InMemoryCredentialStore::new();
    let cred_ref = "cred:proxy:production_dc".to_string();

    cred_store
        .set_credential(&cred_ref, "ops_user", "hunter2_super_secret")
        .await
        .unwrap();

    // Domain ProxyProfile must only hold reference, never plaintext password
    let profile = ProxyProfile {
        id: ProxyId::new(),
        name: "secure-profile".to_string(),
        proxy_type: ProxyType::Socks5,
        host: "proxy.internal".to_string(),
        port: 1080,
        credential_reference: Some(cred_ref.clone()),
        enabled: true,
        dns_policy: DnsPolicy::Remote,
    };

    let serialized = serde_json::to_string(&profile).unwrap();
    assert!(
        !serialized.contains("hunter2_super_secret"),
        "Plaintext password must NEVER appear in serialized database representation!"
    );
    assert!(
        !serialized.contains("ops_user"),
        "Plaintext username should NOT appear in domain profile directly"
    );
    assert!(
        serialized.contains(&cred_ref),
        "Only reference key should appear in domain storage"
    );

    // Retrieve via credential store
    let retrieved = cred_store.get_credential(&cred_ref).await.unwrap();
    assert_eq!(
        retrieved,
        Some(("ops_user".to_string(), "hunter2_super_secret".to_string()))
    );
}

#[tokio::test]
async fn test_proxy_health_check_live_and_failure() {
    let remote_dns = Arc::new(AtomicUsize::new(0));
    let reqs = Arc::new(AtomicUsize::new(0));
    let (socks_port, _socks_h) =
        spawn_mock_socks5_server(Some(("valid_user", "valid_pass")), remote_dns, reqs).await;

    let profile = ProxyProfile {
        id: ProxyId::new(),
        name: "health-test-proxy".to_string(),
        proxy_type: ProxyType::Socks5,
        host: "127.0.0.1".to_string(),
        port: socks_port,
        credential_reference: None,
        enabled: true,
        dns_policy: DnsPolicy::Local,
    };

    // 1. Health check with valid credentials -> Alive
    let health_ok = check_proxy_health(
        &profile,
        Some(("valid_user", "valid_pass")),
        Duration::from_millis(500),
    )
    .await;
    assert!(
        health_ok.is_alive,
        "Proxy should report alive with valid credentials: {health_ok:?}"
    );
    assert!(health_ok.error.is_none());

    // 2. Health check with invalid credentials -> Not alive
    let health_bad_auth = check_proxy_health(
        &profile,
        Some(("valid_user", "wrong_pass")),
        Duration::from_millis(500),
    )
    .await;
    assert!(
        !health_bad_auth.is_alive,
        "Proxy should fail auth with wrong pass"
    );
    assert!(health_bad_auth.error.is_some());

    // 3. Health check against closed port -> Not alive
    let closed_port = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
    };
    let dead_profile = ProxyProfile {
        port: closed_port,
        ..profile
    };
    let health_dead = check_proxy_health(&dead_profile, None, Duration::from_millis(200)).await;
    assert!(!health_dead.is_alive, "Closed port should report not alive");
}
