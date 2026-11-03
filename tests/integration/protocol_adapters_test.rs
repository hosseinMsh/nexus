//! Integration tests for Protocol Extensions (Phase 7).
//!
//! Validates:
//! - ProtocolManager dispatching and fallback across HTTP, FTP, Magnet, and WebDAV.
//! - FTP RFC 959 session handling, SIZE probing, and REST range downloads against mock FTP server.
//! - Bencode parsing, Magnet URI decoding, and metadata extraction.
//! - Metalink 4.0 / 3.0 multi-mirror parsing and priority sorting.
//! - WebDAV URL mapping and Range downloads.

use nexus_domain::{InterfaceId, NetworkPath, PathId, PathLimits};
use nexus_protocols::{
    BencodeValue, FtpProtocolAdapter, HttpProtocolAdapter, MagnetProtocolAdapter, MagnetUri,
    MetalinkParser, ProtocolAdapter, ProtocolKind, ProtocolManager, WebDavProtocolAdapter,
};
use nexus_transport::{ConnectionOptions, HttpTransportClient};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use url::Url;

/// Spawns a hermetic local FTP mock server for testing RFC 959, SIZE, EPSV, and REST.
async fn spawn_mock_ftp_server(payload: Vec<u8>) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let control_port = listener.local_addr().unwrap().port();
    let payload = Arc::new(payload);

    let handle = tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let payload = payload.clone();
            tokio::spawn(async move {
                let (rx, mut tx) = tokio::io::split(socket);
                let mut reader = BufReader::new(rx);

                // Initial 220 banner
                if tx
                    .write_all(b"220 Nexus FTP Server Ready\r\n")
                    .await
                    .is_err()
                {
                    return;
                }

                let mut rest_offset: u64 = 0;
                let data_listener = Arc::new(Mutex::new(None::<TcpListener>));

                let mut line = String::new();
                while let Ok(n) = reader.read_line(&mut line).await {
                    if n == 0 {
                        break;
                    }

                    let trimmed = line.trim();
                    let upper = trimmed.to_uppercase();

                    if upper.starts_with("USER") {
                        let _ = tx
                            .write_all(b"331 User name okay, need password.\r\n")
                            .await;
                    } else if upper.starts_with("PASS") {
                        let _ = tx.write_all(b"230 User logged in, proceed.\r\n").await;
                    } else if upper.starts_with("TYPE") {
                        let _ = tx.write_all(b"200 Type set to I (Binary).\r\n").await;
                    } else if upper.starts_with("SIZE") {
                        let resp = format!("213 {}\r\n", payload.len());
                        let _ = tx.write_all(resp.as_bytes()).await;
                    } else if upper.starts_with("EPSV") {
                        let d_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                        let d_port = d_listener.local_addr().unwrap().port();
                        *data_listener.lock().await = Some(d_listener);

                        let resp = format!("229 Entering Extended Passive Mode (|||{d_port}|)\r\n");
                        let _ = tx.write_all(resp.as_bytes()).await;
                    } else if upper.starts_with("REST") {
                        let parts: Vec<&str> = trimmed.split_whitespace().collect();
                        if parts.len() > 1 {
                            rest_offset = parts[1].parse().unwrap_or(0);
                        }
                        let resp = format!("350 Restarting at {rest_offset}. Send RETR.\r\n");
                        let _ = tx.write_all(resp.as_bytes()).await;
                    } else if upper.starts_with("RETR") {
                        let d_opt = data_listener.lock().await.take();
                        if let Some(d_listener) = d_opt {
                            let _ = tx
                                .write_all(b"150 Opening BINARY mode data connection.\r\n")
                                .await;

                            let payload = payload.clone();
                            let start = (rest_offset as usize).min(payload.len());
                            let chunk = payload[start..].to_vec();

                            tokio::spawn(async move {
                                if let Ok((mut d_socket, _)) = d_listener.accept().await {
                                    let _ = d_socket.write_all(&chunk).await;
                                    let _ = d_socket.shutdown().await;
                                }
                            });

                            let _ = tx.write_all(b"226 Transfer complete.\r\n").await;
                        } else {
                            let _ = tx.write_all(b"425 Use EPSV first.\r\n").await;
                        }
                    } else if upper.starts_with("QUIT") {
                        let _ = tx.write_all(b"221 Goodbye.\r\n").await;
                        break;
                    } else {
                        let _ = tx.write_all(b"502 Command not implemented.\r\n").await;
                    }

                    line.clear();
                }
            });
        }
    });

    (control_port, handle)
}

/// Spawns an HTTP / WebDAV mock server.
async fn spawn_mock_webdav_server(payload: Vec<u8>) -> (u16, tokio::task::JoinHandle<()>) {
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

                let req = String::from_utf8_lossy(&buffer[..n]);
                let first_line = req.lines().next().unwrap_or("");

                if first_line.starts_with("HEAD") {
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
                        payload.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }

                if first_line.starts_with("GET") {
                    let mut range_header = None;
                    for line in req.lines() {
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

#[tokio::test]
async fn test_protocol_manager_dispatch_and_routing() {
    let transport = Arc::new(HttpTransportClient::new(&ConnectionOptions::default()).unwrap());

    let mgr = ProtocolManager::new();
    mgr.register_adapter(Arc::new(HttpProtocolAdapter::new(transport.clone())))
        .await;
    mgr.register_adapter(Arc::new(FtpProtocolAdapter::new()))
        .await;
    mgr.register_adapter(Arc::new(MagnetProtocolAdapter)).await;
    mgr.register_adapter(Arc::new(WebDavProtocolAdapter::new(transport)))
        .await;

    // HTTP / HTTPS
    let http_url = Url::parse("https://cdn.example.org/package.tar.gz").unwrap();
    let adapter = mgr.get_adapter(&http_url).await.unwrap();
    assert_eq!(adapter.kind(), ProtocolKind::Http);

    // FTP / FTPS
    let ftp_url = Url::parse("ftp://ftp.kernel.org/pub/linux/kernel.tar.xz").unwrap();
    let adapter = mgr.get_adapter(&ftp_url).await.unwrap();
    assert_eq!(adapter.kind(), ProtocolKind::Ftp);

    // Magnet
    let magnet_url =
        Url::parse("magnet:?xt=urn:btih:3b245504d5f72b11e08b686ab3460d1fc154ad60").unwrap();
    let adapter = mgr.get_adapter(&magnet_url).await.unwrap();
    assert_eq!(adapter.kind(), ProtocolKind::Magnet);

    // WebDAV
    let webdav_url = Url::parse("webdav://dav.nextcloud.com/files/notes.txt").unwrap();
    let adapter = mgr.get_adapter(&webdav_url).await.unwrap();
    assert_eq!(adapter.kind(), ProtocolKind::WebDav);

    // Unsupported scheme
    let gopher_url = Url::parse("gopher://gopher.floodgap.com/1/").unwrap();
    assert!(mgr.get_adapter(&gopher_url).await.is_err());
}

#[tokio::test]
async fn test_ftp_metadata_probing_and_range_download() {
    let payload = (0..65536).map(|i| (i % 256) as u8).collect::<Vec<u8>>();
    let (ftp_port, _ftp_h) = spawn_mock_ftp_server(payload.clone()).await;

    let ftp_adapter = FtpProtocolAdapter::new();
    let ftp_url = Url::parse(&format!("ftp://127.0.0.1:{ftp_port}/dataset.bin")).unwrap();

    let path = NetworkPath {
        id: PathId::new(),
        name: "test-ftp-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    // 1. Probe metadata (SIZE)
    let metadata = ftp_adapter.probe_metadata(&path, &ftp_url).await.unwrap();
    assert_eq!(metadata.total_size, Some(payload.len() as u64));
    assert!(metadata.supports_ranges);
    assert_eq!(metadata.suggested_filename, Some("dataset.bin".to_string()));

    // 2. Fetch specific byte range using REST (e.g. 1000..2999)
    let start_offset = 1000u64;
    let end_offset = 2999u64;
    let mut stream = ftp_adapter
        .fetch_range(&path, &ftp_url, start_offset, end_offset)
        .await
        .unwrap();

    let mut received_bytes = Vec::new();
    stream.read_to_end(&mut received_bytes).await.unwrap();

    let expected_len = (end_offset - start_offset + 1) as usize;
    assert_eq!(received_bytes.len(), expected_len);
    assert_eq!(
        received_bytes,
        &payload[start_offset as usize..=end_offset as usize]
    );
}

#[tokio::test]
async fn test_bencode_and_magnet_metadata_resolution() {
    // 1. Bencode dictionary roundtrip test
    let mut dict = BTreeMap::new();
    dict.insert(
        b"announce".to_vec(),
        BencodeValue::Bytes(b"http://tracker.example.com/announce".to_vec()),
    );
    dict.insert(b"length".to_vec(), BencodeValue::Integer(104857600)); // 100 MB
    dict.insert(
        b"name".to_vec(),
        BencodeValue::Bytes(b"debian-13.iso".to_vec()),
    );

    let bencode = BencodeValue::Dict(dict);
    let serialized = bencode.encode();
    let (decoded, consumed) = BencodeValue::decode(&serialized).unwrap();
    assert_eq!(consumed, serialized.len());
    assert_eq!(decoded, bencode);

    // 2. Magnet URI decoding and metadata probe
    let uri = "magnet:?xt=urn:btih:3b245504d5f72b11e08b686ab3460d1fc154ad60&dn=debian-live.iso&xl=1073741824";
    let magnet = MagnetUri::parse(uri).unwrap();
    assert_eq!(magnet.info_hash, "3b245504d5f72b11e08b686ab3460d1fc154ad60");
    assert_eq!(magnet.display_name, Some("debian-live.iso".to_string()));
    assert_eq!(magnet.exact_length, Some(1073741824));

    let magnet_adapter = MagnetProtocolAdapter;
    let path = NetworkPath {
        id: PathId::new(),
        name: "test-magnet-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    let magnet_url = Url::parse(uri).unwrap();
    let metadata = magnet_adapter
        .probe_metadata(&path, &magnet_url)
        .await
        .unwrap();
    assert_eq!(metadata.total_size, Some(1073741824));
    assert_eq!(
        metadata.suggested_filename,
        Some("debian-live.iso".to_string())
    );
    assert_eq!(
        metadata.etag,
        Some("3b245504d5f72b11e08b686ab3460d1fc154ad60".to_string())
    );
}

#[tokio::test]
async fn test_metalink_multi_mirror_parsing_and_priority_sorting() {
    let metalink_xml = r#"<?xml version="1.0" encoding="utf-8"?>
<metalink version="3.0" xmlns="http://www.metalinker.org/">
  <files>
    <file name="package.tar.zst">
      <size>45000000</size>
      <verification>
        <hash type="sha-256">a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e</hash>
      </verification>
      <resources>
        <url type="https" location="us" priority="3">https://us-mirror.repo.org/package.tar.zst</url>
        <url type="https" location="eu" priority="1">https://eu-primary.repo.org/package.tar.zst</url>
        <url type="https" location="jp" priority="2">https://jp-fast.repo.org/package.tar.zst</url>
      </resources>
    </file>
  </files>
</metalink>"#;

    let doc = MetalinkParser::parse(metalink_xml).unwrap();
    assert_eq!(doc.files.len(), 1);

    let file = &doc.files[0];
    assert_eq!(file.name, "package.tar.zst");
    assert_eq!(file.size, Some(45000000));

    // Verify hash extraction
    assert_eq!(file.hashes.len(), 1);
    assert_eq!(file.hashes[0].algorithm, "sha-256");
    assert_eq!(
        file.hashes[0].hex_digest,
        "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"
    );

    // Verify sorted mirror priorities: priority 1 (eu), then 2 (jp), then 3 (us)
    let sorted_mirrors = file.sorted_mirrors();
    assert_eq!(sorted_mirrors.len(), 3);
    assert_eq!(
        sorted_mirrors[0].as_str(),
        "https://eu-primary.repo.org/package.tar.zst"
    );
    assert_eq!(
        sorted_mirrors[1].as_str(),
        "https://jp-fast.repo.org/package.tar.zst"
    );
    assert_eq!(
        sorted_mirrors[2].as_str(),
        "https://us-mirror.repo.org/package.tar.zst"
    );
}

#[tokio::test]
async fn test_webdav_propfind_and_download() {
    let payload = b"WEBDAV_TEST_DATA_PAYLOAD_123456789".to_vec();
    let (server_port, _server_h) = spawn_mock_webdav_server(payload.clone()).await;

    let transport = Arc::new(HttpTransportClient::new(&ConnectionOptions::default()).unwrap());
    let webdav_adapter = WebDavProtocolAdapter::new(transport);

    let webdav_url = Url::parse(&format!("webdav://127.0.0.1:{server_port}/doc.pdf")).unwrap();

    let path = NetworkPath {
        id: PathId::new(),
        name: "test-webdav-path".to_string(),
        interface_id: InterfaceId::new(),
        proxy_id: None,
        limits: PathLimits::default(),
        priority: 1,
        enabled: true,
    };

    // 1. Probe metadata
    let metadata = webdav_adapter
        .probe_metadata(&path, &webdav_url)
        .await
        .unwrap();
    assert_eq!(metadata.total_size, Some(payload.len() as u64));
    assert!(metadata.supports_ranges);

    // 2. Fetch byte range
    let mut stream = webdav_adapter
        .fetch_range(&path, &webdav_url, 0, 10)
        .await
        .unwrap();

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf.len(), 11);
    assert_eq!(buf, &payload[0..=10]);
}
