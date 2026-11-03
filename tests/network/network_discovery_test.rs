//! Network Discovery & Interface Lifecycle Integration Tests
//!
//! Verifies:
//! - Accurate identification of Wi-Fi, Ethernet, Loopback, and USB Tethering.
//! - Dynamic interface connection and disconnection detection.
//! - Network path health state reflection.
//! - System sleep and wake event transitions.
//! - Fault resilience: in-flight downloads survive simulated interface drops without crashing.

use nexus_domain::{
    DownloadPolicy, DownloadStatus, DownloadTask, InterfaceType, NetworkPath, PathLimits,
};
use nexus_download::DownloadEngine;
use nexus_events::EventBus;
use nexus_network::{NetworkChangeEvent, NetworkManager, NetworkMonitor};
use nexus_network_platform::{MacOsNetworkPlatform, NetworkPlatform, SimulatedNetworkPlatform};
use nexus_storage::{InMemoryStorage, SegmentRepository, TaskRepository};
use nexus_transport::HttpTransportClient;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use url::Url;

/// Spawns an HTTP server that can be instructed to drop active client sockets.
async fn spawn_interruptible_server(
    payload: Vec<u8>,
    drop_flag: Arc<AtomicBool>,
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
            let drop_ref = Arc::clone(&drop_flag);

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

                    for line in request.lines() {
                        let lower = line.to_ascii_lowercase();
                        if lower.starts_with("range: bytes=") {
                            let range_val = &line["range: bytes=".len()..];
                            let parts: Vec<&str> = range_val.split('-').collect();
                            if let Ok(s) = parts[0].trim().parse::<usize>() {
                                start = s;
                            }
                            if parts.len() > 1 && !parts[1].trim().is_empty() {
                                if let Ok(e) = parts[1].trim().parse::<usize>() {
                                    end = e.min(payload_clone.len() - 1);
                                }
                            }
                            break;
                        }
                    }

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
                    if socket.write_all(resp_header.as_bytes()).await.is_err() {
                        return;
                    }

                    let slice = &payload_clone[start..=end];
                    for chunk in slice.chunks(16 * 1024) {
                        if drop_ref.load(Ordering::SeqCst) {
                            // Sever the socket connection abruptly
                            let _ = socket.shutdown().await;
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
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
async fn test_interface_identification_and_classification() {
    let platform = Arc::new(SimulatedNetworkPlatform::new());
    let manager = NetworkManager::new(platform);

    let ifaces = manager.list_interfaces().unwrap();
    assert!(!ifaces.is_empty());

    let lo = ifaces
        .iter()
        .find(|i| i.interface_type == InterfaceType::Loopback);
    assert!(lo.is_some());
    assert_eq!(lo.unwrap().system_name, "lo0");
    assert!(lo.unwrap().is_up);

    let wifi = ifaces
        .iter()
        .find(|i| i.interface_type == InterfaceType::Wifi);
    assert!(wifi.is_some());
    assert!(wifi.unwrap().is_default_route);
    assert!(wifi.unwrap().mac_address.is_some());

    // Also run live macOS platform inspection if on macOS
    #[cfg(target_os = "macos")]
    {
        let macos_plat = MacOsNetworkPlatform;
        let live_ifaces = macos_plat.list_interfaces().unwrap();
        assert!(!live_ifaces.is_empty());
        assert!(live_ifaces.iter().any(|i| i.system_name == "lo0"));
    }
}

#[tokio::test]
async fn test_usb_tethering_detection_and_dynamic_path_creation() {
    let platform = Arc::new(SimulatedNetworkPlatform::new());
    let manager = NetworkManager::new(platform.clone());
    let monitor = Arc::new(NetworkMonitor::new(platform.clone(), 32));

    // Initial baseline sweep
    let _ = monitor.poll_once().await;

    // Simulate plugging in a phone for USB tethering
    let tether_id = platform.simulate_usb_tethering_plug();

    // Monitor detects the new tethering interface
    let events = monitor.poll_once().await;
    let discovered = events
        .iter()
        .find(|e| matches!(e, NetworkChangeEvent::InterfaceDiscovered(i) if i.id == tether_id));
    assert!(discovered.is_some());

    // Verify filtered query finds the tethering interface
    let tether_list = manager
        .filter_interfaces_by_type(InterfaceType::UsbTethering)
        .unwrap();
    assert_eq!(tether_list.len(), 1);
    assert_eq!(tether_list[0].id, tether_id);

    // Create a NetworkPath bound to the USB tethering interface
    let path = manager
        .create_path(
            "usb-tethering-path".to_string(),
            tether_id,
            PathLimits::default(),
            20,
        )
        .await
        .unwrap();

    assert_eq!(path.interface_id, tether_id);

    // Path is healthy and active
    let health = manager.inspect_path_health(&path.id).await.unwrap();
    assert!(health.is_available);
    assert!(health.is_up);
    assert!(health.has_carrier);
}

#[tokio::test]
async fn test_interface_disconnection_and_reconnection() {
    let platform = Arc::new(SimulatedNetworkPlatform::new());
    let manager = NetworkManager::new(platform.clone());
    let monitor = Arc::new(NetworkMonitor::new(platform.clone(), 32));

    let _ = monitor.poll_once().await;

    let ifaces = manager.list_interfaces().unwrap();
    let wifi = ifaces
        .iter()
        .find(|i| i.interface_type == InterfaceType::Wifi)
        .unwrap();

    let path = manager
        .create_path("wifi-path".to_string(), wifi.id, PathLimits::default(), 10)
        .await
        .unwrap();

    // 1. Disconnect Wi-Fi
    platform.set_interface_up(&wifi.id, false);
    let events_down = monitor.poll_once().await;
    let down_event = events_down.iter().find(|e| {
        matches!(
            e,
            NetworkChangeEvent::InterfaceStatusChanged {
                id,
                is_up: false,
                ..
            } if id == &wifi.id
        )
    });
    assert!(down_event.is_some());

    // Health check reflects interface down
    let health_down = manager.inspect_path_health(&path.id).await.unwrap();
    assert!(!health_down.is_available);
    assert!(!health_down.is_up);

    // 2. Reconnect Wi-Fi
    platform.set_interface_up(&wifi.id, true);
    let events_up = monitor.poll_once().await;
    let up_event = events_up.iter().find(|e| {
        matches!(
            e,
            NetworkChangeEvent::InterfaceStatusChanged {
                id,
                is_up: true,
                ..
            } if id == &wifi.id
        )
    });
    assert!(up_event.is_some());

    // Health check reflects recovery
    let health_up = manager.inspect_path_health(&path.id).await.unwrap();
    assert!(health_up.is_available);
    assert!(health_up.is_up);
}

#[tokio::test]
async fn test_sleep_and_wake_event_handling() {
    let platform = Arc::new(SimulatedNetworkPlatform::new());
    let monitor = Arc::new(NetworkMonitor::new(platform.clone(), 32));

    let _ = monitor.poll_once().await;

    // Simulate system sleep
    platform.simulate_sleep();
    let sleep_events = monitor.poll_once().await;
    assert!(sleep_events.contains(&NetworkChangeEvent::SleepDetected));

    // Simulate system wake
    platform.simulate_wake();
    let wake_events = monitor.poll_once().await;
    assert!(wake_events.contains(&NetworkChangeEvent::WakeDetected));
}

#[tokio::test]
async fn test_download_resilience_when_interface_drops_during_transfer() {
    let size = 256 * 1024; // 256 KB
    let mut payload = vec![0u8; size];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = ((i * 17) % 255) as u8;
    }

    let drop_flag = Arc::new(AtomicBool::new(false));
    let (base_url, _server) =
        spawn_interruptible_server(payload.clone(), Arc::clone(&drop_flag)).await;

    let temp_dir =
        std::env::temp_dir().join(format!("nexus_resilience_test_{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = Arc::new(InMemoryStorage::new());
    let transport = Arc::new(HttpTransportClient::default());
    let event_bus = Arc::new(EventBus::new(128));

    let engine = DownloadEngine::new(
        Arc::clone(&storage) as Arc<dyn TaskRepository>,
        Arc::clone(&storage) as Arc<dyn SegmentRepository>,
        transport,
        event_bus,
    );

    let mut task = DownloadTask::new(
        Url::parse(&format!("{base_url}/resilience.bin")).unwrap(),
        "resilience.bin".to_string(),
        temp_dir.to_str().unwrap().to_string(),
    );
    task.policy = DownloadPolicy {
        max_connections: 4,
        max_speed_bps: None,
        retry_limit: 5,
        auto_resume: true,
    };
    storage.save_task(&task).await.unwrap();

    let path = NetworkPath {
        id: nexus_domain::PathId::new(),
        name: "resilience-path".to_string(),
        interface_id: nexus_domain::InterfaceId::new(),
        proxy_id: None,
        priority: 1,
        enabled: true,
        limits: PathLimits::default(),
    };

    // Trigger interface drop 30ms into the download, and restore it 50ms later
    let drop_signal = Arc::clone(&drop_flag);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop_signal.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop_signal.store(false, Ordering::SeqCst);
    });

    // Execute download — must survive the connection severance without crashing or corrupting
    engine
        .download(&task.id, &path, CancellationToken::new())
        .await
        .unwrap();

    let completed = storage.get_task(&task.id).await.unwrap().unwrap();
    assert_eq!(completed.status, DownloadStatus::Completed);
    assert_eq!(completed.downloaded_size.bytes(), size as u64);

    let final_path = temp_dir.join("resilience.bin");
    assert!(final_path.exists());
    let disk_content = fs::read(&final_path).unwrap();
    assert_eq!(disk_content, payload);

    let _ = fs::remove_dir_all(&temp_dir);
}
