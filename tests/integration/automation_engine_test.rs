//! Integration tests for Automation Engine (Phase 8).
//!
//! Validates:
//! - Automation rules: rule conditions, category assignment, priority ordering, destination folder mapping.
//! - Priority download queue with concurrency limits and automatic promotion.
//! - Scheduled download policies and off-peak recurring time windows.
//! - Clipboard URL extraction and sanitization.
//! - Local JSON-RPC 2.0 dispatcher with session bearer token authentication.

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use nexus_automation::{
    AutomationRule, ClipboardUrlExtractor, QueueManager, RuleActions, RuleConditions, RuleEngine,
    RuleId, SchedulePolicy, TimeWindow,
};
use nexus_domain::TaskId;
use nexus_ipc::{JsonRpcDispatcher, JsonRpcError, JsonRpcRequest, RpcHandler};
use serde_json::{json, Value};
use std::sync::Arc;
use url::Url;

#[test]
fn test_automation_rules_evaluation_and_policy_override() {
    let mut engine = RuleEngine::new();

    // Rule 1: Operating Systems (High priority: 100)
    let os_rule = AutomationRule {
        id: RuleId::new(),
        name: "OS Images".to_string(),
        priority: 100,
        enabled: true,
        conditions: RuleConditions {
            url_contains: None,
            file_extensions: vec!["iso".to_string(), "qcow2".to_string()],
            domains: vec!["kernel.org".to_string(), "ubuntu.com".to_string()],
            min_size: Some(100_000_000), // > 100 MB
            max_size: None,
        },
        actions: RuleActions {
            category: Some("Operating Systems".to_string()),
            destination_dir: Some("/storage/downloads/os".to_string()),
            speed_limit_bps: None,
            max_connections: Some(16),
            priority: Some(5),
        },
    };

    // Rule 2: Generic Archives (Lower priority: 10)
    let archive_rule = AutomationRule {
        id: RuleId::new(),
        name: "Archives".to_string(),
        priority: 10,
        enabled: true,
        conditions: RuleConditions {
            url_contains: None,
            file_extensions: vec![
                "tar.gz".to_string(),
                "zip".to_string(),
                "tar.xz".to_string(),
            ],
            domains: vec![],
            min_size: None,
            max_size: None,
        },
        actions: RuleActions {
            category: Some("Archives".to_string()),
            destination_dir: Some("/storage/downloads/archives".to_string()),
            speed_limit_bps: Some(5_000_000), // 5 MB/s
            max_connections: Some(4),
            priority: Some(1),
        },
    };

    engine.add_rule(os_rule.clone());
    engine.add_rule(archive_rule);

    // Test 1: Matching OS Image
    let os_url =
        Url::parse("https://releases.ubuntu.com/26.04/ubuntu-26.04-desktop-amd64.iso").unwrap();
    let res_os = engine.evaluate(
        &os_url,
        Some("ubuntu-26.04-desktop-amd64.iso"),
        Some(2_500_000_000),
    );
    assert_eq!(res_os.matched_rule_id, Some(os_rule.id));
    assert_eq!(res_os.category, Some("Operating Systems".to_string()));
    assert_eq!(
        res_os.destination_dir,
        Some("/storage/downloads/os".to_string())
    );
    assert_eq!(res_os.max_connections, Some(16));

    // Test 2: Matching Generic Archive
    let archive_url =
        Url::parse("https://github.com/rust-lang/rust/archive/refs/tags/1.99.0.tar.gz").unwrap();
    let res_archive = engine.evaluate(&archive_url, Some("1.99.0.tar.gz"), Some(80_000_000));
    assert_eq!(res_archive.category, Some("Archives".to_string()));
    assert_eq!(
        res_archive.destination_dir,
        Some("/storage/downloads/archives".to_string())
    );
    assert_eq!(res_archive.speed_limit_bps, Some(5_000_000));
    assert_eq!(res_archive.max_connections, Some(4));

    // Test 3: Unmatched candidate
    let unknown_url = Url::parse("https://example.com/document.docx").unwrap();
    let res_unknown = engine.evaluate(&unknown_url, Some("document.docx"), Some(15_000));
    assert_eq!(res_unknown.matched_rule_id, None);
    assert_eq!(res_unknown.category, None);
}

#[tokio::test]
async fn test_download_queue_concurrency_and_priority_promotion() {
    let queue = QueueManager::new(2); // At most 2 concurrent active tasks

    let t1 = TaskId::new();
    let t2 = TaskId::new();
    let t3 = TaskId::new();
    let t4 = TaskId::new();

    // Enqueue with varying priorities
    queue.enqueue(t1, 10).await;
    queue.enqueue(t2, 50).await;
    queue.enqueue(t3, 5).await;
    queue.enqueue(t4, 100).await;

    // Slot 1: Highest priority (t4: 100)
    let s1 = queue.poll_next().await;
    assert_eq!(s1, Some(t4));

    // Slot 2: Next highest (t2: 50)
    let s2 = queue.poll_next().await;
    assert_eq!(s2, Some(t2));

    // Both active slots occupied
    assert_eq!(queue.poll_next().await, None);
    assert_eq!(queue.active_count().await, 2);
    assert_eq!(queue.pending_count().await, 2);

    // Finish t4 -> slot becomes available for next highest pending (t1: 10)
    let promoted = queue.on_task_finished(t4).await;
    assert_eq!(promoted, Some(t1));
    assert_eq!(queue.active_count().await, 2);
    assert_eq!(queue.pending_count().await, 1);

    // Finish t2 -> slot becomes available for t3 (5)
    let promoted2 = queue.on_task_finished(t2).await;
    assert_eq!(promoted2, Some(t3));
    assert_eq!(queue.active_count().await, 2);
    assert_eq!(queue.pending_count().await, 0);

    // Finish all
    queue.on_task_finished(t1).await;
    queue.on_task_finished(t3).await;
    assert_eq!(queue.active_count().await, 0);
    assert_eq!(queue.pending_count().await, 0);
}

#[test]
fn test_scheduler_time_windows_and_off_peak_policies() {
    // Overnight off-peak window: 23:00 to 07:00
    let window = TimeWindow {
        start_hour: 23,
        start_minute: 0,
        end_hour: 7,
        end_minute: 0,
        days_of_week: vec![], // everyday
    };

    let policy = SchedulePolicy {
        scheduled_start: None,
        time_window: Some(window),
        auto_pause_outside_window: true,
        speed_limit_in_window: None, // Unlimited during off-peak
        speed_limit_outside_window: Some(1_000_000), // Throttled to 1 MB/s during peak
    };

    // 02:30 UTC -> Inside overnight window
    let t_offpeak = Utc.with_ymd_and_hms(2026, 11, 4, 2, 30, 0).unwrap();
    assert!(policy.should_be_running(&t_offpeak));
    assert_eq!(policy.effective_speed_limit(&t_offpeak), None);

    // 14:00 UTC -> Outside off-peak window
    let t_peak = Utc.with_ymd_and_hms(2026, 11, 4, 14, 0, 0).unwrap();
    assert!(!policy.should_be_running(&t_peak));
    assert_eq!(policy.effective_speed_limit(&t_peak), Some(1_000_000));

    // Future scheduled start time
    let future_start = Utc.with_ymd_and_hms(2026, 11, 5, 0, 0, 0).unwrap();
    let deferred_policy = SchedulePolicy {
        scheduled_start: Some(future_start),
        ..policy
    };
    assert!(!deferred_policy.should_be_running(&t_offpeak));
}

#[test]
fn test_clipboard_url_detection_and_sanitization() {
    let clipboard_text = r#"
        Please download the kernel image from:
        https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.14.tar.xz.
        Also we have an FTP mirror:
        ftp://ftp.funet.fi/pub/mirrors/kernel.org/pub/linux/v6.x/linux-6.14.tar.xz;
        And the torrent magnet:
        magnet:?xt=urn:btih:3b245504d5f72b11e08b686ab3460d1fc154ad60&dn=linux-6.14.tar.xz
        Duplicate link:
        https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.14.tar.xz
    "#;

    let urls = ClipboardUrlExtractor::extract_urls(clipboard_text);
    assert_eq!(urls.len(), 3);
    assert_eq!(
        urls[0].as_str(),
        "https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.14.tar.xz"
    );
    assert_eq!(
        urls[1].as_str(),
        "ftp://ftp.funet.fi/pub/mirrors/kernel.org/pub/linux/v6.x/linux-6.14.tar.xz"
    );
    assert!(urls[2].as_str().starts_with("magnet:?xt=urn:btih:"));
}

struct MockRpcHandler;

#[async_trait]
impl RpcHandler for MockRpcHandler {
    async fn handle(&self, method: &str, params: Option<Value>) -> Result<Value, JsonRpcError> {
        match method {
            "system.status" => Ok(json!({
                "status": "healthy",
                "active_downloads": 2,
                "version": "0.1.0"
            })),
            "download.add" => {
                let p = params.ok_or_else(|| JsonRpcError {
                    code: JsonRpcError::INVALID_PARAMS,
                    message: "Missing params".to_string(),
                    data: None,
                })?;
                let url = p
                    .get("url")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| JsonRpcError {
                        code: JsonRpcError::INVALID_PARAMS,
                        message: "Missing 'url' field".to_string(),
                        data: None,
                    })?;
                Ok(json!({
                    "task_id": "test-task-1234",
                    "queued_url": url
                }))
            }
            other => Err(JsonRpcError::method_not_found(other)),
        }
    }
}

#[tokio::test]
async fn test_json_rpc_dispatcher_and_authentication() {
    let session_token = "secret_session_bearer_token".to_string();
    let handler = Arc::new(MockRpcHandler);
    let dispatcher = JsonRpcDispatcher::new(session_token.clone(), handler);

    // 1. Authenticated successful request: system.status
    let req = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(Value::from(1)),
        method: "system.status".to_string(),
        params: None,
    };

    let auth_header = format!("Bearer {session_token}");
    let resp = dispatcher.dispatch(Some(&auth_header), req).await;
    assert_eq!(resp.jsonrpc, "2.0");
    assert_eq!(resp.id, Some(Value::from(1)));
    assert!(resp.error.is_none());
    assert_eq!(
        resp.result.unwrap().get("status").and_then(|v| v.as_str()),
        Some("healthy")
    );

    // 2. Authenticated request with params: download.add
    let add_req = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(Value::from(2)),
        method: "download.add".to_string(),
        params: Some(json!({"url": "https://example.com/file.zip"})),
    };
    let add_resp = dispatcher.dispatch(Some(&auth_header), add_req).await;
    assert!(add_resp.error.is_none());
    assert_eq!(
        add_resp
            .result
            .unwrap()
            .get("task_id")
            .and_then(|v| v.as_str()),
        Some("test-task-1234")
    );

    // 3. Unauthenticated request (wrong token) -> Unauthorized (-32001)
    let bad_auth_req = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(Value::from(3)),
        method: "system.status".to_string(),
        params: None,
    };
    let bad_resp = dispatcher
        .dispatch(Some("Bearer invalid_token"), bad_auth_req)
        .await;
    assert!(bad_resp.result.is_none());
    assert_eq!(bad_resp.error.unwrap().code, JsonRpcError::UNAUTHORIZED);

    // 4. Missing auth header -> Unauthorized (-32001)
    let no_auth_req = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(Value::from(4)),
        method: "system.status".to_string(),
        params: None,
    };
    let no_auth_resp = dispatcher.dispatch(None, no_auth_req).await;
    assert_eq!(no_auth_resp.error.unwrap().code, JsonRpcError::UNAUTHORIZED);

    // 5. Unknown method -> Method not found (-32601)
    let unknown_req = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(Value::from(5)),
        method: "nonexistent.method".to_string(),
        params: None,
    };
    let unknown_resp = dispatcher.dispatch(Some(&auth_header), unknown_req).await;
    assert_eq!(
        unknown_resp.error.unwrap().code,
        JsonRpcError::METHOD_NOT_FOUND
    );
}
