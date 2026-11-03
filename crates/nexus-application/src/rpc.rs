//! JSON-RPC 2.0 dispatch handler bridging external clients and frontends to `NexusEngine`.

use crate::engine::NexusEngine;
use async_trait::async_trait;
use nexus_domain::{DownloadStatus, TaskId};
use nexus_ipc::{JsonRpcError, RpcHandler};
use serde_json::{json, Value};
use std::str::FromStr;
use tracing::info;
use url::Url;

#[async_trait]
impl RpcHandler for NexusEngine {
    async fn handle(&self, method: &str, params: Option<Value>) -> Result<Value, JsonRpcError> {
        info!(rpc_method = %method, "Processing incoming JSON-RPC request");

        match method {
            "tasks.add" => {
                let p = params.ok_or_else(|| JsonRpcError {
                    code: JsonRpcError::INVALID_PARAMS,
                    message: "Missing params object for tasks.add".to_string(),
                    data: None,
                })?;

                let raw_url = p
                    .get("url")
                    .and_then(Value::as_str)
                    .ok_or_else(|| JsonRpcError {
                        code: JsonRpcError::INVALID_PARAMS,
                        message: "Field 'url' is required".to_string(),
                        data: None,
                    })?;

                let parsed_url = Url::parse(raw_url).map_err(|e| JsonRpcError {
                    code: JsonRpcError::INVALID_PARAMS,
                    message: format!("Invalid URL: {e}"),
                    data: None,
                })?;

                let filename = p
                    .get("filename")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| {
                        parsed_url
                            .path_segments()
                            .and_then(|mut s| s.next_back())
                            .filter(|s| !s.is_empty())
                            .unwrap_or("download.file")
                            .to_string()
                    });

                let destination = p
                    .get("destination")
                    .and_then(Value::as_str)
                    .unwrap_or(".")
                    .to_string();

                let priority = p.get("priority").and_then(Value::as_u64).map(|v| v as u32);
                let auto_start = p.get("start").and_then(Value::as_bool).unwrap_or(false);

                let task = self
                    .create_download_with_options(parsed_url, filename, destination, priority, None)
                    .await
                    .map_err(|e| JsonRpcError {
                        code: JsonRpcError::INTERNAL_ERROR,
                        message: e.to_string(),
                        data: None,
                    })?;

                if auto_start {
                    let _ = self.start_download(&task.id).await;
                }

                Ok(json!({
                    "id": task.id.to_string(),
                    "url": task.url.as_str(),
                    "filename": task.filename,
                    "destination_dir": task.destination_dir,
                    "status": task.status.to_string(),
                    "category": task.category,
                }))
            }

            "tasks.start" => {
                let task_id = extract_task_id(&params)?;
                self.start_download(&task_id)
                    .await
                    .map_err(|e| JsonRpcError {
                        code: JsonRpcError::INTERNAL_ERROR,
                        message: e.to_string(),
                        data: None,
                    })?;
                Ok(json!({ "id": task_id.to_string(), "status": "Starting" }))
            }

            "tasks.pause" => {
                let task_id = extract_task_id(&params)?;
                self.pause_download(&task_id)
                    .await
                    .map_err(|e| JsonRpcError {
                        code: JsonRpcError::INTERNAL_ERROR,
                        message: e.to_string(),
                        data: None,
                    })?;
                Ok(json!({ "id": task_id.to_string(), "status": "Paused" }))
            }

            "tasks.resume" => {
                let task_id = extract_task_id(&params)?;
                self.resume_download(&task_id)
                    .await
                    .map_err(|e| JsonRpcError {
                        code: JsonRpcError::INTERNAL_ERROR,
                        message: e.to_string(),
                        data: None,
                    })?;
                Ok(json!({ "id": task_id.to_string(), "status": "Downloading" }))
            }

            "tasks.cancel" => {
                let task_id = extract_task_id(&params)?;
                self.cancel_download(&task_id)
                    .await
                    .map_err(|e| JsonRpcError {
                        code: JsonRpcError::INTERNAL_ERROR,
                        message: e.to_string(),
                        data: None,
                    })?;
                Ok(json!({ "id": task_id.to_string(), "status": "Cancelled" }))
            }

            "tasks.list" => {
                let status_filter = params
                    .as_ref()
                    .and_then(|p| p.get("status"))
                    .and_then(Value::as_str)
                    .and_then(parse_status);

                let tasks = self
                    .list_downloads(status_filter)
                    .await
                    .map_err(|e| JsonRpcError {
                        code: JsonRpcError::INTERNAL_ERROR,
                        message: e.to_string(),
                        data: None,
                    })?;

                let items: Vec<Value> = tasks
                    .into_iter()
                    .map(|t| {
                        json!({
                            "id": t.id.to_string(),
                            "url": t.url.as_str(),
                            "filename": t.filename,
                            "destination_dir": t.destination_dir,
                            "status": t.status.to_string(),
                            "downloaded_bytes": t.downloaded_size.bytes(),
                            "total_bytes": t.total_size.map(|s| s.bytes()),
                            "category": t.category,
                        })
                    })
                    .collect();

                Ok(json!(items))
            }

            "tasks.get" => {
                let task_id = extract_task_id(&params)?;
                let task = self
                    .get_download(&task_id)
                    .await
                    .map_err(|e| JsonRpcError {
                        code: JsonRpcError::INTERNAL_ERROR,
                        message: e.to_string(),
                        data: None,
                    })?;

                match task {
                    Some(t) => Ok(json!({
                        "id": t.id.to_string(),
                        "url": t.url.as_str(),
                        "filename": t.filename,
                        "destination_dir": t.destination_dir,
                        "status": t.status.to_string(),
                        "downloaded_bytes": t.downloaded_size.bytes(),
                        "total_bytes": t.total_size.map(|s| s.bytes()),
                        "category": t.category,
                    })),
                    None => Err(JsonRpcError {
                        code: JsonRpcError::INTERNAL_ERROR,
                        message: format!("Task '{task_id}' not found"),
                        data: None,
                    }),
                }
            }

            "network.interfaces" => {
                let ifaces =
                    self.network_manager()
                        .list_interfaces()
                        .map_err(|e| JsonRpcError {
                            code: JsonRpcError::INTERNAL_ERROR,
                            message: e.to_string(),
                            data: None,
                        })?;

                let items: Vec<Value> = ifaces
                    .into_iter()
                    .map(|i| {
                        json!({
                            "id": i.id.to_string(),
                            "system_name": i.system_name,
                            "display_name": i.display_name,
                            "type": format!("{:?}", i.interface_type),
                            "is_up": i.is_up,
                            "ips": i.ip_addresses.iter().map(ToString::to_string).collect::<Vec<_>>(),
                        })
                    })
                    .collect();

                Ok(json!(items))
            }

            "network.paths" => {
                let paths = self.network_manager().list_paths().await;
                let items: Vec<Value> = paths
                    .into_iter()
                    .map(|p| {
                        json!({
                            "id": p.id.to_string(),
                            "name": p.name,
                            "interface_id": p.interface_id.to_string(),
                            "proxy_id": p.proxy_id.map(|pid| pid.to_string()),
                            "priority": p.priority,
                            "enabled": p.enabled,
                        })
                    })
                    .collect();

                Ok(json!(items))
            }

            "proxy.list" => {
                let profiles = self.proxy_manager().list_profiles().await;
                let items: Vec<Value> = profiles
                    .into_iter()
                    .map(|p| {
                        json!({
                            "id": p.id.to_string(),
                            "name": p.name,
                            "type": format!("{:?}", p.proxy_type),
                            "host": p.host,
                            "port": p.port,
                        })
                    })
                    .collect();

                Ok(json!(items))
            }

            "queue.status" => {
                let active = self.queue_manager().active_count().await;
                let pending = self.queue_manager().pending_count().await;
                Ok(json!({
                    "active_tasks": active,
                    "pending_tasks": pending,
                }))
            }

            "rules.list" => {
                let rules = self.rule_engine().list_rules();
                let items: Vec<Value> = rules
                    .into_iter()
                    .map(|r| {
                        json!({
                            "id": r.id.to_string(),
                            "name": r.name,
                            "priority": r.priority,
                            "enabled": r.enabled,
                        })
                    })
                    .collect();

                Ok(json!(items))
            }

            "system.stats" => {
                let stats = self.stats().await;
                Ok(json!(stats))
            }

            "system.status" => Ok(json!({
                "status": "operational",
                "version": env!("CARGO_PKG_VERSION"),
                "app": "NEXUS"
            })),

            _ => Err(JsonRpcError::method_not_found(method)),
        }
    }
}

fn extract_task_id(params: &Option<Value>) -> Result<TaskId, JsonRpcError> {
    let p = params.as_ref().ok_or_else(|| JsonRpcError {
        code: JsonRpcError::INVALID_PARAMS,
        message: "Missing parameters object".to_string(),
        data: None,
    })?;

    let raw_id = p
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| JsonRpcError {
            code: JsonRpcError::INVALID_PARAMS,
            message: "Missing 'id' parameter".to_string(),
            data: None,
        })?;

    TaskId::from_str(raw_id).map_err(|e| JsonRpcError {
        code: JsonRpcError::INVALID_PARAMS,
        message: format!("Invalid Task UUID: {e}"),
        data: None,
    })
}

fn parse_status(s: &str) -> Option<DownloadStatus> {
    match s.to_lowercase().as_str() {
        "created" => Some(DownloadStatus::Created),
        "queued" => Some(DownloadStatus::Queued),
        "preparing" => Some(DownloadStatus::Preparing),
        "downloading" => Some(DownloadStatus::Downloading),
        "paused" => Some(DownloadStatus::Paused),
        "completed" => Some(DownloadStatus::Completed),
        "failed" => Some(DownloadStatus::Failed),
        "cancelled" => Some(DownloadStatus::Cancelled),
        _ => None,
    }
}
