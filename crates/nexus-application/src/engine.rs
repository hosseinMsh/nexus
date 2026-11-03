//! Central application orchestration engine coordinating all NEXUS subsystems.

use crate::stats::EngineStats;
use chrono::Utc;
use nexus_automation::{AppliedRuleResult, QueueManager, RuleEngine};
use nexus_domain::{
    DownloadPolicy, DownloadStatus, DownloadTask, NetworkPath, PathId, PathLimits, TaskId,
};
use nexus_download::{DownloadEngine, DownloadStateError, DownloadStateMachine, EngineError};
use nexus_events::{EventBus, SystemEvent};
use nexus_network::NetworkManager;
use nexus_path_scheduler::{AdaptivePathScheduler, PathMetricsTracker};
use nexus_protocols::ProtocolManager;
use nexus_proxy::ProxyManager;
use nexus_storage::{SegmentRepository, StorageError, TaskRepository};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use url::Url;

#[derive(Error, Debug)]
pub enum ApplicationError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Download engine error: {0}")]
    Engine(#[from] EngineError),

    #[error("State transition error: {0}")]
    State(#[from] DownloadStateError),

    #[error("Task '{0}' not found")]
    TaskNotFound(String),

    #[error("Invalid task operation: {0}")]
    InvalidOperation(String),
}

/// Central application service orchestrating domain operations across all subsystems.
#[derive(Clone)]
pub struct NexusEngine {
    task_repo: Arc<dyn TaskRepository>,
    segment_repo: Arc<dyn SegmentRepository>,
    network_manager: Arc<NetworkManager>,
    proxy_manager: Arc<ProxyManager>,
    protocol_manager: Arc<ProtocolManager>,
    queue_manager: Arc<QueueManager>,
    rule_engine: Arc<RuleEngine>,
    path_scheduler: Arc<AdaptivePathScheduler>,
    metrics_tracker: Arc<PathMetricsTracker>,
    download_engine: Arc<DownloadEngine>,
    event_bus: Arc<EventBus>,
    active_cancellations: Arc<RwLock<HashMap<TaskId, CancellationToken>>>,
}

impl NexusEngine {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        task_repo: Arc<dyn TaskRepository>,
        segment_repo: Arc<dyn SegmentRepository>,
        network_manager: Arc<NetworkManager>,
        proxy_manager: Arc<ProxyManager>,
        protocol_manager: Arc<ProtocolManager>,
        queue_manager: Arc<QueueManager>,
        rule_engine: Arc<RuleEngine>,
        path_scheduler: Arc<AdaptivePathScheduler>,
        metrics_tracker: Arc<PathMetricsTracker>,
        download_engine: Arc<DownloadEngine>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            task_repo,
            segment_repo,
            network_manager,
            proxy_manager,
            protocol_manager,
            queue_manager,
            rule_engine,
            path_scheduler,
            metrics_tracker,
            download_engine,
            event_bus,
            active_cancellations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates and persists a new download task applying automated rules.
    pub async fn create_download(
        &self,
        url: Url,
        filename: String,
        destination_dir: String,
    ) -> Result<DownloadTask, ApplicationError> {
        self.create_download_with_options(url, filename, destination_dir, None, None)
            .await
    }

    /// Creates a download task with explicit priority and rule evaluation.
    pub async fn create_download_with_options(
        &self,
        url: Url,
        filename: String,
        destination_dir: String,
        priority: Option<u32>,
        policy_override: Option<DownloadPolicy>,
    ) -> Result<DownloadTask, ApplicationError> {
        // Step 1: Evaluate automation rules
        let rule_result: AppliedRuleResult = self.rule_engine.evaluate(&url, Some(&filename), None);

        let final_dest = rule_result.destination_dir.unwrap_or(destination_dir);
        let mut task = DownloadTask::new(url, filename, final_dest);

        // Apply rule categorizations and policies
        if let Some(cat) = rule_result.category {
            task.category = Some(cat);
        }

        if let Some(mut policy) = policy_override {
            if let Some(max_conn) = rule_result.max_connections {
                policy.max_connections = max_conn;
            }
            if let Some(speed_limit) = rule_result.speed_limit_bps {
                policy.max_speed_bps = Some(speed_limit);
            }
            task.policy = policy;
        } else {
            if let Some(max_conn) = rule_result.max_connections {
                task.policy.max_connections = max_conn;
            }
            if let Some(speed_limit) = rule_result.speed_limit_bps {
                task.policy.max_speed_bps = Some(speed_limit);
            }
        }

        // Persist task to database
        self.task_repo.save_task(&task).await?;

        // Enqueue task into priority queue (default priority 2 = Medium)
        let prio = priority.unwrap_or(2);
        self.queue_manager.enqueue(task.id, prio).await;

        self.event_bus.publish(SystemEvent::TaskCreated {
            task_id: task.id,
            timestamp: Utc::now(),
        });

        info!(
            task_id = %task.id,
            filename = %task.filename,
            category = ?task.category,
            "Created and queued new download task"
        );

        Ok(task)
    }

    /// Starts or resumes transfer execution for a task across available network paths.
    pub async fn start_download(&self, task_id: &TaskId) -> Result<(), ApplicationError> {
        let task = self
            .task_repo
            .get_task(task_id)
            .await?
            .ok_or_else(|| ApplicationError::TaskNotFound(task_id.to_string()))?;

        if task.status.is_terminal() {
            return Err(ApplicationError::InvalidOperation(format!(
                "Task '{task_id}' is in terminal state '{}'",
                task.status
            )));
        }

        // Fetch configured network paths
        let mut paths = self.network_manager.list_paths().await;
        if paths.is_empty() {
            let ifaces = self.network_manager.list_interfaces().unwrap_or_default();
            if let Some(iface) = ifaces.into_iter().find(|i| i.is_up) {
                if let Ok(p) = self
                    .network_manager
                    .create_path(
                        "Direct Route".to_string(),
                        iface.id,
                        PathLimits::default(),
                        1,
                    )
                    .await
                {
                    paths = vec![p];
                }
            }

            if paths.is_empty() {
                let default_path = NetworkPath {
                    id: PathId::new(),
                    name: "Direct Route".to_string(),
                    interface_id: nexus_domain::InterfaceId::new(),
                    proxy_id: None,
                    priority: 1,
                    enabled: true,
                    limits: PathLimits::default(),
                };
                paths = vec![default_path];
            }
        }

        let cancel_token = CancellationToken::new();
        {
            let mut cancellations = self.active_cancellations.write().await;
            cancellations.insert(*task_id, cancel_token.clone());
        }

        let tid = *task_id;
        let dl_engine = self.download_engine.clone();
        let queue_mgr = self.queue_manager.clone();
        let cancellations = self.active_cancellations.clone();
        let event_bus = self.event_bus.clone();
        let first_path_id = paths[0].id;

        tokio::spawn(async move {
            info!(task_id = %tid, "Spawning asynchronous transfer executor");
            let result = dl_engine
                .download_multi_path(&tid, &paths, cancel_token)
                .await;
            {
                let mut guard = cancellations.write().await;
                guard.remove(&tid);
            }

            match result {
                Ok(()) => {
                    info!(task_id = %tid, "Task transfer completed successfully");
                    let _ = queue_mgr.on_task_finished(tid).await;
                }
                Err(EngineError::CancelledOrPaused) => {
                    info!(task_id = %tid, "Task transfer paused or cancelled");
                }
                Err(err) => {
                    error!(task_id = %tid, error = %err, "Task transfer encountered error");
                    let _ = queue_mgr.on_task_finished(tid).await;
                    event_bus.publish(SystemEvent::PathFailed {
                        path_id: first_path_id,
                        reason: err.to_string(),
                        timestamp: Utc::now(),
                    });
                }
            }
        });

        Ok(())
    }

    /// Lists downloads filtered by optional status.
    pub async fn list_downloads(
        &self,
        status: Option<DownloadStatus>,
    ) -> Result<Vec<DownloadTask>, ApplicationError> {
        Ok(self.task_repo.list_tasks(status).await?)
    }

    /// Retrieves an individual download task.
    pub async fn get_download(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<DownloadTask>, ApplicationError> {
        Ok(self.task_repo.get_task(task_id).await?)
    }

    /// Pauses an active download task.
    pub async fn pause_download(&self, task_id: &TaskId) -> Result<(), ApplicationError> {
        // Cancel active worker stream if running
        {
            let cancellations = self.active_cancellations.read().await;
            if let Some(token) = cancellations.get(task_id) {
                token.cancel();
            }
        }

        let mut task = self
            .task_repo
            .get_task(task_id)
            .await?
            .ok_or_else(|| ApplicationError::TaskNotFound(task_id.to_string()))?;

        let old_status = task.status;
        DownloadStateMachine::transition(&mut task, DownloadStatus::Paused)?;
        self.task_repo.save_task(&task).await?;

        self.event_bus.publish(SystemEvent::TaskStatusChanged {
            task_id: *task_id,
            old_status,
            new_status: DownloadStatus::Paused,
            timestamp: Utc::now(),
        });

        info!(task_id = %task_id, "Download task paused");
        Ok(())
    }

    /// Resumes a paused download task.
    pub async fn resume_download(&self, task_id: &TaskId) -> Result<(), ApplicationError> {
        let mut task = self
            .task_repo
            .get_task(task_id)
            .await?
            .ok_or_else(|| ApplicationError::TaskNotFound(task_id.to_string()))?;

        DownloadStateMachine::transition(&mut task, DownloadStatus::Downloading)?;
        self.task_repo.save_task(&task).await?;

        self.start_download(task_id).await?;

        self.event_bus.publish(SystemEvent::TaskStatusChanged {
            task_id: *task_id,
            old_status: DownloadStatus::Paused,
            new_status: DownloadStatus::Downloading,
            timestamp: Utc::now(),
        });

        info!(task_id = %task_id, "Download task resumed");
        Ok(())
    }

    /// Cancels a download task.
    pub async fn cancel_download(&self, task_id: &TaskId) -> Result<(), ApplicationError> {
        {
            let cancellations = self.active_cancellations.read().await;
            if let Some(token) = cancellations.get(task_id) {
                token.cancel();
            }
        }

        let mut task = self
            .task_repo
            .get_task(task_id)
            .await?
            .ok_or_else(|| ApplicationError::TaskNotFound(task_id.to_string()))?;

        let old = task.status;
        DownloadStateMachine::transition(&mut task, DownloadStatus::Cancelled)?;
        self.task_repo.save_task(&task).await?;

        self.queue_manager.on_task_finished(*task_id).await;

        self.event_bus.publish(SystemEvent::TaskStatusChanged {
            task_id: *task_id,
            old_status: old,
            new_status: DownloadStatus::Cancelled,
            timestamp: Utc::now(),
        });

        info!(task_id = %task_id, "Download task cancelled");
        Ok(())
    }

    /// Calculates aggregated performance metrics across tasks and network paths.
    pub async fn stats(&self) -> EngineStats {
        let all_tasks = self.task_repo.list_tasks(None).await.unwrap_or_default();

        let mut queued_downloads = 0;
        let mut paused_downloads = 0;
        let mut completed_downloads = 0;
        let mut failed_downloads = 0;
        let mut total_downloaded_bytes: u64 = 0;

        for t in &all_tasks {
            total_downloaded_bytes =
                total_downloaded_bytes.saturating_add(t.downloaded_size.bytes());
            match t.status {
                DownloadStatus::Queued | DownloadStatus::Created => queued_downloads += 1,
                DownloadStatus::Paused => paused_downloads += 1,
                DownloadStatus::Completed => completed_downloads += 1,
                DownloadStatus::Failed => failed_downloads += 1,
                _ => {}
            }
        }

        let active_downloads = self.active_cancellations.read().await.len();
        let aggregate_throughput_bps = self.metrics_tracker.aggregate_throughput_bps() as u64;

        let paths = self.network_manager.list_paths().await;
        let active_network_paths = paths.iter().filter(|p| p.enabled).count();

        let active_connections = self
            .metrics_tracker
            .get_all_metrics()
            .iter()
            .map(|(_, m)| m.active_connections as usize)
            .sum();

        EngineStats {
            active_downloads,
            queued_downloads,
            paused_downloads,
            completed_downloads,
            failed_downloads,
            total_downloaded_bytes,
            aggregate_throughput_bps,
            active_network_paths,
            active_connections,
        }
    }

    #[must_use]
    pub fn task_repo(&self) -> &Arc<dyn TaskRepository> {
        &self.task_repo
    }

    #[must_use]
    pub fn segment_repo(&self) -> &Arc<dyn SegmentRepository> {
        &self.segment_repo
    }

    #[must_use]
    pub fn network_manager(&self) -> &Arc<NetworkManager> {
        &self.network_manager
    }

    #[must_use]
    pub fn proxy_manager(&self) -> &Arc<ProxyManager> {
        &self.proxy_manager
    }

    #[must_use]
    pub fn protocol_manager(&self) -> &Arc<ProtocolManager> {
        &self.protocol_manager
    }

    #[must_use]
    pub fn queue_manager(&self) -> &Arc<QueueManager> {
        &self.queue_manager
    }

    #[must_use]
    pub fn rule_engine(&self) -> &Arc<RuleEngine> {
        &self.rule_engine
    }

    #[must_use]
    pub fn path_scheduler(&self) -> &Arc<AdaptivePathScheduler> {
        &self.path_scheduler
    }

    #[must_use]
    pub fn metrics_tracker(&self) -> &Arc<PathMetricsTracker> {
        &self.metrics_tracker
    }

    #[must_use]
    pub fn download_engine(&self) -> &Arc<DownloadEngine> {
        &self.download_engine
    }

    #[must_use]
    pub fn event_bus(&self) -> &Arc<EventBus> {
        &self.event_bus
    }
}
