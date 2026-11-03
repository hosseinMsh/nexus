//! Application use cases and transfer engine orchestration.

use nexus_domain::{DownloadStatus, DownloadTask, TaskId};
use nexus_download::{DownloadStateError, DownloadStateMachine};
use nexus_events::{EventBus, SystemEvent};
use nexus_network::NetworkManager;
use nexus_proxy::ProxyManager;
use nexus_storage::{StorageError, TaskRepository};
use std::sync::Arc;
use thiserror::Error;
use tracing::info;
use url::Url;

#[derive(Error, Debug)]
pub enum ApplicationError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("State transition error: {0}")]
    State(#[from] DownloadStateError),

    #[error("Task '{0}' not found")]
    TaskNotFound(String),
}

/// Central application service orchestrating domain operations across crates.
#[derive(Clone)]
pub struct NexusEngine {
    task_repo: Arc<dyn TaskRepository>,
    network_manager: Arc<NetworkManager>,
    proxy_manager: Arc<ProxyManager>,
    event_bus: Arc<EventBus>,
}

impl NexusEngine {
    #[must_use]
    pub fn new(
        task_repo: Arc<dyn TaskRepository>,
        network_manager: Arc<NetworkManager>,
        proxy_manager: Arc<ProxyManager>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            task_repo,
            network_manager,
            proxy_manager,
            event_bus,
        }
    }

    /// Creates and persists a new download task.
    pub async fn create_download(
        &self,
        url: Url,
        filename: String,
        destination_dir: String,
    ) -> Result<DownloadTask, ApplicationError> {
        let task = DownloadTask::new(url, filename, destination_dir);
        self.task_repo.save_task(&task).await?;

        self.event_bus.publish(SystemEvent::TaskCreated {
            task_id: task.id,
            timestamp: chrono::Utc::now(),
        });

        info!(task_id = %task.id, filename = %task.filename, "Created new download task");
        Ok(task)
    }

    /// Lists downloads filtered by optional status.
    pub async fn list_downloads(
        &self,
        status: Option<DownloadStatus>,
    ) -> Result<Vec<DownloadTask>, ApplicationError> {
        Ok(self.task_repo.list_tasks(status).await?)
    }

    /// Pauses an active download task.
    pub async fn pause_download(&self, task_id: &TaskId) -> Result<(), ApplicationError> {
        let mut task = self
            .task_repo
            .get_task(task_id)
            .await?
            .ok_or_else(|| ApplicationError::TaskNotFound(task_id.to_string()))?;

        DownloadStateMachine::transition(&mut task, DownloadStatus::Paused)?;
        self.task_repo.save_task(&task).await?;

        self.event_bus.publish(SystemEvent::TaskStatusChanged {
            task_id: *task_id,
            old_status: DownloadStatus::Downloading,
            new_status: DownloadStatus::Paused,
            timestamp: chrono::Utc::now(),
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

        self.event_bus.publish(SystemEvent::TaskStatusChanged {
            task_id: *task_id,
            old_status: DownloadStatus::Paused,
            new_status: DownloadStatus::Downloading,
            timestamp: chrono::Utc::now(),
        });

        info!(task_id = %task_id, "Download task resumed");
        Ok(())
    }

    /// Cancels a download task.
    pub async fn cancel_download(&self, task_id: &TaskId) -> Result<(), ApplicationError> {
        let mut task = self
            .task_repo
            .get_task(task_id)
            .await?
            .ok_or_else(|| ApplicationError::TaskNotFound(task_id.to_string()))?;

        let old = task.status;
        DownloadStateMachine::transition(&mut task, DownloadStatus::Cancelled)?;
        self.task_repo.save_task(&task).await?;

        self.event_bus.publish(SystemEvent::TaskStatusChanged {
            task_id: *task_id,
            old_status: old,
            new_status: DownloadStatus::Cancelled,
            timestamp: chrono::Utc::now(),
        });

        info!(task_id = %task_id, "Download task cancelled");
        Ok(())
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
    pub fn event_bus(&self) -> &Arc<EventBus> {
        &self.event_bus
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_network_platform::SystemNetworkPlatform;
    use nexus_storage::InMemoryStorage;

    #[tokio::test]
    async fn test_nexus_engine_workflow() {
        let storage = Arc::new(InMemoryStorage::new());
        let platform = Arc::new(SystemNetworkPlatform);
        let network_mgr = Arc::new(NetworkManager::new(platform));
        let proxy_mgr = Arc::new(ProxyManager::new());
        let event_bus = Arc::new(EventBus::default());

        let engine = NexusEngine::new(storage, network_mgr, proxy_mgr, event_bus);

        let task = engine
            .create_download(
                Url::parse("https://releases.ubuntu.com/jammy.iso").unwrap(),
                "jammy.iso".to_string(),
                "/downloads".to_string(),
            )
            .await
            .unwrap();

        assert_eq!(task.status, DownloadStatus::Created);

        let list = engine.list_downloads(None).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, task.id);
    }
}
