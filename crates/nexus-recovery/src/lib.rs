//! Crash recovery, task reconstruction, and partial file repair coordinator.

use nexus_domain::DownloadStatus;
use nexus_storage::{StorageError, TaskRepository};
use std::sync::Arc;
use thiserror::Error;
use tracing::{info, warn};

#[derive(Error, Debug)]
pub enum RecoveryError {
    #[error("Storage error during recovery: {0}")]
    Storage(#[from] StorageError),

    #[error("File system recovery error: {0}")]
    Io(#[from] std::io::Error),
}

/// Coordinates the recovery of in-flight downloads after an unexpected crash or reboot.
pub struct RecoveryManager {
    task_repo: Arc<dyn TaskRepository>,
}

impl RecoveryManager {
    #[must_use]
    pub fn new(task_repo: Arc<dyn TaskRepository>) -> Self {
        Self { task_repo }
    }

    /// Scans the persistent store for tasks that were left in active states (`Downloading`, `Preparing`, `Recovering`)
    /// and resets them safely to `Paused` or `Queued` to prevent silent corruption.
    pub async fn recover_interrupted_tasks(&self) -> Result<usize, RecoveryError> {
        let mut recovered_count = 0;
        let all_tasks = self.task_repo.list_tasks(None).await?;

        for task in all_tasks {
            if task.status.is_active() {
                warn!(
                    task_id = %task.id,
                    last_status = %task.status,
                    "Detected interrupted task after restart; resetting to Paused"
                );

                self.task_repo
                    .update_status(&task.id, DownloadStatus::Paused)
                    .await?;

                recovered_count += 1;
            }
        }

        info!(
            recovered_count = recovered_count,
            "Crash recovery scan completed"
        );
        Ok(recovered_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_domain::DownloadTask;
    use nexus_storage::InMemoryStorage;
    use url::Url;

    #[tokio::test]
    async fn test_recover_interrupted_tasks() {
        let storage = Arc::new(InMemoryStorage::new());
        let mut task = DownloadTask::new(
            Url::parse("https://example.com/big.iso").unwrap(),
            "big.iso".to_string(),
            "/downloads".to_string(),
        );
        task.status = DownloadStatus::Downloading;
        storage.save_task(&task).await.unwrap();

        let recovery = RecoveryManager::new(storage.clone());
        let count = recovery.recover_interrupted_tasks().await.unwrap();

        assert_eq!(count, 1);
        let recovered = storage.get_task(&task.id).await.unwrap().unwrap();
        assert_eq!(recovered.status, DownloadStatus::Paused);
    }
}
