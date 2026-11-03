//! Persistence traits, repository abstractions, and database storage for NEXUS.

use async_trait::async_trait;
use nexus_domain::{
    DownloadSegment, DownloadStatus, DownloadTask, NetworkPath, PathId, SegmentId, TaskId,
};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Entity not found: {0}")]
    NotFound(String),

    #[error("Transaction failed: {0}")]
    TransactionError(String),
}

/// Abstract contract for managing persisted download tasks.
#[async_trait]
pub trait TaskRepository: Send + Sync {
    async fn save_task(&self, task: &DownloadTask) -> Result<(), StorageError>;
    async fn get_task(&self, id: &TaskId) -> Result<Option<DownloadTask>, StorageError>;
    async fn list_tasks(
        &self,
        status_filter: Option<DownloadStatus>,
    ) -> Result<Vec<DownloadTask>, StorageError>;
    async fn update_status(&self, id: &TaskId, status: DownloadStatus) -> Result<(), StorageError>;
    async fn delete_task(&self, id: &TaskId) -> Result<bool, StorageError>;
}

/// Abstract contract for managing persisted download segments.
#[async_trait]
pub trait SegmentRepository: Send + Sync {
    async fn save_segments(&self, segments: &[DownloadSegment]) -> Result<(), StorageError>;
    async fn get_segments_for_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Vec<DownloadSegment>, StorageError>;
    async fn update_segment_progress(
        &self,
        id: &SegmentId,
        downloaded_bytes: u64,
    ) -> Result<(), StorageError>;
}

/// Abstract contract for managing network paths.
#[async_trait]
pub trait PathRepository: Send + Sync {
    async fn save_path(&self, path: &NetworkPath) -> Result<(), StorageError>;
    async fn list_paths(&self) -> Result<Vec<NetworkPath>, StorageError>;
    async fn get_path(&self, id: &PathId) -> Result<Option<NetworkPath>, StorageError>;
}

/// Thread-safe in-memory repository implementation ideal for tests and mocking.
#[derive(Default, Clone)]
pub struct InMemoryStorage {
    tasks: Arc<RwLock<HashMap<TaskId, DownloadTask>>>,
    segments: Arc<RwLock<HashMap<SegmentId, DownloadSegment>>>,
    paths: Arc<RwLock<HashMap<PathId, NetworkPath>>>,
}

impl InMemoryStorage {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TaskRepository for InMemoryStorage {
    async fn save_task(&self, task: &DownloadTask) -> Result<(), StorageError> {
        let mut tasks = self.tasks.write().await;
        tasks.insert(task.id, task.clone());
        Ok(())
    }

    async fn get_task(&self, id: &TaskId) -> Result<Option<DownloadTask>, StorageError> {
        let tasks = self.tasks.read().await;
        Ok(tasks.get(id).cloned())
    }

    async fn list_tasks(
        &self,
        status_filter: Option<DownloadStatus>,
    ) -> Result<Vec<DownloadTask>, StorageError> {
        let tasks = self.tasks.read().await;
        Ok(tasks
            .values()
            .filter(|t| status_filter.is_none_or(|s| t.status == s))
            .cloned()
            .collect())
    }

    async fn update_status(&self, id: &TaskId, status: DownloadStatus) -> Result<(), StorageError> {
        let mut tasks = self.tasks.write().await;
        let task = tasks
            .get_mut(id)
            .ok_or_else(|| StorageError::NotFound(id.to_string()))?;
        task.status = status;
        task.updated_at = chrono::Utc::now();
        Ok(())
    }

    async fn delete_task(&self, id: &TaskId) -> Result<bool, StorageError> {
        let mut tasks = self.tasks.write().await;
        Ok(tasks.remove(id).is_some())
    }
}

#[async_trait]
impl SegmentRepository for InMemoryStorage {
    async fn save_segments(&self, segments: &[DownloadSegment]) -> Result<(), StorageError> {
        let mut segs = self.segments.write().await;
        for s in segments {
            segs.insert(s.id, s.clone());
        }
        Ok(())
    }

    async fn get_segments_for_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Vec<DownloadSegment>, StorageError> {
        let segs = self.segments.read().await;
        Ok(segs
            .values()
            .filter(|s| &s.download_id == task_id)
            .cloned()
            .collect())
    }

    async fn update_segment_progress(
        &self,
        id: &SegmentId,
        downloaded_bytes: u64,
    ) -> Result<(), StorageError> {
        let mut segs = self.segments.write().await;
        let seg = segs
            .get_mut(id)
            .ok_or_else(|| StorageError::NotFound(id.to_string()))?;
        seg.downloaded_bytes = downloaded_bytes;
        Ok(())
    }
}

#[async_trait]
impl PathRepository for InMemoryStorage {
    async fn save_path(&self, path: &NetworkPath) -> Result<(), StorageError> {
        let mut paths = self.paths.write().await;
        paths.insert(path.id, path.clone());
        Ok(())
    }

    async fn list_paths(&self) -> Result<Vec<NetworkPath>, StorageError> {
        let paths = self.paths.read().await;
        Ok(paths.values().cloned().collect())
    }

    async fn get_path(&self, id: &PathId) -> Result<Option<NetworkPath>, StorageError> {
        let paths = self.paths.read().await;
        Ok(paths.get(id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[tokio::test]
    async fn test_in_memory_task_crud() {
        let storage = InMemoryStorage::new();
        let task = DownloadTask::new(
            Url::parse("https://example.com/test.zip").unwrap(),
            "test.zip".to_string(),
            "/tmp".to_string(),
        );

        storage.save_task(&task).await.unwrap();
        let fetched = storage.get_task(&task.id).await.unwrap();
        assert_eq!(fetched, Some(task.clone()));

        storage
            .update_status(&task.id, DownloadStatus::Downloading)
            .await
            .unwrap();
        let updated = storage.get_task(&task.id).await.unwrap().unwrap();
        assert_eq!(updated.status, DownloadStatus::Downloading);

        let active_tasks = storage
            .list_tasks(Some(DownloadStatus::Downloading))
            .await
            .unwrap();
        assert_eq!(active_tasks.len(), 1);

        assert!(storage.delete_task(&task.id).await.unwrap());
        assert_eq!(storage.get_task(&task.id).await.unwrap(), None);
    }
}
