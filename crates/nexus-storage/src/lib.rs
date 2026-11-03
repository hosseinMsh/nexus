//! Persistence traits, repository abstractions, and database storage for NEXUS.

use async_trait::async_trait;
use nexus_domain::{
    DownloadSegment, DownloadStatus, DownloadTask, NetworkPath, PathId, ProxyId, ProxyProfile,
    SegmentId, TaskId,
};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

pub mod sqlite;
pub use sqlite::SqliteStorage;

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

/// Abstract contract for managing proxy profiles.
#[async_trait]
pub trait ProxyRepository: Send + Sync {
    async fn save_profile(&self, profile: &ProxyProfile) -> Result<(), StorageError>;
    async fn list_profiles(&self) -> Result<Vec<ProxyProfile>, StorageError>;
    async fn get_profile(&self, id: &ProxyId) -> Result<Option<ProxyProfile>, StorageError>;
}

/// Thread-safe in-memory repository implementation ideal for tests and mocking.
#[derive(Default, Clone)]
pub struct InMemoryStorage {
    tasks: Arc<RwLock<HashMap<TaskId, DownloadTask>>>,
    segments: Arc<RwLock<HashMap<SegmentId, DownloadSegment>>>,
    paths: Arc<RwLock<HashMap<PathId, NetworkPath>>>,
    proxies: Arc<RwLock<HashMap<ProxyId, ProxyProfile>>>,
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

#[async_trait]
impl ProxyRepository for InMemoryStorage {
    async fn save_profile(&self, profile: &ProxyProfile) -> Result<(), StorageError> {
        let mut proxies = self.proxies.write().await;
        proxies.insert(profile.id, profile.clone());
        Ok(())
    }

    async fn list_profiles(&self) -> Result<Vec<ProxyProfile>, StorageError> {
        let proxies = self.proxies.read().await;
        Ok(proxies.values().cloned().collect())
    }

    async fn get_profile(&self, id: &ProxyId) -> Result<Option<ProxyProfile>, StorageError> {
        let proxies = self.proxies.read().await;
        Ok(proxies.get(id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_core::ByteSize;
    use nexus_domain::{DnsPolicy, InterfaceId, PathLimits, ProxyType, SegmentStatus};
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

    #[tokio::test]
    async fn test_sqlite_storage_lifecycle_and_foreign_keys() {
        let storage = SqliteStorage::connect_in_memory().await.unwrap();

        // 1. Save and fetch DownloadTask
        let mut task = DownloadTask::new(
            Url::parse("https://releases.ubuntu.com/noble/ubuntu-24.04-desktop-amd64.iso").unwrap(),
            "ubuntu-24.04-desktop-amd64.iso".to_string(),
            "/downloads".to_string(),
        );
        task.total_size = Some(ByteSize(5_000_000_000));
        task.category = Some("Linux ISOs".to_string());
        task.etag = Some("\"abc123etag\"".to_string());

        storage.save_task(&task).await.unwrap();

        let fetched = storage.get_task(&task.id).await.unwrap().unwrap();
        assert_eq!(fetched.id, task.id);
        assert_eq!(fetched.filename, "ubuntu-24.04-desktop-amd64.iso");
        assert_eq!(fetched.total_size, Some(ByteSize(5_000_000_000)));
        assert_eq!(fetched.category, Some("Linux ISOs".to_string()));
        assert_eq!(fetched.status, DownloadStatus::Created);

        // 2. Insert segments for this task
        let seg1 = DownloadSegment::new(task.id, 0, 2_499_999_999);
        let mut seg2 = DownloadSegment::new(task.id, 2_500_000_000, 4_999_999_999);
        seg2.status = SegmentStatus::InProgress;

        storage
            .save_segments(&[seg1.clone(), seg2.clone()])
            .await
            .unwrap();

        let segments = storage.get_segments_for_task(&task.id).await.unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].start_offset, 0);
        assert_eq!(segments[1].start_offset, 2_500_000_000);

        // 3. Update segment progress
        storage
            .update_segment_progress(&seg1.id, 100_000)
            .await
            .unwrap();
        let updated_segs = storage.get_segments_for_task(&task.id).await.unwrap();
        assert_eq!(updated_segs[0].downloaded_bytes, 100_000);

        // 4. Update task status
        storage
            .update_status(&task.id, DownloadStatus::Downloading)
            .await
            .unwrap();
        let after_update = storage.get_task(&task.id).await.unwrap().unwrap();
        assert_eq!(after_update.status, DownloadStatus::Downloading);

        // 5. Test NetworkPath persistence
        let path = NetworkPath {
            id: PathId::new(),
            name: "office-ethernet".to_string(),
            interface_id: InterfaceId::new(),
            proxy_id: None,
            priority: 8,
            enabled: true,
            limits: PathLimits {
                max_connections: Some(16),
                max_bandwidth_bps: Some(100_000_000),
            },
        };

        storage.save_path(&path).await.unwrap();
        let fetched_path = storage.get_path(&path.id).await.unwrap().unwrap();
        assert_eq!(fetched_path.name, "office-ethernet");
        assert_eq!(fetched_path.priority, 8);
        assert_eq!(fetched_path.limits.max_connections, Some(16));

        // 6. Test ProxyProfile persistence
        let proxy = ProxyProfile {
            id: ProxyId::new(),
            name: "secure-socks5".to_string(),
            proxy_type: ProxyType::Socks5,
            host: "10.0.0.1".to_string(),
            port: 1080,
            credential_reference: Some("keychain:proxy:socks5".to_string()),
            enabled: true,
            dns_policy: DnsPolicy::Remote,
        };

        storage.save_profile(&proxy).await.unwrap();
        let fetched_proxy = storage.get_profile(&proxy.id).await.unwrap().unwrap();
        assert_eq!(fetched_proxy.host, "10.0.0.1");
        assert_eq!(fetched_proxy.dns_policy, DnsPolicy::Remote);

        // 7. Test cascade delete (deleting task deletes its segments)
        let deleted = storage.delete_task(&task.id).await.unwrap();
        assert!(deleted);
        assert!(storage.get_task(&task.id).await.unwrap().is_none());
        assert!(storage
            .get_segments_for_task(&task.id)
            .await
            .unwrap()
            .is_empty());
    }
}
