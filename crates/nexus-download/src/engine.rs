//! High-performance download execution engine with HTTP Range support, pause/resume, and retry logic.

use chrono::Utc;
use nexus_core::ByteSize;
use nexus_domain::{DownloadStatus, DownloadTask, NetworkPath, SegmentId, TaskId};
use nexus_events::{EventBus, SystemEvent};
use nexus_segments::partition_segments;
use nexus_storage::{SegmentRepository, StorageError, TaskRepository};
use nexus_transport::{RemoteMetadata, TransportClient, TransportError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::file::{
    final_file_path, open_or_create_part_file, part_file_path, promote_part_file, sync_file,
    write_at_offset, FileError,
};
use crate::worker::WorkerPool;
use crate::writer::spawn_disk_writer;
use crate::DownloadStateMachine;

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Transport error: {0}")]
    Transport(#[from] TransportError),

    #[error("File I/O error: {0}")]
    File(#[from] FileError),

    #[error("Task '{0}' not found")]
    TaskNotFound(String),

    #[error("Download was aborted or paused by user")]
    CancelledOrPaused,

    #[error("Max retries exceeded ({retries}) for segment {segment_id}")]
    MaxRetriesExceeded { segment_id: SegmentId, retries: u32 },
}

/// Download engine managing execution, range allocation, streaming, and retries.
pub struct DownloadEngine {
    task_repo: Arc<dyn TaskRepository>,
    segment_repo: Arc<dyn SegmentRepository>,
    transport: Arc<dyn TransportClient>,
    event_bus: Arc<EventBus>,
}

impl DownloadEngine {
    #[must_use]
    pub fn new(
        task_repo: Arc<dyn TaskRepository>,
        segment_repo: Arc<dyn SegmentRepository>,
        transport: Arc<dyn TransportClient>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            task_repo,
            segment_repo,
            transport,
            event_bus,
        }
    }

    /// Executes the full download lifecycle for a task across a single network path.
    pub async fn download(
        &self,
        task_id: &TaskId,
        path: &NetworkPath,
        cancel_token: CancellationToken,
    ) -> Result<(), EngineError> {
        self.download_multi_path(task_id, std::slice::from_ref(path), cancel_token)
            .await
    }

    /// Executes the full download lifecycle for a task across multiple network paths with dynamic load balancing and failover.
    pub async fn download_multi_path(
        &self,
        task_id: &TaskId,
        paths: &[NetworkPath],
        cancel_token: CancellationToken,
    ) -> Result<(), EngineError> {
        if paths.is_empty() {
            return Err(EngineError::Transport(TransportError::Protocol(
                "No network paths provided for download".to_string(),
            )));
        }

        let primary_path = &paths[0];

        let mut task = self
            .task_repo
            .get_task(task_id)
            .await?
            .ok_or_else(|| EngineError::TaskNotFound(task_id.to_string()))?;

        info!(
            task_id = %task.id,
            url = %task.url,
            num_paths = paths.len(),
            "Starting multi-path download execution"
        );

        if cancel_token.is_cancelled() {
            if task.status != DownloadStatus::Paused {
                DownloadStateMachine::transition(&mut task, DownloadStatus::Paused)
                    .map_err(|e| EngineError::Storage(StorageError::Database(e.to_string())))?;
                self.task_repo.save_task(&task).await?;
            }
            return Err(EngineError::CancelledOrPaused);
        }

        // Step 1: Transition to Preparing if starting fresh
        if task.status == DownloadStatus::Created || task.status == DownloadStatus::Queued {
            DownloadStateMachine::transition(&mut task, DownloadStatus::Preparing)
                .map_err(|e| EngineError::Storage(StorageError::Database(e.to_string())))?;
            self.task_repo.save_task(&task).await?;
        }

        // Step 2: Probe remote metadata via primary path
        let metadata = self
            .transport
            .probe_metadata(primary_path, &task.url)
            .await?;
        self.apply_probed_metadata(&mut task, &metadata).await?;

        if cancel_token.is_cancelled() {
            DownloadStateMachine::transition(&mut task, DownloadStatus::Paused)
                .map_err(|e| EngineError::Storage(StorageError::Database(e.to_string())))?;
            self.task_repo.save_task(&task).await?;
            return Err(EngineError::CancelledOrPaused);
        }

        // Step 3: Check range support and partition segments
        let part_path = part_file_path(&task.destination_dir, &task.filename, &task.id);
        let final_path = final_file_path(&task.destination_dir, &task.filename);

        let mut file =
            open_or_create_part_file(&part_path, task.total_size.map(|s| s.bytes())).await?;

        // Transition to Downloading
        if task.status != DownloadStatus::Downloading {
            DownloadStateMachine::transition(&mut task, DownloadStatus::Downloading)
                .map_err(|e| EngineError::Storage(StorageError::Database(e.to_string())))?;
            self.task_repo.save_task(&task).await?;
        }

        let supports_ranges = metadata.supports_ranges && task.total_size.is_some();

        if supports_ranges {
            let total_bytes = task.total_size.unwrap().bytes();
            let mut segments = self.segment_repo.get_segments_for_task(&task.id).await?;

            let connection_count = task.policy.max_connections.clamp(1, 16) as usize;
            if segments.is_empty() {
                segments = partition_segments(task.id, total_bytes, connection_count)
                    .map_err(|e| EngineError::Storage(StorageError::Database(e.to_string())))?;
                self.segment_repo.save_segments(&segments).await?;
            }

            let (writer_handle, writer_task) = spawn_disk_writer(file, 128);
            let total_downloaded = Arc::new(AtomicU64::new(task.downloaded_size.bytes()));

            let pool = WorkerPool::new(
                task.id,
                task.url.clone(),
                total_bytes,
                task.policy.retry_limit,
                connection_count,
                writer_handle.clone(),
                Arc::clone(&self.transport),
                Arc::clone(&self.segment_repo),
                Arc::clone(&self.event_bus),
                cancel_token.clone(),
            );

            let pool_result = pool
                .execute(segments, paths, Arc::clone(&total_downloaded))
                .await;

            // Close writer to flush all buffered chunks
            let close_res = writer_handle.close().await;
            let _ = writer_task.await;

            let downloaded_so_far = total_downloaded.load(Ordering::Relaxed);
            task.downloaded_size = ByteSize(downloaded_so_far.min(total_bytes));
            self.task_repo.save_task(&task).await?;

            if cancel_token.is_cancelled()
                || matches!(pool_result, Err(EngineError::CancelledOrPaused))
            {
                DownloadStateMachine::transition(&mut task, DownloadStatus::Paused)
                    .map_err(|e| EngineError::Storage(StorageError::Database(e.to_string())))?;
                self.task_repo.save_task(&task).await?;
                info!(task_id = %task.id, "Multi-connection download safely paused and flushed");
                return Err(EngineError::CancelledOrPaused);
            }

            if let Err(e) = close_res {
                return Err(EngineError::File(FileError::Io(match e {
                    crate::writer::WriterError::Io(err) => err,
                    crate::writer::WriterError::ChannelClosed => {
                        std::io::Error::other("Writer channel closed")
                    }
                })));
            }

            pool_result?;
        } else {
            // Non-range / unknown size streaming fallback
            info!(task_id = %task.id, "Server does not support Range; falling back to single stream");
            self.download_single_stream(&mut task, &mut file, primary_path, &cancel_token)
                .await?;

            if cancel_token.is_cancelled() {
                sync_file(&mut file).await.map_err(FileError::Io)?;
                DownloadStateMachine::transition(&mut task, DownloadStatus::Paused)
                    .map_err(|e| EngineError::Storage(StorageError::Database(e.to_string())))?;
                self.task_repo.save_task(&task).await?;
                info!(task_id = %task.id, "Download safely paused and flushed");
                return Err(EngineError::CancelledOrPaused);
            }

            sync_file(&mut file).await.map_err(FileError::Io)?;
            drop(file);
        }

        // Step 5: Transition to Verifying
        DownloadStateMachine::transition(&mut task, DownloadStatus::Verifying)
            .map_err(|e| EngineError::Storage(StorageError::Database(e.to_string())))?;
        self.task_repo.save_task(&task).await?;

        promote_part_file(&part_path, &final_path, true).await?;

        // Step 6: Mark Completed
        DownloadStateMachine::transition(&mut task, DownloadStatus::Completed)
            .map_err(|e| EngineError::Storage(StorageError::Database(e.to_string())))?;
        self.task_repo.save_task(&task).await?;

        self.event_bus.publish(SystemEvent::TaskCompleted {
            task_id: task.id,
            final_bytes: task.downloaded_size.bytes(),
            timestamp: Utc::now(),
        });

        info!(task_id = %task.id, final_path = %final_path.display(), "Download completed successfully");
        Ok(())
    }

    async fn apply_probed_metadata(
        &self,
        task: &mut DownloadTask,
        metadata: &RemoteMetadata,
    ) -> Result<(), EngineError> {
        if let Some(ref redirected) = metadata.redirected_url {
            debug!(old_url = %task.url, new_url = %redirected, "Updating task URL to redirected destination");
            task.url = redirected.clone();
        }
        if let Some(size) = metadata.total_size {
            task.total_size = Some(ByteSize(size));
        }
        if let Some(ref etag) = metadata.etag {
            task.etag = Some(etag.clone());
        }
        if let Some(ref lm) = metadata.last_modified {
            task.last_modified = Some(lm.clone());
        }
        if let Some(ref filename) = metadata.suggested_filename {
            if task.filename == "download.file" || task.filename.is_empty() {
                task.filename = filename.clone();
            }
        }
        task.updated_at = Utc::now();
        self.task_repo.save_task(task).await?;
        Ok(())
    }

    async fn download_single_stream(
        &self,
        task: &mut DownloadTask,
        file: &mut tokio::fs::File,
        path: &NetworkPath,
        cancel_token: &CancellationToken,
    ) -> Result<(), EngineError> {
        let mut stream = self
            .transport
            .fetch_range(path, &task.url, 0, u64::MAX)
            .await?;

        let mut buffer = [0u8; 64 * 1024];

        loop {
            if cancel_token.is_cancelled() {
                return Ok(());
            }

            match stream.read(&mut buffer).await {
                Ok(0) => break,
                Ok(n) => {
                    let write_offset = task.downloaded_size.bytes();
                    write_at_offset(file, write_offset, &buffer[..n])
                        .await
                        .map_err(FileError::Io)?;

                    task.downloaded_size = ByteSize(task.downloaded_size.bytes() + n as u64);
                    task.updated_at = Utc::now();
                    let _ = self.task_repo.save_task(task).await;
                }
                Err(e) => {
                    return Err(EngineError::Transport(TransportError::Io(e)));
                }
            }
        }

        if task.total_size.is_none() {
            task.total_size = Some(task.downloaded_size);
        }

        Ok(())
    }
}
