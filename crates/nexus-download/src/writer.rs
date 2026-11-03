//! Asynchronous, actor-based disk writer providing non-blocking concurrent writes,
//! automatic backpressure, and safe single-handle file synchronization.

use bytes::Bytes;
use std::io::SeekFrom;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, error};

#[derive(Error, Debug)]
pub enum WriterError {
    #[error("File I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Writer channel closed")]
    ChannelClosed,
}

pub enum WriterCommand {
    Write { offset: u64, data: Bytes },
    Sync(oneshot::Sender<Result<(), std::io::Error>>),
    Close(oneshot::Sender<Result<(), std::io::Error>>),
}

/// Handle held by download workers to submit write operations asynchronously.
#[derive(Clone, Debug)]
pub struct AsyncFileWriterHandle {
    sender: mpsc::Sender<WriterCommand>,
    has_error: Arc<AtomicBool>,
    last_error: Arc<Mutex<Option<std::io::Error>>>,
}

impl AsyncFileWriterHandle {
    /// Submits a write chunk at a specific byte offset.
    ///
    /// Applies backpressure when writer queue is full.
    pub async fn write(&self, offset: u64, data: Bytes) -> Result<(), WriterError> {
        if self.has_error.load(Ordering::Relaxed) {
            let mut err_guard = self.last_error.lock().await;
            if let Some(err) = err_guard.take() {
                return Err(WriterError::Io(err));
            }
            return Err(WriterError::ChannelClosed);
        }

        self.sender
            .send(WriterCommand::Write { offset, data })
            .await
            .map_err(|_| {
                self.has_error.store(true, Ordering::Relaxed);
                WriterError::ChannelClosed
            })
    }

    /// Flushes and syncs OS buffers to physical storage.
    pub async fn sync(&self) -> Result<(), WriterError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(WriterCommand::Sync(tx))
            .await
            .map_err(|_| WriterError::ChannelClosed)?;

        rx.await
            .map_err(|_| WriterError::ChannelClosed)?
            .map_err(WriterError::Io)
    }

    /// Flushes OS buffers and cleanly terminates the background writer task.
    pub async fn close(&self) -> Result<(), WriterError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(WriterCommand::Close(tx))
            .await
            .map_err(|_| WriterError::ChannelClosed)?;

        rx.await
            .map_err(|_| WriterError::ChannelClosed)?
            .map_err(WriterError::Io)
    }
}

/// Spawns a dedicated background disk writer task managing an open file handle.
pub fn spawn_disk_writer(
    mut file: File,
    buffer_capacity: usize,
) -> (
    AsyncFileWriterHandle,
    tokio::task::JoinHandle<Result<(), std::io::Error>>,
) {
    let (tx, mut rx) = mpsc::channel(buffer_capacity);
    let has_error = Arc::new(AtomicBool::new(false));
    let last_error = Arc::new(Mutex::new(None));

    let handle = AsyncFileWriterHandle {
        sender: tx,
        has_error: Arc::clone(&has_error),
        last_error: Arc::clone(&last_error),
    };

    let writer_task = tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                WriterCommand::Write { offset, data } => {
                    if let Err(e) = file.seek(SeekFrom::Start(offset)).await {
                        error!(error = %e, offset = offset, "Failed to seek in part file");
                        has_error.store(true, Ordering::Relaxed);
                        *last_error.lock().await =
                            Some(std::io::Error::new(e.kind(), e.to_string()));
                        return Err(e);
                    }

                    if let Err(e) = file.write_all(&data).await {
                        error!(error = %e, offset = offset, len = data.len(), "Failed to write bytes to part file");
                        has_error.store(true, Ordering::Relaxed);
                        *last_error.lock().await =
                            Some(std::io::Error::new(e.kind(), e.to_string()));
                        return Err(e);
                    }
                }
                WriterCommand::Sync(reply) => {
                    let res = file.sync_all().await;
                    let _ = reply.send(res);
                }
                WriterCommand::Close(reply) => {
                    let res = file.sync_all().await;
                    let _ = reply.send(res);
                    debug!("Disk writer closed successfully");
                    return Ok(());
                }
            }
        }

        // Channel dropped; flush final contents
        file.sync_all().await?;
        Ok(())
    });

    (handle, writer_task)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::fs::{self, OpenOptions};
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_async_writer_sparse_writes_and_flush() {
        let dir = std::env::temp_dir().join(format!("nexus_writer_test_{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("writer_test.bin");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await
            .unwrap();

        let (writer, join_handle) = spawn_disk_writer(file, 16);

        // Concurrently write at offsets 10 and 0
        writer
            .write(10, Bytes::from_static(b"WORLD"))
            .await
            .unwrap();
        writer
            .write(0, Bytes::from_static(b"HELLO_"))
            .await
            .unwrap();

        writer.close().await.unwrap();
        join_handle.await.unwrap().unwrap();

        let mut read_file = File::open(&path).await.unwrap();
        let mut content = Vec::new();
        read_file.read_to_end(&mut content).await.unwrap();

        assert_eq!(&content[0..6], b"HELLO_");
        assert_eq!(&content[10..15], b"WORLD");

        let _ = fs::remove_dir_all(&dir).await;
    }
}
