//! Safe temporary file handling, sparse allocation, and atomic file promotion.

use nexus_domain::TaskId;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, info};

#[derive(Error, Debug)]
pub enum FileError {
    #[error("Target file '{0}' already exists and overwrite is forbidden")]
    FileAlreadyExists(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Generates the deterministic temporary `.part` file path: `<dir>/<filename>.<task_id>.part`
#[must_use]
pub fn part_file_path(destination_dir: &str, filename: &str, task_id: &TaskId) -> PathBuf {
    let part_name = format!("{filename}.{task_id}.part");
    Path::new(destination_dir).join(part_name)
}

/// Generates the target final file path: `<dir>/<filename>`
#[must_use]
pub fn final_file_path(destination_dir: &str, filename: &str) -> PathBuf {
    Path::new(destination_dir).join(filename)
}

/// Opens or creates the temporary partial file, ensuring parent directories exist.
/// Pre-allocates file length if `preallocate_size` is specified.
pub async fn open_or_create_part_file(
    part_path: &Path,
    preallocate_size: Option<u64>,
) -> Result<File, FileError> {
    if let Some(parent) = part_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(part_path)
        .await?;

    if let Some(size) = preallocate_size {
        let current_meta = file.metadata().await?;
        if current_meta.len() < size {
            debug!(path = %part_path.display(), size = size, "Pre-allocating partial file space");
            file.set_len(size).await?;
        }
    }

    Ok(file)
}

/// Writes a chunk of data at a specific byte offset in the file using async seek.
pub async fn write_at_offset(
    file: &mut File,
    offset: u64,
    data: &[u8],
) -> Result<(), std::io::Error> {
    file.seek(SeekFrom::Start(offset)).await?;
    file.write_all(data).await?;
    Ok(())
}

/// Flushes in-memory buffers and syncs file contents to the physical storage device (`fsync`).
pub async fn sync_file(file: &mut File) -> Result<(), std::io::Error> {
    file.flush().await?;
    file.sync_all().await?;
    Ok(())
}

/// Promotes the completed `.part` file to its final destination via atomic rename.
/// Fails if the destination exists and `overwrite` is false.
pub async fn promote_part_file(
    part_path: &Path,
    final_path: &Path,
    overwrite: bool,
) -> Result<(), FileError> {
    if final_path.exists() && !overwrite {
        return Err(FileError::FileAlreadyExists(
            final_path.display().to_string(),
        ));
    }

    info!(
        from = %part_path.display(),
        to = %final_path.display(),
        "Promoting temporary file to final destination"
    );

    fs::rename(part_path, final_path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_writing_and_promotion() {
        let temp_dir =
            std::env::temp_dir().join(format!("nexus_file_test_{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&temp_dir).await.unwrap();

        let task_id = TaskId::new();
        let part_path = part_file_path(temp_dir.to_str().unwrap(), "sample.bin", &task_id);
        let final_path = final_file_path(temp_dir.to_str().unwrap(), "sample.bin");

        // Open and write segments out of order
        let mut file = open_or_create_part_file(&part_path, Some(20))
            .await
            .unwrap();
        write_at_offset(&mut file, 10, b"WORLD12345").await.unwrap();
        write_at_offset(&mut file, 0, b"HELLO_____").await.unwrap();
        sync_file(&mut file).await.unwrap();
        drop(file);

        // Verify contents before promotion
        let contents = fs::read(&part_path).await.unwrap();
        assert_eq!(&contents[..10], b"HELLO_____");
        assert_eq!(&contents[10..20], b"WORLD12345");

        // Promote to final file
        promote_part_file(&part_path, &final_path, false)
            .await
            .unwrap();
        assert!(!part_path.exists());
        assert!(final_path.exists());

        let final_contents = fs::read(&final_path).await.unwrap();
        assert_eq!(final_contents.len(), 20);

        // Cannot promote again without overwrite=true
        // Create dummy part again to test
        fs::write(&part_path, b"test").await.unwrap();
        assert!(promote_part_file(&part_path, &final_path, false)
            .await
            .is_err());
        assert!(promote_part_file(&part_path, &final_path, true)
            .await
            .is_ok());

        let _ = fs::remove_dir_all(&temp_dir).await;
    }
}
