//! Integration test verifying database persistence durability and crash recovery across restarts.
//!
//! Acceptance criteria: Restart does not silently lose persisted tasks.

use nexus_core::ByteSize;
use nexus_domain::{DownloadSegment, DownloadStatus, DownloadTask, SegmentStatus};
use nexus_recovery::RecoveryManager;
use nexus_storage::{SegmentRepository, SqliteStorage, TaskRepository};
use std::fs;
use std::sync::Arc;
use url::Url;

#[tokio::test]
async fn test_restart_durability_and_crash_recovery() {
    let db_path =
        std::env::temp_dir().join(format!("nexus_test_restart_{}.db", uuid::Uuid::new_v4()));
    let db_url = format!("sqlite://{}", db_path.display());

    let task_id;
    let seg1_id;

    // STEP 1: First application run: insert tasks and segments, then simulate unexpected termination
    {
        let storage = Arc::new(
            SqliteStorage::connect(&db_url)
                .await
                .expect("Failed to connect to SQLite"),
        );

        let mut task = DownloadTask::new(
            Url::parse("https://releases.ubuntu.com/jammy/ubuntu-22.04.iso").unwrap(),
            "ubuntu-22.04.iso".to_string(),
            "/downloads".to_string(),
        );
        task.total_size = Some(ByteSize(4_000_000_000));
        task.downloaded_size = ByteSize(1_000_000_000);
        task.status = DownloadStatus::Downloading; // In-flight active state when crash occurs

        storage.save_task(&task).await.expect("Failed to save task");
        task_id = task.id;

        let seg1 = DownloadSegment {
            id: nexus_domain::SegmentId::new(),
            download_id: task_id,
            start_offset: 0,
            end_offset: 1_999_999_999,
            downloaded_bytes: 1_000_000_000,
            assigned_path_id: None,
            status: SegmentStatus::InProgress,
            retry_count: 0,
        };
        seg1_id = seg1.id;

        storage
            .save_segments(&[seg1])
            .await
            .expect("Failed to save segment");

        // Explicitly drop storage and close pool to simulate process exit / sudden reboot
        drop(storage);
    }

    // STEP 2: Second application run: reopen database, verify tasks survived, and execute recovery
    {
        let storage = Arc::new(
            SqliteStorage::connect(&db_url)
                .await
                .expect("Failed to reconnect to SQLite"),
        );

        // Verify task still exists with exact progress intact
        let task = storage
            .get_task(&task_id)
            .await
            .expect("Fetch error")
            .expect("Task was lost across restart!");
        assert_eq!(task.id, task_id);
        assert_eq!(task.downloaded_size, ByteSize(1_000_000_000));
        assert_eq!(task.total_size, Some(ByteSize(4_000_000_000)));
        assert_eq!(task.status, DownloadStatus::Downloading);

        // Verify segment progress survived
        let segs = storage
            .get_segments_for_task(&task_id)
            .await
            .expect("Fetch segs error");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].id, seg1_id);
        assert_eq!(segs[0].downloaded_bytes, 1_000_000_000);

        // Run recovery manager
        let recovery = RecoveryManager::new(storage.clone());
        let recovered_count = recovery
            .recover_interrupted_tasks()
            .await
            .expect("Recovery error");
        assert_eq!(recovered_count, 1);

        // Verify task is now safely Paused instead of corrupting or downloading
        let recovered_task = storage
            .get_task(&task_id)
            .await
            .expect("Fetch error")
            .unwrap();
        assert_eq!(recovered_task.status, DownloadStatus::Paused);
    }

    // Cleanup disk file
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));
}
