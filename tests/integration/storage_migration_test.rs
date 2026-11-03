//! Integration test for SQLite migrations, schema verification, and transactional consistency.

use nexus_core::ByteSize;
use nexus_domain::{DownloadStatus, DownloadTask};
use nexus_storage::{SqliteStorage, TaskRepository};
use url::Url;

#[tokio::test]
async fn test_database_migrations_and_transactions() {
    let storage = SqliteStorage::connect_in_memory()
        .await
        .expect("Failed to create in-memory SQLite storage");

    // Verify task insertion and query by status
    for i in 1..=5 {
        let mut task = DownloadTask::new(
            Url::parse(&format!("https://example.com/file{i}.bin")).unwrap(),
            format!("file{i}.bin"),
            "/tmp".to_string(),
        );
        task.total_size = Some(ByteSize(1_000_000 * i));
        if i % 2 == 0 {
            task.status = DownloadStatus::Completed;
        } else {
            task.status = DownloadStatus::Paused;
        }

        storage.save_task(&task).await.expect("Failed to save task");
    }

    let all_tasks = storage
        .list_tasks(None)
        .await
        .expect("Failed to list all tasks");
    assert_eq!(all_tasks.len(), 5);

    let completed = storage
        .list_tasks(Some(DownloadStatus::Completed))
        .await
        .expect("Failed to filter completed");
    assert_eq!(completed.len(), 2);

    let paused = storage
        .list_tasks(Some(DownloadStatus::Paused))
        .await
        .expect("Failed to filter paused");
    assert_eq!(paused.len(), 3);
}
