//! Download queues, automation rules, and scheduled transfer policies.

use nexus_domain::TaskId;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Error, Debug)]
pub enum AutomationError {
    #[error("Queue is empty")]
    QueueEmpty,

    #[error("Task already in queue")]
    AlreadyQueued,
}

/// Download rule for auto-categorization and routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationRule {
    pub name: String,
    pub url_pattern: String,
    pub target_category: Option<String>,
    pub destination_dir: Option<String>,
    pub enabled: bool,
}

/// Thread-safe download queue orchestrator.
#[derive(Clone, Default)]
pub struct DownloadQueue {
    queue: Arc<RwLock<VecDeque<TaskId>>>,
}

impl DownloadQueue {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Enqueues a task ID at the end of the queue.
    pub async fn push_back(&self, task_id: TaskId) {
        let mut q = self.queue.write().await;
        if !q.contains(&task_id) {
            info!(task_id = %task_id, "Enqueued task");
            q.push_back(task_id);
        }
    }

    /// Pops the next task ID from the front of the queue.
    pub async fn pop_front(&self) -> Option<TaskId> {
        let mut q = self.queue.write().await;
        q.pop_front()
    }

    /// Returns the number of tasks in the queue.
    pub async fn len(&self) -> usize {
        self.queue.read().await.len()
    }

    /// Returns true if the queue is empty.
    pub async fn is_empty(&self) -> bool {
        self.queue.read().await.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_queue_ordering() {
        let queue = DownloadQueue::new();
        let t1 = TaskId::new();
        let t2 = TaskId::new();

        queue.push_back(t1).await;
        queue.push_back(t2).await;
        assert_eq!(queue.len().await, 2);

        assert_eq!(queue.pop_front().await, Some(t1));
        assert_eq!(queue.pop_front().await, Some(t2));
        assert_eq!(queue.pop_front().await, None);
    }
}
