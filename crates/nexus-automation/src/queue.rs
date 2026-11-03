//! Download queue manager enforcing concurrency limits and priority-ordered execution.

use nexus_domain::TaskId;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Debug, Clone, Eq, PartialEq)]
struct QueuedItem {
    task_id: TaskId,
    priority: u32,
    seq: u64,
}

impl Ord for QueuedItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first; if equal, lower seq first (FIFO)
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for QueuedItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct QueueState {
    max_active_tasks: usize,
    active_tasks: HashSet<TaskId>,
    pending_tasks: BinaryHeap<QueuedItem>,
    next_seq: u64,
}

/// Concurrent priority queue coordinator for download tasks.
#[derive(Clone)]
pub struct QueueManager {
    state: Arc<RwLock<QueueState>>,
}

impl Default for QueueManager {
    fn default() -> Self {
        Self::new(3)
    }
}

impl QueueManager {
    #[must_use]
    pub fn new(max_active_tasks: usize) -> Self {
        Self {
            state: Arc::new(RwLock::new(QueueState {
                max_active_tasks: max_active_tasks.max(1),
                active_tasks: HashSet::new(),
                pending_tasks: BinaryHeap::new(),
                next_seq: 0,
            })),
        }
    }

    /// Sets the maximum concurrent active downloads.
    pub async fn set_max_active_tasks(&self, limit: usize) {
        let mut st = self.state.write().await;
        st.max_active_tasks = limit.max(1);
    }

    /// Enqueues a task with a given priority.
    pub async fn enqueue(&self, task_id: TaskId, priority: u32) {
        let mut st = self.state.write().await;
        let seq = st.next_seq;
        st.next_seq += 1;

        st.pending_tasks.push(QueuedItem {
            task_id,
            priority,
            seq,
        });

        info!(task_id = %task_id, priority = priority, "Task added to download queue");
    }

    /// Polls the next task to run if concurrency slot is available.
    pub async fn poll_next(&self) -> Option<TaskId> {
        let mut st = self.state.write().await;
        if st.active_tasks.len() >= st.max_active_tasks {
            return None;
        }

        while let Some(item) = st.pending_tasks.pop() {
            if !st.active_tasks.contains(&item.task_id) {
                st.active_tasks.insert(item.task_id);
                return Some(item.task_id);
            }
        }

        None
    }

    /// Marks a task as finished (completed, failed, or cancelled) and returns the next task to run, if any.
    pub async fn on_task_finished(&self, task_id: TaskId) -> Option<TaskId> {
        let mut st = self.state.write().await;
        st.active_tasks.remove(&task_id);

        if st.active_tasks.len() >= st.max_active_tasks {
            return None;
        }

        while let Some(item) = st.pending_tasks.pop() {
            if !st.active_tasks.contains(&item.task_id) {
                st.active_tasks.insert(item.task_id);
                return Some(item.task_id);
            }
        }

        None
    }

    /// Returns the number of currently active transfers.
    pub async fn active_count(&self) -> usize {
        self.state.read().await.active_tasks.len()
    }

    /// Returns the number of pending tasks waiting in the queue.
    pub async fn pending_count(&self) -> usize {
        self.state.read().await.pending_tasks.len()
    }

    /// Cancels or removes a task from the queue or active set.
    pub async fn remove(&self, task_id: &TaskId) -> bool {
        let mut st = self.state.write().await;
        let removed_active = st.active_tasks.remove(task_id);

        // Filter pending
        let mut retained = Vec::new();
        let mut found_pending = false;
        while let Some(item) = st.pending_tasks.pop() {
            if &item.task_id == task_id {
                found_pending = true;
            } else {
                retained.push(item);
            }
        }
        for item in retained {
            st.pending_tasks.push(item);
        }

        removed_active || found_pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_queue_priority_and_concurrency_slots() {
        let manager = QueueManager::new(2); // Max 2 active tasks

        let t_low = TaskId::new();
        let t_med = TaskId::new();
        let t_high = TaskId::new();

        manager.enqueue(t_low, 1).await;
        manager.enqueue(t_high, 10).await;
        manager.enqueue(t_med, 5).await;

        // Slot 1: highest priority (t_high)
        let first = manager.poll_next().await;
        assert_eq!(first, Some(t_high));

        // Slot 2: second highest (t_med)
        let second = manager.poll_next().await;
        assert_eq!(second, Some(t_med));

        // Slots full (max 2 active)
        let third = manager.poll_next().await;
        assert_eq!(third, None);
        assert_eq!(manager.active_count().await, 2);
        assert_eq!(manager.pending_count().await, 1);

        // Finish slot 1 -> slot opens for t_low
        let promoted = manager.on_task_finished(t_high).await;
        assert_eq!(promoted, Some(t_low));
        assert_eq!(manager.active_count().await, 2);
        assert_eq!(manager.pending_count().await, 0);

        // Finish remaining
        manager.on_task_finished(t_med).await;
        manager.on_task_finished(t_low).await;
        assert_eq!(manager.active_count().await, 0);
    }
}
