//! Decoupled cross-crate asynchronous event bus for NEXUS.

use chrono::{DateTime, Utc};
use nexus_domain::{DownloadStatus, PathId, SegmentId, TaskId};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::debug;

/// System events emitted by NEXUS components.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemEvent {
    TaskCreated {
        task_id: TaskId,
        timestamp: DateTime<Utc>,
    },
    TaskStatusChanged {
        task_id: TaskId,
        old_status: DownloadStatus,
        new_status: DownloadStatus,
        timestamp: DateTime<Utc>,
    },
    SegmentProgress {
        task_id: TaskId,
        segment_id: SegmentId,
        downloaded_bytes: u64,
        total_bytes: u64,
        timestamp: DateTime<Utc>,
    },
    PathFailed {
        path_id: PathId,
        reason: String,
        timestamp: DateTime<Utc>,
    },
    TaskCompleted {
        task_id: TaskId,
        final_bytes: u64,
        timestamp: DateTime<Utc>,
    },
}

/// Central publish-subscribe asynchronous event bus.
#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<SystemEvent>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(256)
    }
}

impl EventBus {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Publishes an event to all active subscribers.
    pub fn publish(&self, event: SystemEvent) {
        debug!(?event, "Publishing system event");
        let _ = self.sender.send(event);
    }

    /// Subscribes to the event stream.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<SystemEvent> {
        self.sender.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_event_bus_broadcast() {
        let bus = EventBus::new(16);
        let mut rx = bus.subscribe();

        let task_id = TaskId::new();
        bus.publish(SystemEvent::TaskCreated {
            task_id,
            timestamp: Utc::now(),
        });

        let received = rx.recv().await.unwrap();
        match received {
            SystemEvent::TaskCreated {
                task_id: received_id,
                ..
            } => {
                assert_eq!(received_id, task_id);
            }
            _ => panic!("Unexpected event received"),
        }
    }
}
