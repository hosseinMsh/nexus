//! Download lifecycle coordinator, finite state machine, and execution orchestration.

use chrono::Utc;
use nexus_domain::{DownloadStatus, DownloadTask};
use thiserror::Error;
use tracing::{debug, info, warn};

pub mod engine;
pub mod file;
pub mod worker;
pub mod writer;

pub use engine::{DownloadEngine, EngineError};
pub use file::{
    final_file_path, open_or_create_part_file, part_file_path, promote_part_file, sync_file,
    write_at_offset, FileError,
};
pub use worker::WorkerPool;
pub use writer::{spawn_disk_writer, AsyncFileWriterHandle, WriterError};

#[derive(Error, Debug, PartialEq, Eq)]
pub enum DownloadStateError {
    #[error("Illegal state transition from {from} to {to}")]
    IllegalTransition {
        from: DownloadStatus,
        to: DownloadStatus,
    },

    #[error("Task {0} cannot be paused because it is not active")]
    CannotPauseInactiveTask(String),

    #[error("Task {0} cannot be resumed because it is not paused or recovering")]
    CannotResumeActiveTask(String),
}

/// Download state machine enforcing the state transitions specified in PRD Section 11.1.
#[derive(Debug, Clone, Copy, Default)]
pub struct DownloadStateMachine;

impl DownloadStateMachine {
    /// Checks whether transitioning from `current` to `target` is valid.
    #[must_use]
    pub fn is_valid_transition(current: DownloadStatus, target: DownloadStatus) -> bool {
        use DownloadStatus::*;

        if current == target {
            return true;
        }

        match (current, target) {
            // Initial transitions
            (Created, Queued) => true,
            (Created, Preparing) => true,
            (Created, Paused) => true,
            (Created, Cancelled) => true,

            // Queued
            (Queued, Preparing) => true,
            (Queued, Paused) => true,
            (Queued, Cancelled) => true,

            // Preparing
            (Preparing, Downloading) => true,
            (Preparing, Paused) => true,
            (Preparing, Failed) => true,
            (Preparing, Cancelled) => true,

            // Downloading
            (Downloading, Paused) => true,
            (Downloading, Recovering) => true,
            (Downloading, Verifying) => true,
            (Downloading, Completed) => true,
            (Downloading, Failed) => true,
            (Downloading, Cancelled) => true,

            // Paused
            (Paused, Downloading) => true,
            (Paused, Queued) => true,
            (Paused, Cancelled) => true,

            // Recovering
            (Recovering, Downloading) => true,
            (Recovering, Failed) => true,
            (Recovering, Cancelled) => true,

            // Verifying
            (Verifying, Completed) => true,
            (Verifying, Failed) => true,

            // Failed
            (Failed, Queued) => true,
            (Failed, Preparing) => true,

            // Terminal states
            (Completed, _) => false,
            (Cancelled, _) => false,

            // All other combinations are illegal
            _ => false,
        }
    }

    /// Attempts to apply a state transition to a DownloadTask.
    /// Returns `Ok(())` and updates `updated_at` on success, or an error if invalid.
    pub fn transition(
        task: &mut DownloadTask,
        target: DownloadStatus,
    ) -> Result<(), DownloadStateError> {
        let current = task.status;
        if !Self::is_valid_transition(current, target) {
            warn!(
                task_id = %task.id,
                current_state = %current,
                target_state = %target,
                "Illegal state transition rejected"
            );
            return Err(DownloadStateError::IllegalTransition {
                from: current,
                to: target,
            });
        }

        debug!(task_id = %task.id, from = %current, to = %target, "State transition applied");
        task.status = target;
        task.updated_at = Utc::now();
        Ok(())
    }
}

/// Transition convenience helpers for high-level operations.
pub fn mark_queued(task: &mut DownloadTask) -> Result<(), DownloadStateError> {
    DownloadStateMachine::transition(task, DownloadStatus::Queued)
}

pub fn mark_preparing(task: &mut DownloadTask) -> Result<(), DownloadStateError> {
    DownloadStateMachine::transition(task, DownloadStatus::Preparing)
}

pub fn mark_downloading(task: &mut DownloadTask) -> Result<(), DownloadStateError> {
    DownloadStateMachine::transition(task, DownloadStatus::Downloading)
}

pub fn mark_paused(task: &mut DownloadTask) -> Result<(), DownloadStateError> {
    DownloadStateMachine::transition(task, DownloadStatus::Paused)
}

pub fn mark_recovering(task: &mut DownloadTask) -> Result<(), DownloadStateError> {
    DownloadStateMachine::transition(task, DownloadStatus::Recovering)
}

pub fn mark_verifying(task: &mut DownloadTask) -> Result<(), DownloadStateError> {
    DownloadStateMachine::transition(task, DownloadStatus::Verifying)
}

pub fn mark_completed(task: &mut DownloadTask) -> Result<(), DownloadStateError> {
    info!(task_id = %task.id, "Download task marked as Completed");
    DownloadStateMachine::transition(task, DownloadStatus::Completed)
}

pub fn mark_failed(task: &mut DownloadTask) -> Result<(), DownloadStateError> {
    warn!(task_id = %task.id, "Download task marked as Failed");
    DownloadStateMachine::transition(task, DownloadStatus::Failed)
}

pub fn mark_cancelled(task: &mut DownloadTask) -> Result<(), DownloadStateError> {
    DownloadStateMachine::transition(task, DownloadStatus::Cancelled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    fn sample_task() -> DownloadTask {
        DownloadTask::new(
            Url::parse("https://example.com/file.bin").unwrap(),
            "file.bin".to_string(),
            "/downloads".to_string(),
        )
    }

    #[test]
    fn test_valid_lifecycle_transitions() {
        let mut task = sample_task();
        assert_eq!(task.status, DownloadStatus::Created);

        mark_queued(&mut task).unwrap();
        assert_eq!(task.status, DownloadStatus::Queued);

        mark_preparing(&mut task).unwrap();
        assert_eq!(task.status, DownloadStatus::Preparing);

        mark_downloading(&mut task).unwrap();
        assert_eq!(task.status, DownloadStatus::Downloading);

        mark_paused(&mut task).unwrap();
        assert_eq!(task.status, DownloadStatus::Paused);

        mark_downloading(&mut task).unwrap();
        assert_eq!(task.status, DownloadStatus::Downloading);

        mark_verifying(&mut task).unwrap();
        assert_eq!(task.status, DownloadStatus::Verifying);

        mark_completed(&mut task).unwrap();
        assert_eq!(task.status, DownloadStatus::Completed);
    }

    #[test]
    fn test_illegal_transition_rejection() {
        let mut task = sample_task();
        // Cannot jump directly from Created to Completed
        let err = mark_completed(&mut task).unwrap_err();
        assert_eq!(
            err,
            DownloadStateError::IllegalTransition {
                from: DownloadStatus::Created,
                to: DownloadStatus::Completed,
            }
        );
    }

    #[test]
    fn test_completed_is_terminal() {
        let mut task = sample_task();
        task.status = DownloadStatus::Completed;
        let err = mark_downloading(&mut task).unwrap_err();
        assert_eq!(
            err,
            DownloadStateError::IllegalTransition {
                from: DownloadStatus::Completed,
                to: DownloadStatus::Downloading,
            }
        );
    }
}
