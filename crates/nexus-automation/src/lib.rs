//! Download queues, automation rules, and scheduled transfer policies.

use thiserror::Error;

pub mod clipboard;
pub mod queue;
pub mod rules;
pub mod scheduler;

pub use clipboard::ClipboardUrlExtractor;
pub use queue::QueueManager;
pub use rules::{
    AppliedRuleResult, AutomationRule, RuleActions, RuleConditions, RuleEngine, RuleId,
};
pub use scheduler::{SchedulePolicy, TimeWindow};

#[derive(Error, Debug)]
pub enum AutomationError {
    #[error("Queue is empty")]
    QueueEmpty,

    #[error("Task already in queue")]
    AlreadyQueued,

    #[error("Rule error: {0}")]
    Rule(String),
}
