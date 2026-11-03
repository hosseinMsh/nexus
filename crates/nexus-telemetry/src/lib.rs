//! Structured telemetry, logging initialization, and transfer metrics tracking.

use thiserror::Error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Error, Debug)]
pub enum TelemetryError {
    #[error("Failed to initialize telemetry subscriber: {0}")]
    InitError(String),
}

/// Initializes the global tracing subscriber for structured terminal output.
pub fn init_telemetry(default_level: &str) -> Result<(), TelemetryError> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_thread_ids(false)
        .with_level(true);

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .try_init()
        .map_err(|e| TelemetryError::InitError(e.to_string()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_init_does_not_panic() {
        // May fail if already initialized in another test, so ignore error.
        let _ = init_telemetry("info");
    }
}
