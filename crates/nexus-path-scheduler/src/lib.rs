//! Weighted adaptive multi-path scheduling algorithms and segment allocation.

use nexus_domain::{DownloadSegment, NetworkPath, PathId};
use thiserror::Error;
use tracing::debug;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("No enabled and healthy network paths available for scheduling")]
    NoAvailablePaths,

    #[error("Path '{0}' is not registered with the scheduler")]
    PathNotTracked(String),
}

/// Real-time path metrics used for calculating scheduling scores.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PathMetrics {
    /// Moving average throughput in bytes per second.
    pub throughput_bps: f64,
    /// Round-trip latency in milliseconds.
    pub latency_ms: f64,
    /// Ratio of failed transfers to total transfers [0.0, 1.0].
    pub failure_rate: f64,
    /// Current number of active connections executing on this path.
    pub active_connections: u32,
}

impl Default for PathMetrics {
    fn default() -> Self {
        Self {
            throughput_bps: 1_000_000.0, // default 1 MB/s initial estimate
            latency_ms: 50.0,
            failure_rate: 0.0,
            active_connections: 0,
        }
    }
}

/// Configurable weights for calculating composite path scores.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SchedulerWeights {
    pub throughput_weight: f64,
    pub latency_weight: f64,
    pub failure_weight: f64,
    pub load_weight: f64,
}

impl Default for SchedulerWeights {
    fn default() -> Self {
        Self {
            throughput_weight: 0.50,
            latency_weight: 0.20,
            failure_weight: 0.20,
            load_weight: 0.10,
        }
    }
}

/// Evaluates and ranks network paths based on real-time metrics.
#[derive(Debug, Clone)]
pub struct AdaptivePathScheduler {
    weights: SchedulerWeights,
}

impl Default for AdaptivePathScheduler {
    fn default() -> Self {
        Self::new(SchedulerWeights::default())
    }
}

impl AdaptivePathScheduler {
    #[must_use]
    pub fn new(weights: SchedulerWeights) -> Self {
        Self { weights }
    }

    /// Computes a normalized score for a path based on its metrics.
    /// Higher score indicates a better path candidate.
    #[must_use]
    pub fn score_path(&self, metrics: &PathMetrics, max_observed_tput: f64) -> f64 {
        let norm_tput = if max_observed_tput > 0.0 {
            (metrics.throughput_bps / max_observed_tput).min(1.0)
        } else {
            0.5
        };

        // Latency: normalize (lower latency gives higher score)
        let norm_lat = (metrics.latency_ms / 500.0).min(1.0);

        let norm_fail = metrics.failure_rate.clamp(0.0, 1.0);
        let norm_load = (metrics.active_connections as f64 / 16.0).min(1.0);

        (self.weights.throughput_weight * norm_tput)
            - (self.weights.latency_weight * norm_lat)
            - (self.weights.failure_weight * norm_fail)
            - (self.weights.load_weight * norm_load)
    }

    /// Selects the best candidate path from a list of paths and corresponding metrics.
    pub fn select_best_path(
        &self,
        paths: &[NetworkPath],
        metrics_map: &[(PathId, PathMetrics)],
    ) -> Result<PathId, SchedulerError> {
        let enabled_paths: Vec<&NetworkPath> = paths.iter().filter(|p| p.enabled).collect();
        if enabled_paths.is_empty() {
            return Err(SchedulerError::NoAvailablePaths);
        }

        let max_tput = metrics_map
            .iter()
            .map(|(_, m)| m.throughput_bps)
            .fold(0.0f64, f64::max);

        let mut best_path_id = None;
        let mut highest_score = f64::NEG_INFINITY;

        for path in &enabled_paths {
            let metrics = metrics_map
                .iter()
                .find(|(id, _)| id == &path.id)
                .map(|(_, m)| *m)
                .unwrap_or_default();

            // Check connection limits
            if let Some(max_conn) = path.limits.max_connections {
                if metrics.active_connections >= max_conn {
                    continue; // Skip paths that have reached connection limit
                }
            }

            let score = self.score_path(&metrics, max_tput) + (path.priority as f64 * 0.05);

            if score > highest_score {
                highest_score = score;
                best_path_id = Some(path.id);
            }
        }

        best_path_id.ok_or(SchedulerError::NoAvailablePaths)
    }

    /// Assigns pending segments to available paths according to their relative performance.
    pub fn assign_segments(
        &self,
        segments: &mut [DownloadSegment],
        paths: &[NetworkPath],
        metrics_map: &[(PathId, PathMetrics)],
    ) -> Result<usize, SchedulerError> {
        let mut assigned_count = 0;

        for seg in segments.iter_mut() {
            if seg.assigned_path_id.is_none() {
                let chosen_path = self.select_best_path(paths, metrics_map)?;
                debug!(segment_id = %seg.id, path_id = %chosen_path, "Assigned segment to path");
                seg.assigned_path_id = Some(chosen_path);
                assigned_count += 1;
            }
        }

        Ok(assigned_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_domain::{InterfaceId, PathLimits};

    #[test]
    fn test_adaptive_path_selection_favors_high_throughput_low_latency() {
        let scheduler = AdaptivePathScheduler::default();

        let path1 = NetworkPath {
            id: PathId::new(),
            name: "fast-wifi".to_string(),
            interface_id: InterfaceId::new(),
            proxy_id: None,
            priority: 5,
            enabled: true,
            limits: PathLimits::default(),
        };

        let path2 = NetworkPath {
            id: PathId::new(),
            name: "slow-backup".to_string(),
            interface_id: InterfaceId::new(),
            proxy_id: None,
            priority: 1,
            enabled: true,
            limits: PathLimits::default(),
        };

        let paths = vec![path1.clone(), path2.clone()];
        let metrics = vec![
            (
                path1.id,
                PathMetrics {
                    throughput_bps: 10_000_000.0,
                    latency_ms: 15.0,
                    failure_rate: 0.0,
                    active_connections: 1,
                },
            ),
            (
                path2.id,
                PathMetrics {
                    throughput_bps: 1_000_000.0,
                    latency_ms: 120.0,
                    failure_rate: 0.1,
                    active_connections: 1,
                },
            ),
        ];

        let chosen = scheduler.select_best_path(&paths, &metrics).unwrap();
        assert_eq!(chosen, path1.id);
    }
}
