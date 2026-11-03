//! Weighted adaptive multi-path scheduling algorithms and segment allocation.

use nexus_domain::{DownloadSegment, NetworkPath, PathId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, warn};

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

#[derive(Debug, Clone)]
struct PathRuntimeState {
    throughput_ema: f64,
    latency_ema: f64,
    success_count: u64,
    failure_count: u64,
    consecutive_failures: u32,
    active_connections: u32,
    is_healthy: bool,
    total_bytes: u64,
}

impl Default for PathRuntimeState {
    fn default() -> Self {
        Self {
            throughput_ema: 1_000_000.0,
            latency_ema: 50.0,
            success_count: 0,
            failure_count: 0,
            consecutive_failures: 0,
            active_connections: 0,
            is_healthy: true,
            total_bytes: 0,
        }
    }
}

/// Tracks real-time throughput, latency, failure rates, and health status per NetworkPath.
#[derive(Debug, Clone)]
pub struct PathMetricsTracker {
    states: Arc<RwLock<HashMap<PathId, PathRuntimeState>>>,
    unhealthy_threshold: u32,
}

impl Default for PathMetricsTracker {
    fn default() -> Self {
        Self::new(3)
    }
}

impl PathMetricsTracker {
    #[must_use]
    pub fn new(unhealthy_threshold: u32) -> Self {
        Self {
            states: Arc::new(RwLock::new(HashMap::new())),
            unhealthy_threshold,
        }
    }

    /// Registers a path if not already tracked.
    pub fn register_path(&self, path_id: PathId) {
        let mut states = self.states.write().unwrap();
        states.entry(path_id).or_default();
    }

    /// Records a successful chunk or segment transfer on a path.
    pub fn record_success(&self, path_id: PathId, bytes: u64, elapsed: Duration) {
        let mut states = self.states.write().unwrap();
        let state = states.entry(path_id).or_default();

        state.total_bytes = state.total_bytes.saturating_add(bytes);
        state.success_count = state.success_count.saturating_add(1);
        state.consecutive_failures = 0;
        state.is_healthy = true;

        let elapsed_secs = elapsed.as_secs_f64().max(0.001);
        let sample_bps = bytes as f64 / elapsed_secs;

        // Exponential moving average (alpha = 0.3)
        state.throughput_ema = 0.7 * state.throughput_ema + 0.3 * sample_bps;

        let sample_lat = elapsed.as_millis() as f64;
        state.latency_ema = 0.8 * state.latency_ema + 0.2 * sample_lat;
    }

    /// Records a network failure or timeout on a path.
    pub fn record_failure(&self, path_id: PathId) {
        let mut states = self.states.write().unwrap();
        let state = states.entry(path_id).or_default();

        state.failure_count = state.failure_count.saturating_add(1);
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);

        if state.consecutive_failures >= self.unhealthy_threshold {
            state.is_healthy = false;
            warn!(
                path_id = %path_id,
                consecutive_failures = state.consecutive_failures,
                "Path exceeded failure threshold; marked unhealthy"
            );
        }
    }

    /// Checks if a path is considered operational and healthy.
    #[must_use]
    pub fn is_healthy(&self, path_id: PathId) -> bool {
        let states = self.states.read().unwrap();
        states.get(&path_id).map(|s| s.is_healthy).unwrap_or(true)
    }

    /// Manually marks a path healthy.
    pub fn mark_healthy(&self, path_id: PathId) {
        let mut states = self.states.write().unwrap();
        let state = states.entry(path_id).or_default();
        state.is_healthy = true;
        state.consecutive_failures = 0;
    }

    /// Manually marks a path unhealthy.
    pub fn mark_unhealthy(&self, path_id: PathId) {
        let mut states = self.states.write().unwrap();
        let state = states.entry(path_id).or_default();
        state.is_healthy = false;
    }

    /// Increments the active connection count on a path.
    pub fn inc_active_connections(&self, path_id: PathId) {
        let mut states = self.states.write().unwrap();
        let state = states.entry(path_id).or_default();
        state.active_connections = state.active_connections.saturating_add(1);
    }

    /// Decrements the active connection count on a path.
    pub fn dec_active_connections(&self, path_id: PathId) {
        let mut states = self.states.write().unwrap();
        if let Some(state) = states.get_mut(&path_id) {
            state.active_connections = state.active_connections.saturating_sub(1);
        }
    }

    /// Retrieves snapshot metrics for a specific path.
    #[must_use]
    pub fn get_metrics(&self, path_id: PathId) -> PathMetrics {
        let states = self.states.read().unwrap();
        states
            .get(&path_id)
            .map(|s| {
                let total = (s.success_count + s.failure_count).max(1) as f64;
                PathMetrics {
                    throughput_bps: s.throughput_ema,
                    latency_ms: s.latency_ema,
                    failure_rate: s.failure_count as f64 / total,
                    active_connections: s.active_connections,
                }
            })
            .unwrap_or_default()
    }

    /// Retrieves snapshot metrics for all registered paths.
    #[must_use]
    pub fn get_all_metrics(&self) -> Vec<(PathId, PathMetrics)> {
        let states = self.states.read().unwrap();
        states
            .iter()
            .map(|(&id, s)| {
                let total = (s.success_count + s.failure_count).max(1) as f64;
                (
                    id,
                    PathMetrics {
                        throughput_bps: s.throughput_ema,
                        latency_ms: s.latency_ema,
                        failure_rate: s.failure_count as f64 / total,
                        active_connections: s.active_connections,
                    },
                )
            })
            .collect()
    }

    /// Returns the total bytes transferred across this path.
    #[must_use]
    pub fn total_bytes(&self, path_id: PathId) -> u64 {
        let states = self.states.read().unwrap();
        states.get(&path_id).map(|s| s.total_bytes).unwrap_or(0)
    }

    /// Calculates aggregate throughput across all currently healthy paths.
    #[must_use]
    pub fn aggregate_throughput_bps(&self) -> f64 {
        let states = self.states.read().unwrap();
        states
            .values()
            .filter(|s| s.is_healthy && s.active_connections > 0)
            .map(|s| s.throughput_ema)
            .sum()
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

    /// Evaluates healthy candidate paths using real-time metrics and limits from the tracker.
    pub fn select_best_candidate(
        &self,
        paths: &[NetworkPath],
        tracker: &PathMetricsTracker,
    ) -> Result<NetworkPath, SchedulerError> {
        let metrics_map = tracker.get_all_metrics();
        let eligible_paths: Vec<&NetworkPath> = paths
            .iter()
            .filter(|p| p.enabled && tracker.is_healthy(p.id))
            .collect();

        if eligible_paths.is_empty() {
            return Err(SchedulerError::NoAvailablePaths);
        }

        let max_tput = metrics_map
            .iter()
            .map(|(_, m)| m.throughput_bps)
            .fold(0.0f64, f64::max);

        let mut best_path = None;
        let mut highest_score = f64::NEG_INFINITY;

        for &path in &eligible_paths {
            let metrics = tracker.get_metrics(path.id);

            // Check connection limits
            if let Some(max_conn) = path.limits.max_connections {
                if metrics.active_connections >= max_conn {
                    continue;
                }
            }

            let score = self.score_path(&metrics, max_tput) + (path.priority as f64 * 0.05);
            if score > highest_score {
                highest_score = score;
                best_path = Some(path.clone());
            }
        }

        // If all eligible paths reached max_connections, pick the one with lowest active connections
        if best_path.is_none() {
            best_path = eligible_paths
                .iter()
                .min_by_key(|p| tracker.get_metrics(p.id).active_connections)
                .cloned()
                .cloned();
        }

        best_path.ok_or(SchedulerError::NoAvailablePaths)
    }

    /// Selects an alternative healthy candidate when a specific path has failed.
    pub fn select_alternative_path(
        &self,
        paths: &[NetworkPath],
        failed_path_id: PathId,
        tracker: &PathMetricsTracker,
    ) -> Result<NetworkPath, SchedulerError> {
        let remaining_paths: Vec<NetworkPath> = paths
            .iter()
            .filter(|p| p.id != failed_path_id && p.enabled && tracker.is_healthy(p.id))
            .cloned()
            .collect();

        if remaining_paths.is_empty() {
            return Err(SchedulerError::NoAvailablePaths);
        }

        self.select_best_candidate(&remaining_paths, tracker)
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

    #[test]
    fn test_tracker_health_and_failover_candidate_selection() {
        let scheduler = AdaptivePathScheduler::default();
        let tracker = PathMetricsTracker::new(2);

        let path1 = NetworkPath {
            id: PathId::new(),
            name: "primary-eth".to_string(),
            interface_id: InterfaceId::new(),
            proxy_id: None,
            priority: 2,
            enabled: true,
            limits: PathLimits::default(),
        };

        let path2 = NetworkPath {
            id: PathId::new(),
            name: "secondary-wifi".to_string(),
            interface_id: InterfaceId::new(),
            proxy_id: None,
            priority: 1,
            enabled: true,
            limits: PathLimits::default(),
        };

        let paths = vec![path1.clone(), path2.clone()];
        tracker.register_path(path1.id);
        tracker.register_path(path2.id);

        tracker.record_success(path1.id, 100_000, Duration::from_millis(50));
        tracker.record_success(path2.id, 50_000, Duration::from_millis(100));

        // Primary should be selected
        let candidate = scheduler.select_best_candidate(&paths, &tracker).unwrap();
        assert_eq!(candidate.id, path1.id);

        // Path 1 suffers consecutive failures
        tracker.record_failure(path1.id);
        tracker.record_failure(path1.id);
        assert!(!tracker.is_healthy(path1.id));

        // Selection should automatically fail over to Path 2
        let fallback = scheduler.select_best_candidate(&paths, &tracker).unwrap();
        assert_eq!(fallback.id, path2.id);

        // Alternative path lookup should return Path 2
        let alt = scheduler
            .select_alternative_path(&paths, path1.id, &tracker)
            .unwrap();
        assert_eq!(alt.id, path2.id);
    }
}
