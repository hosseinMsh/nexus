//! Parallel multi-connection worker pool with lock-free atomic segment tracking,
//! dynamic work stealing, exponential backoff retries, and bounded memory streaming.

use bytes::Bytes;
use chrono::Utc;
use nexus_domain::{DownloadSegment, NetworkPath, SegmentId, SegmentStatus, TaskId};
use nexus_events::{EventBus, SystemEvent};
use nexus_path_scheduler::{AdaptivePathScheduler, PathMetricsTracker};
use nexus_segments::validate_segments;
use nexus_storage::SegmentRepository;
use nexus_transport::TransportClient;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::engine::EngineError;
use crate::writer::AsyncFileWriterHandle;

/// Minimum segment size eligible for work stealing (64 KB).
pub const MIN_WORK_STEAL_BYTES: u64 = 64 * 1024;

const STATUS_PENDING: u8 = 0;
const STATUS_IN_PROGRESS: u8 = 1;
const STATUS_COMPLETED: u8 = 2;
const STATUS_FAILED: u8 = 3;
const STATUS_CANCELLED: u8 = 4;

/// Lock-free thread-safe representation of a segment during parallel execution.
#[derive(Clone, Debug)]
pub struct AtomicSegment {
    pub id: SegmentId,
    pub task_id: TaskId,
    pub start_offset: u64,
    pub end_offset: Arc<AtomicU64>,
    pub downloaded_bytes: Arc<AtomicU64>,
    pub status: Arc<AtomicU8>,
    pub retry_count: Arc<AtomicU32>,
}

impl AtomicSegment {
    pub fn from_domain(seg: &DownloadSegment) -> Self {
        let st = match seg.status {
            SegmentStatus::Pending => STATUS_PENDING,
            SegmentStatus::InProgress => STATUS_IN_PROGRESS,
            SegmentStatus::Completed => STATUS_COMPLETED,
            SegmentStatus::Failed => STATUS_FAILED,
            SegmentStatus::Cancelled => STATUS_CANCELLED,
        };
        Self {
            id: seg.id,
            task_id: seg.download_id,
            start_offset: seg.start_offset,
            end_offset: Arc::new(AtomicU64::new(seg.end_offset)),
            downloaded_bytes: Arc::new(AtomicU64::new(seg.downloaded_bytes)),
            status: Arc::new(AtomicU8::new(st)),
            retry_count: Arc::new(AtomicU32::new(seg.retry_count)),
        }
    }

    pub fn to_domain(&self) -> DownloadSegment {
        let st = match self.status.load(Ordering::Acquire) {
            STATUS_IN_PROGRESS => SegmentStatus::InProgress,
            STATUS_COMPLETED => SegmentStatus::Completed,
            STATUS_FAILED => SegmentStatus::Failed,
            STATUS_CANCELLED => SegmentStatus::Cancelled,
            _ => SegmentStatus::Pending,
        };
        let mut d = DownloadSegment::new(
            self.task_id,
            self.start_offset,
            self.end_offset.load(Ordering::Acquire),
        );
        d.id = self.id;
        d.downloaded_bytes = self.downloaded_bytes.load(Ordering::Acquire);
        d.status = st;
        d.retry_count = self.retry_count.load(Ordering::Acquire);
        d
    }
}

/// Coordinator for parallel download workers across multiple connections and network paths.
pub struct WorkerPool {
    task_id: TaskId,
    task_url: url::Url,
    total_size: u64,
    retry_limit: u32,
    max_connections: usize,
    writer: AsyncFileWriterHandle,
    transport: Arc<dyn TransportClient>,
    segment_repo: Arc<dyn SegmentRepository>,
    event_bus: Arc<EventBus>,
    cancel_token: CancellationToken,
    scheduler: Arc<AdaptivePathScheduler>,
    tracker: Arc<PathMetricsTracker>,
}

impl WorkerPool {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: TaskId,
        task_url: url::Url,
        total_size: u64,
        retry_limit: u32,
        max_connections: usize,
        writer: AsyncFileWriterHandle,
        transport: Arc<dyn TransportClient>,
        segment_repo: Arc<dyn SegmentRepository>,
        event_bus: Arc<EventBus>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            task_id,
            task_url,
            total_size,
            retry_limit,
            max_connections: max_connections.clamp(1, 32),
            writer,
            transport,
            segment_repo,
            event_bus,
            cancel_token,
            scheduler: Arc::new(AdaptivePathScheduler::default()),
            tracker: Arc::new(PathMetricsTracker::default()),
        }
    }

    /// Configures a custom scheduler and metrics tracker.
    #[must_use]
    pub fn with_scheduler(
        mut self,
        scheduler: Arc<AdaptivePathScheduler>,
        tracker: Arc<PathMetricsTracker>,
    ) -> Self {
        self.scheduler = scheduler;
        self.tracker = tracker;
        self
    }

    /// Returns a reference to the active path metrics tracker.
    #[must_use]
    pub fn tracker(&self) -> Arc<PathMetricsTracker> {
        Arc::clone(&self.tracker)
    }

    /// Executes parallel multi-connection downloading across the provided segments and network paths.
    pub async fn execute(
        &self,
        initial_segments: Vec<DownloadSegment>,
        paths: &[NetworkPath],
        total_downloaded: Arc<AtomicU64>,
    ) -> Result<Vec<DownloadSegment>, EngineError> {
        if paths.is_empty() {
            return Err(EngineError::Transport(
                nexus_transport::TransportError::Protocol("No network paths provided".to_string()),
            ));
        }

        for path in paths {
            self.tracker.register_path(path.id);
        }

        let paths_arc = Arc::new(paths.to_vec());
        let atomic_segments: Vec<AtomicSegment> = initial_segments
            .iter()
            .map(AtomicSegment::from_domain)
            .collect();
        let segments_state = Arc::new(Mutex::new(atomic_segments));
        let notify = Arc::new(Notify::new());

        let num_workers = self.max_connections;
        debug!(
            task_id = %self.task_id,
            concurrency = num_workers,
            num_paths = paths.len(),
            "Spawning parallel download worker pool across paths"
        );

        let mut join_set = tokio::task::JoinSet::new();

        for worker_id in 0..num_workers {
            let task_id = self.task_id;
            let task_url = self.task_url.clone();
            let retry_limit = self.retry_limit;
            let initial_path = paths[worker_id % paths.len()].clone();
            let all_paths = Arc::clone(&paths_arc);
            let scheduler_clone = Arc::clone(&self.scheduler);
            let tracker_clone = Arc::clone(&self.tracker);
            let segments_ref = Arc::clone(&segments_state);
            let notify_ref = Arc::clone(&notify);
            let writer_clone = self.writer.clone();
            let transport_clone = Arc::clone(&self.transport);
            let segment_repo_clone = Arc::clone(&self.segment_repo);
            let event_bus_clone = Arc::clone(&self.event_bus);
            let cancel_clone = self.cancel_token.clone();
            let total_downloaded_clone = Arc::clone(&total_downloaded);

            join_set.spawn(async move {
                Self::run_worker(
                    worker_id,
                    task_id,
                    task_url,
                    retry_limit,
                    initial_path,
                    all_paths,
                    scheduler_clone,
                    tracker_clone,
                    segments_ref,
                    notify_ref,
                    writer_clone,
                    transport_clone,
                    segment_repo_clone,
                    event_bus_clone,
                    cancel_clone,
                    total_downloaded_clone,
                )
                .await
            });
        }

        // Wait for all workers to finish or error
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(())) => {
                    // Worker exited cleanly
                }
                Ok(Err(e)) => {
                    error!(task_id = %self.task_id, error = ?e, "Worker failed; aborting pool");
                    join_set.abort_all();
                    return Err(e);
                }
                Err(join_err) => {
                    error!(task_id = %self.task_id, error = %join_err, "Worker task panicked or aborted");
                    return Err(EngineError::CancelledOrPaused);
                }
            }
        }

        if self.cancel_token.is_cancelled() {
            return Err(EngineError::CancelledOrPaused);
        }

        // Final verification of segments coverage
        let final_atomics = segments_state.lock().await;
        let final_segments: Vec<DownloadSegment> =
            final_atomics.iter().map(AtomicSegment::to_domain).collect();

        validate_segments(&final_segments, self.total_size).map_err(|e| {
            EngineError::Storage(nexus_storage::StorageError::Database(e.to_string()))
        })?;

        Ok(final_segments)
    }

    /// Single-path backward-compatibility execution helper.
    pub async fn execute_single_path(
        &self,
        initial_segments: Vec<DownloadSegment>,
        path: &NetworkPath,
        total_downloaded: Arc<AtomicU64>,
    ) -> Result<Vec<DownloadSegment>, EngineError> {
        self.execute(
            initial_segments,
            std::slice::from_ref(path),
            total_downloaded,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_worker(
        worker_id: usize,
        task_id: TaskId,
        task_url: url::Url,
        retry_limit: u32,
        mut current_path: NetworkPath,
        all_paths: Arc<Vec<NetworkPath>>,
        scheduler: Arc<AdaptivePathScheduler>,
        tracker: Arc<PathMetricsTracker>,
        segments_state: Arc<Mutex<Vec<AtomicSegment>>>,
        notify: Arc<Notify>,
        writer: AsyncFileWriterHandle,
        transport: Arc<dyn TransportClient>,
        segment_repo: Arc<dyn SegmentRepository>,
        event_bus: Arc<EventBus>,
        cancel_token: CancellationToken,
        total_downloaded: Arc<AtomicU64>,
    ) -> Result<(), EngineError> {
        loop {
            if cancel_token.is_cancelled() {
                return Ok(());
            }

            // Step 1: Claim or steal a segment
            let segment_opt = {
                let mut segments = segments_state.lock().await;

                // 1a. Try to find an unstarted / pending segment
                if let Some(seg) = segments
                    .iter()
                    .find(|s| s.status.load(Ordering::Acquire) == STATUS_PENDING)
                {
                    seg.status.store(STATUS_IN_PROGRESS, Ordering::Release);
                    let claimed = seg.clone();
                    let domain = claimed.to_domain();
                    let _ = segment_repo
                        .save_segments(std::slice::from_ref(&domain))
                        .await;
                    Some(claimed)
                } else if segments
                    .iter()
                    .all(|s| s.status.load(Ordering::Acquire) == STATUS_COMPLETED)
                {
                    // All segments finished
                    None
                } else {
                    // 1b. Work stealing: find an in-progress segment with ample remaining bytes
                    let mut stolen = None;
                    for active in segments.iter() {
                        if active.status.load(Ordering::Acquire) == STATUS_IN_PROGRESS {
                            let cur_dl = active.downloaded_bytes.load(Ordering::Acquire);
                            let cur_end = active.end_offset.load(Ordering::Acquire);
                            let uncommitted_start = active.start_offset + cur_dl;

                            if cur_end > uncommitted_start {
                                let remaining = cur_end - uncommitted_start + 1;
                                if remaining >= MIN_WORK_STEAL_BYTES * 2 {
                                    let half = remaining / 2;
                                    let split_point = uncommitted_start + half;

                                    // Atomically shorten active end_offset to split_point - 1
                                    active.end_offset.store(split_point - 1, Ordering::Release);

                                    let stolen_atomic = AtomicSegment {
                                        id: SegmentId::new(),
                                        task_id,
                                        start_offset: split_point,
                                        end_offset: Arc::new(AtomicU64::new(cur_end)),
                                        downloaded_bytes: Arc::new(AtomicU64::new(0)),
                                        status: Arc::new(AtomicU8::new(STATUS_IN_PROGRESS)),
                                        retry_count: Arc::new(AtomicU32::new(0)),
                                    };

                                    info!(
                                        worker_id = worker_id,
                                        original_seg = %active.id,
                                        stolen_seg = %stolen_atomic.id,
                                        split_point = split_point,
                                        "Work stealing successfully split segment"
                                    );

                                    // Persist active segment update
                                    let active_domain = active.to_domain();
                                    let _ = segment_repo
                                        .save_segments(std::slice::from_ref(&active_domain))
                                        .await;

                                    // Persist new stolen segment
                                    let stolen_domain = stolen_atomic.to_domain();
                                    let _ = segment_repo
                                        .save_segments(std::slice::from_ref(&stolen_domain))
                                        .await;

                                    stolen = Some(stolen_atomic);
                                    break;
                                }
                            }
                        }
                    }

                    if let Some(ref st) = stolen {
                        segments.push(st.clone());
                    }
                    stolen
                }
            };

            match segment_opt {
                Some(seg) => {
                    // Download the segment
                    Self::download_segment(
                        worker_id,
                        task_id,
                        &task_url,
                        retry_limit,
                        &mut current_path,
                        &all_paths,
                        &scheduler,
                        &tracker,
                        &seg,
                        &notify,
                        &writer,
                        &transport,
                        &segment_repo,
                        &event_bus,
                        &cancel_token,
                        &total_downloaded,
                    )
                    .await?;
                }
                None => {
                    // Check if everything is truly finished
                    let all_done = {
                        let segs = segments_state.lock().await;
                        segs.iter()
                            .all(|s| s.status.load(Ordering::Acquire) == STATUS_COMPLETED)
                    };

                    if all_done {
                        notify.notify_waiters();
                        return Ok(());
                    }

                    // Otherwise, wait for active workers to progress or finish
                    tokio::select! {
                        () = notify.notified() => {}
                        () = tokio::time::sleep(Duration::from_millis(50)) => {}
                        () = cancel_token.cancelled() => return Ok(()),
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn download_segment(
        worker_id: usize,
        task_id: TaskId,
        task_url: &url::Url,
        retry_limit: u32,
        current_path: &mut NetworkPath,
        all_paths: &[NetworkPath],
        scheduler: &AdaptivePathScheduler,
        tracker: &PathMetricsTracker,
        seg: &AtomicSegment,
        notify: &Arc<Notify>,
        writer: &AsyncFileWriterHandle,
        transport: &Arc<dyn TransportClient>,
        segment_repo: &Arc<dyn SegmentRepository>,
        event_bus: &Arc<EventBus>,
        cancel_token: &CancellationToken,
        total_downloaded: &Arc<AtomicU64>,
    ) -> Result<(), EngineError> {
        let mut retries = 0;

        while retries <= retry_limit {
            if cancel_token.is_cancelled() {
                return Ok(());
            }

            let start = seg.start_offset + seg.downloaded_bytes.load(Ordering::Acquire);
            let end = seg.end_offset.load(Ordering::Acquire);

            if start > end {
                seg.status.store(STATUS_COMPLETED, Ordering::Release);
                let domain = seg.to_domain();
                let _ = segment_repo
                    .save_segments(std::slice::from_ref(&domain))
                    .await;
                notify.notify_waiters();
                return Ok(());
            }

            // Ensure current path is healthy; if not, attempt failover
            if !tracker.is_healthy(current_path.id) {
                if let Ok(alt_path) =
                    scheduler.select_alternative_path(all_paths, current_path.id, tracker)
                {
                    warn!(
                        worker_id = worker_id,
                        from = %current_path.id,
                        to = %alt_path.id,
                        "Pre-emptively failing over to healthy path"
                    );
                    *current_path = alt_path;
                }
            }

            debug!(
                worker_id = worker_id,
                path_id = %current_path.id,
                segment_id = %seg.id,
                start = start,
                end = end,
                attempt = retries + 1,
                "Worker fetching range"
            );

            tracker.inc_active_connections(current_path.id);
            let start_fetch = std::time::Instant::now();

            let fetch_res = transport
                .fetch_range(current_path, task_url, start, end)
                .await;

            match fetch_res {
                Ok(mut stream) => {
                    let mut buffer = [0u8; 64 * 1024]; // 64 KB buffer
                    let mut stream_failed = false;

                    loop {
                        if cancel_token.is_cancelled() {
                            tracker.dec_active_connections(current_path.id);
                            return Ok(());
                        }

                        let cur_end = seg.end_offset.load(Ordering::Acquire);
                        let cur_dl = seg.downloaded_bytes.load(Ordering::Acquire);
                        let current_offset = seg.start_offset + cur_dl;

                        if current_offset > cur_end {
                            break;
                        }

                        let max_read_this_turn =
                            ((cur_end - current_offset + 1) as usize).min(buffer.len());

                        match stream.read(&mut buffer[..max_read_this_turn]).await {
                            Ok(0) => break, // EOF for this range
                            Ok(n) => {
                                let chunk = Bytes::copy_from_slice(&buffer[..n]);
                                if let Err(e) = writer.write(current_offset, chunk).await {
                                    tracker.dec_active_connections(current_path.id);
                                    error!(error = %e, "Disk writer error in worker");
                                    return Err(EngineError::File(crate::file::FileError::Io(
                                        match e {
                                            crate::writer::WriterError::Io(io_err) => io_err,
                                            crate::writer::WriterError::ChannelClosed => {
                                                std::io::Error::other("Writer channel closed")
                                            }
                                        },
                                    )));
                                }

                                seg.downloaded_bytes.fetch_add(n as u64, Ordering::Release);
                                total_downloaded.fetch_add(n as u64, Ordering::Release);
                                tracker.record_success(
                                    current_path.id,
                                    n as u64,
                                    start_fetch.elapsed(),
                                );

                                let new_dl = seg.downloaded_bytes.load(Ordering::Acquire);
                                let _ = segment_repo.update_segment_progress(&seg.id, new_dl).await;

                                let total_seg_bytes = seg
                                    .end_offset
                                    .load(Ordering::Acquire)
                                    .saturating_sub(seg.start_offset)
                                    .saturating_add(1);

                                event_bus.publish(SystemEvent::SegmentProgress {
                                    task_id,
                                    segment_id: seg.id,
                                    downloaded_bytes: new_dl,
                                    total_bytes: total_seg_bytes,
                                    timestamp: Utc::now(),
                                });

                                if seg.start_offset + new_dl
                                    > seg.end_offset.load(Ordering::Acquire)
                                {
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!(
                                    worker_id = worker_id,
                                    path_id = %current_path.id,
                                    error = %e,
                                    "Stream read error in worker; triggering failover / retry"
                                );
                                stream_failed = true;
                                tracker.record_failure(current_path.id);
                                break;
                            }
                        }
                    }

                    tracker.dec_active_connections(current_path.id);

                    let final_dl = seg.downloaded_bytes.load(Ordering::Acquire);
                    let final_end = seg.end_offset.load(Ordering::Acquire);
                    let required_bytes =
                        final_end.saturating_sub(seg.start_offset).saturating_add(1);

                    if !stream_failed && final_dl >= required_bytes {
                        seg.status.store(STATUS_COMPLETED, Ordering::Release);
                        let domain = seg.to_domain();
                        let _ = segment_repo
                            .save_segments(std::slice::from_ref(&domain))
                            .await;
                        notify.notify_waiters();
                        return Ok(());
                    }

                    // If stream failed, attempt path failover before next retry
                    if stream_failed {
                        if let Ok(alt_path) =
                            scheduler.select_alternative_path(all_paths, current_path.id, tracker)
                        {
                            warn!(
                                worker_id = worker_id,
                                from = %current_path.id,
                                to = %alt_path.id,
                                "Failover: stream failed, switching to healthy alternative path"
                            );
                            *current_path = alt_path;
                        }
                    }
                }
                Err(e) => {
                    tracker.dec_active_connections(current_path.id);
                    tracker.record_failure(current_path.id);

                    warn!(
                        worker_id = worker_id,
                        path_id = %current_path.id,
                        error = ?e,
                        attempt = retries + 1,
                        "Worker transport fetch range error; checking for path failover"
                    );

                    if let Ok(alt_path) =
                        scheduler.select_alternative_path(all_paths, current_path.id, tracker)
                    {
                        warn!(
                            worker_id = worker_id,
                            from = %current_path.id,
                            to = %alt_path.id,
                            "Failover: fetch failed, switching to healthy alternative path"
                        );
                        *current_path = alt_path;
                    }
                }
            }

            retries += 1;
            seg.retry_count.store(retries, Ordering::Release);

            if retries <= retry_limit {
                let backoff_ms = (50u64 * (1 << retries)).min(1500);
                debug!(
                    worker_id = worker_id,
                    backoff_ms = backoff_ms,
                    attempt = retries,
                    "Worker backing off before retry"
                );
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                    () = cancel_token.cancelled() => return Ok(()),
                }
            }
        }

        seg.status.store(STATUS_FAILED, Ordering::Release);
        Err(EngineError::MaxRetriesExceeded {
            segment_id: seg.id,
            retries: retry_limit,
        })
    }
}
