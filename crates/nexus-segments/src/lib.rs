//! Segment partitioning, boundary verification, and non-overlapping byte commitment logic.

use nexus_domain::{DownloadSegment, SegmentId, TaskId};
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum SegmentError {
    #[error("File size is zero")]
    ZeroFileSize,

    #[error("Segment count must be at least 1, requested: {0}")]
    InvalidSegmentCount(usize),

    #[error("Overlapping segments detected: [{start1}, {end1}] overlaps with [{start2}, {end2}]")]
    OverlappingSegments {
        start1: u64,
        end1: u64,
        start2: u64,
        end2: u64,
    },

    #[error("Missing byte gap detected between offset {expected} and offset {actual}")]
    MissingByteGap { expected: u64, actual: u64 },

    #[error("Total segments do not cover file size: covered {covered}, expected {expected}")]
    IncompleteCoverage { covered: u64, expected: u64 },

    #[error("Cannot split segment with remaining bytes ({remaining}) smaller than minimum threshold ({min_bytes})")]
    SegmentTooSmallToSplit { remaining: u64, min_bytes: u64 },
}

/// Partitions a total file size $N$ into $K$ non-overlapping continuous byte segments.
///
/// Guaranteed properties:
/// 1. $S_0 = 0$
/// 2. $E_{K-1} = N - 1$
/// 3. For all $0 \le i < K - 1$, $S_{i+1} = E_i + 1$
/// 4. $\sum_{i=0}^{K-1} (E_i - S_i + 1) = N$
pub fn partition_segments(
    task_id: TaskId,
    total_size: u64,
    segment_count: usize,
) -> Result<Vec<DownloadSegment>, SegmentError> {
    if total_size == 0 {
        return Err(SegmentError::ZeroFileSize);
    }
    if segment_count == 0 {
        return Err(SegmentError::InvalidSegmentCount(0));
    }

    // Limit segment count to total_size so no segment has 0 bytes.
    let actual_count = segment_count.min(total_size as usize);
    let chunk_size = total_size / (actual_count as u64);
    let remainder = total_size % (actual_count as u64);

    let mut segments = Vec::with_capacity(actual_count);
    let mut current_offset = 0u64;

    for i in 0..actual_count {
        // Distribute remainder bytes across the first few segments
        let extra = if (i as u64) < remainder { 1 } else { 0 };
        let current_chunk = chunk_size + extra;
        let end_offset = current_offset + current_chunk - 1;

        segments.push(DownloadSegment::new(task_id, current_offset, end_offset));
        current_offset = end_offset + 1;
    }

    // Validate the generated segments
    validate_segments(&segments, total_size)?;

    Ok(segments)
}

/// Validates that a collection of segments forms a strictly continuous, non-overlapping coverage
/// of the range `[0, total_size - 1]`.
pub fn validate_segments(
    segments: &[DownloadSegment],
    total_size: u64,
) -> Result<(), SegmentError> {
    if segments.is_empty() {
        return Err(SegmentError::IncompleteCoverage {
            covered: 0,
            expected: total_size,
        });
    }

    let mut sorted = segments.to_vec();
    sorted.sort_by_key(|s| s.start_offset);

    let mut expected_offset = 0u64;

    for i in 0..sorted.len() {
        let seg = &sorted[i];

        if seg.start_offset < expected_offset {
            let prev = &sorted[i - 1];
            return Err(SegmentError::OverlappingSegments {
                start1: prev.start_offset,
                end1: prev.end_offset,
                start2: seg.start_offset,
                end2: seg.end_offset,
            });
        }

        if seg.start_offset > expected_offset {
            return Err(SegmentError::MissingByteGap {
                expected: expected_offset,
                actual: seg.start_offset,
            });
        }

        expected_offset = seg.end_offset.saturating_add(1);
    }

    if expected_offset != total_size {
        return Err(SegmentError::IncompleteCoverage {
            covered: expected_offset,
            expected: total_size,
        });
    }

    Ok(())
}

/// Splits an active segment into two parts to enable work stealing by a faster network path.
///
/// Returns the updated original segment (truncated to current uncommitted position)
/// and the newly created second segment covering the remaining interval.
pub fn split_segment_for_work_stealing(
    segment: &mut DownloadSegment,
    min_split_bytes: u64,
) -> Result<DownloadSegment, SegmentError> {
    let uncommitted_start = segment.start_offset + segment.downloaded_bytes;
    let remaining = segment.end_offset.saturating_sub(uncommitted_start) + 1;

    if remaining < min_split_bytes * 2 {
        return Err(SegmentError::SegmentTooSmallToSplit {
            remaining,
            min_bytes: min_split_bytes * 2,
        });
    }

    let half = remaining / 2;
    let split_point = uncommitted_start + half;

    let original_end = segment.end_offset;
    // Truncate original segment to split_point - 1
    segment.end_offset = split_point - 1;

    // Create the new segment covering [split_point, original_end]
    let mut new_segment = DownloadSegment::new(segment.download_id, split_point, original_end);
    new_segment.id = SegmentId::new();

    Ok(new_segment)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_equal_distribution() {
        let task_id = TaskId::new();
        let segments = partition_segments(task_id, 1000, 4).unwrap();
        assert_eq!(segments.len(), 4);
        assert_eq!((segments[0].start_offset, segments[0].end_offset), (0, 249));
        assert_eq!(
            (segments[1].start_offset, segments[1].end_offset),
            (250, 499)
        );
        assert_eq!(
            (segments[2].start_offset, segments[2].end_offset),
            (500, 749)
        );
        assert_eq!(
            (segments[3].start_offset, segments[3].end_offset),
            (750, 999)
        );
    }

    #[test]
    fn test_partition_with_remainder() {
        let task_id = TaskId::new();
        let total = 10;
        let segments = partition_segments(task_id, total, 3).unwrap();
        assert_eq!(segments.len(), 3);
        // 10 / 3 = 3 with remainder 1 -> segments sizes: 4, 3, 3
        assert_eq!(segments[0].total_bytes(), 4);
        assert_eq!(segments[1].total_bytes(), 3);
        assert_eq!(segments[2].total_bytes(), 3);
        assert_eq!(segments[0].start_offset, 0);
        assert_eq!(segments[2].end_offset, 9);
    }

    #[test]
    fn test_work_stealing_split() {
        let task_id = TaskId::new();
        let mut seg = DownloadSegment::new(task_id, 0, 999);
        seg.downloaded_bytes = 200; // 0..199 downloaded, 200..999 remaining (800 bytes)

        let stolen = split_segment_for_work_stealing(&mut seg, 100).unwrap();
        assert_eq!(seg.start_offset, 0);
        assert_eq!(seg.end_offset, 599); // 200 + 400 - 1 = 599
        assert_eq!(stolen.start_offset, 600);
        assert_eq!(stolen.end_offset, 999);
        assert_eq!(seg.total_bytes() + stolen.total_bytes(), 1000);
    }
}
