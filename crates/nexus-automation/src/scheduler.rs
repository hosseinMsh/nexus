//! Scheduled download execution, recurring time windows, and off-peak transfer policies.

use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};

/// Recurring time window (e.g. off-peak hours overnight).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeWindow {
    pub start_hour: u8,
    pub start_minute: u8,
    pub end_hour: u8,
    pub end_minute: u8,
    /// Days of week when this window applies (0 = Sunday, 1 = Monday, ..., 6 = Saturday).
    /// If empty, applies to all days.
    pub days_of_week: Vec<u8>,
}

impl TimeWindow {
    /// Determines whether the specified timestamp falls within this time window.
    #[must_use]
    pub fn is_active_at(&self, dt: &DateTime<Utc>) -> bool {
        // 1. Check day of week
        if !self.days_of_week.is_empty() {
            let weekday = dt.weekday().num_days_from_sunday() as u8;
            if !self.days_of_week.contains(&weekday) {
                return false;
            }
        }

        let current_minutes = dt.hour() * 60 + dt.minute();
        let start_minutes = self.start_hour as u32 * 60 + self.start_minute as u32;
        let end_minutes = self.end_hour as u32 * 60 + self.end_minute as u32;

        if start_minutes <= end_minutes {
            // Standard daytime window: e.g. 02:00 to 06:00
            current_minutes >= start_minutes && current_minutes < end_minutes
        } else {
            // Overnight window spanning midnight: e.g. 23:00 to 07:00
            current_minutes >= start_minutes || current_minutes < end_minutes
        }
    }
}

/// Scheduling policy for automated transfers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchedulePolicy {
    pub scheduled_start: Option<DateTime<Utc>>,
    pub time_window: Option<TimeWindow>,
    pub auto_pause_outside_window: bool,
    pub speed_limit_in_window: Option<u64>,
    pub speed_limit_outside_window: Option<u64>,
}

impl SchedulePolicy {
    /// Evaluates whether the transfer should currently be actively running.
    #[must_use]
    pub fn should_be_running(&self, now: &DateTime<Utc>) -> bool {
        // If scheduled start is in the future, don't run yet
        if let Some(start) = self.scheduled_start {
            if *now < start {
                return false;
            }
        }

        // If time window is configured and auto-pause is enabled
        if let Some(ref window) = self.time_window {
            if self.auto_pause_outside_window && !window.is_active_at(now) {
                return false;
            }
        }

        true
    }

    /// Computes the effective bandwidth speed limit at the given timestamp.
    #[must_use]
    pub fn effective_speed_limit(&self, now: &DateTime<Utc>) -> Option<u64> {
        if let Some(ref window) = self.time_window {
            if window.is_active_at(now) {
                self.speed_limit_in_window
            } else {
                self.speed_limit_outside_window
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_daytime_window_evaluation() {
        let window = TimeWindow {
            start_hour: 2,
            start_minute: 0,
            end_hour: 6,
            end_minute: 30,
            days_of_week: vec![], // all days
        };

        // 03:15 is inside
        let t1 = Utc.with_ymd_and_hms(2026, 11, 3, 3, 15, 0).unwrap();
        assert!(window.is_active_at(&t1));

        // 01:45 is before
        let t2 = Utc.with_ymd_and_hms(2026, 11, 3, 1, 45, 0).unwrap();
        assert!(!window.is_active_at(&t2));

        // 07:00 is after
        let t3 = Utc.with_ymd_and_hms(2026, 11, 3, 7, 0, 0).unwrap();
        assert!(!window.is_active_at(&t3));
    }

    #[test]
    fn test_overnight_midnight_window_evaluation() {
        let window = TimeWindow {
            start_hour: 23,
            start_minute: 0,
            end_hour: 7,
            end_minute: 0,
            days_of_week: vec![],
        };

        // 23:30 is inside
        let t1 = Utc.with_ymd_and_hms(2026, 11, 3, 23, 30, 0).unwrap();
        assert!(window.is_active_at(&t1));

        // 04:00 is inside
        let t2 = Utc.with_ymd_and_hms(2026, 11, 4, 4, 0, 0).unwrap();
        assert!(window.is_active_at(&t2));

        // 12:00 is outside
        let t3 = Utc.with_ymd_and_hms(2026, 11, 4, 12, 0, 0).unwrap();
        assert!(!window.is_active_at(&t3));
    }
}
