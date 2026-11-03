//! SQLite database repository implementation with SQLx migrations and WAL mode.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use nexus_core::ByteSize;
use nexus_domain::{
    DnsPolicy, DownloadPolicy, DownloadSegment, DownloadStatus, DownloadTask, InterfaceId,
    NetworkPath, PathId, PathLimits, ProxyId, ProxyProfile, ProxyType, SegmentId, SegmentStatus,
    TaskId,
};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use tracing::{debug, info};
use url::Url;

use crate::{PathRepository, ProxyRepository, SegmentRepository, StorageError, TaskRepository};

/// Production SQLite storage provider backed by SQLx connection pool.
#[derive(Clone, Debug)]
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// Connects to a SQLite database file at `database_url` (e.g. `sqlite://nexus.db`).
    /// Configures WAL mode, normal synchronous durability, and foreign keys.
    pub async fn connect(database_url: &str) -> Result<Self, StorageError> {
        let connection_options = SqliteConnectOptions::from_str(database_url)
            .map_err(|e| StorageError::Database(format!("Invalid connection string: {e}")))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .foreign_keys(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect_with(connection_options)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to connect to SQLite: {e}")))?;

        let storage = Self { pool };
        storage.run_migrations().await?;
        info!("SQLite database connected and migrations applied");
        Ok(storage)
    }

    /// Creates an isolated in-memory SQLite database instance for fast testing.
    pub async fn connect_in_memory() -> Result<Self, StorageError> {
        let connection_options = SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(|e| StorageError::Database(format!("Invalid connection string: {e}")))?
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(connection_options)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to connect to in-memory SQLite: {e}"))
            })?;

        let storage = Self { pool };
        storage.run_migrations().await?;
        Ok(storage)
    }

    /// Executes embedded schema migrations.
    pub async fn run_migrations(&self) -> Result<(), StorageError> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Migration failed: {e}")))?;
        Ok(())
    }

    /// Returns a reference to the internal connection pool.
    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[async_trait]
impl TaskRepository for SqliteStorage {
    async fn save_task(&self, task: &DownloadTask) -> Result<(), StorageError> {
        let query = r#"
            INSERT INTO downloads (
                id, url, filename, destination_dir, total_size, downloaded_size, status,
                etag, last_modified, category, policy_max_connections, policy_max_speed_bps,
                policy_retry_limit, policy_auto_resume, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                url = excluded.url,
                filename = excluded.filename,
                destination_dir = excluded.destination_dir,
                total_size = excluded.total_size,
                downloaded_size = excluded.downloaded_size,
                status = excluded.status,
                etag = excluded.etag,
                last_modified = excluded.last_modified,
                category = excluded.category,
                policy_max_connections = excluded.policy_max_connections,
                policy_max_speed_bps = excluded.policy_max_speed_bps,
                policy_retry_limit = excluded.policy_retry_limit,
                policy_auto_resume = excluded.policy_auto_resume,
                updated_at = excluded.updated_at
        "#;

        let total_size_val = task.total_size.map(|s| s.bytes() as i64);
        let downloaded_size_val = task.downloaded_size.bytes() as i64;
        let speed_bps_val = task.policy.max_speed_bps.map(|s| s as i64);
        let auto_resume_val = if task.policy.auto_resume { 1i64 } else { 0i64 };
        let status_str = task.status.to_string();
        let created_str = task.created_at.to_rfc3339();
        let updated_str = task.updated_at.to_rfc3339();

        sqlx::query(query)
            .bind(task.id.to_string())
            .bind(task.url.as_str())
            .bind(&task.filename)
            .bind(&task.destination_dir)
            .bind(total_size_val)
            .bind(downloaded_size_val)
            .bind(status_str)
            .bind(&task.etag)
            .bind(&task.last_modified)
            .bind(&task.category)
            .bind(task.policy.max_connections as i64)
            .bind(speed_bps_val)
            .bind(task.policy.retry_limit as i64)
            .bind(auto_resume_val)
            .bind(created_str)
            .bind(updated_str)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to save task {}: {e}", task.id)))?;

        debug!(task_id = %task.id, "Persisted task to SQLite");
        Ok(())
    }

    async fn get_task(&self, id: &TaskId) -> Result<Option<DownloadTask>, StorageError> {
        let query = "SELECT * FROM downloads WHERE id = ?";
        let row_opt = sqlx::query(query)
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to fetch task {id}: {e}")))?;

        match row_opt {
            Some(row) => Ok(Some(map_row_to_task(&row)?)),
            None => Ok(None),
        }
    }

    async fn list_tasks(
        &self,
        status_filter: Option<DownloadStatus>,
    ) -> Result<Vec<DownloadTask>, StorageError> {
        let (query, status_str) = match status_filter {
            Some(status) => (
                "SELECT * FROM downloads WHERE status = ? ORDER BY created_at DESC",
                Some(status.to_string()),
            ),
            None => ("SELECT * FROM downloads ORDER BY created_at DESC", None),
        };

        let mut q = sqlx::query(query);
        if let Some(s) = status_str {
            q = q.bind(s);
        }

        let rows = q
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to list tasks: {e}")))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            tasks.push(map_row_to_task(&row)?);
        }

        Ok(tasks)
    }

    async fn update_status(&self, id: &TaskId, status: DownloadStatus) -> Result<(), StorageError> {
        let now_str = Utc::now().to_rfc3339();
        let query = "UPDATE downloads SET status = ?, updated_at = ? WHERE id = ?";

        let result = sqlx::query(query)
            .bind(status.to_string())
            .bind(now_str)
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to update status for {id}: {e}"))
            })?;

        if result.rows_affected() == 0 {
            return Err(StorageError::NotFound(id.to_string()));
        }

        Ok(())
    }

    async fn delete_task(&self, id: &TaskId) -> Result<bool, StorageError> {
        let query = "DELETE FROM downloads WHERE id = ?";
        let result = sqlx::query(query)
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to delete task {id}: {e}")))?;

        Ok(result.rows_affected() > 0)
    }
}

#[async_trait]
impl SegmentRepository for SqliteStorage {
    async fn save_segments(&self, segments: &[DownloadSegment]) -> Result<(), StorageError> {
        if segments.is_empty() {
            return Ok(());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::TransactionError(e.to_string()))?;

        for seg in segments {
            let query = r#"
                INSERT INTO segments (
                    id, download_id, start_offset, end_offset, downloaded_bytes,
                    assigned_path_id, status, retry_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    downloaded_bytes = excluded.downloaded_bytes,
                    assigned_path_id = excluded.assigned_path_id,
                    status = excluded.status,
                    retry_count = excluded.retry_count
            "#;

            let path_str = seg.assigned_path_id.map(|p| p.to_string());
            let status_str = format!("{:?}", seg.status);

            sqlx::query(query)
                .bind(seg.id.to_string())
                .bind(seg.download_id.to_string())
                .bind(seg.start_offset as i64)
                .bind(seg.end_offset as i64)
                .bind(seg.downloaded_bytes as i64)
                .bind(path_str)
                .bind(status_str)
                .bind(seg.retry_count as i64)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    StorageError::Database(format!("Failed to save segment {}: {e}", seg.id))
                })?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::TransactionError(e.to_string()))?;

        debug!(
            segment_count = segments.len(),
            "Persisted segments to SQLite"
        );
        Ok(())
    }

    async fn get_segments_for_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Vec<DownloadSegment>, StorageError> {
        let query = "SELECT * FROM segments WHERE download_id = ? ORDER BY start_offset ASC";
        let rows = sqlx::query(query)
            .bind(task_id.to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to fetch segments for task {task_id}: {e}"))
            })?;

        let mut segments = Vec::with_capacity(rows.len());
        for row in rows {
            segments.push(map_row_to_segment(&row)?);
        }

        Ok(segments)
    }

    async fn update_segment_progress(
        &self,
        id: &SegmentId,
        downloaded_bytes: u64,
    ) -> Result<(), StorageError> {
        let query = "UPDATE segments SET downloaded_bytes = ? WHERE id = ?";
        let result = sqlx::query(query)
            .bind(downloaded_bytes as i64)
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to update segment {id}: {e}")))?;

        if result.rows_affected() == 0 {
            return Err(StorageError::NotFound(id.to_string()));
        }

        Ok(())
    }
}

#[async_trait]
impl PathRepository for SqliteStorage {
    async fn save_path(&self, path: &NetworkPath) -> Result<(), StorageError> {
        let query = r#"
            INSERT INTO network_paths (
                id, name, interface_id, proxy_id, priority, enabled, max_connections, max_bandwidth_bps
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                interface_id = excluded.interface_id,
                proxy_id = excluded.proxy_id,
                priority = excluded.priority,
                enabled = excluded.enabled,
                max_connections = excluded.max_connections,
                max_bandwidth_bps = excluded.max_bandwidth_bps
        "#;

        let proxy_str = path.proxy_id.map(|p| p.to_string());
        let enabled_val = if path.enabled { 1i64 } else { 0i64 };
        let max_conn_val = path.limits.max_connections.map(|c| c as i64);
        let max_bw_val = path.limits.max_bandwidth_bps.map(|b| b as i64);

        sqlx::query(query)
            .bind(path.id.to_string())
            .bind(&path.name)
            .bind(path.interface_id.to_string())
            .bind(proxy_str)
            .bind(path.priority as i64)
            .bind(enabled_val)
            .bind(max_conn_val)
            .bind(max_bw_val)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to save path {}: {e}", path.id)))?;

        Ok(())
    }

    async fn list_paths(&self) -> Result<Vec<NetworkPath>, StorageError> {
        let query = "SELECT * FROM network_paths ORDER BY priority DESC";
        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to list paths: {e}")))?;

        let mut paths = Vec::with_capacity(rows.len());
        for row in rows {
            paths.push(map_row_to_path(&row)?);
        }

        Ok(paths)
    }

    async fn get_path(&self, id: &PathId) -> Result<Option<NetworkPath>, StorageError> {
        let query = "SELECT * FROM network_paths WHERE id = ?";
        let row_opt = sqlx::query(query)
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to fetch path {id}: {e}")))?;

        match row_opt {
            Some(row) => Ok(Some(map_row_to_path(&row)?)),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl ProxyRepository for SqliteStorage {
    async fn save_profile(&self, profile: &ProxyProfile) -> Result<(), StorageError> {
        let query = r#"
            INSERT INTO proxy_profiles (
                id, name, proxy_type, host, port, credential_reference, enabled, dns_policy
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                proxy_type = excluded.proxy_type,
                host = excluded.host,
                port = excluded.port,
                credential_reference = excluded.credential_reference,
                enabled = excluded.enabled,
                dns_policy = excluded.dns_policy
        "#;

        let enabled_val = if profile.enabled { 1i64 } else { 0i64 };
        let type_str = format!("{:?}", profile.proxy_type);
        let dns_str = format!("{:?}", profile.dns_policy);

        sqlx::query(query)
            .bind(profile.id.to_string())
            .bind(&profile.name)
            .bind(type_str)
            .bind(&profile.host)
            .bind(profile.port as i64)
            .bind(&profile.credential_reference)
            .bind(enabled_val)
            .bind(dns_str)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to save proxy profile {}: {e}", profile.id))
            })?;

        Ok(())
    }

    async fn list_profiles(&self) -> Result<Vec<ProxyProfile>, StorageError> {
        let query = "SELECT * FROM proxy_profiles ORDER BY name ASC";
        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to list proxy profiles: {e}")))?;

        let mut profiles = Vec::with_capacity(rows.len());
        for row in rows {
            profiles.push(map_row_to_proxy(&row)?);
        }

        Ok(profiles)
    }

    async fn get_profile(&self, id: &ProxyId) -> Result<Option<ProxyProfile>, StorageError> {
        let query = "SELECT * FROM proxy_profiles WHERE id = ?";
        let row_opt = sqlx::query(query)
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to fetch proxy profile {id}: {e}"))
            })?;

        match row_opt {
            Some(row) => Ok(Some(map_row_to_proxy(&row)?)),
            None => Ok(None),
        }
    }
}

// Row mapper helper functions

fn map_row_to_task(row: &sqlx::sqlite::SqliteRow) -> Result<DownloadTask, StorageError> {
    let id_str: String = row.get("id");
    let id: TaskId = id_str
        .parse()
        .map_err(|e| StorageError::Database(format!("Corrupt task id in db: {e}")))?;

    let url_str: String = row.get("url");
    let url: Url = Url::parse(&url_str)
        .map_err(|e| StorageError::Database(format!("Corrupt task url in db: {e}")))?;

    let total_size_val: Option<i64> = row.get("total_size");
    let total_size = total_size_val.map(|s| ByteSize(s as u64));

    let downloaded_size_val: i64 = row.get("downloaded_size");
    let downloaded_size = ByteSize(downloaded_size_val as u64);

    let status_str: String = row.get("status");
    let status = match status_str.as_str() {
        "Created" => DownloadStatus::Created,
        "Queued" => DownloadStatus::Queued,
        "Preparing" => DownloadStatus::Preparing,
        "Downloading" => DownloadStatus::Downloading,
        "Paused" => DownloadStatus::Paused,
        "Recovering" => DownloadStatus::Recovering,
        "Verifying" => DownloadStatus::Verifying,
        "Completed" => DownloadStatus::Completed,
        "Failed" => DownloadStatus::Failed,
        "Cancelled" => DownloadStatus::Cancelled,
        _ => DownloadStatus::Failed,
    };

    let max_speed_val: Option<i64> = row.get("policy_max_speed_bps");
    let auto_resume_val: i64 = row.get("policy_auto_resume");

    let policy = DownloadPolicy {
        max_connections: row.get::<i64, _>("policy_max_connections") as u32,
        max_speed_bps: max_speed_val.map(|s| s as u64),
        retry_limit: row.get::<i64, _>("policy_retry_limit") as u32,
        auto_resume: auto_resume_val != 0,
    };

    let created_str: String = row.get("created_at");
    let updated_str: String = row.get("updated_at");

    let created_at = DateTime::parse_from_rfc3339(&created_str)
        .map_or_else(|_| Utc::now(), |dt| dt.with_timezone(&Utc));

    let updated_at = DateTime::parse_from_rfc3339(&updated_str)
        .map_or_else(|_| Utc::now(), |dt| dt.with_timezone(&Utc));

    Ok(DownloadTask {
        id,
        url,
        filename: row.get("filename"),
        destination_dir: row.get("destination_dir"),
        total_size,
        downloaded_size,
        status,
        etag: row.get("etag"),
        last_modified: row.get("last_modified"),
        category: row.get("category"),
        created_at,
        updated_at,
        policy,
    })
}

fn map_row_to_segment(row: &sqlx::sqlite::SqliteRow) -> Result<DownloadSegment, StorageError> {
    let id_str: String = row.get("id");
    let id: SegmentId = id_str
        .parse()
        .map_err(|e| StorageError::Database(format!("Corrupt segment id: {e}")))?;

    let task_id_str: String = row.get("download_id");
    let download_id: TaskId = task_id_str
        .parse()
        .map_err(|e| StorageError::Database(format!("Corrupt download_id: {e}")))?;

    let assigned_path_opt: Option<String> = row.get("assigned_path_id");
    let assigned_path_id = assigned_path_opt.and_then(|s| s.parse::<PathId>().ok());

    let status_str: String = row.get("status");
    let status = match status_str.as_str() {
        "InProgress" => SegmentStatus::InProgress,
        "Completed" => SegmentStatus::Completed,
        "Failed" => SegmentStatus::Failed,
        "Cancelled" => SegmentStatus::Cancelled,
        _ => SegmentStatus::Pending,
    };

    Ok(DownloadSegment {
        id,
        download_id,
        start_offset: row.get::<i64, _>("start_offset") as u64,
        end_offset: row.get::<i64, _>("end_offset") as u64,
        downloaded_bytes: row.get::<i64, _>("downloaded_bytes") as u64,
        assigned_path_id,
        status,
        retry_count: row.get::<i64, _>("retry_count") as u32,
    })
}

fn map_row_to_path(row: &sqlx::sqlite::SqliteRow) -> Result<NetworkPath, StorageError> {
    let id_str: String = row.get("id");
    let id: PathId = id_str
        .parse()
        .map_err(|e| StorageError::Database(format!("Corrupt path id: {e}")))?;

    let iface_str: String = row.get("interface_id");
    let interface_id: InterfaceId = iface_str
        .parse()
        .map_err(|e| StorageError::Database(format!("Corrupt interface id: {e}")))?;

    let proxy_str: Option<String> = row.get("proxy_id");
    let proxy_id = proxy_str.and_then(|s| s.parse::<ProxyId>().ok());

    let enabled_val: i64 = row.get("enabled");
    let max_conn_val: Option<i64> = row.get("max_connections");
    let max_bw_val: Option<i64> = row.get("max_bandwidth_bps");

    Ok(NetworkPath {
        id,
        name: row.get("name"),
        interface_id,
        proxy_id,
        priority: row.get::<i64, _>("priority") as u8,
        enabled: enabled_val != 0,
        limits: PathLimits {
            max_connections: max_conn_val.map(|c| c as u32),
            max_bandwidth_bps: max_bw_val.map(|b| b as u64),
        },
    })
}

fn map_row_to_proxy(row: &sqlx::sqlite::SqliteRow) -> Result<ProxyProfile, StorageError> {
    let id_str: String = row.get("id");
    let id: ProxyId = id_str
        .parse()
        .map_err(|e| StorageError::Database(format!("Corrupt proxy id: {e}")))?;

    let type_str: String = row.get("proxy_type");
    let proxy_type = match type_str.as_str() {
        "Http" => ProxyType::Http,
        "Https" => ProxyType::Https,
        "Socks4" => ProxyType::Socks4,
        "Socks4a" => ProxyType::Socks4a,
        _ => ProxyType::Socks5,
    };

    let dns_str: String = row.get("dns_policy");
    let dns_policy = match dns_str.as_str() {
        "Local" => DnsPolicy::Local,
        "Remote" => DnsPolicy::Remote,
        _ => DnsPolicy::Auto,
    };

    let enabled_val: i64 = row.get("enabled");

    Ok(ProxyProfile {
        id,
        name: row.get("name"),
        proxy_type,
        host: row.get("host"),
        port: row.get::<i64, _>("port") as u16,
        credential_reference: row.get("credential_reference"),
        enabled: enabled_val != 0,
        dns_policy,
    })
}
