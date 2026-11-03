-- NEXUS Storage Engine Migration 0001: Initial Schema
-- PRD Section 15 & SAD Section 1.1

PRAGMA foreign_keys = ON;

-- Downloads master table
CREATE TABLE IF NOT EXISTS downloads (
    id TEXT PRIMARY KEY NOT NULL,
    url TEXT NOT NULL,
    filename TEXT NOT NULL,
    destination_dir TEXT NOT NULL,
    total_size INTEGER,
    downloaded_size INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    etag TEXT,
    last_modified TEXT,
    category TEXT,
    policy_max_connections INTEGER NOT NULL DEFAULT 8,
    policy_max_speed_bps INTEGER,
    policy_retry_limit INTEGER NOT NULL DEFAULT 5,
    policy_auto_resume INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);

-- Byte range segments table
CREATE TABLE IF NOT EXISTS segments (
    id TEXT PRIMARY KEY NOT NULL,
    download_id TEXT NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
    start_offset INTEGER NOT NULL,
    end_offset INTEGER NOT NULL,
    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
    assigned_path_id TEXT,
    status TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_segments_download_id ON segments(download_id);

-- Configured network paths table
CREATE TABLE IF NOT EXISTS network_paths (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    interface_id TEXT NOT NULL,
    proxy_id TEXT,
    priority INTEGER NOT NULL DEFAULT 1,
    enabled INTEGER NOT NULL DEFAULT 1,
    max_connections INTEGER,
    max_bandwidth_bps INTEGER
);

-- Proxy profiles table
CREATE TABLE IF NOT EXISTS proxy_profiles (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    proxy_type TEXT NOT NULL,
    host TEXT NOT NULL,
    port INTEGER NOT NULL,
    credential_reference TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    dns_policy TEXT NOT NULL DEFAULT 'Auto'
);

-- Audit and telemetry event log table
CREATE TABLE IF NOT EXISTS download_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_task_id ON download_events(task_id);

-- Application settings and persistent key-value configuration
CREATE TABLE IF NOT EXISTS application_settings (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
