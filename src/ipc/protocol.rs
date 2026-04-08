use std::path::PathBuf;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum DaemonMessage {
    /// Sent immediately upon client connection.
    Hello(HelloPayload),
    Event(TelemetryEvent),
    Log(LogLine),
    /// Daemon is shutting down — client should exit.
    Goodbye,
}

/// Instance metadata sent as the first message on every new connection.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HelloPayload {
    pub version: String,
    pub instance_name: String,
    pub mounts: Vec<MountInfoWire>,
    pub window_start: String,
    pub window_end: String,
    pub preset_name: String,
    pub budget_max_bytes: u64,
    pub min_free_bytes: u64,
    pub expiry_secs: u64,
    /// Absolute path to the per-instance SQLite database.
    pub db_path: String,
    /// Base cache directory (for display purposes).
    pub cache_directory: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum ClientMessage {
    /// Ctrl+Q in TUI sends this to trigger clean daemon shutdown.
    Shutdown,
}

/// One variant per telemetry event constant in `telemetry.rs`,
/// carrying only the fields that EventVisitor currently extracts.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "event")]
pub enum TelemetryEvent {
    FuseOpen,
    CacheHit,
    CacheMiss,
    HandleClosed {
        bytes_read: Option<u64>,
    },
    CopyQueued,
    CopyStarted {
        path: Option<String>,
        size_bytes: Option<u64>,
    },
    CopyComplete {
        path: Option<String>,
    },
    CopyFailed {
        path: Option<String>,
    },
    DeferredChanged {
        count: Option<u64>,
    },
    BudgetUpdated {
        used_bytes: Option<u64>,
        max_bytes: Option<u64>,
    },
    CachingWindow {
        allowed: Option<bool>,
    },
    Eviction {
        path: Option<String>,
        reason: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LogLine {
    pub timestamp: String,
    pub level: String,
    pub message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MountInfoWire {
    pub target: PathBuf,
    /// The mount's cache subdirectory (used as `mount_id` in the database).
    pub cache_dir: PathBuf,
    pub active: bool,
}
