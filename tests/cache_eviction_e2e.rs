/// End-to-end eviction tests: FUSE mount + full copy pipeline + eviction.
///
/// These tests exercise scenarios that the unit tests in cache_overlay.rs cannot:
/// files are copied to the cache by the real copy pipeline (CacheIO + ActionEngine),
/// and eviction fires through the same async eviction worker that runs in production.
///
/// Three scenarios are covered:
///
///   1. LRU eviction during active copying — cache fills beyond budget while the
///      copy pipeline is running; the background eviction worker trims the oldest
///      files, leaving the most recently accessed ones intact.
///
///   2. TTL expiry removes entries whose last_hit_at has passed the expiry window.
///      Files are accessed through FUSE, land in cache, their timestamps are
///      back-dated to simulate staleness, then the eviction worker sweeps them out.
///
///   3. FUSE continues to serve evicted files — after LRU eviction removes a file
///      from the SSD cache, reads through the FUSE mount fall back to the backing
///      store transparently.  No errors, no data loss.
///
/// Pipeline under test (all three scenarios):
///   FUSE open() → AccessEvent → ActionEngine → Prefetch::on_miss()
///   → CacheIO::submit_cache() → copy_worker → mark_cached()
///   → eviction_worker ticks → evict_if_needed() → file deleted from SSD
mod common;

use std::sync::Arc;
use std::time::Duration;

use fscache::presets::prefetch::{Prefetch, PrefetchMode};

use common::{write_backing_file, FuseHarness};

/// Budget in GB that fits exactly 3 files of FILE_BYTES each, leaving no room for a 4th.
const FILE_BYTES: usize = 400;
const BUDGET_FILES: usize = 3;
const BUDGET_GB: f64 = (FILE_BYTES * BUDGET_FILES) as f64 / 1_073_741_824.0;

fn cache_hit_only_preset() -> Arc<Prefetch> {
    Arc::new(
        Prefetch::new(PrefetchMode::CacheHitOnly, 1, vec![], &[], &[])
            .expect("preset should compile"),
    )
}

fn write_file(h: &FuseHarness, rel: &str) {
    write_backing_file(h, rel, &vec![0xABu8; FILE_BYTES]);
}

// ---------------------------------------------------------------------------
// Scenario 1: LRU eviction trims oldest cached files when budget is exceeded
// ---------------------------------------------------------------------------

/// Full pipeline: access 5 files → all 5 land in cache (exceeding the 3-file budget)
/// → LRU timestamps backdated to make order deterministic → evict_if_needed() removes
/// the 2 oldest → ep3..ep5 survive.
///
/// Uses eviction_interval_secs = 0 so the eviction worker does not race with copies.
/// evict_if_needed() is called directly after all copies land, which is exactly the
/// same function the background eviction worker calls.
#[tokio::test]
async fn e2e_lru_eviction_removes_oldest_files_when_over_budget() {
    let preset = cache_hit_only_preset();
    // Budget fits 3 files; eviction_interval_secs = 0 for synchronous control.
    let h = FuseHarness::new_full_pipeline_with_config(preset, BUDGET_GB, 9999, 0).unwrap();

    // Create 5 backing files.
    let files = ["ep1.mkv", "ep2.mkv", "ep3.mkv", "ep4.mkv", "ep5.mkv"];
    for f in &files {
        write_file(&h, f);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Access all 5 through FUSE — each triggers a copy into cache.
    for f in &files {
        let _ = std::fs::read(h.mount_path().join(f)).unwrap();
    }

    // Wait for all 5 copies to complete.
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    for f in &files {
        assert!(
            h.cache_path().join(f).exists(),
            "all files should be in cache before eviction: {f} missing"
        );
    }

    // Back-date ep1 and ep2 so they are definitively the LRU candidates.
    let mount_id = h.cache_path().to_string_lossy().into_owned();
    let db = h.cache_mgr().cache_db();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    db.set_last_hit_at_for_test(std::path::Path::new("ep1.mkv"), &mount_id, now - 7200);
    db.set_last_hit_at_for_test(std::path::Path::new("ep2.mkv"), &mount_id, now - 3600);
    // ep3..ep5 keep their natural timestamps (recent).

    // Trigger eviction synchronously — same call the background eviction worker makes.
    h.cache_mgr().evict_if_needed();

    assert!(!h.cache_path().join("ep1.mkv").exists(), "ep1 (oldest) should be evicted");
    assert!(!h.cache_path().join("ep2.mkv").exists(), "ep2 (second oldest) should be evicted");
    assert!(h.cache_path().join("ep3.mkv").exists(), "ep3 should survive (within budget)");
    assert!(h.cache_path().join("ep4.mkv").exists(), "ep4 should survive");
    assert!(h.cache_path().join("ep5.mkv").exists(), "ep5 (newest) should survive");
}

/// Verifies that after LRU eviction the total cache size stays within budget.
/// Does not assert which specific files were evicted — only that the aggregate is correct.
#[tokio::test]
async fn e2e_lru_eviction_keeps_cache_within_budget() {
    let preset = cache_hit_only_preset();
    // Budget = 3 files; eviction_interval_secs = 0 (we call evict_if_needed directly for
    // synchronous, timing-independent control).
    let h = FuseHarness::new_full_pipeline_with_config(preset, BUDGET_GB, 9999, 0).unwrap();

    let files = ["a.mkv", "b.mkv", "c.mkv", "d.mkv", "e.mkv"];
    for f in &files {
        write_file(&h, f);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    for f in &files {
        let _ = std::fs::read(h.mount_path().join(f)).unwrap();
    }

    // Wait for all 5 copies to complete.
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    // Back-date a.mkv and b.mkv to ensure they are the LRU candidates.
    let mount_id = h.cache_path().to_string_lossy().into_owned();
    let db = h.cache_mgr().cache_db();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    db.set_last_hit_at_for_test(std::path::Path::new("a.mkv"), &mount_id, now - 7200);
    db.set_last_hit_at_for_test(std::path::Path::new("b.mkv"), &mount_id, now - 3600);

    // Trigger eviction synchronously — same call the eviction worker makes.
    h.cache_mgr().evict_if_needed();

    let cached: Vec<_> = files
        .iter()
        .filter(|f| h.cache_path().join(f).exists())
        .collect();

    assert_eq!(
        cached.len(),
        BUDGET_FILES,
        "exactly {BUDGET_FILES} files should remain after eviction, got {}: {:?}",
        cached.len(),
        cached,
    );

    // The two oldest must be gone.
    assert!(!h.cache_path().join("a.mkv").exists(), "a.mkv (oldest) must be evicted");
    assert!(!h.cache_path().join("b.mkv").exists(), "b.mkv (second oldest) must be evicted");
}

// ---------------------------------------------------------------------------
// Scenario 2: TTL expiry removes stale cache entries via the eviction worker
// ---------------------------------------------------------------------------

/// Files are copied to cache via the full pipeline, then their last_hit_at timestamps
/// are back-dated to simulate a long-idle cache.  The background eviction worker fires
/// and removes all expired entries.
#[tokio::test]
async fn e2e_ttl_expiry_removes_idle_cached_files() {
    let preset = cache_hit_only_preset();
    // expiry_hours = 1, eviction_interval_secs = 1. Budget large enough to hold all files.
    let h = FuseHarness::new_full_pipeline_with_config(preset, 1.0, 1, 1).unwrap();

    let files = ["movie1.mkv", "movie2.mkv", "movie3.mkv"];
    for f in &files {
        write_file(&h, f);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    for f in &files {
        let _ = std::fs::read(h.mount_path().join(f)).unwrap();
    }

    // Wait for all copies to complete.
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    for f in &files {
        assert!(
            h.cache_path().join(f).exists(),
            "file should be in cache before expiry: {f}"
        );
    }

    // Back-date all entries to 2 hours ago — past the 1-hour expiry window.
    let mount_id = h.cache_path().to_string_lossy().into_owned();
    let db = h.cache_mgr().cache_db();
    let two_hours_ago = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 7200;
    for f in &files {
        db.set_last_hit_at_for_test(std::path::Path::new(f), &mount_id, two_hours_ago);
    }

    // Wait for the eviction worker to fire (interval = 1 s).
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    for f in &files {
        assert!(
            !h.cache_path().join(f).exists(),
            "expired file should have been removed by eviction worker: {f}"
        );
    }
}

/// TTL expiry is selective: fresh files survive while expired files are removed.
#[tokio::test]
async fn e2e_ttl_expiry_preserves_recently_accessed_files() {
    let preset = cache_hit_only_preset();
    let h = FuseHarness::new_full_pipeline_with_config(preset, 1.0, 1, 1).unwrap();

    write_file(&h, "stale.mkv");
    write_file(&h, "fresh.mkv");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let _ = std::fs::read(h.mount_path().join("stale.mkv")).unwrap();
    let _ = std::fs::read(h.mount_path().join("fresh.mkv")).unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    // Back-date only stale.mkv.
    let mount_id = h.cache_path().to_string_lossy().into_owned();
    let db = h.cache_mgr().cache_db();
    let two_hours_ago = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 7200;
    db.set_last_hit_at_for_test(std::path::Path::new("stale.mkv"), &mount_id, two_hours_ago);

    tokio::time::sleep(Duration::from_millis(1_500)).await;

    assert!(!h.cache_path().join("stale.mkv").exists(), "stale file should be evicted");
    assert!(h.cache_path().join("fresh.mkv").exists(), "fresh file should survive");
}

// ---------------------------------------------------------------------------
// Scenario 3: FUSE serves evicted files from backing store (no error, no data loss)
// ---------------------------------------------------------------------------

/// After a cached file is evicted, FUSE must transparently fall back to the backing
/// store on the next read.  The caller sees correct data; no ENOENT or garbage.
#[tokio::test]
async fn e2e_evicted_file_falls_back_to_backing() {
    let preset = cache_hit_only_preset();
    // Budget fits 1 file; eviction_interval_secs = 0 (synchronous control).
    let single_file_budget_gb = FILE_BYTES as f64 / 1_073_741_824.0;
    let h = FuseHarness::new_full_pipeline_with_config(preset, single_file_budget_gb, 9999, 0).unwrap();

    // file_a goes in cache first (will be evicted); file_b is the newcomer.
    let content_a = vec![0xAAu8; FILE_BYTES];
    let content_b = vec![0xBBu8; FILE_BYTES];
    write_backing_file(&h, "file_a.mkv", &content_a);
    write_backing_file(&h, "file_b.mkv", &content_b);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let _ = std::fs::read(h.mount_path().join("file_a.mkv")).unwrap();
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert!(h.cache_path().join("file_a.mkv").exists(), "file_a should be cached");

    // Access file_b — it also gets copied, now both files exceed the 1-file budget.
    let _ = std::fs::read(h.mount_path().join("file_b.mkv")).unwrap();
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert!(h.cache_path().join("file_b.mkv").exists(), "file_b should be cached");

    // Make file_a the LRU candidate and evict synchronously.
    let mount_id = h.cache_path().to_string_lossy().into_owned();
    let db = h.cache_mgr().cache_db();
    let old_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 7200;
    db.set_last_hit_at_for_test(std::path::Path::new("file_a.mkv"), &mount_id, old_ts);
    h.cache_mgr().evict_if_needed();

    assert!(!h.cache_path().join("file_a.mkv").exists(), "file_a should be evicted");

    // FUSE must still serve file_a from the backing store — transparently.
    let data = std::fs::read(h.mount_path().join("file_a.mkv"))
        .expect("FUSE must serve evicted file from backing store without error");
    assert_eq!(data, content_a, "backing store content must be returned after eviction");
}

/// After eviction clears the entire cache, every cached file falls back to backing.
/// All files must still be readable through FUSE with correct content.
#[tokio::test]
async fn e2e_full_cache_eviction_all_reads_fall_back_to_backing() {
    let preset = cache_hit_only_preset();
    let h = FuseHarness::new_full_pipeline_with_config(preset, BUDGET_GB, 9999, 0).unwrap();

    let files = [
        ("dir/ep1.mkv", vec![0x11u8; FILE_BYTES]),
        ("dir/ep2.mkv", vec![0x22u8; FILE_BYTES]),
        ("dir/ep3.mkv", vec![0x33u8; FILE_BYTES]),
    ];
    for (rel, content) in &files {
        write_backing_file(&h, rel, content);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    for (rel, _) in &files {
        let _ = std::fs::read(h.mount_path().join(rel)).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    // Evict everything by back-dating all entries far into the past.
    let mount_id = h.cache_path().to_string_lossy().into_owned();
    let db = h.cache_mgr().cache_db();
    let ancient = 0i64; // Unix epoch — always expired under any reasonable TTL
    for (rel, _) in &files {
        db.set_last_hit_at_for_test(std::path::Path::new(rel), &mount_id, ancient);
    }
    h.cache_mgr().evict_if_needed();

    for (rel, _) in &files {
        assert!(!h.cache_path().join(rel).exists(), "all files should be evicted: {rel}");
    }

    // Every file must still be readable through FUSE (backing fallback).
    for (rel, expected) in &files {
        let data = std::fs::read(h.mount_path().join(rel))
            .unwrap_or_else(|e| panic!("FUSE read failed for {rel} after eviction: {e}"));
        assert_eq!(&data, expected, "wrong content from backing for {rel}");
    }
}

// ---------------------------------------------------------------------------
// Scenario 4: copy pipeline interaction — eviction and new copies coexist
// ---------------------------------------------------------------------------

/// While the copy pipeline is actively working, the eviction worker runs in parallel
/// on its own cadence.  This test verifies that concurrent eviction and copying do not
/// corrupt data, skip copies, or panic.
///
/// Pattern: access 5 files → copies start → eviction fires mid-flight → all 5 files
/// end up correctly served (either from cache or backing), cache stays within budget.
#[tokio::test]
async fn e2e_eviction_concurrent_with_active_copying() {
    let preset = cache_hit_only_preset();
    // Budget = 3 files; eviction worker fires every 1 s.
    let h = FuseHarness::new_full_pipeline_with_config(preset, BUDGET_GB, 9999, 1).unwrap();

    let files = [
        ("race/f1.mkv", vec![0x01u8; FILE_BYTES]),
        ("race/f2.mkv", vec![0x02u8; FILE_BYTES]),
        ("race/f3.mkv", vec![0x03u8; FILE_BYTES]),
        ("race/f4.mkv", vec![0x04u8; FILE_BYTES]),
        ("race/f5.mkv", vec![0x05u8; FILE_BYTES]),
    ];
    for (rel, content) in &files {
        write_backing_file(&h, rel, content);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Fire all 5 accesses quickly — copies and eviction may interleave.
    for (rel, _) in &files {
        let _ = std::fs::read(h.mount_path().join(rel)).unwrap();
    }

    // Allow time for copies + at least one eviction worker tick.
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    let cached_count = files
        .iter()
        .filter(|(rel, _)| h.cache_path().join(rel).exists())
        .count();
    assert!(
        cached_count <= BUDGET_FILES,
        "cache must not exceed budget: {cached_count} files cached, budget is {BUDGET_FILES}"
    );

    // Every file must still be readable through FUSE — no corruption, no errors.
    for (rel, expected) in &files {
        let data = std::fs::read(h.mount_path().join(rel))
            .unwrap_or_else(|e| panic!("FUSE read failed for {rel}: {e}"));
        assert_eq!(&data, expected, "data mismatch for {rel}");
    }
}

// ---------------------------------------------------------------------------
// Scenario 5: no false eviction — fresh files within budget are never removed
// ---------------------------------------------------------------------------

/// When the cache is within budget, the eviction worker must be a no-op.
/// No files should disappear even after the worker has fired multiple times.
#[tokio::test]
async fn e2e_eviction_worker_does_not_remove_files_within_budget() {
    let preset = cache_hit_only_preset();
    // 2 files accessed into a budget large enough for 10 — nothing should be evicted.
    let large_budget_gb = (FILE_BYTES * 10) as f64 / 1_073_741_824.0;
    let h = FuseHarness::new_full_pipeline_with_config(preset, large_budget_gb, 9999, 1).unwrap();

    write_file(&h, "safe1.mkv");
    write_file(&h, "safe2.mkv");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let _ = std::fs::read(h.mount_path().join("safe1.mkv")).unwrap();
    let _ = std::fs::read(h.mount_path().join("safe2.mkv")).unwrap();

    // Wait for copies, then let eviction worker fire several times.
    tokio::time::sleep(Duration::from_millis(3_000)).await;

    assert!(h.cache_path().join("safe1.mkv").exists(), "safe1.mkv must not be evicted");
    assert!(h.cache_path().join("safe2.mkv").exists(), "safe2.mkv must not be evicted");
}
