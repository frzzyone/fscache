/// End-to-end tests for cache invalidation.
///
/// Every test here goes through a real FUSE mount — reads are `open()` + `read()`
/// syscalls that traverse the kernel and hit the fuser session, exactly as they
/// would in production.  We exercise both invalidation paths:
///
///   - `check_on_hit`  — stale check fires inside the FUSE `open()` handler
///   - `sweep_stale`   — direct call to the maintenance sweep (the same call that
///                       `run_maintenance_task` makes on each tick)
///
/// The harness uses `FuseHarness::new_with_cache_and_invalidation`, which wires
/// `Some(backing_store)` into `CacheManager` — matching `main.rs` production setup.
/// Existing harness constructors pass `None`, making `is_stale()` return `NotTracked`.
///
/// # Kernel page-cache / inode-cache design note
///
/// `getattr` in FUSE always reads the file size from the backing store (not the
/// cache file). The kernel caches this size in its inode.  On the next read the
/// kernel uses the cached size to bound how many bytes it requests.
///
/// Consequence: if a stale cache entry is dropped and the new backing file is a
/// *different size*, the kernel may read fewer bytes than the new file contains
/// (because it has the old size cached). To avoid flaky byte-count assertions, all
/// tests that check exact read content use same-size content between the old and
/// new backing versions — only the bytes differ.  Tests that specifically exercise
/// size-change detection assert file-system state (cache file deleted, sweep count)
/// rather than the exact bytes returned by the FUSE read.
///
/// # Backing-deleted and FUSE getattr
///
/// When a backing file is deleted, the FUSE `getattr`/`lookup` handler returns
/// ENOENT *before* `open()` is called.  This means the `check_on_hit` stale check
/// (which lives in `open()`) never fires for deleted files.  The cache entry is
/// cleaned up by the maintenance sweep (`sweep_stale`), not on-hit — this is correct
/// production behaviour.  The `check_on_hit` path handles `BackingGone` for the case
/// where the backing disappears between `getattr` succeeding and `open()` running,
/// but in practice the sweep is the primary cleanup path for deleted sources.
mod common;
use common::{write_backing_file, FuseHarness};
use fscache::config::InvalidationConfig;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Build a harness and plant a hot cache entry.
///
/// Returns `(harness, backing_path, cache_file_path)`.
/// After this call:
///   - backing file contains `backing_content`
///   - cache file contains `cache_content` (intentionally different bytes, SAME size)
///   - DB fingerprint matches the current backing mtime/size → `is_stale()` returns Fresh
///   - FUSE read returns `cache_content`
///
/// **Both byte slices must be the same length.** FUSE `getattr` reads the file
/// size from the backing store; the kernel uses that size to bound reads, so a
/// size mismatch between backing and cache would produce truncated reads.
fn setup_hot_cache(
    invalidation: &InvalidationConfig,
    rel: &str,
    backing_content: &[u8],
    cache_content: &[u8],
) -> (FuseHarness, std::path::PathBuf, std::path::PathBuf) {
    assert_eq!(
        backing_content.len(), cache_content.len(),
        "setup_hot_cache: backing and cache content must be the same length \
         (kernel bounds reads by the size returned from getattr → backing)"
    );

    let h = FuseHarness::new_with_cache_and_invalidation(1.0, 72, invalidation).unwrap();

    // Write the backing file.
    write_backing_file(&h, rel, backing_content);
    let backing_path = h.backing_path().join(rel);

    // Plant a cache file with distinct content so we can tell which path was served.
    let cache_file = h.cache_path().join(rel);
    if let Some(p) = cache_file.parent() {
        std::fs::create_dir_all(p).unwrap();
    }
    std::fs::write(&cache_file, cache_content).unwrap();

    // Register the fingerprint from the BACKING file's real mtime/size.
    let meta = std::fs::metadata(&backing_path).unwrap();
    h.cache_mgr().mark_cached(
        Path::new(rel),
        meta.len(),
        meta.mtime(),
        meta.mtime_nsec(),
    );

    // Brief settle so the FUSE session is ready.
    std::thread::sleep(Duration::from_millis(50));

    (h, backing_path, cache_file)
}

// ---------------------------------------------------------------------------
// check_on_hit tests
// ---------------------------------------------------------------------------

/// Baseline: a hot cache entry is served from the SSD cache, not the backing store.
/// This is the precondition for every other test in this file.
#[test]
fn e2e_hot_cache_entry_is_served_from_cache() {
    let cfg = InvalidationConfig { check_on_hit: true, check_on_maintenance: false };
    let (h, _backing, _cache_file) =
        setup_hot_cache(&cfg, "movie.mkv", b"backing_v1____", b"cached___v1___");

    let data = std::fs::read(h.mount_path().join("movie.mkv")).unwrap();
    assert_eq!(data, b"cached___v1___", "expected cache hit to serve cached content");
}

/// `check_on_hit=true`: stale file (backing mtime changed, same size) is detected
/// on the very next FUSE open(). The cache entry is dropped and the backing content
/// is served — all in a single read, no maintenance sweep needed.
///
/// Same-size content ensures the kernel reads the correct number of bytes after the
/// inode is updated via the FUSE getattr that precedes open().
#[test]
fn e2e_check_on_hit_drops_stale_after_mtime_change() {
    let cfg = InvalidationConfig { check_on_hit: true, check_on_maintenance: false };
    let (h, backing_path, cache_file) =
        setup_hot_cache(&cfg, "movie.mkv", b"backing_v1____", b"cached___v1___");

    // Confirm cache is hot before touching anything.
    let before = std::fs::read(h.mount_path().join("movie.mkv")).unwrap();
    assert_eq!(before, b"cached___v1___", "precondition: cache should be hot");

    // Overwrite backing with same-size content — mtime changes on write.
    std::fs::write(&backing_path, b"backing__v2___").unwrap();

    // Next read through FUSE: stale check fires in open(), cache dropped, backing served.
    let after = std::fs::read(h.mount_path().join("movie.mkv")).unwrap();
    assert_eq!(after, b"backing__v2___", "stale-on-hit must serve fresh backing content");

    assert!(!cache_file.exists(), "stale cache file must be removed on hit");
}

/// `check_on_hit=false` (shipped default): stale backing content does NOT evict the
/// cached copy.  This is the negative-control test — it proves the default is opt-in.
#[test]
fn e2e_check_on_hit_disabled_serves_stale_data() {
    let cfg = InvalidationConfig::default(); // check_on_hit: false
    let (h, backing_path, cache_file) =
        setup_hot_cache(&cfg, "movie.mkv", b"backing_v1____", b"cached___v1___");

    std::fs::write(&backing_path, b"backing__v2___").unwrap();

    // With check_on_hit=false the stale cached bytes are still served.
    let data = std::fs::read(h.mount_path().join("movie.mkv")).unwrap();
    assert_eq!(data, b"cached___v1___",
        "stale cache must still be served when check_on_hit=false");

    assert!(cache_file.exists(),
        "cache file must not be deleted when check_on_hit=false");
}

/// `check_on_hit=true`: size change (with or without mtime change) is sufficient to
/// detect staleness.  This exercises the size arm of the three-field fingerprint
/// (`source_mtime_secs`, `source_mtime_nsecs`, `size_bytes`).
///
/// After the stale drop, we verify that:
///   1. The cache file was deleted (proves size change triggered stale detection).
///   2. The FUSE read returns bytes from the backing store, not the old cached content.
///
/// We do NOT assert the exact byte count of the FUSE read here — the kernel's cached
/// inode size may bound the read to the old length until getattr is refreshed (OS
/// concern, not a FUSE or cache concern).
#[test]
fn e2e_check_on_hit_detects_size_change() {
    let cfg = InvalidationConfig { check_on_hit: true, check_on_maintenance: false };
    // Use 5-byte content so the size change to a longer payload is dramatic.
    let h = FuseHarness::new_with_cache_and_invalidation(1.0, 72, &cfg).unwrap();

    write_backing_file(&h, "movie.mkv", b"short");
    let backing_path = h.backing_path().join("movie.mkv");
    let cache_file = h.cache_path().join("movie.mkv");
    std::fs::write(&cache_file, b"cache").unwrap(); // same 5 bytes as backing

    let meta = std::fs::metadata(&backing_path).unwrap();
    h.cache_mgr().mark_cached(Path::new("movie.mkv"), meta.len(), meta.mtime(), meta.mtime_nsec());
    std::thread::sleep(Duration::from_millis(50));

    // Baseline: cache is hot, served from SSD.
    let before = std::fs::read(h.mount_path().join("movie.mkv")).unwrap();
    assert_eq!(before, b"cache", "precondition: cache must be hot");

    // Overwrite backing with a LARGER payload — only the size field changes if the
    // write lands in the same nanosecond (extremely unlikely but theoretically possible).
    std::fs::write(&backing_path, b"this_content_is_longer_than_before").unwrap();

    // Trigger the FUSE open() stale check.
    let result = std::fs::read(h.mount_path().join("movie.mkv")).unwrap();

    // Cache file must have been deleted — this is the primary proof that size change
    // was detected.
    assert!(!cache_file.exists(),
        "cache file must be removed when size change is detected by check_on_hit");

    // The result must NOT be the cached content (we're reading from the backing now).
    assert_ne!(&result[..], b"cache",
        "FUSE read after size-stale-drop must not return the old cached content");
}

/// `check_on_hit=true`: a pre-migration DB row (`source_mtime_secs=0`, `source_mtime_nsecs=0`)
/// triggers the `NeedsBackfill` branch in `open()`.  The fingerprint is backfilled
/// silently and the cache file is served — the entry is NOT dropped.  The next read
/// takes the normal `Fresh` path.
#[test]
fn e2e_check_on_hit_backfills_zero_fingerprint_and_serves_cache() {
    let cfg = InvalidationConfig { check_on_hit: true, check_on_maintenance: false };
    let h = FuseHarness::new_with_cache_and_invalidation(1.0, 72, &cfg).unwrap();

    write_backing_file(&h, "episode.mkv", b"backing_data__");
    let cache_file = h.cache_path().join("episode.mkv");
    std::fs::write(&cache_file, b"cached__data__").unwrap(); // same 14-byte length

    let meta = std::fs::metadata(h.backing_path().join("episode.mkv")).unwrap();
    // Simulate a pre-migration row: size is set, mtime fields are zero.
    h.cache_mgr().mark_cached(Path::new("episode.mkv"), meta.len(), 0, 0);

    std::thread::sleep(Duration::from_millis(50));

    // First read: NeedsBackfill → backfill fingerprint → serve from cache.
    let data = std::fs::read(h.mount_path().join("episode.mkv")).unwrap();
    assert_eq!(data, b"cached__data__",
        "zero-fingerprint row must be backfilled and served from cache (not dropped)");
    assert!(cache_file.exists(), "cache file must NOT be deleted during backfill");

    // Fingerprint must now be populated.
    let fp = h.cache_mgr().cache_db()
        .fingerprint_row(Path::new("episode.mkv"), h.cache_mgr().mount_id())
        .expect("fingerprint row must exist after backfill");
    assert_ne!(fp.source_mtime_secs, 0, "mtime_secs must be non-zero after backfill");
    assert_eq!(fp.size_bytes, meta.len(), "size_bytes must match backing file");

    // Second read: fingerprint is live → Fresh path → still served from cache.
    let data2 = std::fs::read(h.mount_path().join("episode.mkv")).unwrap();
    assert_eq!(data2, b"cached__data__",
        "subsequent read after backfill must still hit cache");
}

/// When the backing file is deleted, FUSE `getattr`/`lookup` returns ENOENT
/// *before* `open()` is called.  The stale check in `open()` therefore never fires
/// for deleted files — the cache entry is left in place and cleaned up by the
/// maintenance sweep (`sweep_stale`), not by `check_on_hit`.
///
/// This test verifies the observable FUSE behaviour:
///   - reads fail with ENOENT (correct — the file appears gone)
///   - the cache entry persists until the maintenance sweep runs
#[test]
fn e2e_backing_deleted_fuse_returns_enoent_cache_cleaned_by_sweep() {
    let cfg = InvalidationConfig { check_on_hit: true, check_on_maintenance: true };
    let (h, backing_path, cache_file) =
        setup_hot_cache(&cfg, "movie.mkv", b"backing_v1____", b"cached___v1___");

    std::fs::remove_file(&backing_path).unwrap();

    // FUSE read fails: getattr returns ENOENT (backing gone → lookup fails).
    let result = std::fs::read(h.mount_path().join("movie.mkv"));
    assert!(result.is_err(), "FUSE read must fail when backing file is deleted");

    // The cache file persists — open() was never called so check_on_hit couldn't fire.
    // The maintenance sweep is responsible for cleaning up stale entries whose backing
    // has been deleted.
    let (checked, dropped) = h.cache_mgr().sweep_stale();
    assert_eq!(checked, 1);
    assert_eq!(dropped, 1, "sweep must drop the entry whose backing was deleted");
    assert!(!cache_file.exists(), "cache file must be removed by sweep after backing is gone");

    // After sweep, FUSE still returns ENOENT (backing is gone).
    let result2 = std::fs::read(h.mount_path().join("movie.mkv"));
    assert!(result2.is_err(), "FUSE read must still fail after sweep (backing is still gone)");
}

// ---------------------------------------------------------------------------
// sweep_stale (maintenance path) tests
// ---------------------------------------------------------------------------

/// `sweep_stale()` drops a stale entry and the next FUSE read returns the updated
/// backing content.  This is the primary maintenance-path E2E test:
/// `sweep_stale()` is the exact call made by `run_maintenance_task` on each tick.
///
/// Steps:
///   1. Hot cache set up.
///   2. Backing updated (same size, mtime bumps) — stale but check_on_hit=false.
///   3. First FUSE read returns stale cached content (no on-hit check).
///   4. `sweep_stale()` detects and drops the stale entry.
///   5. Second FUSE read falls through to backing store.
#[test]
fn e2e_maintenance_sweep_drops_stale_and_next_read_hits_backing() {
    let cfg = InvalidationConfig { check_on_hit: false, check_on_maintenance: true };
    let (h, backing_path, cache_file) =
        setup_hot_cache(&cfg, "movie.mkv", b"backing_v1____", b"cached___v1___");

    // Update backing (same size — mtime change is the staleness signal).
    std::fs::write(&backing_path, b"backing__v2___").unwrap();

    // With check_on_hit=false, first read returns stale cached content.
    let stale = std::fs::read(h.mount_path().join("movie.mkv")).unwrap();
    assert_eq!(stale, b"cached___v1___",
        "precondition: stale content must be served when check_on_hit=false");

    // Maintenance sweep — same call as run_maintenance_task.
    let (checked, dropped) = h.cache_mgr().sweep_stale();
    assert_eq!(checked, 1, "sweep must check 1 entry");
    assert_eq!(dropped, 1, "sweep must drop the 1 stale entry");
    assert!(!cache_file.exists(), "cache file must be removed by sweep");

    // Second read falls through to backing store.
    let fresh = std::fs::read(h.mount_path().join("movie.mkv")).unwrap();
    assert_eq!(fresh, b"backing__v2___",
        "read after sweep must return fresh backing content");
}

/// `sweep_stale()` does nothing when all entries are fresh: the cache file survives
/// and reads continue to return the cached content.
#[test]
fn e2e_maintenance_sweep_preserves_fresh_entries() {
    let cfg = InvalidationConfig { check_on_hit: false, check_on_maintenance: true };
    let (h, _backing, cache_file) =
        setup_hot_cache(&cfg, "movie.mkv", b"backing_v1____", b"cached___v1___");

    let (checked, dropped) = h.cache_mgr().sweep_stale();
    assert_eq!(checked, 1);
    assert_eq!(dropped, 0, "sweep must not drop a fresh entry");
    assert!(cache_file.exists(), "cache file must survive a no-op sweep");

    let data = std::fs::read(h.mount_path().join("movie.mkv")).unwrap();
    assert_eq!(data, b"cached___v1___", "cache must still be served after a no-op sweep");
}

/// `sweep_stale()` backfills a zero-fingerprint (pre-migration) row without dropping
/// the cache entry.  After backfill, reads continue to return the cached content.
#[test]
fn e2e_maintenance_sweep_backfills_zero_fingerprint() {
    let cfg = InvalidationConfig { check_on_hit: false, check_on_maintenance: true };
    let h = FuseHarness::new_with_cache_and_invalidation(1.0, 72, &cfg).unwrap();

    write_backing_file(&h, "show.mkv", b"backing_data__");
    let cache_file = h.cache_path().join("show.mkv");
    std::fs::write(&cache_file, b"cached__data__").unwrap();

    let meta = std::fs::metadata(h.backing_path().join("show.mkv")).unwrap();
    // Simulate pre-migration row: size set, mtime fields zero.
    h.cache_mgr().mark_cached(Path::new("show.mkv"), meta.len(), 0, 0);
    std::thread::sleep(Duration::from_millis(50));

    // Sweep must backfill, NOT drop.
    let (checked, dropped) = h.cache_mgr().sweep_stale();
    assert_eq!(checked, 1);
    assert_eq!(dropped, 0, "sweep must not drop a zero-fingerprint row — it should backfill");
    assert!(cache_file.exists(), "cache file must survive a backfill sweep");

    // Fingerprint must now be populated.
    let fp = h.cache_mgr().cache_db()
        .fingerprint_row(Path::new("show.mkv"), h.cache_mgr().mount_id())
        .expect("fingerprint row must exist after backfill sweep");
    assert_ne!(fp.source_mtime_secs, 0, "mtime_secs must be non-zero after sweep backfill");

    // Read through FUSE still returns cached content.
    let data = std::fs::read(h.mount_path().join("show.mkv")).unwrap();
    assert_eq!(data, b"cached__data__", "cache must still be served after backfill sweep");
}

/// `sweep_stale()` drops an entry whose backing file was deleted.
#[test]
fn e2e_maintenance_sweep_drops_entry_when_backing_deleted() {
    let cfg = InvalidationConfig { check_on_hit: false, check_on_maintenance: true };
    let (h, backing_path, cache_file) =
        setup_hot_cache(&cfg, "movie.mkv", b"backing_v1____", b"cached___v1___");

    std::fs::remove_file(&backing_path).unwrap();

    let (checked, dropped) = h.cache_mgr().sweep_stale();
    assert_eq!(checked, 1);
    assert_eq!(dropped, 1, "sweep must drop entry when backing file is gone");
    assert!(!cache_file.exists(), "cache file must be deleted when backing is gone");

    // After sweep, FUSE returns ENOENT.
    let result = std::fs::read(h.mount_path().join("movie.mkv"));
    assert!(result.is_err(), "expected ENOENT after cache and backing are both gone");
}

/// Sweep with multiple files correctly distinguishes fresh and stale entries,
/// dropping only the stale ones and leaving fresh entries intact.
#[test]
fn e2e_maintenance_sweep_mixed_fresh_and_stale() {
    let cfg = InvalidationConfig { check_on_hit: false, check_on_maintenance: true };
    let h = FuseHarness::new_with_cache_and_invalidation(1.0, 72, &cfg).unwrap();

    for name in &["fresh.mkv", "stale.mkv"] {
        write_backing_file(&h, name, b"backing_v1____");
        std::fs::write(h.cache_path().join(name), b"cached___v1___").unwrap();
        let meta = std::fs::metadata(h.backing_path().join(name)).unwrap();
        h.cache_mgr().mark_cached(
            Path::new(name), meta.len(), meta.mtime(), meta.mtime_nsec(),
        );
    }
    std::thread::sleep(Duration::from_millis(50));

    // Update only the stale file (same size — mtime is the signal).
    std::fs::write(h.backing_path().join("stale.mkv"), b"backing__v2___").unwrap();

    let (checked, dropped) = h.cache_mgr().sweep_stale();
    assert_eq!(checked, 2);
    assert_eq!(dropped, 1, "only the stale entry should be dropped");

    assert!(h.cache_path().join("fresh.mkv").exists(), "fresh entry must survive sweep");
    assert!(!h.cache_path().join("stale.mkv").exists(), "stale entry must be removed by sweep");

    // Fresh file still served from cache.
    let fresh = std::fs::read(h.mount_path().join("fresh.mkv")).unwrap();
    assert_eq!(fresh, b"cached___v1___");

    // Stale file now falls through to updated backing.
    let stale = std::fs::read(h.mount_path().join("stale.mkv")).unwrap();
    assert_eq!(stale, b"backing__v2___");
}
