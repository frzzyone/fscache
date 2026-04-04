/// True end-to-end tests: FUSE mount + cache overlay + predictor + copier all
/// wired together exactly as they run in production.
///
/// The FUSE kernel callbacks call open(), which sends an AccessEvent through
/// the channel to the predictor task, which scans the backing dir via regex,
/// enqueues copy requests, and the copier task writes files into the cache dir.
/// Subsequent reads through the FUSE mount are served from the SSD cache.
mod common;
use common::{write_backing_file, FuseHarness, OvermountHarness};
use std::time::Duration;

fn wait_for_pipeline() {
    // Generous sleep: predictor scans dir + copier copies N small files.
    std::thread::sleep(Duration::from_millis(800));
}

/// Full pipeline: read E01 through FUSE → predictor caches E02–E05 →
/// subsequent reads of E02–E05 through FUSE are served from SSD cache.
///
/// The cache copies contain DIFFERENT content than the backing store.
/// We verify this by pre-populating the backing store, letting the pipeline
/// cache them, then overwriting the backing copies with new content.
/// The FUSE reads must still return the original (cached) content.
#[tokio::test]
async fn full_pipeline_caches_and_serves_from_ssd() {
    let h = FuseHarness::new_full_pipeline(4).unwrap();

    // Write 5 episodes to the backing store
    for i in 1..=5u32 {
        write_backing_file(
            &h,
            &format!("tv/Show/Show.S01E0{}.mkv", i),
            format!("original content ep{}", i).as_bytes(),
        );
    }

    // Wait for FUSE to see the directory (lookup happens lazily)
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Read E01 through the FUSE mount — this triggers the access event
    let e1_data = tokio::fs::read(h.mount_path().join("tv/Show/Show.S01E01.mkv"))
        .await
        .unwrap();
    assert_eq!(e1_data, b"original content ep1");

    // Give the predictor and copier time to complete all 4 copies
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Verify E02–E05 are now in the cache directory
    for i in 2..=5u32 {
        let cached = h.cache_path().join(format!("tv/Show/Show.S01E0{}.mkv", i));
        assert!(cached.exists(), "expected ep{} in cache dir", i);
        assert_eq!(
            std::fs::read(&cached).unwrap(),
            format!("original content ep{}", i).as_bytes(),
            "cached content mismatch for ep{}",
            i
        );
    }

    // Now overwrite the backing files with different content.
    // Subsequent FUSE reads should still return the cached version.
    for i in 2..=5u32 {
        write_backing_file(
            &h,
            &format!("tv/Show/Show.S01E0{}.mkv", i),
            format!("overwritten backing ep{}", i).as_bytes(),
        );
    }

    // Read E02–E05 through FUSE — must come from cache, not the overwritten backing
    for i in 2..=5u32 {
        let data = tokio::fs::read(h.mount_path().join(format!("tv/Show/Show.S01E0{}.mkv", i)))
            .await
            .unwrap();
        assert_eq!(
            data,
            format!("original content ep{}", i).as_bytes(),
            "ep{} should be served from cache, not overwritten backing",
            i
        );
    }
}

/// Cache hits do NOT trigger further prediction. Only misses advance the lookahead.
/// E01 miss → caches E02+E03. Reading E02 (hit) → no new caching. E04 miss → caches E05+E06.
#[tokio::test]
async fn pipeline_advances_lookahead_on_each_access() {
    let h = FuseHarness::new_full_pipeline(2).unwrap(); // lookahead = 2

    for i in 1..=6u32 {
        write_backing_file(
            &h,
            &format!("tv/Show/Show.S01E0{}.mkv", i),
            format!("ep{}", i).as_bytes(),
        );
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Access E01 (miss) → should cache E02 and E03
    let _ = tokio::fs::read(h.mount_path().join("tv/Show/Show.S01E01.mkv"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert!(h.cache_path().join("tv/Show/Show.S01E02.mkv").exists(), "E02 should be cached after E01 miss");
    assert!(h.cache_path().join("tv/Show/Show.S01E03.mkv").exists(), "E03 should be cached after E01 miss");
    assert!(!h.cache_path().join("tv/Show/Show.S01E04.mkv").exists(), "E04 should NOT be cached yet");

    // Access E02 (cache HIT) → should NOT trigger any new caching
    let e2_data = tokio::fs::read(h.mount_path().join("tv/Show/Show.S01E02.mkv"))
        .await
        .unwrap();
    assert_eq!(e2_data, b"ep2");
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert!(!h.cache_path().join("tv/Show/Show.S01E04.mkv").exists(), "E04 should NOT be cached after a cache hit");

    // Access E04 (miss) → should cache E05 and E06
    let _ = tokio::fs::read(h.mount_path().join("tv/Show/Show.S01E04.mkv"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert!(h.cache_path().join("tv/Show/Show.S01E05.mkv").exists(), "E05 should be cached after E04 miss");
    assert!(h.cache_path().join("tv/Show/Show.S01E06.mkv").exists(), "E06 should be cached after E04 miss");
}

/// Rolling-buffer strategy fires on every access, including cache hits.
/// E01 miss → caches E02+E03. E02 hit → triggers prediction again → caches E04+E05.
#[tokio::test]
async fn rolling_buffer_triggers_on_cache_hit() {
    use common::FuseHarness;
    use plex_hot_cache::fuse_fs::TriggerStrategy;

    let h = FuseHarness::new_full_pipeline_with_strategy(2, TriggerStrategy::RollingBuffer).unwrap();

    for i in 1..=5u32 {
        write_backing_file(
            &h,
            &format!("tv/Show/Show.S01E0{}.mkv", i),
            format!("ep{}", i).as_bytes(),
        );
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Access E01 (miss) → should cache E02 and E03
    let _ = tokio::fs::read(h.mount_path().join("tv/Show/Show.S01E01.mkv"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert!(h.cache_path().join("tv/Show/Show.S01E02.mkv").exists(), "E02 should be cached after E01 miss");
    assert!(h.cache_path().join("tv/Show/Show.S01E03.mkv").exists(), "E03 should be cached after E01 miss");
    assert!(!h.cache_path().join("tv/Show/Show.S01E04.mkv").exists(), "E04 should not be cached yet");

    // Access E02 (cache HIT) — rolling-buffer fires an event, so E04+E05 should get cached
    let e2_data = tokio::fs::read(h.mount_path().join("tv/Show/Show.S01E02.mkv"))
        .await
        .unwrap();
    assert_eq!(e2_data, b"ep2");
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Predictor finds [E03, E04] after E02. E03 is already cached so it's skipped;
    // E04 is the new cache target. E05 is outside the lookahead window.
    assert!(h.cache_path().join("tv/Show/Show.S01E04.mkv").exists(), "E04 should be cached after E02 hit (rolling-buffer)");
    assert!(!h.cache_path().join("tv/Show/Show.S01E05.mkv").exists(), "E05 is outside lookahead range");
}

/// True overmount E2E: FUSE is mounted ON TOP of the same directory the media
/// files live in, exactly as in production.
///
/// Reading `dir/tv/Show/Show.S01E01.mkv` goes through the FUSE filesystem,
/// which opens the real file via the pre-mount O_PATH fd underneath.  The
/// access event triggers the predictor, which copies E02–E05 to the SSD cache.
/// Subsequent reads of E02–E05 are served from the SSD cache — not from the
/// underlying backing files.
///
/// This is the scenario Plex sees in production: it reads from `/mnt/media/...`
/// unaware that FUSE is intercepting every syscall.
#[tokio::test]
async fn overmount_pipeline_caches_and_serves() {
    let h = OvermountHarness::new(4, |dir| {
        // All files must be written BEFORE the overmount — once FUSE is mounted
        // over this path the directory appears read-only to normal file operations.
        for i in 1..=5u32 {
            let path = dir.join(format!("tv/Show/Show.S01E0{}.mkv", i));
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, format!("original content ep{}", i)).unwrap();
        }
    })
    .unwrap();

    // Allow FUSE to finish initialising before the first read.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Read E01 through the overmounted path — identical to Plex opening the file.
    let data = tokio::fs::read(h.path().join("tv/Show/Show.S01E01.mkv"))
        .await
        .unwrap();
    assert_eq!(data, b"original content ep1");

    // Give the predictor and copier time to cache E02–E05.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Verify the cache directory contains the predicted episodes.
    for i in 2..=5u32 {
        let cached = h.cache_path().join(format!("tv/Show/Show.S01E0{}.mkv", i));
        assert!(cached.exists(), "E0{} should be in the cache dir", i);
        assert_eq!(
            std::fs::read(&cached).unwrap(),
            format!("original content ep{}", i).as_bytes(),
        );
    }

    // Read E02–E05 through the overmounted path — served from the SSD cache.
    for i in 2..=5u32 {
        let data = tokio::fs::read(h.path().join(format!("tv/Show/Show.S01E0{}.mkv", i)))
            .await
            .unwrap();
        assert_eq!(
            data,
            format!("original content ep{}", i).as_bytes(),
            "E0{} read through overmount should return original content",
            i
        );
    }
}
