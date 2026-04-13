use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex, Semaphore};

use crate::backing_store::BackingStore;
use crate::cache::manager::CacheManager;
use crate::telemetry;

/// A job submitted to CacheIO to copy a file from the backing store into the cache.
pub struct CacheJob {
    pub rel_path: PathBuf,
    pub cache_dest: PathBuf,
    /// Notifies the ActionEngine when this job completes or is skipped, so it can
    /// remove the path from its in-flight dedupe set.
    pub done_tx: mpsc::UnboundedSender<PathBuf>,
}

pub struct CacheIoConfig {
    /// Maximum number of backing→cache copies running concurrently.
    /// Default 1 preserves the single-copy behaviour from before this feature.
    pub max_concurrent_copies: usize,
    /// Depth of the bounded copy-submission channel. Callers block when full.
    pub copy_queue_depth: usize,
    /// How often the autonomous eviction worker runs (seconds). 0 disables it.
    pub eviction_interval_secs: u64,
}

/// Owner of all cache filesystem I/O: the copy pipeline and the eviction pipeline.
///
/// Both pipelines run on independent tokio tasks — eviction is never gated by copy
/// traffic. Copy workers can also trigger on-demand LRU eviction when they need to
/// make room for an incoming file (serialised internally so workers don't race).
#[derive(Clone)]
pub struct CacheIO {
    cache_tx: mpsc::Sender<CacheJob>,
    /// Serialises on-demand force-eviction from concurrent copy workers.
    evict_lock: Arc<Mutex<()>>,
    cache: Arc<CacheManager>,
}

impl CacheIO {
    /// Spawns the copy dispatcher and eviction worker tasks, then returns a handle.
    pub fn spawn(
        cfg: CacheIoConfig,
        cache: Arc<CacheManager>,
        backing_store: Arc<BackingStore>,
    ) -> Self {
        let (cache_tx, cache_rx) = mpsc::channel::<CacheJob>(cfg.copy_queue_depth);
        let evict_lock = Arc::new(Mutex::new(()));

        let handle = CacheIO {
            cache_tx,
            evict_lock: Arc::clone(&evict_lock),
            cache: Arc::clone(&cache),
        };

        let semaphore = Arc::new(Semaphore::new(cfg.max_concurrent_copies));

        let dispatcher_cache = Arc::clone(&cache);
        let dispatcher_bs = Arc::clone(&backing_store);
        let dispatcher_evict_lock = Arc::clone(&evict_lock);
        tokio::spawn(async move {
            cache_dispatcher(
                cache_rx,
                dispatcher_cache,
                dispatcher_bs,
                semaphore,
                dispatcher_evict_lock,
            )
            .await;
        });

        if cfg.eviction_interval_secs > 0 {
            let evict_cache = Arc::clone(&cache);
            let interval_secs = cfg.eviction_interval_secs;
            tokio::spawn(async move {
                eviction_worker(evict_cache, interval_secs).await;
            });
        }

        handle
    }

    /// Submit a file for caching. Returns as soon as the job is queued.
    /// Backpressure: awaits if the copy queue is full (`copy_queue_depth` reached).
    pub async fn submit_cache(&self, job: CacheJob) {
        let _ = self.cache_tx.send(job).await;
    }
}

// ---- internal tasks -------------------------------------------------------

async fn cache_dispatcher(
    mut rx: mpsc::Receiver<CacheJob>,
    cache: Arc<CacheManager>,
    backing_store: Arc<BackingStore>,
    semaphore: Arc<Semaphore>,
    evict_lock: Arc<Mutex<()>>,
) {
    while let Some(job) = rx.recv().await {
        // Acquire a concurrency slot before spawning. When max_concurrent_copies == 1
        // this is equivalent to waiting for the previous copy to finish.
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");

        let cache = Arc::clone(&cache);
        let backing_store = Arc::clone(&backing_store);
        let evict_lock = Arc::clone(&evict_lock);

        tokio::spawn(async move {
            copy_worker(job, cache, backing_store, evict_lock).await;
            drop(permit); // release slot when the worker finishes
        });
    }
}

async fn copy_worker(
    job: CacheJob,
    cache: Arc<CacheManager>,
    backing_store: Arc<BackingStore>,
    evict_lock: Arc<Mutex<()>>,
) {
    let rel = job.rel_path.clone();

    if cache.is_cached(&rel) {
        tracing::debug!("cache_io: {} already cached, skipping", rel.display());
        let _ = job.done_tx.send(rel);
        return;
    }

    // On-demand eviction: if there is not enough space, evict LRU files to fit
    // this one. The evict_lock serialises concurrent workers so they don't all
    // compute the same deletion set and over-evict.
    if !cache.has_free_space() {
        let size_bytes = backing_store.file_size(&rel).unwrap_or(0);
        let _guard = evict_lock.lock().await;
        // Re-check inside the lock — another worker may have already freed space.
        if !cache.has_free_space() {
            let freed = cache.evict_to_fit(size_bytes);
            tracing::info!(
                "cache_io: evicted {:.1} MB on demand to fit {}",
                freed as f64 / 1_048_576.0,
                rel.display()
            );
        }
    }

    if !cache.has_free_space() {
        tracing::warn!(
            event = telemetry::EVENT_COPY_FAILED,
            path = %rel.display(),
            "cache_io: insufficient free space after eviction, skipping {}", rel.display()
        );
        let _ = job.done_tx.send(rel);
        return;
    }

    let size_bytes = backing_store.file_size(&rel).unwrap_or(0);
    tracing::info!(
        event = telemetry::EVENT_COPY_STARTED,
        path = %rel.display(),
        size_bytes,
        "cache_io: caching {}", rel.display()
    );

    let dest = job.cache_dest.clone();
    let bs = Arc::clone(&backing_store);
    let result = tokio::task::spawn_blocking(move || {
        crate::engine::copier::copy_to_cache(&bs, &rel, &dest)
    })
    .await;

    match result {
        Ok(Ok(())) => {
            use std::os::unix::fs::MetadataExt;
            let (size, mtime_secs, mtime_nsecs) = match std::fs::metadata(&job.cache_dest) {
                Ok(m) => (m.len(), m.mtime(), m.mtime_nsec()),
                Err(_) => (0, 0, 0),
            };
            cache.mark_cached(&job.rel_path, size, mtime_secs, mtime_nsecs);
            tracing::info!(
                event = telemetry::EVENT_COPY_COMPLETE,
                path = %job.rel_path.display(),
                "cache_io: cached {}", job.rel_path.display()
            );
        }
        Ok(Err(e)) => {
            tracing::warn!(
                event = telemetry::EVENT_COPY_FAILED,
                path = %job.rel_path.display(),
                "cache_io: copy failed {}: {e}", job.rel_path.display()
            );
        }
        Err(e) => {
            tracing::warn!(
                event = telemetry::EVENT_COPY_FAILED,
                path = %job.rel_path.display(),
                "cache_io: task panicked {}: {e}", job.rel_path.display()
            );
        }
    }

    let _ = job.done_tx.send(job.rel_path);
}

async fn eviction_worker(cache: Arc<CacheManager>, interval_secs: u64) {
    let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
    ticker.tick().await; // consume the immediate first tick
    loop {
        ticker.tick().await;
        let cache_clone = Arc::clone(&cache);
        tokio::task::spawn_blocking(move || cache_clone.evict_if_needed())
            .await
            .ok();
    }
}
