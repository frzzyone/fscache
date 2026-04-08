use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use futures::StreamExt;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use tokio::sync::watch;
use tokio::time::interval;

use crate::cache::db::CacheDb;
use crate::ipc::client;
use crate::ipc::protocol::ClientMessage;
use crate::ipc::{send_msg, IpcFramedWriter};
use super::state::{CachedFileInfo, CacheSort, DashboardState, MountInfo, Page};
use super::ui;

const RENDER_TICK_MS: u64 = 500;
const POLL_TICK_SECS: u64 = 3;

/// Connect to a running daemon socket and launch the TUI monitoring dashboard.
///
/// `q`      — detach TUI, daemon keeps running.
/// `Ctrl+Q` — send `Shutdown` to daemon, then exit.
pub async fn run_client(socket_path: PathBuf) -> anyhow::Result<()> {
    // 1. Connect and read Hello.
    let (hello, mut reader, writer) = client::connect(&socket_path).await?;

    // 2. Build local DashboardState from Hello metadata.
    let state = Arc::new(DashboardState::new());
    {
        let mut mounts = state.mounts.lock().unwrap();
        for m in &hello.mounts {
            mounts.push(MountInfo {
                target:    m.target.clone(),
                cache_dir: m.cache_dir.clone(),
                active:    m.active,
            });
        }
        *state.window_start.lock().unwrap() = hello.window_start.clone();
        *state.window_end.lock().unwrap()   = hello.window_end.clone();
        *state.preset_name.lock().unwrap()  = hello.preset_name.clone();
        if hello.budget_max_bytes > 0 {
            state.budget_max_bytes.store(hello.budget_max_bytes, Relaxed);
        }
        state.cache_max_bytes.store(hello.budget_max_bytes, Relaxed);
        state.cache_min_free_bytes.store(hello.min_free_bytes, Relaxed);
        state.expiry_secs.store(hello.expiry_secs, Relaxed);
    }

    // 3. Open the daemon's SQLite database read-only (WAL = concurrent access).
    let db_path = PathBuf::from(&hello.db_path);
    let db = Arc::new(CacheDb::open_readonly(&db_path)?);

    let mount_dirs: Vec<PathBuf> = hello.mounts.iter()
        .map(|m| m.cache_dir.clone())
        .collect();

    // 4. Spawn the IPC stream reader task.
    let state_for_ipc = Arc::clone(&state);
    let (disc_tx, mut disc_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = client::run_client_stream(&mut reader, state_for_ipc).await;
        let _ = disc_tx.send(true); // signal disconnect
    });

    // 5. Enter TUI.
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = event_loop(
        &mut terminal,
        Arc::clone(&state),
        db,
        mount_dirs,
        writer,
        &mut disc_rx,
    ).await;

    disable_raw_mode()?;
    terminal.backend_mut().execute(LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

async fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    state: Arc<DashboardState>,
    db: Arc<CacheDb>,
    mount_dirs: Vec<PathBuf>,
    mut writer: IpcFramedWriter,
    disc_rx: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut render_tick = interval(Duration::from_millis(RENDER_TICK_MS));
    let mut poll_tick   = interval(Duration::from_secs(POLL_TICK_SECS));
    poll_tick.tick().await;
    poll_cache_stats(&state, &db, &mount_dirs);

    let mut events     = EventStream::new();
    let mut page       = Page::Status;
    let mut log_scroll = 0usize;
    let mut cache_sel  = 0usize;
    let mut cache_sort = CacheSort::Newest;

    loop {
        tokio::select! {
            _ = render_tick.tick() => {
                terminal.draw(|f| ui::render(f, &state, page, log_scroll, cache_sel, cache_sort))?;
            }

            _ = poll_tick.tick() => {
                poll_cache_stats(&state, &db, &mount_dirs);
            }

            Some(Ok(event)) = events.next() => {
                if let Event::Key(key) = event {
                    if key.kind != KeyEventKind::Press {
                        continue;
                    }
                    match key.code {
                        // q — detach TUI, daemon keeps running.
                        KeyCode::Char('q') | KeyCode::Char('Q')
                            if !key.modifiers.contains(KeyModifiers::CONTROL) =>
                        {
                            break;
                        }
                        // Ctrl+Q — request daemon shutdown.
                        KeyCode::Char('q') | KeyCode::Char('Q')
                            if key.modifiers.contains(KeyModifiers::CONTROL) =>
                        {
                            let _ = send_msg(&mut writer, &ClientMessage::Shutdown).await;
                            break;
                        }
                        KeyCode::Right => {
                            page = page.next();
                            log_scroll = 0;
                        }
                        KeyCode::Left => {
                            page = page.prev();
                            log_scroll = 0;
                        }
                        KeyCode::Char('1') => { page = Page::Status; log_scroll = 0; }
                        KeyCode::Char('2') => { page = Page::Cache;  cache_sel = 0; }
                        KeyCode::Char('3') => { page = Page::Logs;   log_scroll = 0; }
                        KeyCode::Down => match page {
                            Page::Cache => {
                                let len = state.cached_files.lock().unwrap().len();
                                if len > 0 { cache_sel = (cache_sel + 1).min(len - 1); }
                            }
                            Page::Logs => {
                                let len = state.recent_logs.lock().unwrap().len();
                                if len > 0 { log_scroll = (log_scroll + 1).min(len.saturating_sub(1)); }
                            }
                            _ => {}
                        }
                        KeyCode::Up => match page {
                            Page::Cache => { cache_sel = cache_sel.saturating_sub(1); }
                            Page::Logs  => { log_scroll = log_scroll.saturating_sub(1); }
                            _ => {}
                        }
                        KeyCode::Home => match page {
                            Page::Cache => { cache_sel = 0; }
                            Page::Logs  => { log_scroll = 0; }
                            _ => {}
                        }
                        KeyCode::End => match page {
                            Page::Cache => {
                                let len = state.cached_files.lock().unwrap().len();
                                cache_sel = len.saturating_sub(1);
                            }
                            Page::Logs => {
                                let len = state.recent_logs.lock().unwrap().len();
                                log_scroll = len.saturating_sub(1);
                            }
                            _ => {}
                        }
                        KeyCode::Char('s') | KeyCode::Char('S') => {
                            if page == Page::Cache {
                                cache_sort = cache_sort.next();
                            }
                        }
                        _ => {}
                    }
                    terminal.draw(|f| ui::render(f, &state, page, log_scroll, cache_sel, cache_sort))?;
                }
            }

            Ok(_) = disc_rx.changed() => {
                if *disc_rx.borrow() {
                    tracing::info!("Daemon disconnected");
                    break;
                }
            }
        }
    }

    Ok(())
}

fn poll_cache_stats(state: &Arc<DashboardState>, db: &Arc<CacheDb>, mount_dirs: &[PathBuf]) {
    let mut total_used  = 0u64;
    let mut total_files = 0usize;
    let mut combined:   Vec<CachedFileInfo> = Vec::new();
    let mut free_bytes: Option<u64> = None;

    let expiry = Duration::from_secs(state.expiry_secs.load(Relaxed));

    for cache_dir in mount_dirs {
        let mount_id = cache_dir.to_string_lossy().into_owned();
        let (used, files) = db.client_files_for_mount(&mount_id);
        total_used  += used;
        total_files += files.len();

        if let Some(free) = free_space_bytes(cache_dir) {
            free_bytes = Some(match free_bytes {
                None    => free,
                Some(p) => p.min(free),
            });
        }

        for (path, size, cached_at, last_hit_at) in files {
            let evicts_at = last_hit_at + expiry;
            combined.push(CachedFileInfo { path, size_bytes: size, cached_at, last_hit_at, evicts_at });
        }
    }

    state.cache_used_bytes.store(total_used, Relaxed);
    state.cache_free_bytes.store(free_bytes.unwrap_or(0), Relaxed);
    state.cache_file_count.store(total_files as u64, Relaxed);
    *state.cached_files.lock().unwrap() = combined;
}

fn free_space_bytes(path: &std::path::Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut stat) } == 0 {
        Some(stat.f_bavail * stat.f_bsize as u64)
    } else {
        None
    }
}
