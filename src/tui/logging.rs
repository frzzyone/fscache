use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::SystemTime;

use tracing::Level;
use tracing_subscriber::Layer;

use super::state::{DashboardState, LogEntry};

/// A `tracing::Layer` that captures formatted log messages into the
/// `DashboardState` ring buffer for display in the TUI log panels.
///
/// Replaces the console fmt layer when `--tui` is active so log output
/// doesn't corrupt the terminal display.
pub struct LoggingLayer {
    state: Arc<DashboardState>,
}

impl LoggingLayer {
    pub fn new(state: Arc<DashboardState>) -> Self {
        Self { state }
    }
}

impl<S> Layer<S> for LoggingLayer
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let level = match *event.metadata().level() {
            Level::ERROR => "ERROR",
            Level::WARN  => "WARN ",
            Level::INFO  => "INFO ",
            Level::DEBUG => "DEBUG",
            Level::TRACE => "TRACE",
        };

        // Extract the message field (the unstructured human-readable part).
        let mut msg_visitor = MessageVisitor::default();
        event.record(&mut msg_visitor);

        let now = super::ui::fmt_time(SystemTime::now());
        if self.state.tui_exited.load(Relaxed) {
            eprintln!("{} {} {}", now, level.trim(), msg_visitor.message);
        } else {
            self.state.push_log(LogEntry {
                timestamp: now,
                level:     level.to_string(),
                message:   msg_visitor.message,
            });
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value).trim_matches('"').to_string();
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }
}
