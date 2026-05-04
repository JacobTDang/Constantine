use tracing_subscriber::{fmt, EnvFilter};

/// Initialize the global tracing subscriber.
///
/// Respects RUST_LOG if set; otherwise falls back to `log_level`.
/// Uses try_init so calling this from tests (where a subscriber may
/// already be installed) is a no-op rather than a panic.
pub fn init(log_level: &str) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(log_level));

    let _ = fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(false)
        .compact()
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_does_not_panic() {
        init("debug");
    }

    #[test]
    fn double_init_does_not_panic() {
        init("info");
        init("info"); // second call must be silently ignored
    }

    #[test]
    fn emitting_events_does_not_panic() {
        init("trace");
        tracing::trace!("trace event");
        tracing::debug!("debug event");
        tracing::info!(value = 42, "info event with field");
        tracing::warn!("warn event");
        tracing::error!("error event");
    }
}
