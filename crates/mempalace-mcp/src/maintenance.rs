//! Connection-scoped maintenance for stdio; HTTP scheduling belongs to the hub.

use std::time::{Duration, SystemTime};

use mempalace_config::MaintenanceRuntimeConfig;
use mempalace_storage::{MaintenanceSettings, StorageEngine};
use tokio::sync::oneshot;

pub(crate) async fn run(
    storage: &StorageEngine,
    config: &MaintenanceRuntimeConfig,
    mut stopped: oneshot::Receiver<()>,
) {
    if !config.enabled || !config.background_enabled {
        return;
    }
    let settings = MaintenanceSettings {
        enabled: config.enabled,
        idle_secs: config.idle_secs as u64,
        version_retention_hours: config.version_retention_hours as u64,
        tail_threshold_rows: config.tail_threshold_rows as u64,
        small_fragment_threshold: config.small_fragment_threshold as u64,
    };
    loop {
        // Also check before the startup pass: an already-closed connection must
        // not start maintenance. Never cancel a pass after it acquires a lease.
        if !matches!(stopped.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
            return;
        }
        if storage.elapsed_since_last_activity() >= Duration::from_secs(settings.idle_secs) {
            // Clear activity only after a complete idle window, then recheck to
            // preserve a write racing the transition. The engine checks again
            // before and between tiers and uses the same cross-process lease
            // as `serve` and `maintain`.
            storage.take_activity_signal();
            if storage.elapsed_since_last_activity() >= Duration::from_secs(settings.idle_secs) {
                tracing::info!("stdio MCP maintenance check");
                match storage.run_maintenance(&settings).await {
                    Ok(summary) => tracing::info!(
                        run_id = summary.run_id,
                        status = ?summary.status,
                        "stdio MCP maintenance finished"
                    ),
                    Err(error) => tracing::warn!(%error, "stdio MCP maintenance failed"),
                }
            }
        }
        // A positive floor also protects programmatically constructed configs.
        let base = Duration::from_secs(settings.idle_secs.max(1));
        let fraction = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as f64
            / 1_000_000_000.0;
        let jitter = Duration::from_secs_f64(base.as_secs_f64() * fraction * 0.1);
        tokio::select! {
            _ = &mut stopped => return,
            _ = tokio::time::sleep(base + jitter) => {}
        }
    }
}
