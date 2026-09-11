//! Connection-scoped maintenance for stdio; HTTP scheduling belongs to the hub.

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
        if storage.maintenance_window_open(settings.idle_secs) {
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
        tokio::select! {
            _ = &mut stopped => return,
            _ = storage.wait_for_maintenance_window(settings.idle_secs) => {}
        }
    }
}
