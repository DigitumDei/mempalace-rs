//! `ttlMs` ↔ `expires_at` conversion.
//!
//! MCP Tasks' `ttlMs` (`schema/2026-07-28/schema.ts` in `modelcontextprotocol/ext-tasks`) is a
//! **duration in milliseconds measured from task creation**: `number | null`, where `null` means
//! the task has no TTL and persists indefinitely, and a number means the server may discard the
//! task after that many milliseconds have elapsed since `createdAt`. `null`/absent are treated
//! identically here — the extension gives them the same meaning ("no TTL"), so both round-trip
//! to `None` on the MemPalace side.
//!
//! MemPalace's `expires_at` (`mempalace_storage::coordination::Task::expires_at`/
//! `NewTask::expires_at`) is an **absolute timestamp**, not a duration. Converting between the
//! two therefore always needs `created_at` as well as the value being converted — neither
//! direction is a pure `Option<u64>` <-> `Option<OffsetDateTime>` mapping. This module makes that
//! third input explicit in both function signatures rather than hiding it.
//!
//! `pollIntervalMs` is deliberately **not** handled here (or anywhere in this crate): it is
//! adapter/transport policy for how often a client should poll `tasks/get`, not state that
//! outlives a single exchange, and it has no MemPalace column to round-trip through. Neither
//! `ttlMs` nor `pollIntervalMs` becomes a new column in `mempalace_storage` — `ttlMs` maps onto
//! the *existing* `expires_at` column, and `pollIntervalMs` is not stored at all.

use time::{Duration, OffsetDateTime};

use crate::error::McpTasksError;

/// Convert a wire `ttlMs` into an absolute `expires_at`, relative to `created_at`.
///
/// `ttl_ms: None` (wire `null` or an absent field — this module treats them identically, see the
/// module docs) means "no TTL" and maps to `expires_at: None`. `ttl_ms: Some(ms)` maps to
/// `created_at + ms` milliseconds.
///
/// # Errors
///
/// Returns [`McpTasksError::TtlOutOfRange`] if `created_at + ttl_ms` falls outside the range
/// [`OffsetDateTime`] can represent.
pub fn ttl_ms_to_expires_at(
    created_at: OffsetDateTime,
    ttl_ms: Option<u64>,
) -> Result<Option<OffsetDateTime>, McpTasksError> {
    let Some(ttl_ms) = ttl_ms else {
        return Ok(None);
    };
    let ttl_ms_i64 = i64::try_from(ttl_ms).map_err(|_err| McpTasksError::TtlOutOfRange)?;
    let expires_at = created_at
        .checked_add(Duration::milliseconds(ttl_ms_i64))
        .ok_or(McpTasksError::TtlOutOfRange)?;
    Ok(Some(expires_at))
}

/// Convert an absolute `expires_at` into a wire `ttlMs`, relative to `created_at`.
///
/// `expires_at: None` means "no TTL" and maps to `ttl_ms: None`, which serializes as wire `null`
/// (see [`crate::detailed_task`]) — the correct MCP Tasks spelling of "persists indefinitely".
///
/// `expires_at: Some(t)` where `t` is at or after `created_at` maps to the whole number of
/// milliseconds between them. If `t` is *before* `created_at` — an already-expired task, or data
/// with an inconsistent `created_at`/`expires_at` pair — the result saturates to `0` rather than
/// producing a negative `ttlMs` the extension's `number | null` type does not anticipate; a
/// duration too large to fit `u64` milliseconds (never observed in practice, since MemPalace's
/// own lease-duration overflow guard rejects timestamps that extreme long before this function
/// would see them) saturates to [`u64::MAX`].
pub fn expires_at_to_ttl_ms(
    created_at: OffsetDateTime,
    expires_at: Option<OffsetDateTime>,
) -> Option<u64> {
    let expires_at = expires_at?;
    let millis = (expires_at - created_at).whole_milliseconds();
    let ttl_ms = u64::try_from(millis).unwrap_or(if millis < 0 { 0 } else { u64::MAX });
    Some(ttl_ms)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn ttl_ms_none_maps_to_no_expiry() {
        let created_at = OffsetDateTime::now_utc();
        assert_eq!(ttl_ms_to_expires_at(created_at, None).unwrap(), None);
    }

    #[test]
    fn ttl_ms_some_maps_to_created_at_plus_duration() {
        let created_at = OffsetDateTime::now_utc();
        let expires_at = ttl_ms_to_expires_at(created_at, Some(60_000)).unwrap().unwrap();
        assert_eq!(expires_at, created_at + Duration::minutes(1));
    }

    #[test]
    fn no_expiry_maps_to_ttl_ms_none() {
        let created_at = OffsetDateTime::now_utc();
        assert_eq!(expires_at_to_ttl_ms(created_at, None), None);
    }

    #[test]
    fn expires_at_maps_to_the_elapsed_milliseconds() {
        let created_at = OffsetDateTime::now_utc();
        let expires_at = created_at + Duration::minutes(1);
        assert_eq!(expires_at_to_ttl_ms(created_at, Some(expires_at)), Some(60_000));
    }

    #[test]
    fn expires_at_before_created_at_saturates_to_zero() {
        let created_at = OffsetDateTime::now_utc();
        let expires_at = created_at - Duration::minutes(1);
        assert_eq!(expires_at_to_ttl_ms(created_at, Some(expires_at)), Some(0));
    }

    #[test]
    fn round_trips_through_both_directions() {
        let created_at = OffsetDateTime::now_utc();
        let original_ttl_ms = Some(123_456_u64);
        let expires_at = ttl_ms_to_expires_at(created_at, original_ttl_ms).unwrap();
        assert_eq!(expires_at_to_ttl_ms(created_at, expires_at), original_ttl_ms);
    }
}
