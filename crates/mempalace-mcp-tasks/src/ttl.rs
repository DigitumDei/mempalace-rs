//! `ttlMs` ↔ absolute-deadline conversion.
//!
//! MCP Tasks' `ttlMs` (`schema/2026-07-28/schema.ts` in `modelcontextprotocol/ext-tasks`) is a
//! **duration in milliseconds measured from task creation**: `number | null`, where `null` means
//! the task has no TTL and persists indefinitely, and a number means the server may discard the
//! task after that many milliseconds have elapsed since `createdAt`. `null`/absent are treated
//! identically here — the extension gives them the same meaning ("no TTL"), so both round-trip
//! to `None` on the MemPalace side.
//!
//! # This is retention, not lifecycle — it must never become `expires_at`
//!
//! `ttlMs` and MemPalace's `expires_at`
//! (`mempalace_storage::coordination::Task::expires_at`/`NewTask::expires_at`) look like the same
//! shape (a deadline computed from a creation time), but they mean different things and must not
//! be conflated:
//!
//! - MCP `ttlMs` is **retention**: the spec says the server *"may discard the task"* after it
//!   elapses. A `completed`/`failed`/`cancelled` task past its TTL is still exactly that status;
//!   the server is merely permitted to stop remembering it.
//! - MemPalace `expires_at` is **lifecycle**: `CoordinationStore::claim_task`
//!   (`crates/mempalace-storage/src/coordination.rs`) checks it in-transaction and, if it has
//!   passed, transitions the record to `TaskState::Expired` and returns the `TASK_HAS_EXPIRED`
//!   invariant — a terminal, unsuccessful outcome. This crate's own outbound mapping
//!   ([`crate::status::map_outbound_task_state`]) then reports `Expired` as MCP `failed`.
//!
//! Feeding `ttlMs` into `expires_at` therefore does not just lose a distinction — it actively
//! fabricates failures: a successfully `completed` MCP task with a one-hour retention hint,
//! queried an hour and one minute later, would come back as MCP `failed`, and a target state that
//! should still be reachable (e.g. transitioning a claimed task to `Completed`) could become
//! impossible because `claim_task` expired it first. This module therefore computes the absolute
//! deadline `ttlMs` implies (still useful — see below) but does **not** write it anywhere near
//! `expires_at`; see [`crate::detailed_task::NewTaskConversion::provenance`] for where the
//! computed deadline is actually returned, under the name `retention_deadline`, precisely so it
//! is not mistaken for a lifecycle field. A caller that genuinely wants MCP retention to also
//! drive MemPalace expiry may set `NewTask::expires_at` from `retention_deadline` itself — that is
//! a deliberate per-caller choice this adapter must not make silently.
//!
//! MemPalace's `expires_at` is an **absolute timestamp**, not a duration. Converting between the
//! two therefore always needs `created_at` as well as the value being converted — neither
//! direction is a pure `Option<u64>` <-> `Option<OffsetDateTime>` mapping. This module makes that
//! third input explicit in both function signatures rather than hiding it.
//!
//! `pollIntervalMs` is deliberately **not** handled here (or anywhere in this crate): it is
//! adapter/transport policy for how often a client should poll `tasks/get`, not state that
//! outlives a single exchange, and it has no MemPalace column to round-trip through. Neither
//! `ttlMs` nor `pollIntervalMs` becomes a new column in `mempalace_storage` — `ttlMs`'s computed
//! deadline is handed back to the caller as `retention_deadline` rather than written to any
//! column, and `pollIntervalMs` is not stored at all.

use time::{Duration, OffsetDateTime};

use crate::error::McpTasksError;

/// Convert a wire `ttlMs` into an absolute retention deadline, relative to `created_at`.
///
/// This is **not** `expires_at` — see the module docs' "This is retention, not lifecycle"
/// section. The returned value is a candidate `retention_deadline`
/// ([`crate::detailed_task::ImportedTaskProvenance::retention_deadline`]); it is the caller's
/// decision, never this crate's, whether it should also drive `NewTask::expires_at`.
///
/// `ttl_ms: None` (wire `null` or an absent field — this module treats them identically, see the
/// module docs) means "no TTL" and maps to `None`. `ttl_ms: Some(ms)` maps to `created_at + ms`
/// milliseconds.
///
/// # Errors
///
/// Returns [`McpTasksError::TtlOutOfRange`] if `created_at + ttl_ms` falls outside the range
/// [`OffsetDateTime`] can represent.
pub fn ttl_ms_to_deadline(
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

/// Convert an absolute deadline into a wire `ttlMs`, relative to `created_at`.
///
/// This is the outbound counterpart of [`ttl_ms_to_deadline`], used to emit `ttlMs` from
/// whichever absolute deadline the caller supplies — it is agnostic to what that deadline
/// means; nothing here assumes it came from (or must be) `expires_at`.
///
/// `deadline: None` means "no TTL" and maps to `ttl_ms: None`, which serializes as wire `null`
/// (see [`crate::detailed_task`]) — the correct MCP Tasks spelling of "persists indefinitely".
///
/// `deadline: Some(t)` where `t` is at or after `created_at` maps to the whole number of
/// milliseconds between them. If `t` is *before* `created_at` — an already-expired deadline, or
/// data with an inconsistent `created_at`/deadline pair — the result saturates to `0` rather than
/// producing a negative `ttlMs` the extension's `number | null` type does not anticipate; a
/// duration too large to fit `u64` milliseconds (never observed in practice, since MemPalace's
/// own lease-duration overflow guard rejects timestamps that extreme long before this function
/// would see them) saturates to [`u64::MAX`].
pub fn deadline_to_ttl_ms(
    created_at: OffsetDateTime,
    deadline: Option<OffsetDateTime>,
) -> Option<u64> {
    let deadline = deadline?;
    let millis = (deadline - created_at).whole_milliseconds();
    let ttl_ms = u64::try_from(millis).unwrap_or(if millis < 0 { 0 } else { u64::MAX });
    Some(ttl_ms)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use time::PrimitiveDateTime;

    use super::*;

    #[test]
    fn ttl_ms_none_maps_to_no_expiry() {
        let created_at = OffsetDateTime::now_utc();
        assert_eq!(ttl_ms_to_deadline(created_at, None).unwrap(), None);
    }

    #[test]
    fn ttl_ms_some_maps_to_created_at_plus_duration() {
        let created_at = OffsetDateTime::now_utc();
        let expires_at = ttl_ms_to_deadline(created_at, Some(60_000)).unwrap().unwrap();
        assert_eq!(expires_at, created_at + Duration::minutes(1));
    }

    #[test]
    fn no_expiry_maps_to_ttl_ms_none() {
        let created_at = OffsetDateTime::now_utc();
        assert_eq!(deadline_to_ttl_ms(created_at, None), None);
    }

    #[test]
    fn expires_at_maps_to_the_elapsed_milliseconds() {
        let created_at = OffsetDateTime::now_utc();
        let expires_at = created_at + Duration::minutes(1);
        assert_eq!(deadline_to_ttl_ms(created_at, Some(expires_at)), Some(60_000));
    }

    #[test]
    fn expires_at_before_created_at_saturates_to_zero() {
        let created_at = OffsetDateTime::now_utc();
        let expires_at = created_at - Duration::minutes(1);
        assert_eq!(deadline_to_ttl_ms(created_at, Some(expires_at)), Some(0));
    }

    /// [`ttl_ms_to_deadline`] has two fallible operations, either of which can produce
    /// [`McpTasksError::TtlOutOfRange`]: converting `ttl_ms: u64` to `i64` (this test), and
    /// `created_at.checked_add(...)` (the next test). This one drives the first: a `ttl_ms` too
    /// large to fit in `i64` fails the `u64 -> i64` conversion regardless of `created_at`.
    #[test]
    fn ttl_ms_exceeding_i64_range_is_out_of_range() {
        let created_at = OffsetDateTime::now_utc();
        let ttl_ms = u64::try_from(i64::MAX).unwrap() + 1;
        let err = ttl_ms_to_deadline(created_at, Some(ttl_ms))
            .expect_err("ttl_ms beyond i64::MAX must be rejected, not silently truncated");
        assert!(matches!(err, McpTasksError::TtlOutOfRange));
    }

    /// The second fallible operation in [`ttl_ms_to_deadline`]: `ttl_ms` fits comfortably in
    /// `i64`, but `created_at` is already at [`OffsetDateTime`]'s representable maximum (without
    /// the `large-dates` feature, year 9999), so adding any positive duration overflows.
    #[test]
    fn created_at_plus_ttl_ms_past_offset_date_time_max_is_out_of_range() {
        let created_at = PrimitiveDateTime::MAX.assume_utc();
        let err = ttl_ms_to_deadline(created_at, Some(1))
            .expect_err("created_at + ttl_ms past OffsetDateTime::MAX must be rejected");
        assert!(matches!(err, McpTasksError::TtlOutOfRange));
    }

    #[test]
    fn round_trips_through_both_directions() {
        let created_at = OffsetDateTime::now_utc();
        let original_ttl_ms = Some(123_456_u64);
        let expires_at = ttl_ms_to_deadline(created_at, original_ttl_ms).unwrap();
        assert_eq!(deadline_to_ttl_ms(created_at, expires_at), original_ttl_ms);
    }
}
