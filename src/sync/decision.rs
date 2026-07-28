//! Pure sync-loop decisions — no async, no locks, no I/O.
//!
//! Each function below names one branch that `run`'s steady-state loop used to
//! make inline, so it can be unit-tested in isolation. Semantics match the
//! previous inline expressions EXACTLY; see the per-function boundary notes.

use std::time::{Duration, Instant};

/// Cache size below which we skip the mass-drop resync check — small mempools
/// can legitimately shrink by more than 80% during normal operation (e.g. a few
/// txs all confirming in one block), which would otherwise trigger a needless
/// full resync.
pub(crate) const MASS_DROP_MIN_CACHE_SIZE: usize = 100;

/// Reciprocal of the mass-drop threshold: we treat the cache as having suffered
/// a mass drop when the node reports fewer than `cache_len / MASS_DROP_INVERSE`
/// txs, i.e. the node holds less than 1/5 (20%) of what we have cached.
pub(crate) const MASS_DROP_INVERSE: usize = 5;

/// Minimum time between `bulk_resync` calls triggered by desync detection
/// (node reload / mass drop). Without this, a provider or node that flaps
/// `loaded=false` or oscillates mempool size can force a full verbose
/// mempool download on nearly every tick.
pub(crate) const RESYNC_COOLDOWN: Duration = Duration::from_secs(60);

/// True when the cache is large enough to bother checking AND the node now
/// reports less than 1/`MASS_DROP_INVERSE` of what we have cached.
///
/// BOUNDARY: the comparison is strict `<` — if `node_txid_count * 5` exactly
/// equals `cache_len` (e.g. cache 100, node 20) this is NOT a mass drop.
pub(crate) fn is_mass_drop(cache_len: usize, node_txid_count: usize) -> bool {
    cache_len >= MASS_DROP_MIN_CACHE_SIZE
        && node_txid_count.saturating_mul(MASS_DROP_INVERSE) < cache_len
}

/// True while a prior bulk resync is still within `RESYNC_COOLDOWN` of `now`.
///
/// The caller passes `Instant::now()` at the decision site (not tick start), so
/// the cooldown window is measured from the moment the desync is being decided.
pub(crate) fn resync_cooling_down(last: Option<Instant>, now: Instant) -> bool {
    last.is_some_and(|t| now.duration_since(t) < RESYNC_COOLDOWN)
}

/// What to do when a desync is detected. `reason` is carried through for the
/// `set_synced` staleness log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DesyncAction {
    /// A bulk resync is warranted but still in cooldown; mark stale and wait.
    WaitCooldown { reason: &'static str },
    /// Do a full bulk resync now.
    BulkResync { reason: &'static str },
}

/// Decide whether — and how — to react to the node's current state.
///
/// Returns `None` when in sync (`loaded && !mass_drop`). Otherwise the reason
/// is `"node_not_loaded"` when the node isn't loaded (PRECEDENCE: this wins even
/// if a mass drop is also present), else `"mass_drop"`; and the action is
/// `WaitCooldown` while cooling down, `BulkResync` otherwise.
pub(crate) fn decide_desync(
    loaded: bool,
    cache_len: usize,
    node_txid_count: usize,
    cooling_down: bool,
) -> Option<DesyncAction> {
    let mass_drop = is_mass_drop(cache_len, node_txid_count);
    if loaded && !mass_drop {
        return None;
    }
    let reason = if !loaded {
        "node_not_loaded"
    } else {
        "mass_drop"
    };
    Some(if cooling_down {
        DesyncAction::WaitCooldown { reason }
    } else {
        DesyncAction::BulkResync { reason }
    })
}

/// True when this tick did not fully catch the cache up to the node: either more
/// new txids than the per-tick cap (`new_count` is the UNCAPPED `diff.new.len()`),
/// or at least one fetch errored, or the fetch was cut short (`incomplete`).
pub(crate) fn is_backlog(
    new_count: usize,
    max_per_tick: usize,
    fetch_errors: usize,
    incomplete: bool,
) -> bool {
    new_count > max_per_tick || fetch_errors > 0 || incomplete
}

/// Cache size after a tick's removals and inserts, from counts already in hand.
///
/// KEPT NON-SATURATING deliberately: matches the previous inline
/// `cache_len - gone + fetched` exactly (a `gone` derived from the cache's own
/// keys can't exceed `cache_len`, so this doesn't underflow in practice).
pub(crate) fn projected_mempool_size(cache_len: usize, gone: usize, fetched: usize) -> usize {
    cache_len - gone + fetched
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mass_drop_ignored_below_min_cache_size() {
        // cache_len 99 < MASS_DROP_MIN_CACHE_SIZE (100): never a mass drop,
        // even when the node reports zero.
        assert!(!is_mass_drop(99, 0));
        assert!(!is_mass_drop(99, 1));
    }

    #[test]
    fn mass_drop_boundary_at_min_cache_size() {
        // cache_len 100 is the first size we check.
        // node 20 -> 20*5 == 100, NOT < 100 -> NOT a mass drop (strict `<`).
        assert!(!is_mass_drop(100, 20));
        // node 19 -> 19*5 == 95 < 100 -> mass drop.
        assert!(is_mass_drop(100, 19));
        // node 0 -> 0 < 100 -> mass drop.
        assert!(is_mass_drop(100, 0));
    }

    #[test]
    fn mass_drop_saturates_instead_of_overflowing() {
        // node_txid_count * 5 would overflow usize; saturating_mul clamps to
        // usize::MAX, which is not < cache_len -> not a mass drop.
        assert!(!is_mass_drop(1_000_000, usize::MAX));
    }

    #[test]
    fn cooling_down_none_and_boundaries() {
        let now = Instant::now();
        // No prior resync -> never cooling down.
        assert!(!resync_cooling_down(None, now));
        // Just resynced -> cooling down.
        assert!(resync_cooling_down(Some(now), now));
        // Within the window -> cooling down.
        assert!(resync_cooling_down(
            Some(now - Duration::from_secs(59)),
            now
        ));
        // At/after the window (>= RESYNC_COOLDOWN) -> not cooling down (strict `<`).
        assert!(!resync_cooling_down(Some(now - RESYNC_COOLDOWN), now));
        assert!(!resync_cooling_down(
            Some(now - Duration::from_secs(61)),
            now
        ));
    }

    #[test]
    fn decide_desync_in_sync_returns_none() {
        // loaded && !mass_drop -> None regardless of cooldown.
        assert_eq!(decide_desync(true, 100, 100, false), None);
        assert_eq!(decide_desync(true, 100, 100, true), None);
        // Small cache that shrank a lot is still "in sync" (below min size).
        assert_eq!(decide_desync(true, 99, 0, false), None);
    }

    #[test]
    fn decide_desync_precedence_node_not_loaded_wins() {
        // Both not-loaded AND mass-drop true -> reason is "node_not_loaded".
        assert_eq!(
            decide_desync(false, 100, 0, false),
            Some(DesyncAction::BulkResync {
                reason: "node_not_loaded"
            })
        );
        assert_eq!(
            decide_desync(false, 100, 0, true),
            Some(DesyncAction::WaitCooldown {
                reason: "node_not_loaded"
            })
        );
    }

    #[test]
    fn decide_desync_mass_drop_routing() {
        // loaded but mass drop, not cooling -> BulkResync/mass_drop.
        assert_eq!(
            decide_desync(true, 100, 0, false),
            Some(DesyncAction::BulkResync {
                reason: "mass_drop"
            })
        );
        // loaded but mass drop, cooling -> WaitCooldown/mass_drop.
        assert_eq!(
            decide_desync(true, 100, 0, true),
            Some(DesyncAction::WaitCooldown {
                reason: "mass_drop"
            })
        );
    }

    #[test]
    fn backlog_truth_table() {
        let max = 2000;
        // Nothing wrong -> not backlog.
        assert!(!is_backlog(0, max, 0, false));
        assert!(!is_backlog(max, max, 0, false)); // == cap, not > cap.
                                                  // Over the cap -> backlog.
        assert!(is_backlog(max + 1, max, 0, false));
        // Any fetch error -> backlog.
        assert!(is_backlog(0, max, 1, false));
        // Incomplete -> backlog.
        assert!(is_backlog(0, max, 0, true));
        // All three -> backlog.
        assert!(is_backlog(max + 1, max, 3, true));
    }

    #[test]
    fn projected_mempool_size_values() {
        // 100 cached, 10 gone, 5 fetched -> 95.
        assert_eq!(projected_mempool_size(100, 10, 5), 95);
        // No churn -> unchanged.
        assert_eq!(projected_mempool_size(50, 0, 0), 50);
        // All gone, some new -> just the new ones.
        assert_eq!(projected_mempool_size(10, 10, 3), 3);
    }
}
