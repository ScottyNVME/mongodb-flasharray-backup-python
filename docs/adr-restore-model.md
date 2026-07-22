# ADR: Restore model — Path A (single-source clone-to-all) over the original per-member restore

**Status:** Accepted (2026-07-22). **Supersedes** the original in-place restore as the default.
**Context docs:** [restore-consistency-remediation.md](restore-consistency-remediation.md), [path-a-implementation-plan.md](path-a-implementation-plan.md).

## Context

Two restore designs were on the table:

- **Original (`restore-mongo-snapshot`, shipped):** out-of-band storage restore — overwrite **each member's volume from its own snapshot**, restart agents, WiredTiger crash-recovers, RS re-forms. No OM restore API.
- **Path A:** OM-orchestrated (`volumeRestore: true`) — clone the **one OM-frozen secondary's snapshot** (pre-replicated to sibling arrays at snapshot time) onto **every** member, `filesCopied` per node.

## Decision

**Adopt Path A as the default restore. Retire the original as the default; keep it only as a clearly-labeled break-glass tool.**

Rationale (correctness dominates for a backup/restore system):

- The original is sound **only at ~zero replication lag**. Under production write load the members' independent snapshots hold divergent oplog endpoints → `OplogStartMissing` / rollback / fassert (the April 2026 incident). It is off the MongoDB third-party contract and not certifiable.
- Path A is **consistent by construction** — every member is byte-identical at one oplog point, so there is no divergence regardless of lag — and it is on the documented contract (OM restore API + `volumeRestore`), so it is certifiable.
- Restore RTO stays fast (the source is pre-replicated, so the restore-time clone is a local CoW). Path A shifts the data-movement cost to snapshot time (replication), which is the right place to pay it.

## Consequences

Accepted costs of Path A:

- **Hard dependency on the OM restore API at restore time** (the one axis where the original is better — DR independence). Mitigation: retain the original as break-glass for the case where OM is unavailable and the consistency risk is knowingly accepted.
- **Operational weight:** a complete async-replication mesh per RS (no conflicting sync-replication pods), replication capacity on sibling arrays, and re-wiring on topology change.
- Not yet proven end-to-end (blocked on a complete async mesh and two MongoDB open questions: exact `volumeRestore` placement, and whether cloning the secondary's `local`/oplog to all members is accepted `filesCopied` provenance).

Disposition:

- **Do NOT keep the original as a "fast option."** You can't tell at restore time whether a snapshot was taken at low lag, so trusting it re-imports the exact flaw.
- The original will be relabeled **break-glass** (OM-restore-API-unavailable DR only), not removed, at cutover (Phase 6).
- If MongoDB steers us to Path B (seed + `rs.add`) instead of clone-to-all, the original stays disqualified on correctness either way; only Path A's internals change.
