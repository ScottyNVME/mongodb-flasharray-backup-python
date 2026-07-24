#!/usr/bin/env bash
#
# rs-restore-demo.sh — Replica-set NON-PITR restore demo (snapshot -> mutate -> restore -> verify)
#
# Demonstrates the certified replica-set self-restore path (cert item 1.A.1.a) end-to-end against a
# standalone MongoDB replica set backed by FlashArray + Ops Manager third-party backup:
#
#   1. baseline the data
#   2. new-mongo-snapshot        (opens $backupCursor on the primary, takes an FA PG snapshot)
#   3. MUTATE                    (delete the test data + write a `sentinel` collection that did NOT
#                                 exist at snapshot time — so a no-op "restore" would be detectable)
#   4. restore-mongo-snapshot    (CoW volume overwrite -> WiredTiger crash recovery -> RS re-forms)
#   5. VERIFY                    (counts revert to the snapshot with drift 0 AND the sentinel is gone)
#
# The sentinel is the fidelity proof: a restore that reverts the counts but leaves the sentinel behind
# would be a no-op that happened to match on counts. A true point-in-time revert removes it.
#
# This is a self-restore (in place, source == target) — the only certified restore path. It is
# destructive to the target replica set's `testdb`. Run only against a lab/test deployment.
#
# Usage:
#   demo/rs-restore-demo.sh [--deployment aen-rs-00] [--tag om-YYYYMMDD-HHMMSS] [--yes]
#
#   --deployment   deployment name from .env (default: aen-rs-00; must be TOPOLOGY=replicaset)
#   --tag          snapshot tag (default: auto-generated om-<UTC timestamp>)
#   --yes          skip the interactive confirmation before the destructive steps
#
set -euo pipefail

# ---- locate repo root + venv -------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

DEPLOYMENT="aen-rs-00"
TAG=""
ASSUME_YES=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --deployment) DEPLOYMENT="$2"; shift 2 ;;
        --tag)        TAG="$2";        shift 2 ;;
        --yes|-y)     ASSUME_YES=1;    shift   ;;
        -h|--help)    sed -n '2,40p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# Put the console scripts (new-mongo-snapshot, …) on PATH and export .env for the mongosh helper.
if [[ -f .venv/bin/activate ]]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
else
    echo "ERROR: .venv not found — create it and 'pip install -e .' first (see GETTING-STARTED.md)." >&2
    exit 1
fi
set -a; . ./.env; set +a

# Snapshot tags must match ^om-\d{8}-\d{6}$ (om-YYYYMMDD-HHMMSS).
if [[ -z "${TAG}" ]]; then
    TAG="om-$(date -u +%Y%m%d-%H%M%S)"
fi
if [[ ! "${TAG}" =~ ^om-[0-9]{8}-[0-9]{6}$ ]]; then
    echo "ERROR: --tag '${TAG}' does not match ^om-YYYYMMDD-HHMMSS\$" >&2
    exit 2
fi

# ---- resolve deployment-scoped settings from .env --------------------------------------------
# .env uses <DEPLOYMENT>__KEY overrides (e.g. AEN_RS_00__CLUSTER_NODES). Build the prefix and read.
PREFIX="$(echo "${DEPLOYMENT}" | tr '[:lower:]-' '[:upper:]_')__"
get() { # get KEY  -> value of <PREFIX>KEY, falling back to bare KEY
    local key="$1" val
    val="$(printenv "${PREFIX}${key}" || true)"
    [[ -z "${val}" ]] && val="$(printenv "${key}" || true)"
    echo "${val}"
}

TOPOLOGY="$(get TOPOLOGY)"
NODES_CSV="$(get CLUSTER_NODES)"
MONGOSH="$(get MONGOSH_PATH)"
SSH_USER_V="$(get SSH_USER)"

if [[ "${TOPOLOGY}" != "replicaset" ]]; then
    echo "ERROR: deployment '${DEPLOYMENT}' has TOPOLOGY='${TOPOLOGY}', expected 'replicaset'." >&2
    echo "       This demo is for a standalone replica set. Use the sharded demo for a cluster." >&2
    exit 2
fi
if [[ -z "${NODES_CSV}" || -z "${MONGOSH}" ]]; then
    echo "ERROR: could not resolve CLUSTER_NODES / MONGOSH_PATH for '${DEPLOYMENT}' from .env." >&2
    exit 2
fi

IFS=',' read -r -a NODES <<< "${NODES_CSV}"
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=8)

# Run a mongosh --eval on a specific RS member (localhost connection on that node).
node_eval() { # node_eval HOST 'js'
    ssh "${SSH_OPTS[@]}" "${SSH_USER_V}@$1" "${MONGOSH} --quiet --eval '$2'" 2>/dev/null
}

# Find the current writable primary among the configured members.
find_primary() {
    local n
    for n in "${NODES[@]}"; do
        if [[ "$(node_eval "${n}" 'print(db.hello().isWritablePrimary)')" == "true" ]]; then
            echo "${n}"; return 0
        fi
    done
    return 1
}

# Convenience: print the three verify metrics from the primary.
show_counts() { # show_counts HOST label
    node_eval "$1" 'var d=db.getSiblingDB("testdb"); print("  loadtest="+d.loadtest.countDocuments()+"  payload="+d.payload.countDocuments()+"  sentinel="+d.sentinel.countDocuments())'
}

hr() { printf '%s\n' "------------------------------------------------------------------------"; }

echo
echo "========================================================================"
echo " Replica-set NON-PITR restore demo"
echo "   deployment : ${DEPLOYMENT}   (topology: ${TOPOLOGY})"
echo "   members    : ${NODES_CSV}"
echo "   snapshot   : ${TAG}"
echo "========================================================================"
echo
echo "This will SNAPSHOT the replica set, then DELETE testdb + write a sentinel,"
echo "then RESTORE from the snapshot and verify the delete/sentinel were reverted."
echo "It is DESTRUCTIVE to '${DEPLOYMENT}' testdb (a self-restore overwrites the"
echo "cluster's own FlashArray volumes)."
echo
if [[ "${ASSUME_YES}" -ne 1 ]]; then
    read -r -p "Proceed against '${DEPLOYMENT}'? [y/N] " ans
    [[ "${ans}" =~ ^[Yy]$ ]] || { echo "aborted."; exit 0; }
fi

# ---- 0. locate the primary + baseline --------------------------------------------------------
hr; echo "STEP 0 — locate primary and baseline"
PRIMARY="$(find_primary)" || { echo "ERROR: no writable primary reachable in ${NODES_CSV}"; exit 1; }
echo "  primary: ${PRIMARY}"
echo "  baseline data:"
show_counts "${PRIMARY}"

# ---- 1. snapshot -----------------------------------------------------------------------------
hr; echo "STEP 1 — new-mongo-snapshot (opens \$backupCursor on the primary, takes FA PG snapshot)"
new-mongo-snapshot --deployment "${DEPLOYMENT}" --snapshot-tag "${TAG}"

# ---- 2. mutate (diverge from the snapshot in BOTH directions) --------------------------------
hr; echo "STEP 2 — MUTATE: delete testdb data + insert a post-snapshot 'sentinel'"
node_eval "${PRIMARY}" 'var d=db.getSiblingDB("testdb"); d.loadtest.deleteMany({}); d.payload.deleteMany({}); d.sentinel.insertOne({s:"post-snapshot-divergence", t:new Date()}); print("  after mutation:");'
show_counts "${PRIMARY}"
echo "  (expect loadtest=0 payload=0 sentinel=1 — on-disk state now differs from the snapshot)"

# ---- 3. restore ------------------------------------------------------------------------------
hr; echo "STEP 3 — restore-mongo-snapshot --force (CoW overwrite -> WiredTiger recovery -> RS re-forms)"
restore-mongo-snapshot --deployment "${DEPLOYMENT}" --snapshot-tag "${TAG}" --force

# ---- 4. verify -------------------------------------------------------------------------------
hr; echo "STEP 4 — VERIFY fidelity (restore may elect a new primary)"
PRIMARY_AFTER="$(find_primary)" || { echo "ERROR: no writable primary after restore"; exit 1; }
echo "  primary after restore: ${PRIMARY_AFTER}"
echo "  post-restore data:"
show_counts "${PRIMARY_AFTER}"

SENTINEL="$(node_eval "${PRIMARY_AFTER}" 'print(db.getSiblingDB("testdb").sentinel.countDocuments())')"
echo
if [[ "${SENTINEL}" == "0" ]]; then
    echo "RESULT: PASS — restore STEP 8 reported drift 0 (above) AND the sentinel is GONE (${SENTINEL})."
    echo "        The delete was reverted and the post-snapshot write did not survive => true"
    echo "        point-in-time revert, not a no-op. (cert item 1.A.1.a)"
    exit 0
else
    echo "RESULT: FAIL — sentinel count is ${SENTINEL}, expected 0. The post-snapshot collection"
    echo "        survived the restore, so the volumes were not truly reverted. Investigate."
    exit 1
fi
