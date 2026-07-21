###############################################################################################################################
# Shared configuration for all mongodb-flasharray-backup scripts.
#
# Each command calls `from . import config` then `config.load_config()` at the start of its main().
# load_config() reads the .env file, throwing if it or a required key is missing.
#
# All values are loaded from a .env file. Copy .env.example to .env and fill in your values before running
# any script. The .env is located via $MONGO_FA_BACKUP_ENV, else python-dotenv's search from the CWD, else
# ./.env.
#
# Cluster topology and FlashArray volume mappings are discovered at runtime from authoritative sources
# (Ops Manager API and FlashArray SCSI serial numbers). .env values are used only for credentials and as a
# fallback when live discovery is unavailable.
###############################################################################################################################

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import socket
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlsplit

import requests
import typer
from dotenv import find_dotenv

# ---------------------------------------------------------------------------------------------------------
# Color map for operator-visible console output, expressed as click/typer color names
# (Cyan/Green/Yellow/DarkYellow/Red/DarkGray).
# ---------------------------------------------------------------------------------------------------------
CYAN = "cyan"
GREEN = "green"
YELLOW = "bright_yellow"       # "Yellow" renders bright
DARK_YELLOW = "yellow"         # "DarkYellow" renders as plain (olive) yellow
RED = "red"
DARK_GRAY = "bright_black"


def write_host(message: str, fg: Optional[str] = None) -> None:
    """Print a message to the console, optionally colored with the given foreground color."""
    typer.secho(message, fg=fg)


# region --- Load .env ---


def _read_env_file(env_file: Path) -> dict[str, str]:
    """Parse a .env file: keep lines whose first non-whitespace char is neither '#' nor whitespace, split
    on the first '=' only, and trim both sides. No quote stripping or variable expansion.
    """
    env_vars: dict[str, str] = {}
    for line in env_file.read_text().splitlines():
        if re.match(r"^\s*[^#\s]", line):
            parts = line.split("=", 1)
            if len(parts) == 2:
                env_vars[parts[0].strip()] = parts[1].strip()
    return env_vars


def get_env_var(env_vars: dict[str, str], key: str, optional: bool = False) -> Optional[str]:
    """Return the value for key, or None if optional, else raise."""
    if key in env_vars:
        return env_vars[key]
    if optional:
        return None
    raise RuntimeError(f".env is missing required key: {key}")


def deployment_env_key(deployment_name: Optional[str], key: str) -> str:
    """Return the .env key for `key` scoped to a deployment ('<NAME>__<key>'), or the flat key when no
    deployment is selected. Mirrors the prefix scheme used by load_config()."""
    if deployment_name:
        return deployment_name.upper().replace("-", "_") + "__" + key
    return key


def update_env_var(env_file: Path, key: str, value: str) -> bool:
    """Set key=value in the .env file in place, preserving all other lines and comments. Rewrites the
    first uncommented assignment to `key` if one exists; otherwise appends `key=value` at the end.
    Returns True if the value changed (or the key was added), False if it already matched."""
    lines = env_file.read_text().splitlines()
    new_line = f"{key}={value}"
    for i, line in enumerate(lines):
        if re.match(r"^\s*[^#\s]", line):
            parts = line.split("=", 1)
            if len(parts) == 2 and parts[0].strip() == key:
                if parts[1].strip() == value:
                    return False
                lines[i] = new_line
                env_file.write_text("\n".join(lines) + "\n")
                return True
    # Key not present - append it.
    lines.append(new_line)
    env_file.write_text("\n".join(lines) + "\n")
    return True


@dataclass
class Config:
    """Holds every value derived from .env. Populated by load_config()."""

    EnvVars: dict[str, str]
    # Absolute path of the .env file these values were loaded from (for in-place updates).
    EnvFilePath: Path
    # MongoDB cluster topology
    MongoshPath: str
    MongosHost: str
    MongosPort: int
    SshUser: str
    ClusterNodesFallback: Optional[list[str]]
    # Topology: 'sharded' (route via mongos/listShards) or 'replicaset' (target the RS directly).
    Topology: str
    # Selected deployment name, or None when the flat (single-deployment) keys are used.
    DeploymentName: Optional[str]
    # MongoDB database tools
    MongoToolsBase: str
    MongodumpPath: str
    MongorestorePath: str
    # Pure Storage FlashArray
    FaEndpoint: str
    FaUsername: str
    FaApiToken: Optional[str]
    FaPassword: Optional[str]
    FaApiVersion: str
    ProtectionGroupName: str
    ClusterName: str
    # Ops Manager
    OmHost: str
    OmApiVersion: str
    OpsManagerBaseUrl: str
    GroupId: str
    ClusterId: str
    OmPublicKey: str
    OmPrivateKey: str


# Module-level config singleton, available to all functions in this module. None until load_config() runs.
CFG: Optional[Config] = None


def _resolve_env_file(env_path: Optional[os.PathLike | str]) -> Path:
    if env_path is not None:
        return Path(env_path)
    override = os.environ.get("MONGO_FA_BACKUP_ENV")
    if override:
        return Path(override)
    found = find_dotenv(usecwd=True)
    if found:
        return Path(found)
    return Path.cwd() / ".env"


def load_config(
    env_path: Optional[os.PathLike | str] = None, deployment: Optional[str] = None
) -> Config:
    """Load .env and populate CFG.

    A single .env can describe several deployments. Shared infrastructure (FA/OM credentials, SSH user,
    tool paths) lives in flat keys; deployment-specific keys may be overridden per deployment with a
    "<NAME>__" prefix (NAME upper-cased, hyphens -> underscores). When no deployment is selected the flat
    keys are used as-is, so single-deployment .env files keep working unchanged.

    Deployment resolution order: `deployment` arg > MONGO_FA_BACKUP_DEPLOYMENT env > DEFAULT_DEPLOYMENT
    key in .env > None (flat keys).

    Throws if the .env file is missing or a required key is absent.
    """
    global CFG
    env_file = _resolve_env_file(env_path)
    if not env_file.exists():
        raise RuntimeError(
            f".env file not found at '{env_file}'. Copy .env.example to .env and fill in your values."
        )
    env_vars = _read_env_file(env_file)

    def g(key: str, optional: bool = False) -> Optional[str]:
        return get_env_var(env_vars, key, optional)

    # --- Deployment selection (single-.env, multi-deployment) ---
    deployment_name = (
        deployment
        or os.environ.get("MONGO_FA_BACKUP_DEPLOYMENT")
        or env_vars.get("DEFAULT_DEPLOYMENT")
        or None
    )
    dep_prefix = (deployment_name.upper().replace("-", "_") + "__") if deployment_name else ""

    def gd(key: str, optional: bool = False) -> Optional[str]:
        """Deployment-scoped get: try '<PREFIX>__<key>' first, else fall back to the flat key."""
        if dep_prefix and (dep_prefix + key) in env_vars:
            return env_vars[dep_prefix + key]
        return get_env_var(env_vars, key, optional)

    # --- MongoDB cluster topology ---
    mongosh_path = g("MONGOSH_PATH")
    mongos_host = gd("MONGOS_HOST")
    mongos_port = int(gd("MONGOS_PORT"))
    ssh_user = g("SSH_USER")
    # 'sharded' (default, route via mongos/listShards) or 'replicaset' (target the single RS directly).
    topology = (gd("TOPOLOGY", optional=True) or "sharded").strip().lower()
    if topology not in ("sharded", "replicaset"):
        raise RuntimeError(f"TOPOLOGY must be 'sharded' or 'replicaset', got '{topology}'")
    # CLUSTER_NODES is optional - fallback only when Ops Manager is unreachable.
    cluster_nodes_raw = gd("CLUSTER_NODES", optional=True)
    cluster_nodes_fallback = cluster_nodes_raw.split(",") if cluster_nodes_raw else None

    # --- MongoDB database tools ---
    mongo_tools_base = g("MONGO_TOOLS_BASE")
    mongodump_path = f"{mongo_tools_base}/mongodump"
    mongorestore_path = f"{mongo_tools_base}/mongorestore"

    # --- Pure Storage FlashArray ---
    fa_endpoint = g("FA_ENDPOINT")
    fa_username = g("FA_USERNAME")
    # Authenticate with EITHER FA_APITOKEN (array-local) OR FA_USERNAME+FA_PASSWORD (directory login,
    # which authorizes on every fleet member). Both are optional individually; at least one auth method
    # must be present or connect_fa() fails.
    fa_api_token = g("FA_APITOKEN", optional=True)
    fa_password = g("FA_PASSWORD", optional=True)
    # FA REST API version to pin (skips version auto-negotiation).
    fa_api_version = g("FA_API_VERSION", optional=True) or "2.51"
    protection_group_name = gd("FA_PROTECTION_GROUP")
    cluster_name = gd("FA_CLUSTER_NAME")

    # --- Ops Manager ---
    om_host = g("OM_BASE_URL").rstrip("/")
    om_api_version = g("OM_API_VERSION")
    ops_manager_base_url = f"{om_host}/api/public/{om_api_version}"
    group_id = g("OM_GROUP_ID")
    cluster_id = gd("OM_CLUSTER_ID")
    om_public_key = g("OM_PUBLIC_KEY")
    om_private_key = g("OM_PRIVATE_KEY")

    CFG = Config(
        EnvVars=env_vars,
        EnvFilePath=env_file,
        MongoshPath=mongosh_path,
        MongosHost=mongos_host,
        MongosPort=mongos_port,
        SshUser=ssh_user,
        ClusterNodesFallback=cluster_nodes_fallback,
        Topology=topology,
        DeploymentName=deployment_name,
        MongoToolsBase=mongo_tools_base,
        MongodumpPath=mongodump_path,
        MongorestorePath=mongorestore_path,
        FaEndpoint=fa_endpoint,
        FaUsername=fa_username,
        FaApiToken=fa_api_token,
        FaPassword=fa_password,
        FaApiVersion=fa_api_version,
        ProtectionGroupName=protection_group_name,
        ClusterName=cluster_name,
        OmHost=om_host,
        OmApiVersion=om_api_version,
        OpsManagerBaseUrl=ops_manager_base_url,
        GroupId=group_id,
        ClusterId=cluster_id,
        OmPublicKey=om_public_key,
        OmPrivateKey=om_private_key,
    )
    return CFG


def _require_cfg() -> Config:
    if CFG is None:
        raise RuntimeError("Config not loaded. Call config.load_config() first.")
    return CFG


# endregion

# region --- SSH options ---

# SSH options: never prompt interactively, fail fast on auth/host-key issues, and multiplex concurrent ssh
# invocations onto a single TCP connection per remote host (ControlMaster), which avoids saturating sshd's
# MaxStartups limit. These are passed verbatim to the system `ssh`/`scp`.
SSH_OPTS: list[str] = [
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=no",
    "-o", "ConnectTimeout=15",
    "-o", "ControlMaster=auto",
    "-o", "ControlPath=/tmp/ssh-mux-%C",
    "-o", "ControlPersist=60s",
]

# endregion

# region --- Shared helpers (locking + parallel) ---


def _as_int(value: str) -> Optional[int]:
    """Returns the int parsed from value, or None if it cannot be parsed."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _process_alive(pid: int) -> bool:
    """Returns True if a process with the given pid exists, False otherwise."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def new_script_lock(lock_path: str) -> None:
    """Atomically acquires a per-script lock file, writing pid/host/started.
    Throws if a live process already holds the lock; clears and re-acquires if the PID is dead."""
    p = Path(lock_path)
    if p.exists():
        locked_pid: Optional[str] = None
        try:
            for line in p.read_text().splitlines():
                if line.startswith("pid="):
                    locked_pid = line.split("=", 1)[1].strip()
                    break
        except OSError:
            locked_pid = None
        if locked_pid and _as_int(locked_pid) is not None:
            if _process_alive(int(locked_pid)):
                raise RuntimeError(
                    f"Lock held by live PID {locked_pid} (lock file: {lock_path}). Another run is in progress."
                )
            write_host(f"  Stale lock detected (PID {locked_pid} not running) - removing {lock_path}", fg=DARK_YELLOW)
            try:
                p.unlink()
            except OSError:
                pass
        else:
            raise RuntimeError(
                f"Lock file exists but has no parseable PID: {lock_path}. Inspect and delete manually if no script is running."
            )

    # O_CREAT|O_EXCL is atomic - fails if the file already exists, so two concurrent starts can't both win
    # the race past the exists() check above.
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except OSError as e:
        raise RuntimeError(
            f"Could not acquire lock at {lock_path}: {e}. Another run may have started concurrently."
        )
    try:
        content = (
            f"pid={os.getpid()}\n"
            f"host={socket.gethostname()}\n"
            f"started={datetime.now(timezone.utc).isoformat()}\n"
        )
        os.write(fd, content.encode("utf-8"))
    finally:
        os.close(fd)


def remove_script_lock(lock_path: str) -> None:
    """Releases the script lock. Idempotent; safe to call from finally blocks."""
    p = Path(lock_path)
    if p.exists():
        try:
            p.unlink()
        except OSError:
            pass


def invoke_parallel_or_throw(
    input_objects: list[Any],
    script_block: Callable[[Any], dict],
    step_name: str,
    throttle_limit: int = 10,
) -> list[dict]:
    """Runs script_block in parallel across input_objects.
    Each call must return a dict with at minimum {'Success': bool, 'Message': ...} plus a key field named
    either 'Key' or 'Node'. Throws if any item failed. Returns the list of result dicts."""
    with ThreadPoolExecutor(max_workers=throttle_limit) as ex:
        results = list(ex.map(script_block, input_objects))
    failures = [r for r in results if not r.get("Success")]
    if failures:
        lines = []
        for f in failures:
            ident = f.get("Key") if f.get("Key") else f.get("Node")
            lines.append(f"  [{ident}] {f.get('Message')}")
        msg = "\n".join(lines)
        raise RuntimeError(f"{step_name} failed on {len(failures)} item(s):\n{msg}")
    return results


# endregion

# region --- HTTP Digest auth for Ops Manager API ---

# Implements the standard two-step Digest challenge/response flow (RFC 2617 / qop=auth) manually. Shared by
# snapshot, restore, tailer and replay.


def get_md5_hash(plain_text: str) -> str:
    """Returns the lowercase hex MD5 of the UTF-8 bytes of plain_text."""
    return hashlib.md5(plain_text.encode("utf-8")).hexdigest()


def _match(text: str, pattern: str) -> str:
    """Returns capture group 1 of pattern in text, or '' if no match."""
    m = re.search(pattern, text)
    return m.group(1) if m else ""


def _path_and_query(uri: str) -> str:
    """Returns the path and query portion of uri (e.g. '/foo?bar=1')."""
    parts = urlsplit(uri)
    return parts.path + (f"?{parts.query}" if parts.query else "")


def invoke_om_api(
    method: str = "GET",
    path: str = "",
    body: Any = None,
    path_prefix: str = "backup/third_party/",
) -> Any:
    """Manual Digest auth against the Ops Manager API.

    path_prefix controls what is inserted between the base URL and path. Default 'backup/third_party/' for the
    third-party backup API; pass '' to reach the public API root (e.g. /groups/{id}/hosts)."""
    cfg = _require_cfg()
    uri = f"{cfg.OpsManagerBaseUrl}/{path_prefix}{path}"

    body_data: Optional[str] = None
    content_type: Optional[str] = None
    if body:
        body_data = json.dumps(body, separators=(",", ":"))
        content_type = "application/json"
    elif method != "GET":
        # OM requires Content-Type: application/json on all POST/DELETE even when the body is empty
        body_data = "{}"
        content_type = "application/json"

    # Step 1: Probe to get Digest challenge - server returns 401. Include Content-Type on the probe for
    # non-GET requests to avoid 415.
    probe_headers = {"Accept": "application/json"}
    if method != "GET":
        probe_headers["Content-Type"] = "application/json"
    probe = requests.request(method, uri, headers=probe_headers, data=body_data, timeout=30)
    if probe.status_code != 401:
        raise RuntimeError(
            f"Expected 401 Digest challenge from {uri} but got HTTP {probe.status_code}: {probe.text}"
        )
    challenge = probe.headers.get("WWW-Authenticate", "")

    # Step 2: Parse challenge fields
    realm = _match(challenge, r'realm="([^"]*)"')
    nonce = _match(challenge, r'nonce="([^"]*)"')
    qop = _match(challenge, r'qop="?([^",\s]*)"?')
    opaque = _match(challenge, r'opaque="([^"]*)"')

    # Step 3: Compute Digest response (RFC 2617 / qop=auth)
    uri_path = _path_and_query(uri)
    ha1 = get_md5_hash(f"{cfg.OmPublicKey}:{realm}:{cfg.OmPrivateKey}")
    ha2 = get_md5_hash(f"{method}:{uri_path}")
    nc = "00000001"
    cnonce = uuid.uuid4().hex[:8]

    if qop in ("auth", "auth-int"):
        digest_response = get_md5_hash(f"{ha1}:{nonce}:{nc}:{cnonce}:{qop}:{ha2}")
        auth_header = (
            f'Digest username="{cfg.OmPublicKey}", realm="{realm}", nonce="{nonce}", '
            f'uri="{uri_path}", qop={qop}, nc={nc}, cnonce="{cnonce}", response="{digest_response}"'
        )
    else:
        digest_response = get_md5_hash(f"{ha1}:{nonce}:{ha2}")
        auth_header = (
            f'Digest username="{cfg.OmPublicKey}", realm="{realm}", nonce="{nonce}", '
            f'uri="{uri_path}", response="{digest_response}"'
        )
    if opaque:
        auth_header += f', opaque="{opaque}"'

    # Step 4: Authenticated request
    headers = {"Accept": "application/json", "Authorization": auth_header}
    if content_type:
        headers["Content-Type"] = content_type
    resp = requests.request(method, uri, headers=headers, data=body_data, timeout=30)
    resp.raise_for_status()
    text = resp.text
    if not text:
        return None
    try:
        return resp.json()
    except ValueError:
        return text


def invoke_om_api_with_retry(
    method: str = "GET",
    path: str = "",
    body: Any = None,
    max_attempts: int = 5,
    backoff_sec: int = 5,
) -> Any:
    """Retries transient errors (network blips, 5xx). Does NOT retry permanent 4xx errors."""
    for attempt in range(1, max_attempts + 1):
        try:
            return invoke_om_api(method=method, path=path, body=body)
        except Exception as e:  # noqa: BLE001 - intentional catch-all for transient-error retry
            msg = str(e)
            # Don't retry on permanent client errors
            if re.search(r"\b(400|401|403|404|409|415|422)\b", msg):
                raise
            if attempt == max_attempts:
                raise
            write_host(f"    Transient error on attempt {attempt}: {msg} - retrying in {backoff_sec}s", fg=DARK_YELLOW)
            time.sleep(backoff_sec)


# endregion

# region --- Mongo shell + snapshot-state helpers ---


def invoke_mongos(eval_str: str) -> str:
    """Runs JavaScript against the mongos router over SSH."""
    cfg = _require_cfg()
    remote = f"{cfg.MongoshPath} --quiet --eval '{eval_str}' mongodb://{cfg.MongosHost}:{cfg.MongosPort} 2>/dev/null"
    proc = subprocess.run(
        ["ssh", *SSH_OPTS, f"{cfg.SshUser}@{cfg.MongosHost}", remote],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"mongosh failed (exit {proc.returncode}): {proc.stdout}")
    return proc.stdout.strip()


# Shared mongosh JavaScript snippets. Centralized here so Snapshot, Replay, and Tailer all emit the same
# shape. Python strings do not interpolate $, so the literal $natural is written directly.
LIST_SHARDS_JS = (
    'var shards=db.adminCommand({listShards:1}).shards; var out=[]; '
    'for(var i=0;i<shards.length;i++){var s=shards[i]; '
    'out.push({shardId:s._id,rsHosts:s.host,host:s.host.split("/")[1].split(",")[0]});} '
    'print(JSON.stringify(out));'
)
OPLOG_TOP_JS = (
    'var latest=db.getSiblingDB("local").oplog.rs.find().sort({"$natural":-1}).limit(1).toArray()[0]; '
    'print(JSON.stringify({t:latest.ts.t,i:latest.ts.i}));'
)


def invoke_mongosh_js(
    ssh_target: str,
    uri: str,
    js: str,
    max_attempts: int = 1,
    context: str = "mongosh",
) -> str:
    """Runs a mongosh --eval expression on a remote host via SSH and returns the raw stdout. Throws on
    non-zero exit (after any requested retries). Caller parses the returned string."""
    cfg = _require_cfg()
    last_exit = 0
    raw: Optional[str] = None
    remote = f"{cfg.MongoshPath} --quiet --eval '{js}' {uri} 2>/dev/null"
    for attempt in range(1, max_attempts + 1):
        proc = subprocess.run(
            ["ssh", *SSH_OPTS, f"{cfg.SshUser}@{ssh_target}", remote],
            capture_output=True,
            text=True,
        )
        raw = proc.stdout
        last_exit = proc.returncode
        if last_exit == 0:
            return raw
        if attempt < max_attempts:
            sleep_sec = int(math.pow(2, attempt - 1))
            write_host(f"    {context} attempt {attempt} failed (exit {last_exit}) - retrying in {sleep_sec}s ...", fg=YELLOW)
            time.sleep(sleep_sec)
    raise RuntimeError(f"{context} failed after {max_attempts} attempt(s) (exit {last_exit}, output: {raw})")


def wait_om_snapshot_state(
    snapshot_id: str,
    target_state: str,
    timeout_minutes: int = 150,
    poll_interval_sec: int = 10,
) -> None:
    """Polls the third-party backup API until the snapshot reaches target_state, aborting on FAILED/FAILING
    or after a timeout."""
    cfg = _require_cfg()
    state = ""
    deadline = time.monotonic() + timeout_minutes * 60
    while state != target_state:
        if time.monotonic() > deadline:
            raise RuntimeError(f"Snapshot {snapshot_id} timed out waiting for {target_state} state.")
        if state in ("FAILED", "FAILING"):
            raise RuntimeError(f"Snapshot {snapshot_id} entered {state} state - aborting.")
        # Check before sleeping so we catch an immediate transition without paying a full interval.
        status_response = invoke_om_api_with_retry(
            path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}/snapshot/{snapshot_id}"
        )
        state = status_response.get("state") if isinstance(status_response, dict) else getattr(status_response, "state", "")
        write_host(f"  {datetime.now().strftime('%H:%M:%S')}  state = {state}", fg=CYAN)
        if state != target_state:
            time.sleep(poll_interval_sec)


# endregion

# region --- FlashArray connection + response helpers (direct REST) ---

# The direct-REST client (fa_rest.Client) returns ValidResponse (with .items) or ErrorResponse, and does
# NOT raise on API errors. _fa() centralizes the unwrap:
#   allow_error=False  -> raise on ErrorResponse
#   allow_error=True   -> return [] on ErrorResponse
# Callers can then test the returned list for truthiness instead of handling errors at each call site.


def connect_fa(verify_ssl: bool = False):
    """Connect to the FlashArray fleet via direct REST (fa_rest.Client) and authenticate the gateway.

    Per-array routing is done by connecting DIRECTLY to each fleet member, because the gateway's
    `context_names` routing is not permitted for a token identity here. Auth prefers FA_USERNAME+FA_PASSWORD
    (directory login -> per-array api token, authorized fleet-wide) and falls back to FA_APITOKEN
    (array-local). verify_ssl is accepted for signature parity; TLS verification is always disabled."""
    cfg = _require_cfg()
    import sys
    from . import fa_rest

    try:
        return fa_rest.Client(
            gateway=cfg.FaEndpoint,
            version=cfg.FaApiVersion,
            api_token=cfg.FaApiToken,
            username=cfg.FaUsername,
            password=cfg.FaPassword,
        ).connect()
    except Exception as exc:  # noqa: BLE001
        print(f"Authentication failed: {exc}")
        sys.exit(1)


def _fa(resp: Any, allow_error: bool = False) -> list:
    """Unwrap a fa_rest response to a list of items (see region note)."""
    from . import fa_rest

    if isinstance(resp, fa_rest.ValidResponse):
        return list(resp.items)
    if allow_error:
        return []
    errs = getattr(resp, "errors", None)
    raise RuntimeError(f"FlashArray API error: {errs}")


# endregion

# region --- Runtime topology discovery ---


def get_cluster_nodes() -> list[str]:
    """Returns the hostnames of all nodes belonging to ClusterId in GroupId.
    Queries the OM /hosts API first (authoritative), falling back to CLUSTER_NODES from .env if OM is
    unreachable. Throws if neither source can provide nodes."""
    cfg = _require_cfg()
    try:
        # Resolve the clusterName for ClusterId and collect all sibling/child cluster IDs.
        clusters_response = invoke_om_api(path=f"groups/{cfg.GroupId}/clusters", path_prefix="")
        results = clusters_response.get("results", []) if isinstance(clusters_response, dict) else []
        parent_cluster = next((c for c in results if c.get("id") == cfg.ClusterId), None)
        if not parent_cluster:
            raise RuntimeError(f"ClusterId '{cfg.ClusterId}' not found in group '{cfg.GroupId}'.")
        om_cluster_name = parent_cluster.get("clusterName")
        # Collect IDs of all clusters in the group that share this clusterName (parent + all shards).
        all_cluster_ids = [c.get("id") for c in results if c.get("clusterName") == om_cluster_name]

        # The /hosts endpoint returns all agents in the group; filter to hosts whose clusterId matches any
        # of the cluster IDs collected above (shard or config RS members).
        hosts_response = invoke_om_api(path=f"groups/{cfg.GroupId}/hosts", path_prefix="")
        host_results = hosts_response.get("results", []) if isinstance(hosts_response, dict) else []
        nodes = sorted({h.get("hostname") for h in host_results if h.get("clusterId") in all_cluster_ids})
        if len(nodes) > 0:
            write_host(f"  Cluster nodes discovered from Ops Manager ({len(nodes)}): {', '.join(nodes)}", fg=CYAN)
            return nodes
        write_host(f"  WARNING: OM returned 0 hosts for clusterId={cfg.ClusterId} - falling back to .env", fg=YELLOW)
    except Exception as e:  # noqa: BLE001 - intentional catch-all so any OM failure falls back to .env
        write_host(f"  WARNING: OM node discovery failed ({e}) - falling back to .env", fg=YELLOW)
    if not cfg.ClusterNodesFallback:
        raise RuntimeError("Could not discover cluster nodes from Ops Manager and CLUSTER_NODES is not set in .env")
    write_host(
        f"  Using CLUSTER_NODES from .env ({len(cfg.ClusterNodesFallback)} nodes): {', '.join(cfg.ClusterNodesFallback)}",
        fg=DARK_YELLOW,
    )
    return cfg.ClusterNodesFallback


def resolve_fa_context_names(fa: Any, pg_name: str) -> list[str]:
    """Returns the short names of all fleet FlashArrays that have pg_name as a protection group. Filters
    out FlashBlades. Throws if no arrays have the PG."""
    # Get all fleet members with minimal retries (fast path)
    fleet_members = None
    for attempt in range(1, 3):
        try:
            fleet_members = _fa(fa.get_fleets_members())
            break
        except Exception as e:  # noqa: BLE001
            if attempt < 2:
                write_host(f"  WARNING: fleet-member lookup attempt {attempt} failed: {e}. Retrying in 2s...", fg=YELLOW)
                time.sleep(2)
            else:
                raise
    all_member_names = [m.member.name for m in fleet_members]
    write_host(f"  Fleet members discovered ({len(all_member_names)}): {', '.join(all_member_names)}", fg=CYAN)

    # Classify each member as FlashArray vs. other (FlashBlade, etc.) by probing get_arrays directly.
    # A transient network/auth blip returns the same empty result as a genuine non-FlashArray, so retry
    # the probe (same 2-attempt treatment as the fleet-member call above) before concluding "not a
    # FlashArray" — otherwise a momentary hiccup could silently drop a real FlashArray from the backup set.
    flasharray_names: list[str] = []
    for member_name in all_member_names:
        array_info: list = []
        for attempt in range(1, 3):
            array_info = _fa(fa.get_arrays(context_names=[member_name]), allow_error=True)
            if array_info:
                break
            if attempt < 2:
                time.sleep(2)
        if array_info and array_info[0].os == "Purity//FA":
            flasharray_names.append(member_name)
        else:
            write_host(f"    {member_name}: not a FlashArray (skipped)", fg=DARK_GRAY)
    write_host(f"  FlashArrays in fleet ({len(flasharray_names)}): {', '.join(flasharray_names)}", fg=CYAN)

    # Check which FlashArrays have the protection group (no retries - fast and reliable)
    contexts_with_pg: list[str] = []
    for array_name in flasharray_names:
        pg = _fa(fa.get_protection_groups(context_names=[array_name], names=[pg_name]), allow_error=True)
        if not pg:
            write_host(f"    {array_name}: PG '{pg_name}' NOT found", fg=DARK_GRAY)
            continue
        # The PG can exist on a fleet array but hold no volumes (e.g. after a node/shard removal +
        # prune left the PG empty on that array). Snapshotting an empty PG fails ("Protection group
        # has no volumes to snapshot"), and restore would expect a snapshot there that never gets
        # taken. Only treat an array as a backup target if its PG actually has volume members.
        members = _fa(
            fa.get_protection_groups_volumes(context_names=[array_name], group_names=[pg_name]),
            allow_error=True,
        )
        if members:
            contexts_with_pg.append(array_name)
            write_host(f"    {array_name}: PG '{pg_name}' present ({len(members)} volume(s))", fg=CYAN)
        else:
            write_host(f"    {array_name}: PG '{pg_name}' present but EMPTY (no volumes) - skipping", fg=DARK_GRAY)
    if len(contexts_with_pg) == 0:
        raise RuntimeError(
            f"No FlashArrays found with protection group '{pg_name}'. Run initialize-protection-groups first."
        )
    write_host(f"  Resolved {len(contexts_with_pg)} FlashArray(s) with PG.", fg=GREEN)
    return contexts_with_pg


def get_fa_snapshot_tags(fa: Any, context_names: list[str], snapshot_name: str) -> dict[str, str]:
    """Reads metadata tags from a PG snapshot, trying each array in context_names in order. Returns a dict
    of tag key -> value from the first array that returns any tag."""
    for ctx_name in context_names:
        tags = _fa(
            fa.get_protection_group_snapshots_tags(context_names=[ctx_name], resource_names=[snapshot_name]),
            allow_error=True,
        )
        if tags and len(tags) > 0:
            tag_map: dict[str, str] = {}
            for t in tags:
                tag_map[t.key] = t.value
            write_host(f"  Snapshot tags loaded from {ctx_name} ({len(tags)} tags)", fg=CYAN)
            return tag_map
    write_host(
        f"  WARNING: No snapshot tags found on any of {len(context_names)} array(s) for '{snapshot_name}'",
        fg=YELLOW,
    )
    return {}


# SCSI serial discovery command, used by resolve_node_to_array_volume_map. Primary path: findmnt -> lsblk
# PKNAME -> lsblk SERIAL. Fallback (volume unmounted, e.g. mid-restore): scan all disks for an FA-format serial.
_SERIAL_CMD = (
    'p=$(findmnt -no SOURCE /data/mongo 2>/dev/null); if [ -n "$p" ]; then '
    'pk=$(lsblk -no PKNAME "$p" 2>/dev/null); lsblk -no SERIAL "/dev/$pk" 2>/dev/null | head -1; '
    'else lsblk -dno SERIAL 2>/dev/null | grep -E "^[0-9a-fA-F]{20,}$" | head -1; fi'
)


def resolve_node_to_array_volume_map(
    fa: Any,
    nodes: list[str],
    ssh_user_param: str,
    ssh_opts_param: list[str],
    context_names: list[str],
) -> dict[str, dict[str, str]]:
    """For each node, SSH to read the SCSI serial of the device backing /data/mongo, then query each fleet
    array to find which one owns a volume with that serial.
    Returns node -> {'ShortName': <array>, 'VolumeName': <vol>}. Throws if any node cannot be resolved."""
    node_map: dict[str, dict[str, str]] = {}
    for node in nodes:
        # Pure FlashArray volume serials are 24-character NAA-format hex strings. Enforce a minimum length
        # so garbled output cannot pass validation and silently match the wrong volume.
        serial: Optional[str] = None
        for attempt in range(1, 4):
            if serial:
                break
            proc = subprocess.run(
                ["ssh", *ssh_opts_param, f"{ssh_user_param}@{node}", _SERIAL_CMD],
                capture_output=True,
                text=True,
            )
            candidates = [
                line.strip() for line in proc.stdout.splitlines() if re.match(r"^[0-9a-fA-F]{20,}$", line.strip())
            ]
            serial = candidates[-1].lower() if candidates else None
            if not serial and attempt < 3:
                time.sleep(1)
        if not serial or not re.match(r"^[0-9a-f]{20,}$", serial):
            raise RuntimeError(
                f"Could not read FA volume serial from {node} (got: '{serial}'). "
                "Verify /data/mongo is mounted and the block device is a Pure Storage pRDM."
            )
        write_host(f"  {node} serial: {serial}", fg=CYAN)

        # Query each context array to find which one owns the volume with this serial.
        # FA stores serials as uppercase hex; apply upper() in the filter string.
        found = False
        for ctx_name in context_names:
            vols = _fa(
                fa.get_volumes(context_names=[ctx_name], filter=f"serial='{serial.upper()}'"),
                allow_error=True,
            )
            if vols:
                vol = vols[0]
                node_map[node] = {"ShortName": ctx_name, "VolumeName": vol.name, "Serial": serial}
                write_host(f"    -> {ctx_name} / {vol.name}", fg=GREEN)
                found = True
                break
        if not found:
            searched = ", ".join(context_names)
            raise RuntimeError(
                f"No FlashArray volume with serial '{serial}' found for node {node} on any of the "
                f"{len(context_names)} array(s) searched ({searched}). If this volume lives on a fleet "
                "array not listed here, that array is not a current backup target (it has no protection "
                "group) — run Initialize-ProtectionGroups to add it. Otherwise verify the pRDM is "
                "presented from the expected array."
            )
    return node_map


# Multi-volume discovery: the full downward device tree of the data mount, so a mount backed by an LVM VG
# over several PVs (each a FlashArray volume, possibly via device-mapper multipath) resolves to ALL its
# backing FA volumes. -s = inverse (toward physical); -r = raw (parsed by token, robust to empty columns).
_MULTI_SERIAL_CMD = (
    'src=$(findmnt -no SOURCE --target /data/mongo 2>/dev/null); [ -z "$src" ] && exit 0; '
    'lsblk -s -rno TYPE,NAME,SERIAL,WWN "$src" 2>/dev/null'
)


def parse_fa_volume_serials(lsblk_output: str) -> list[str]:
    """Extract the distinct Pure FlashArray volume serials backing a mount, from `lsblk -s -rno
    TYPE,NAME,SERIAL,WWN` output. A serial comes from the SERIAL column (24-hex, a direct pRDM) or the WWN
    column (NAA `0x624a9370<serial>`, a multipath device). Order-preserving and de-duplicated, so multiple
    paths to the same FA volume collapse to one. Pure (unit-testable)."""
    serials: list[str] = []
    seen: set[str] = set()
    for line in (lsblk_output or "").splitlines():
        serial: Optional[str] = None
        for tok in line.split()[1:]:  # skip the TYPE column; scan remaining tokens for a serial form
            tl = tok.strip().lower()
            if re.fullmatch(r"[0-9a-f]{24}", tl):
                serial = tl
                break
            m = re.fullmatch(r"(?:0x)?624a9370([0-9a-f]{24})", tl)
            if m:
                serial = m.group(1)
                break
        if serial and serial not in seen:
            seen.add(serial)
            serials.append(serial)
    return serials


def discover_node_volumes(fa: Any, nodes: list[str], ssh_user_param: str, ssh_opts_param: list[str],
                          context_names: list[str]) -> dict[str, list[dict]]:
    """Multi-volume node->volumes discovery. For each node, SSH the data mount's full device tree and map
    EVERY backing FA volume. Returns node -> [ {'ShortName','VolumeName','Serial','PvIndex'} ] (a
    single-volume node yields a 1-element list). Throws if a node resolves to zero volumes or a serial has
    no matching FA volume. This is the slow path, run at tag time (initialize-protection-groups) and as the
    resolver's fallback."""
    node_map: dict[str, list[dict]] = {}
    for node in nodes:
        serials: list[str] = []
        for attempt in range(1, 4):
            proc = subprocess.run(["ssh", *ssh_opts_param, f"{ssh_user_param}@{node}", _MULTI_SERIAL_CMD],
                                  capture_output=True, text=True)
            serials = parse_fa_volume_serials(proc.stdout)
            if serials:
                break
            if attempt < 3:
                time.sleep(1)
        if not serials:
            raise RuntimeError(
                f"Could not resolve any FlashArray volume backing /data/mongo on {node}. Verify the mount "
                "exists and its block device(s) are Pure pRDMs / multipath LUNs."
            )
        vols: list[dict] = []
        for idx, serial in enumerate(serials):
            found = None
            for ctx_name in context_names:
                r = _fa(fa.get_volumes(context_names=[ctx_name], filter=f"serial='{serial.upper()}'"),
                        allow_error=True)
                if r:
                    found = {"ShortName": ctx_name, "VolumeName": r[0].name, "Serial": serial, "PvIndex": idx}
                    break
            if not found:
                raise RuntimeError(
                    f"No FlashArray volume with serial '{serial}' found for node {node} on any of "
                    f"{len(context_names)} array(s). Run initialize-protection-groups, or verify the volume "
                    "is presented from a fleet array with a protection group."
                )
            vols.append(found)
        node_map[node] = vols
        write_host(f"  {node}: {len(vols)} volume(s) -> "
                   + ", ".join(f"{v['ShortName']}/{v['VolumeName']}" for v in vols), fg=GREEN)
    return node_map


# --- Tag-based OS-disk -> FA-volume map ----------------------------------------------------------------
# At scale, resolving the node->volume map by SSH + SCSI serial on every snapshot/restore is slow. Instead
# we precompute it once per topology change (initialize-protection-groups) and store it on the FA volumes
# as tags, then read it on the hot path (one GET /volumes/tags per array, no SSH). Tags use the 'mongo:'
# key prefix in the default namespace and are copyable (so the map travels with snapshots/clones).
MONGO_DATA_MOUNT = "/data/mongo"
VOLMAP_TAG_DEPLOYMENT = "mongo:deployment"
VOLMAP_TAG_NODE = "mongo:node"
VOLMAP_TAG_MOUNT = "mongo:mountpoint"
VOLMAP_TAG_SERIAL = "mongo:serial"
VOLMAP_TAG_VG = "mongo:vg"            # LVM volume group (empty for a direct device); for multi-volume nodes
VOLMAP_TAG_PVINDEX = "mongo:pvindex"  # ordinal of this volume within its VG (0 for single-volume)
VOLMAP_TAG_PVCOUNT = "mongo:pvcount"  # total volumes backing this node's mount; lets the resolver detect a
                                      # missing volume (e.g. a PV that moved arrays/lost its tag) and refuse
                                      # to act on an incomplete set instead of silently skipping a volume
VOLMAP_TAG_RS = "mongo:rs"            # replica-set id this member belongs to (Path A replication wiring)
VOLMAP_TAG_REPLPG = "mongo:replpg"    # this member's replication PG (Path A: fans its snapshot to siblings)


def write_volume_map_tags(fa: Any, deployment: Optional[str], node_map: dict, mountpoint: str = MONGO_DATA_MOUNT) -> int:
    """Write the OS-disk -> FA-volume mapping onto each FA volume as copyable tags. `node_map` is
    node -> [ {'ShortName','VolumeName','Serial', optional 'Vg','PvIndex'} ] (one entry per backing
    volume; a single-volume node has a 1-element list). Returns the number of volumes tagged."""
    dep = deployment or ""
    n = 0
    for node, vols in node_map.items():
        pvcount = len(vols)  # how many volumes back this node's mount; stored on each so the resolver can
                             # detect (and refuse) an incomplete set rather than silently dropping a volume
        for info in vols:
            tags = [
                {"key": VOLMAP_TAG_DEPLOYMENT, "value": dep, "copyable": True},
                {"key": VOLMAP_TAG_NODE, "value": node, "copyable": True},
                {"key": VOLMAP_TAG_MOUNT, "value": mountpoint, "copyable": True},
                {"key": VOLMAP_TAG_SERIAL, "value": info.get("Serial", ""), "copyable": True},
                {"key": VOLMAP_TAG_VG, "value": info.get("Vg", ""), "copyable": True},
                {"key": VOLMAP_TAG_PVINDEX, "value": str(info.get("PvIndex", 0)), "copyable": True},
                {"key": VOLMAP_TAG_PVCOUNT, "value": str(pvcount), "copyable": True},
            ]
            _fa(fa.put_volumes_tags_batch(resource_names=[info["VolumeName"]], tag=tags,
                                          context_names=[info["ShortName"]]))
            write_host(f"    tagged {info['ShortName']}/{info['VolumeName']} <- node={node} "
                       f"pv={info.get('PvIndex', 0)} mount={mountpoint}", fg=GREEN)
            n += 1
    return n


def parse_volume_map_tags(tag_rows: list, ctx_name: str, deployment: Optional[str]) -> dict:
    """Group a GET /volumes/tags response (rows with .resource=<volume name>, .key, .value) by volume and
    return node -> [ {'ShortName','VolumeName','Serial','PvIndex'} ] for tags whose mongo:deployment
    matches (one entry per volume; a node may have several). Pure (unit-testable)."""
    by_vol: dict[str, dict] = {}
    for t in tag_rows or []:
        key = getattr(t, "key", None)
        # The FA GET /volumes/tags row carries `resource` as a nested object {name,id}; fall back to a
        # plain string for unit tests / older shapes.
        res = getattr(t, "resource", None)
        vol = getattr(res, "name", res) if res is not None else None
        if not key or not str(key).startswith("mongo:") or not vol or not isinstance(vol, str):
            continue
        by_vol.setdefault(vol, {})[key] = getattr(t, "value", None)
    out: dict[str, list] = {}
    for vol, kv in by_vol.items():
        if (kv.get(VOLMAP_TAG_DEPLOYMENT) or "") != (deployment or ""):
            continue
        node = kv.get(VOLMAP_TAG_NODE)
        if not node:
            continue
        try:
            pvindex = int(kv.get(VOLMAP_TAG_PVINDEX) or 0)
        except (TypeError, ValueError):
            pvindex = 0
        # PvCount is None when the tag is absent (older tags) so the completeness guard simply skips —
        # backward-compatible; re-running initialize-protection-groups stamps it and enables the guard.
        pvcount_raw = kv.get(VOLMAP_TAG_PVCOUNT)
        try:
            pvcount = int(pvcount_raw) if pvcount_raw not in (None, "") else None
        except (TypeError, ValueError):
            pvcount = None
        out.setdefault(node, []).append({
            "ShortName": ctx_name, "VolumeName": vol,
            "Serial": (kv.get(VOLMAP_TAG_SERIAL) or ""), "PvIndex": pvindex, "PvCount": pvcount,
        })
    return out


def read_volume_map_tags(fa: Any, deployment: Optional[str], context_names: list[str]) -> dict:
    """Read the volume-map tags across the given arrays (one GET /volumes/tags per array, NO SSH) and
    return node -> [ {'ShortName','VolumeName','Serial','PvIndex'} ] for the deployment (a node's volumes
    may span arrays). Each node's list is sorted by PvIndex."""
    node_map: dict[str, list] = {}
    for ctx in context_names:
        rows = _fa(fa.get_volumes_tags(context_names=[ctx]), allow_error=True) or []
        for node, vols in parse_volume_map_tags(rows, ctx, deployment).items():
            node_map.setdefault(node, []).extend(vols)
    for node in node_map:
        node_map[node].sort(key=lambda v: v.get("PvIndex", 0))
    return node_map


def resolve_node_volume_map(fa: Any, nodes: list[str], ssh_user_param: str, ssh_opts_param: list[str],
                            context_names: list[str], deployment: Optional[str],
                            mountpoint: str = MONGO_DATA_MOUNT, verify: bool = True) -> dict[str, list]:
    """Fast-path node -> [ {'ShortName','VolumeName','Serial','PvIndex'} ] resolver. Reads the precomputed
    volume-map tags (no SSH); when verify=True, cross-checks every tagged volume's serial against the array
    (one GET /volumes per array, no SSH) and, for any node that is untagged or has ANY stale/missing
    volume, falls back to live multi-volume SSH discovery for that node only. With no tags present this
    degrades to full discovery, so it is safe/backward-compatible."""
    tagged = read_volume_map_tags(fa, deployment, context_names)
    resolved: dict[str, list] = {}
    fallback_nodes: list[str] = []

    actual_serial: dict[str, dict[str, str]] = {}  # array -> {volume: SERIAL}
    if verify and tagged:
        # Fetch ONLY the tagged volumes per array (names filter) — never an unfiltered get_volumes,
        # which would pull every volume on a production array and make verify slower than SSH discovery.
        names_by_ctx: dict[str, list] = {}
        for vols in tagged.values():
            for v in vols:
                names = names_by_ctx.setdefault(v["ShortName"], [])
                if v["VolumeName"] not in names:
                    names.append(v["VolumeName"])
        for ctx, names in names_by_ctx.items():
            vols = _fa(fa.get_volumes(names=names, context_names=[ctx]), allow_error=True) or []
            actual_serial[ctx] = {getattr(v, "name", None): (getattr(v, "serial", "") or "") for v in vols}

    for node in nodes:
        vols = tagged.get(node)
        if not vols:
            fallback_nodes.append(node)
            continue
        stale = False
        if verify:
            for v in vols:
                if not v.get("Serial"):
                    continue
                got = (actual_serial.get(v["ShortName"], {}).get(v["VolumeName"]) or "").lower()
                if got != v["Serial"].lower():
                    write_host(f"    volume-map tag stale for {node} vol {v['VolumeName']} "
                               f"(serial {v['Serial']} != {got or 'none'}) - rediscovering", fg=YELLOW)
                    stale = True
                    break
        # Completeness guard (no API cost, runs regardless of verify): if the tags record how many volumes
        # this node should have (mongo:pvcount) and fewer resolved, a volume's tag is missing -- e.g. a PV
        # that moved to an array outside the current PG set, or lost its tag on an array-to-array move.
        # Rediscover via SSH rather than snapshot/restore a silently incomplete (and thus corrupt) set.
        if not stale:
            want = next((v["PvCount"] for v in vols if v.get("PvCount")), None)
            if want is not None and want != len(vols):
                write_host(f"    volume-map tags incomplete for {node} ({len(vols)} of {want} volume(s) "
                           "found) - rediscovering", fg=YELLOW)
                stale = True
        if stale:
            fallback_nodes.append(node)
            continue
        resolved[node] = [{"ShortName": v["ShortName"], "VolumeName": v["VolumeName"],
                           "Serial": v.get("Serial", ""), "PvIndex": v.get("PvIndex", 0)} for v in vols]

    if resolved:
        total = sum(len(v) for v in resolved.values())
        write_host(f"  Resolved {len(resolved)} node(s) / {total} volume(s) from volume tags (no SSH).", fg=GREEN)
    if fallback_nodes:
        write_host(f"  SSH-discovery fallback for {len(fallback_nodes)} node(s): {', '.join(fallback_nodes)}",
                   fg=DARK_YELLOW)
        resolved.update(discover_node_volumes(fa, fallback_nodes, ssh_user_param, ssh_opts_param, context_names))
    return resolved


# endregion

# region --- Path A: replica-set replication wiring (per-member repl-PGs + async mesh) -----------------
# Path A makes in-place restore consistent-by-construction: every member of an RS is restored from the ONE
# OM-frozen secondary's snapshot, pre-replicated at snapshot time to the arrays hosting that RS's other
# members. That requires, per RS, a per-member replication PG whose targets are the sibling-member arrays,
# and a COMPLETE async-replication mesh among those arrays. See docs/path-a-implementation-plan.md.
REPL_PG_SUFFIX = "-repl"


def repl_pg_name(volume_name: str) -> str:
    """Per-member replication PG name for a data volume: holds that one member's volume and replicates it
    to the arrays hosting the RS's OTHER members."""
    return f"{volume_name}{REPL_PG_SUFFIX}"


def get_replica_set_membership(cluster_nodes: list[str]) -> dict[str, list[str]]:
    """Return rs_id -> [node hostnames] from the OM cluster detail. For a standalone replica set
    (TOPOLOGY=replicaset) with OM unreachable, every node is one RS, so fall back to a single group.
    Raises for a sharded cluster when OM is unavailable (shard grouping cannot be inferred)."""
    try:
        detail = invoke_om_api(path=f"group/{CFG.GroupId}/clusters/{CFG.ClusterId}")
        membership: dict[str, list[str]] = {}
        for rs in detail.get("replicaSets") or []:
            rid = rs.get("id")
            for n in rs.get("nodes") or []:
                host = n.get("hostname")
                if rid and host:
                    membership.setdefault(rid, []).append(host)
        if membership:
            return membership
    except Exception as e:  # noqa: BLE001 - OM optional for a standalone RS
        write_host(f"  WARNING: OM RS-membership lookup failed ({e}).", fg=YELLOW)
    if (CFG.Topology or "").lower() == "replicaset":
        rid = CFG.DeploymentName or "replicaset"
        write_host(f"  Using single-RS fallback ({len(cluster_nodes)} node(s)) for '{rid}'.", fg=DARK_YELLOW)
        return {rid: list(cluster_nodes)}
    raise RuntimeError(
        "Cannot determine replica-set membership: Ops Manager cluster detail is unavailable and TOPOLOGY "
        "is not 'replicaset'. RS grouping is required to wire replication on a sharded cluster."
    )


def build_rs_array_map(rs_membership: dict, node_volume_map: dict) -> dict:
    """Pure. rs_id -> [ {'Node','VolumeName','ShortName'} ] for members present in node_volume_map (a
    node's volumes each become an entry; a single-volume member yields one)."""
    out: dict[str, list] = {}
    for rid, hosts in rs_membership.items():
        for host in hosts:
            for entry in node_volume_map.get(host, []):
                out.setdefault(rid, []).append(
                    {"Node": host, "VolumeName": entry["VolumeName"], "ShortName": entry["ShortName"]}
                )
    return out


def missing_async_mesh_links(rs_array_map: dict, connected_pairs: set) -> list[dict]:
    """Pure. For each RS, every DISTINCT pair of member arrays lacking an async-replication connection
    (connected_pairs = set of frozenset({a,b})). Returns [{'Rs','ArrayA','ArrayB'}]."""
    missing: list[dict] = []
    for rid, entries in rs_array_map.items():
        arrays = sorted({e["ShortName"] for e in entries})
        for i in range(len(arrays)):
            for j in range(i + 1, len(arrays)):
                if frozenset((arrays[i], arrays[j])) not in connected_pairs:
                    missing.append({"Rs": rid, "ArrayA": arrays[i], "ArrayB": arrays[j]})
    return missing


def async_replication_pairs(fa: Any, arrays: list[str]) -> set:
    """Live. Set of frozenset({a,b}) array pairs with a CONNECTED async-replication connection. Reads each
    array's array-connections (the routed GET is per-array; only connection-key is not gateway-routed)."""
    pairs: set = set()
    for arr in arrays:
        for c in _fa(fa.get_array_connections(context_names=[arr]), allow_error=True) or []:
            if getattr(c, "type", None) != "async-replication" or getattr(c, "status", None) != "connected":
                continue
            remote = getattr(c, "remote", None)
            rname = getattr(remote, "name", None) if remote is not None else None
            if rname:
                pairs.add(frozenset((arr, rname)))
    return pairs


# endregion
