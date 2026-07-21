###############################################################################################################################
# Hybrid FlashArray REST client.
#
# Object and read operations are routed THROUGH THE FUSION GATEWAY: a single authenticated session to
# FA_ENDPOINT, with `context_names=[<array>]` sent as a query parameter so the gateway routes the request to
# that fleet member. Tag operations are the exception — the gateway does NOT route the
# /protection-group-snapshots/tags endpoints to remote members (they fail against the gateway's local store),
# so tag read/write connect DIRECTLY to each target array instead. Tags are therefore written to (and read
# from) every array's own snapshot copy, so every PG snapshot carries the full tag set.
#
# Auth flow (used for both the gateway session and any direct per-array session):
#   1. (preferred) username/password -> POST /api/1.16/auth/apitoken  -> that array's api token
#   2.             POST /api/1.16/auth/session  (v1 session cookie; some calls rely on it)
#   3.             POST /api/{version}/login    (api-token header) -> x-auth-token
# Username/password logs in as the directory user (full fleet/array_admin permissions), which is what lets
# the gateway route to remote members and lets direct per-array tag writes succeed. A configured api-token is
# a fallback (only valid on the array that issued it).
#
# Routing rules:
#   * Object/read ops -> the gateway session; `context_names` becomes a CSV query param. Routed GETs add
#     `allow_errors=true` (required when a routed read carries a search parameter). Routed writes do NOT set
#     allow_errors, so a failed write still surfaces as an error rather than being silently tolerated.
#   * Tag ops (get/put .../tags[/batch]) -> a direct session to context_names[0]'s FQDN (derived from the
#     gateway's domain suffix), with no context_names param.
###############################################################################################################################

from __future__ import annotations

import json
from typing import Any, Optional

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------------------------------------------------------------------------------------------------
# Response wrappers + attribute-accessible JSON, so existing call sites keep working unchanged:
#   _fa() tests isinstance(resp, ValidResponse); call sites read item.name / item.member.name / item.os ...
# REST returns plain JSON, so a recursive namespace wrapper exposes those keys as attributes.
# ---------------------------------------------------------------------------------------------------------
class _Obj:
    """Recursive attribute view over a JSON dict. Missing attributes return None."""

    __slots__ = ("_d",)

    def __init__(self, d: dict):
        object.__setattr__(self, "_d", d)

    def __getattr__(self, key: str) -> Any:
        return _wrap(self._d.get(key)) if key in self._d else None

    def __repr__(self) -> str:
        return f"_Obj({self._d!r})"


def _wrap(value: Any) -> Any:
    if isinstance(value, dict):
        return _Obj(value)
    if isinstance(value, list):
        return [_wrap(v) for v in value]
    return value


class ValidResponse:
    """Response wrapper exposing the `.items` surface the call sites use."""

    def __init__(self, items: list):
        self.items = items


class ErrorResponse:
    """Error response wrapper. `.errors` carries the unmasked FA error dicts."""

    def __init__(self, status_code: Optional[int], errors: list):
        self.status_code = status_code
        self.errors = errors


# ---------------------------------------------------------------------------------------------------------
# The client
# ---------------------------------------------------------------------------------------------------------
class Client:
    def __init__(
        self,
        *,
        gateway: str,
        version: str,
        api_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 30,
    ):
        self._gateway = gateway
        self._version = version
        self._api_token = api_token or None
        self._username = username or None
        self._password = password or None
        self._timeout = timeout
        self._gw_short = gateway.split(".")[0]
        self._domain = gateway.split(".", 1)[1] if "." in gateway else ""
        self._sessions: dict[str, requests.Session] = {}  # endpoint FQDN -> authenticated session

    # ---- endpoint resolution -----------------------------------------------------------------------------
    def _endpoint_for(self, array_name: Optional[str]) -> str:
        """Map a fleet-member short name to its FQDN, derived from the gateway's domain suffix.
        `None` or the gateway's own short name -> the gateway endpoint. Used for the gateway session and for
        the direct per-array sessions that tag ops require."""
        if array_name is None or array_name == self._gw_short:
            return self._gateway
        if not self._domain:
            raise RuntimeError(
                f"Cannot reach array '{array_name}' directly: gateway '{self._gateway}' is not an FQDN. "
                "Use a fully-qualified FA_ENDPOINT so member hostnames can be derived for direct tag ops."
            )
        return f"{array_name}.{self._domain}"

    # ---- authentication (per endpoint) -------------------------------------------------------------------
    def _obtain_raw_token(self, endpoint: str, session: requests.Session) -> str:
        """Prefer username/password -> /api/1.16/auth/apitoken (directory identity).
        Fall back to the configured api-token (only valid on its issuing array)."""
        if self._username and self._password:
            r = session.post(
                f"https://{endpoint}/api/1.16/auth/apitoken",
                json={"username": self._username, "password": self._password},
                timeout=self._timeout,
            )
            if r.status_code == 200:
                token = (r.json() or {}).get("api_token")
                if token:
                    return token
            # fall through to the configured token on failure
        if self._api_token:
            return self._api_token
        raise RuntimeError(f"No credentials available to authenticate to {endpoint}")

    def _session_for(self, array_name: Optional[str]) -> requests.Session:
        endpoint = self._endpoint_for(array_name)
        cached = self._sessions.get(endpoint)
        if cached is not None:
            return cached

        session = requests.Session()
        session.verify = False
        raw_token = self._obtain_raw_token(endpoint, session)

        session.post(
            f"https://{endpoint}/api/1.16/auth/session",
            json={"api_token": raw_token},
            timeout=self._timeout,
        )
        login = session.post(
            f"https://{endpoint}/api/{self._version}/login",
            headers={"api-token": raw_token},
            timeout=self._timeout,
        )
        if login.status_code != 200:
            raise RuntimeError(f"Login to {endpoint} failed: HTTP {login.status_code} - {login.text}")
        x_auth = login.headers.get("x-auth-token")
        if not x_auth:
            raise RuntimeError(f"Login to {endpoint} returned no x-auth-token")
        session.headers.update({"x-auth-token": x_auth, "Content-Type": "application/json"})
        self._sessions[endpoint] = session
        return session

    def connect(self) -> "Client":
        """Eagerly authenticate the gateway session so credential problems surface immediately."""
        self._session_for(None)
        return self

    # ---- shared request engine (paginated, error-as-ErrorResponse) ---------------------------------------
    def _do(self, array_name: Optional[str], method: str, path: str, params: dict, body: Any) -> Any:
        try:
            session = self._session_for(array_name)
            endpoint = self._endpoint_for(array_name)
        except Exception as exc:  # auth/endpoint failure -> ErrorResponse
            return ErrorResponse(None, [{"message": str(exc), "context": None}])

        url = f"https://{endpoint}/api/{self._version}/{path}"
        # Body present -> JSON. Body-less non-GET -> empty JSON object (some FA endpoints reject the
        # Content-Type header with no body, HTTP 415). GET -> no body.
        if body is not None:
            data = json.dumps(body)
        elif method != "GET":
            data = "{}"
        else:
            data = None

        local_params = dict(params)
        items: list = []
        reauthed = False
        try:
            while True:
                resp = session.request(method, url, params=local_params, data=data, timeout=self._timeout)
                if resp.status_code == 401 and not reauthed:
                    # session expired -> drop it, re-auth once, retry
                    self._sessions.pop(endpoint, None)
                    reauthed = True
                    session = self._session_for(array_name)
                    continue
                if resp.status_code in (200, 201, 204):
                    if not resp.content:
                        return ValidResponse(items)
                    payload = resp.json()
                    if isinstance(payload, dict):
                        # With allow_errors a routed read can return HTTP 200 carrying per-array errors;
                        # surface those so a failure is not mistaken for an empty (but valid) result.
                        errs = payload.get("errors")
                        if errs:
                            return ErrorResponse(resp.status_code, errs)
                        page = payload.get("items", [])
                    else:
                        page = []
                    items.extend(_wrap(x) for x in page)
                    token = payload.get("continuation_token") if isinstance(payload, dict) else None
                    if token and method == "GET":
                        local_params["continuation_token"] = token
                        continue
                    return ValidResponse(items)
                # non-2xx -> surface the unmasked FA errors
                try:
                    errs = (resp.json() or {}).get("errors") or [{"message": resp.text, "context": None}]
                except ValueError:
                    errs = [{"message": resp.text, "context": None}]
                return ErrorResponse(resp.status_code, errs)
        except Exception as exc:  # network error etc. -> ErrorResponse
            return ErrorResponse(None, [{"message": str(exc), "context": None}])

    # ---- routed (via gateway) vs direct (per array) dispatch ---------------------------------------------
    def _routed(self, method: str, path: str, *, context_names: Optional[list] = None,
                params: Optional[dict] = None, body: Any = None) -> Any:
        """Send through the gateway session; carry context_names as a query param so the gateway routes
        to that member. Routed GETs add allow_errors=true (required when a search param is present)."""
        p = self._params(**(params or {}))
        target = self._csv(context_names) if context_names else None
        if target:
            p["context_names"] = target
            if method == "GET":
                p["allow_errors"] = "true"
        return self._do(None, method, path, p, body)

    def _direct(self, array_name: Optional[str], method: str, path: str, *,
                params: Optional[dict] = None, body: Any = None) -> Any:
        """Connect directly to array_name's endpoint (no context_names routing). Used for tag ops, which
        the gateway does not route to remote members."""
        return self._do(array_name, method, path, self._params(**(params or {})), body)

    # ---- helpers -----------------------------------------------------------------------------------------
    @staticmethod
    def _target(context_names: Optional[list]) -> Optional[str]:
        """context_names carries exactly one array. None -> gateway."""
        if not context_names:
            return None
        return context_names[0]

    @staticmethod
    def _csv(values: Optional[list]) -> Optional[str]:
        return ",".join(values) if values else None

    def _params(self, **kwargs) -> dict:
        return {k: v for k, v in kwargs.items() if v is not None}

    # ---- SDK-shaped methods (routed through the gateway) -------------------------------------------------
    def get_fleets_members(self, fleet_names: Optional[list] = None):
        return self._routed("GET", "fleets/members", params={"fleet_names": self._csv(fleet_names)})

    def get_arrays(self, context_names: Optional[list] = None):
        return self._routed("GET", "arrays", context_names=context_names)

    def get_protection_groups(self, names: Optional[list] = None, filter: Optional[str] = None,
                              context_names: Optional[list] = None):
        return self._routed("GET", "protection-groups", context_names=context_names,
                            params={"names": self._csv(names), "filter": filter})

    def post_protection_groups(self, names: Optional[list] = None, context_names: Optional[list] = None):
        return self._routed("POST", "protection-groups", context_names=context_names,
                            params={"names": self._csv(names)})

    def patch_protection_groups(self, names: Optional[list] = None, protection_group: Any = None,
                                context_names: Optional[list] = None):
        """Update protection group(s) named `names`. Pass protection_group={'name': '<new>'} to rename.
        On a rename, Purity also renames the group's existing snapshots to the new prefix."""
        return self._routed("PATCH", "protection-groups", context_names=context_names,
                            params={"names": self._csv(names)}, body=protection_group)

    def get_protection_groups_volumes(self, group_names: Optional[list] = None,
                                      member_names: Optional[list] = None, context_names: Optional[list] = None):
        return self._routed("GET", "protection-groups/volumes", context_names=context_names,
                            params={"group_names": self._csv(group_names), "member_names": self._csv(member_names)})

    def post_protection_groups_volumes(self, group_names: Optional[list] = None,
                                       member_names: Optional[list] = None, context_names: Optional[list] = None):
        return self._routed("POST", "protection-groups/volumes", context_names=context_names,
                            params={"group_names": self._csv(group_names), "member_names": self._csv(member_names)})

    def delete_protection_groups_volumes(self, group_names: Optional[list] = None,
                                         member_names: Optional[list] = None, context_names: Optional[list] = None):
        return self._routed("DELETE", "protection-groups/volumes", context_names=context_names,
                            params={"group_names": self._csv(group_names), "member_names": self._csv(member_names)})

    def get_protection_group_snapshots(self, names: Optional[list] = None, filter: Optional[str] = None,
                                       context_names: Optional[list] = None):
        return self._routed("GET", "protection-group-snapshots", context_names=context_names,
                            params={"names": self._csv(names), "filter": filter})

    def post_protection_group_snapshots(self, source_names: Optional[list] = None,
                                        protection_group_snapshot: Any = None, replicate_now: Optional[bool] = None,
                                        replicate: Optional[bool] = None, context_names: Optional[list] = None):
        """Create a PG snapshot. replicate_now=True snapshots AND immediately replicates to the PG's
        configured targets (Path A: fan the frozen secondary's snapshot out to sibling-member arrays)."""
        return self._routed("POST", "protection-group-snapshots", context_names=context_names,
                            params={"source_names": self._csv(source_names),
                                    "replicate_now": ("true" if replicate_now else None),
                                    "replicate": ("true" if replicate else None)},
                            body=protection_group_snapshot)

    def delete_protection_group_snapshots(self, names: Optional[list] = None, context_names: Optional[list] = None):
        return self._routed("DELETE", "protection-group-snapshots", context_names=context_names,
                            params={"names": self._csv(names)})

    # ---- async replication: array connections + PG replication targets ----------------------------------
    # Path A pre-positions the OM-frozen secondary's snapshot on the arrays hosting that RS's other members,
    # so restore can clone one consistent source onto every member. That requires (a) an async-replication
    # connection between the source and each sibling array, and (b) those sibling arrays set as replication
    # targets on the per-member PG. See docs/path-a-implementation-plan.md.
    def get_array_connections(self, context_names: Optional[list] = None):
        return self._routed("GET", "array-connections", context_names=context_names)

    def post_array_connections(self, connection: Any = None, context_names: Optional[list] = None):
        """Establish an async-replication connection to a remote array. `connection` carries the remote
        management/replication addresses and connection key (FA array-connection POST shape)."""
        return self._routed("POST", "array-connections", context_names=context_names, body=connection)

    def delete_array_connections(self, names: Optional[list] = None, context_names: Optional[list] = None):
        return self._routed("DELETE", "array-connections", context_names=context_names,
                            params={"names": self._csv(names)})

    def get_protection_groups_targets(self, group_names: Optional[list] = None,
                                      member_names: Optional[list] = None, context_names: Optional[list] = None):
        return self._routed("GET", "protection-groups/targets", context_names=context_names,
                            params={"group_names": self._csv(group_names), "member_names": self._csv(member_names)})

    def post_protection_groups_targets(self, group_names: Optional[list] = None,
                                       member_names: Optional[list] = None, context_names: Optional[list] = None):
        """Add replication target array(s) to the PG(s). member_names = the target array names."""
        return self._routed("POST", "protection-groups/targets", context_names=context_names,
                            params={"group_names": self._csv(group_names), "member_names": self._csv(member_names)})

    def patch_protection_groups_targets(self, group_names: Optional[list] = None, member_names: Optional[list] = None,
                                        protection_group_target: Any = None, context_names: Optional[list] = None):
        """Allow/disallow a replication target (protection_group_target={'allowed': True}); run against the
        TARGET array's context so the target permits replicas from the source."""
        return self._routed("PATCH", "protection-groups/targets", context_names=context_names,
                            params={"group_names": self._csv(group_names), "member_names": self._csv(member_names)},
                            body=protection_group_target)

    def delete_protection_groups_targets(self, group_names: Optional[list] = None,
                                         member_names: Optional[list] = None, context_names: Optional[list] = None):
        return self._routed("DELETE", "protection-groups/targets", context_names=context_names,
                            params={"group_names": self._csv(group_names), "member_names": self._csv(member_names)})

    def get_volumes(self, names: Optional[list] = None, filter: Optional[str] = None,
                    context_names: Optional[list] = None):
        return self._routed("GET", "volumes", context_names=context_names,
                            params={"names": self._csv(names), "filter": filter})

    def get_volume_snapshots(self, names: Optional[list] = None, context_names: Optional[list] = None):
        return self._routed("GET", "volume-snapshots", context_names=context_names,
                            params={"names": self._csv(names)})

    def post_volumes(self, names: Optional[list] = None, volume: Any = None, overwrite: Optional[bool] = None,
                     context_names: Optional[list] = None):
        return self._routed("POST", "volumes", context_names=context_names,
                            params={"names": self._csv(names), "overwrite": ("true" if overwrite else None)},
                            body=volume)

    # ---- tag ops: DIRECT to the target array (the gateway does not route the /tags endpoints) ------------
    def get_protection_group_snapshots_tags(self, resource_names: Optional[list] = None,
                                            context_names: Optional[list] = None):
        return self._direct(self._target(context_names), "GET", "protection-group-snapshots/tags",
                           params={"resource_names": self._csv(resource_names)})

    def put_protection_group_snapshots_tags_batch(self, resource_names: Optional[list] = None,
                                                  tag: Any = None, context_names: Optional[list] = None):
        return self._direct(self._target(context_names), "PUT", "protection-group-snapshots/tags/batch",
                           params={"resource_names": self._csv(resource_names)}, body=tag)

    # ---- volume tags: the precomputed OS-disk -> FA-volume map (namespace 'mongo-backup') ----------------
    # Also DIRECT to the target array (the gateway does not route /tags). resource_names = volume names.
    def get_volumes_tags(self, resource_names: Optional[list] = None, namespaces: Optional[list] = None,
                         context_names: Optional[list] = None):
        return self._direct(self._target(context_names), "GET", "volumes/tags",
                           params={"resource_names": self._csv(resource_names),
                                   "namespaces": self._csv(namespaces)})

    def put_volumes_tags_batch(self, resource_names: Optional[list] = None, tag: Any = None,
                               context_names: Optional[list] = None):
        # tag = list of {"key","value","namespace"} dicts applied to every resource in resource_names.
        return self._direct(self._target(context_names), "PUT", "volumes/tags/batch",
                           params={"resource_names": self._csv(resource_names)}, body=tag)

    def delete_volumes_tags(self, resource_names: Optional[list] = None, keys: Optional[list] = None,
                            namespaces: Optional[list] = None, context_names: Optional[list] = None):
        return self._direct(self._target(context_names), "DELETE", "volumes/tags",
                           params={"resource_names": self._csv(resource_names), "keys": self._csv(keys),
                                   "namespaces": self._csv(namespaces)})
