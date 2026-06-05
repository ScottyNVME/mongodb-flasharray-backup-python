###############################################################################################################################
# Direct-REST FlashArray client — replaces py-pure-client.
#
# WHY DIRECT REST (and not the SDK + Fusion `context_names` routing):
#   FlashArray API tokens are array-local, and this fleet refuses gateway->remote `context_names`
#   routing for a token identity ("Operation not permitted"). The only path that reaches every
#   fleet member is to authenticate and call EACH array DIRECTLY. This mirrors the snapshotui
#   backend's `_authenticate` / `_authenticate_endpoint` / `_make_request_direct` flow:
#
#     1. (preferred) username/password -> POST /api/1.16/auth/apitoken  -> that array's api token
#     2.             POST /api/1.16/auth/session  (establishes the v1 session cookie)
#     3.             POST /api/{version}/login    (api-token header) -> x-auth-token
#
#   Username/password logs in as the directory user (full fleet/array_admin permissions on every
#   member), exactly like the PowerShell `Connect-Pfa2Array -Credential` flow. A configured
#   api-token is used as a fallback, but it only authorizes on the array that issued it.
#
# The `Client` exposes the same method names/kwargs the py-pure-client calls used, so call sites
# barely change. The key difference: `context_names=[<array>]` no longer means "ask the gateway to
# route" — it means "connect directly to <array>". Each list carries exactly one target array, which
# is how the existing call sites already use it.
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
# REST returns plain JSON whose keys match the SDK model attribute names (same swagger), so a recursive
# namespace wrapper reproduces the SDK's attribute access exactly.
# ---------------------------------------------------------------------------------------------------------
class _Obj:
    """Recursive attribute view over a JSON dict. Missing attributes return None (like the SDK models)."""

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
    """Mirror of pypureclient.responses.ValidResponse (only the `.items` surface we use)."""

    def __init__(self, items: list):
        self.items = items


class ErrorResponse:
    """Mirror of pypureclient.responses.ErrorResponse. `.errors` carries the *unmasked* FA error dicts."""

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
        `None` or the gateway's own short name -> the gateway endpoint."""
        if array_name is None or array_name == self._gw_short:
            return self._gateway
        if not self._domain:
            raise RuntimeError(
                f"Cannot reach remote array '{array_name}': gateway '{self._gateway}' is not an FQDN. "
                "Use a fully-qualified FA_ENDPOINT so remote member hostnames can be derived."
            )
        return f"{array_name}.{self._domain}"

    # ---- authentication (per array, snapshotui flow) -----------------------------------------------------
    def _obtain_raw_token(self, endpoint: str, session: requests.Session) -> str:
        """Prefer username/password -> /api/1.16/auth/apitoken (directory identity, per-array token).
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

        # v1 session cookie (needed by some calls; matches snapshotui)
        session.post(
            f"https://{endpoint}/api/1.16/auth/session",
            json={"api_token": raw_token},
            timeout=self._timeout,
        )
        # 2.x login -> x-auth-token
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
        """Eagerly authenticate to the gateway so credential problems surface immediately."""
        self._session_for(None)
        return self

    # ---- generic request (per-array, paginated, error-as-ErrorResponse) ----------------------------------
    def _request(
        self,
        array_name: Optional[str],
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        body: Any = None,
    ) -> Any:
        try:
            session = self._session_for(array_name)
            endpoint = self._endpoint_for(array_name)
        except Exception as exc:  # auth/endpoint failure -> ErrorResponse (so _fa(allow_error=True) returns [])
            return ErrorResponse(None, [{"message": str(exc), "context": None}])

        url = f"https://{endpoint}/api/{self._version}/{path}"
        # Non-GET requests carry Content-Type: application/json (set on the session). Some FA endpoints
        # reject that header with no body (HTTP 415), so send an empty JSON object when there is no body —
        # mirroring the Ops Manager helper in config.py. This covers body-less POST/DELETE (PG create/add,
        # snapshot/member delete); requests that already supply a body are unchanged.
        if body is not None:
            data = json.dumps(body)
        elif method != "GET":
            data = "{}"
        else:
            data = None
        local_params = dict(params or {})
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
                    page = payload.get("items", []) if isinstance(payload, dict) else []
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

    # ---- helpers -----------------------------------------------------------------------------------------
    @staticmethod
    def _target(context_names: Optional[list]) -> Optional[str]:
        """Our model: context_names carries exactly one array to connect to directly. None -> gateway."""
        if not context_names:
            return None
        return context_names[0]

    @staticmethod
    def _csv(values: Optional[list]) -> Optional[str]:
        return ",".join(values) if values else None

    def _params(self, **kwargs) -> dict:
        return {k: v for k, v in kwargs.items() if v is not None}

    # ---- SDK-shaped methods (only those the codebase uses) -----------------------------------------------
    def get_fleets(self):
        return self._request(None, "GET", "fleets")

    def get_fleets_members(self, fleet_names: Optional[list] = None):
        return self._request(None, "GET", "fleets/members",
                             params=self._params(fleet_names=self._csv(fleet_names)))

    def get_arrays(self, context_names: Optional[list] = None):
        return self._request(self._target(context_names), "GET", "arrays")

    def get_protection_groups(self, names: Optional[list] = None, filter: Optional[str] = None,
                              context_names: Optional[list] = None):
        return self._request(self._target(context_names), "GET", "protection-groups",
                             params=self._params(names=self._csv(names), filter=filter))

    def post_protection_groups(self, names: Optional[list] = None, context_names: Optional[list] = None):
        return self._request(self._target(context_names), "POST", "protection-groups",
                             params=self._params(names=self._csv(names)))

    def get_protection_groups_volumes(self, group_names: Optional[list] = None,
                                      member_names: Optional[list] = None, context_names: Optional[list] = None):
        return self._request(self._target(context_names), "GET", "protection-groups/volumes",
                             params=self._params(group_names=self._csv(group_names),
                                                 member_names=self._csv(member_names)))

    def post_protection_groups_volumes(self, group_names: Optional[list] = None,
                                       member_names: Optional[list] = None, context_names: Optional[list] = None):
        return self._request(self._target(context_names), "POST", "protection-groups/volumes",
                             params=self._params(group_names=self._csv(group_names),
                                                 member_names=self._csv(member_names)))

    def delete_protection_groups_volumes(self, group_names: Optional[list] = None,
                                         member_names: Optional[list] = None, context_names: Optional[list] = None):
        return self._request(self._target(context_names), "DELETE", "protection-groups/volumes",
                             params=self._params(group_names=self._csv(group_names),
                                                 member_names=self._csv(member_names)))

    def get_protection_group_snapshots(self, names: Optional[list] = None, filter: Optional[str] = None,
                                       context_names: Optional[list] = None):
        return self._request(self._target(context_names), "GET", "protection-group-snapshots",
                             params=self._params(names=self._csv(names), filter=filter))

    def post_protection_group_snapshots(self, source_names: Optional[list] = None,
                                        protection_group_snapshot: Any = None, context_names: Optional[list] = None):
        return self._request(self._target(context_names), "POST", "protection-group-snapshots",
                             params=self._params(source_names=self._csv(source_names)),
                             body=protection_group_snapshot)

    def delete_protection_group_snapshots(self, names: Optional[list] = None, context_names: Optional[list] = None):
        return self._request(self._target(context_names), "DELETE", "protection-group-snapshots",
                             params=self._params(names=self._csv(names)))

    def get_protection_group_snapshots_tags(self, resource_names: Optional[list] = None,
                                            context_names: Optional[list] = None):
        return self._request(self._target(context_names), "GET", "protection-group-snapshots/tags",
                             params=self._params(resource_names=self._csv(resource_names)))

    def put_protection_group_snapshots_tags_batch(self, resource_names: Optional[list] = None,
                                                  tag: Any = None, context_names: Optional[list] = None):
        return self._request(self._target(context_names), "PUT", "protection-group-snapshots/tags/batch",
                             params=self._params(resource_names=self._csv(resource_names)), body=tag)

    def get_volumes(self, names: Optional[list] = None, filter: Optional[str] = None,
                    context_names: Optional[list] = None):
        return self._request(self._target(context_names), "GET", "volumes",
                             params=self._params(names=self._csv(names), filter=filter))

    def get_volume_snapshots(self, names: Optional[list] = None, context_names: Optional[list] = None):
        return self._request(self._target(context_names), "GET", "volume-snapshots",
                             params=self._params(names=self._csv(names)))

    def post_volumes(self, names: Optional[list] = None, volume: Any = None, overwrite: Optional[bool] = None,
                     context_names: Optional[list] = None):
        return self._request(self._target(context_names), "POST", "volumes",
                             params=self._params(names=self._csv(names),
                                                 overwrite=("true" if overwrite else None)),
                             body=volume)
