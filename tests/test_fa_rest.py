"""Unit tests for the FlashArray REST client request construction (no network).

Each test stubs Client._do to capture the (array_name, method, path, params, body) tuple the method builds,
so we assert routing + params + body without hitting an array. Focus: the Path A replication primitives
(array-connections, PG replication targets, replicate_now) plus a backward-compat check on the existing
snapshot method.
"""
from mongodb_flasharray_backup import fa_rest


def _client_capturing():
    c = fa_rest.Client(gateway="gw.example.com", version="2.51", api_token="t")
    calls = []

    def fake_do(array_name, method, path, params, body):
        calls.append({"array": array_name, "method": method, "path": path, "params": params, "body": body})
        return []

    c._do = fake_do
    return c, calls


def test_post_pg_snapshots_replicate_now_sets_param():
    c, calls = _client_capturing()
    c.post_protection_group_snapshots(source_names=["p-repl"], replicate_now=True, context_names=["arrA"])
    call = calls[-1]
    assert call["method"] == "POST"
    assert call["path"] == "protection-group-snapshots"
    assert call["array"] is None  # routed through the gateway
    assert call["params"]["source_names"] == "p-repl"
    assert call["params"]["replicate_now"] == "true"
    assert call["params"]["context_names"] == "arrA"
    assert "replicate" not in call["params"]  # not passed -> filtered out


def test_post_pg_snapshots_backward_compatible_without_replicate():
    # The existing snapshot.py call passes neither replicate_now nor replicate; those keys must be absent.
    c, calls = _client_capturing()
    c.post_protection_group_snapshots(source_names=["pg"], context_names=["arrA"])
    params = calls[-1]["params"]
    assert params["source_names"] == "pg"
    assert "replicate_now" not in params
    assert "replicate" not in params


def test_get_array_connections_routed():
    c, calls = _client_capturing()
    c.get_array_connections(context_names=["arrA"])
    call = calls[-1]
    assert call["method"] == "GET"
    assert call["path"] == "array-connections"
    assert call["params"]["context_names"] == "arrA"
    assert call["params"]["allow_errors"] == "true"  # routed GET with a context param


def test_post_array_connections_sends_body():
    c, calls = _client_capturing()
    body = {"management_address": "10.0.0.9", "replication_address": "10.0.1.9", "connection_key": "k"}
    c.post_array_connections(connection=body, context_names=["arrA"])
    call = calls[-1]
    assert call["method"] == "POST"
    assert call["path"] == "array-connections"
    assert call["body"] == body


def test_post_pg_targets_adds_target_array():
    c, calls = _client_capturing()
    c.post_protection_groups_targets(group_names=["aen-rs-00-05-repl"], member_names=["arrB"],
                                     context_names=["arrA"])
    call = calls[-1]
    assert call["method"] == "POST"
    assert call["path"] == "protection-groups/targets"
    assert call["params"]["group_names"] == "aen-rs-00-05-repl"
    assert call["params"]["member_names"] == "arrB"


def test_patch_pg_targets_allows_target():
    c, calls = _client_capturing()
    c.patch_protection_groups_targets(group_names=["g"], member_names=["arrB"],
                                      protection_group_target={"allowed": True}, context_names=["arrB"])
    call = calls[-1]
    assert call["method"] == "PATCH"
    assert call["path"] == "protection-groups/targets"
    assert call["body"] == {"allowed": True}


def test_delete_pg_targets():
    c, calls = _client_capturing()
    c.delete_protection_groups_targets(group_names=["g"], member_names=["arrB"], context_names=["arrA"])
    call = calls[-1]
    assert call["method"] == "DELETE"
    assert call["path"] == "protection-groups/targets"
    assert call["params"]["member_names"] == "arrB"
