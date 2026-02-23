"""
tests/integration/test_remote_protocol_http.py
───────────────────────────────────────────────
Integration tests for ``vcs.remote.protocol.RemoteClient``.

Each test spins up a *real* ``wsgiref.simple_server`` in a background
thread, points ``RemoteClient`` at ``http://127.0.0.1:<port>``, and lets
the actual ``urllib`` code paths run.  This covers the ~10 % of
``remote/protocol.py`` that unit-level mocks cannot reach:

  * ``_request_json`` happy path (JSON encode → HTTP → JSON decode)
  * ``_upload_blob`` / ``_download_blob`` octet-stream paths
  * HTTP 4xx / 5xx → ``RemoteError`` / ``AuthenticationError`` mapping
  * ``urllib.error.URLError`` (connection-refused) → ``RemoteError``
  * Token redaction in live error bodies

Design principles
-----------------
* One ``_VCSServer`` WSGI application owns all routing.  Each endpoint is
  a tiny, self-contained function — easy to read and change independently.
* The server is created fresh per test class (``autouse`` session-scoped
  fixture) so port allocation is automatic and tests are isolated.
* No third-party libraries.  Everything is stdlib + the vcs package.
* All assertions follow the same style as the existing unit tests.
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from io import BytesIO
from typing import Callable, Iterator
from wsgiref.simple_server import WSGIServer, make_server

import pytest

from vcs.remote.protocol import RemoteClient
from vcs.store.exceptions import AuthenticationError, RemoteError


# ---------------------------------------------------------------------------
# WSGI server helpers
# ---------------------------------------------------------------------------

_ResponseStartFn = Callable[[str, list[tuple[str, str]]], None]
_WSGIEnv = dict


def _json_response(
    start_response: _ResponseStartFn,
    status: str,
    data: dict,
) -> list[bytes]:
    body = json.dumps(data).encode()
    start_response(status, [
        ("Content-Type", "application/json"),
        ("Content-Length", str(len(body))),
    ])
    return [body]


def _bytes_response(
    start_response: _ResponseStartFn,
    status: str,
    data: bytes,
) -> list[bytes]:
    start_response(status, [
        ("Content-Type", "application/octet-stream"),
        ("Content-Length", str(len(data))),
    ])
    return [data]


class _VCSServer:
    """
    Minimal WSGI application that implements the six VCS sync endpoints.

    State is stored in plain dicts so each ``_VCSServer`` instance is an
    independent in-process "remote".
    """

    def __init__(self) -> None:
        # { branch: commit_hash }
        self.refs: dict[str, str] = {"main": "a" * 64}
        # { hex_hash: raw_bytes }
        self.objects: dict[str, bytes] = {}
        # Last commit body received via POST /commit
        self.last_commit: dict | None = None
        # If set, the next request to this path returns this HTTP status code
        self.force_error: dict[str, int] = {}

    def __call__(self, environ: _WSGIEnv, start_response: _ResponseStartFn) -> list[bytes]:
        method = environ["REQUEST_METHOD"]
        path = environ["PATH_INFO"]

        # ---- Force-error injection (for error-path tests) ---------------
        forced = self.force_error.get(path)
        if forced:
            msg = json.dumps({"error": "forced error"}).encode()
            start_response(f"{forced} Error", [
                ("Content-Type", "application/json"),
                ("Content-Length", str(len(msg))),
            ])
            return [msg]

        # ---- POST /refs  →  negotiate_refs --------------------------------
        if method == "POST" and path == "/refs":
            length = int(environ.get("CONTENT_LENGTH", 0) or 0)
            body = json.loads(environ["wsgi.input"].read(length))
            local_refs: dict[str, str] = body.get("refs", {})
            need = [h for h in local_refs.values() if h not in self.objects and h not in self.refs.values()]
            return _json_response(start_response, "200 OK", {"need": need})

        # ---- GET /refs  →  fetch_refs -------------------------------------
        if method == "GET" and path == "/refs":
            return _json_response(start_response, "200 OK", self.refs)

        # ---- PUT /objects/<hash>  →  upload_blob -------------------------
        if method == "PUT" and path.startswith("/objects/"):
            hex_hash = path.split("/objects/", 1)[1]
            length = int(environ.get("CONTENT_LENGTH", 0) or 0)
            self.objects[hex_hash] = environ["wsgi.input"].read(length)
            return _json_response(start_response, "200 OK", {"ok": True})

        # ---- GET /objects/<hash>  →  download_blob -----------------------
        if method == "GET" and path.startswith("/objects/"):
            hex_hash = path.split("/objects/", 1)[1]
            data = self.objects.get(hex_hash)
            if data is None:
                return _json_response(start_response, "404 Not Found", {"error": "not found"})
            return _bytes_response(start_response, "200 OK", data)

        # ---- POST /commit  →  upload_commit ------------------------------
        if method == "POST" and path == "/commit":
            length = int(environ.get("CONTENT_LENGTH", 0) or 0)
            self.last_commit = json.loads(environ["wsgi.input"].read(length))
            return _json_response(start_response, "200 OK", {"ok": True})

        # ---- POST /refs/update  →  update_ref ----------------------------
        if method == "POST" and path == "/refs/update":
            length = int(environ.get("CONTENT_LENGTH", 0) or 0)
            body = json.loads(environ["wsgi.input"].read(length))
            branch: str = body["branch"]
            commit_hash: str = body["hash"]
            # Reject if the branch already exists and is ahead (simulate diverged)
            current = self.refs.get(branch)
            if current and current != "a" * 64 and current != commit_hash:
                return _json_response(start_response, "200 OK", {"ok": False})
            self.refs[branch] = commit_hash
            return _json_response(start_response, "200 OK", {"ok": True})

        # ---- 404 fallthrough ----------------------------------------------
        return _json_response(start_response, "404 Not Found", {"error": "unknown route"})


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------

class _ServerContext:
    """Holds server + thread + URL, with a clean shutdown method."""

    def __init__(self, app: _VCSServer, httpd: WSGIServer, thread: threading.Thread) -> None:
        self.app = app
        self._httpd = httpd
        self._thread = thread
        host, port = httpd.server_address
        self.base_url = f"http://{host}:{port}"

    def client(self) -> RemoteClient:
        return RemoteClient(self.base_url)

    def shutdown(self) -> None:
        self._httpd.shutdown()
        self._thread.join(timeout=5)


@pytest.fixture(scope="function")
def server() -> Iterator[_ServerContext]:
    """
    Spin up a fresh _VCSServer for each test.

    ``scope="function"`` gives full isolation: each test starts with an
    empty object store and default refs.
    """
    app = _VCSServer()
    httpd = make_server("127.0.0.1", 0, app)  # port=0 → OS picks a free port
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    # Brief pause to ensure the server is accepting connections
    time.sleep(0.02)
    ctx = _ServerContext(app, httpd, thread)
    yield ctx
    ctx.shutdown()


@pytest.fixture(autouse=True)
def _set_auth_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test needs VCS_AUTH_TOKEN set; inject a dummy value."""
    monkeypatch.setenv("VCS_AUTH_TOKEN", "test-integration-token")


# ---------------------------------------------------------------------------
# negotiate_refs
# ---------------------------------------------------------------------------

class TestNegotiateRefs:
    def test_returns_empty_when_server_has_all_objects(self, server: _ServerContext) -> None:
        """If the server already knows all hashes, need list is empty."""
        # The default server has refs["main"] = "a"*64.
        # Send that exact hash as a local ref → server should say it needs nothing.
        client = server.client()
        needed = client.negotiate_refs({"main": "a" * 64})
        assert needed == []

    def test_returns_missing_hashes(self, server: _ServerContext) -> None:
        """Hashes the server has never seen should appear in the need list."""
        client = server.client()
        new_hash = "b" * 64
        needed = client.negotiate_refs({"main": new_hash})
        assert new_hash in needed

    def test_http_error_raises_remote_error(self, server: _ServerContext) -> None:
        """A 5xx from the server is surfaced as RemoteError."""
        server.app.force_error["/refs"] = 500
        client = server.client()
        with pytest.raises(RemoteError):
            client.negotiate_refs({"main": "c" * 64})

    def test_connection_refused_raises_remote_error(self) -> None:
        """Pointing RemoteClient at a closed port raises RemoteError."""
        # Port 1 is almost certainly not listening; use it to provoke ECONNREFUSED.
        client = RemoteClient("http://127.0.0.1:1")
        with pytest.raises(RemoteError):
            client.negotiate_refs({"main": "d" * 64})


# ---------------------------------------------------------------------------
# upload_blob / download_blob
# ---------------------------------------------------------------------------

class TestBlobRoundtrip:
    def test_upload_then_download_roundtrip(self, server: _ServerContext) -> None:
        """Bytes written via upload_blob are returned verbatim by download_blob."""
        client = server.client()
        blob_hash = "e" * 64
        payload = b"hello, integration test\x00\xff\xfe"
        client.upload_blob(blob_hash, payload)
        retrieved = client.download_blob(blob_hash)
        assert retrieved == payload

    def test_upload_blob_stores_exact_bytes(self, server: _ServerContext) -> None:
        """Server-side dict reflects the exact bytes uploaded."""
        client = server.client()
        blob_hash = "f" * 64
        data = b"\x00" * 1024
        client.upload_blob(blob_hash, data)
        assert server.app.objects[blob_hash] == data

    def test_upload_blob_http_error_raises_remote_error(self, server: _ServerContext) -> None:
        """A server-side failure during upload raises RemoteError."""
        server.app.force_error["/objects/" + "0" * 64] = 500
        client = server.client()
        with pytest.raises(RemoteError):
            client.upload_blob("0" * 64, b"data")

    def test_download_blob_missing_raises_remote_error(self, server: _ServerContext) -> None:
        """Downloading a hash that was never uploaded raises RemoteError."""
        client = server.client()
        # 404 from server → RemoteError
        with pytest.raises(RemoteError):
            client.download_blob("9" * 64)

    def test_download_blob_network_error_raises_remote_error(self) -> None:
        """Network-level failure during download raises RemoteError."""
        client = RemoteClient("http://127.0.0.1:1")
        with pytest.raises(RemoteError):
            client.download_blob("1" * 64)

    def test_large_blob_roundtrip(self, server: _ServerContext) -> None:
        """Blobs approaching 1 MB roundtrip correctly (no chunking surprises)."""
        client = server.client()
        blob_hash = "a" * 62 + "01"
        payload = b"x" * (512 * 1024)  # 512 KB
        client.upload_blob(blob_hash, payload)
        assert client.download_blob(blob_hash) == payload


# ---------------------------------------------------------------------------
# upload_commit
# ---------------------------------------------------------------------------

class TestUploadCommit:
    def test_commit_metadata_received_by_server(self, server: _ServerContext) -> None:
        """upload_commit delivers the full dict to the server."""
        client = server.client()
        commit_data = {
            "hash": "c" * 64,
            "tree_hash": "d" * 64,
            "parent_hashes": [],
            "author": "Test <test@example.com>",
            "timestamp": "2026-01-01T00:00:00Z",
            "message": "integration test commit",
        }
        client.upload_commit(commit_data)
        assert server.app.last_commit == commit_data

    def test_upload_commit_http_error_raises_remote_error(self, server: _ServerContext) -> None:
        server.app.force_error["/commit"] = 422
        client = server.client()
        with pytest.raises(RemoteError):
            client.upload_commit({"hash": "e" * 64})


# ---------------------------------------------------------------------------
# update_ref
# ---------------------------------------------------------------------------

class TestUpdateRef:
    def test_update_ref_accepted(self, server: _ServerContext) -> None:
        """Server accepts a ref update and advances the branch tip."""
        client = server.client()
        new_hash = "f" * 64
        client.update_ref("main", new_hash)
        assert server.app.refs["main"] == new_hash

    def test_update_ref_creates_new_branch(self, server: _ServerContext) -> None:
        """update_ref can create a branch that did not previously exist."""
        client = server.client()
        client.update_ref("feature", "1" * 64)
        assert server.app.refs.get("feature") == "1" * 64

    def test_update_ref_rejected_raises_remote_error(self, server: _ServerContext) -> None:
        """When the server returns ok=False, RemoteError is raised."""
        # Simulate a diverged remote: set a non-default, non-matching ref so
        # _VCSServer.__call__ returns {"ok": False}.
        server.app.refs["main"] = "diverged" + "0" * 57
        client = server.client()
        with pytest.raises(RemoteError, match="rejected|diverged"):
            client.update_ref("main", "different" + "0" * 55)

    def test_update_ref_http_error_raises_remote_error(self, server: _ServerContext) -> None:
        server.app.force_error["/refs/update"] = 500
        client = server.client()
        with pytest.raises(RemoteError):
            client.update_ref("main", "2" * 64)


# ---------------------------------------------------------------------------
# fetch_refs
# ---------------------------------------------------------------------------

class TestFetchRefs:
    def test_returns_server_refs(self, server: _ServerContext) -> None:
        """fetch_refs returns exactly the server's current refs dict."""
        server.app.refs = {"main": "a" * 64, "dev": "b" * 64}
        client = server.client()
        refs = client.fetch_refs()
        assert refs == {"main": "a" * 64, "dev": "b" * 64}

    def test_returns_empty_when_no_refs(self, server: _ServerContext) -> None:
        server.app.refs = {}
        client = server.client()
        assert client.fetch_refs() == {}

    def test_http_error_raises_remote_error(self, server: _ServerContext) -> None:
        server.app.force_error["/refs"] = 503
        client = server.client()
        with pytest.raises(RemoteError):
            client.fetch_refs()

    def test_network_error_raises_remote_error(self) -> None:
        client = RemoteClient("http://127.0.0.1:1")
        with pytest.raises(RemoteError):
            client.fetch_refs()


# ---------------------------------------------------------------------------
# Authentication / token-redaction live paths
# ---------------------------------------------------------------------------

class TestAuthLivePaths:
    def test_missing_token_raises_authentication_error(
        self,
        server: _ServerContext,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Unset VCS_AUTH_TOKEN raises AuthenticationError before any HTTP call."""
        monkeypatch.delenv("VCS_AUTH_TOKEN", raising=False)
        client = server.client()
        with pytest.raises(AuthenticationError):
            client.fetch_refs()

    def test_token_not_in_remote_error_message(
        self,
        server: _ServerContext,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Even when the server echoes the token back in an error body,
        the raised RemoteError must NOT contain the raw token value.
        """
        secret = "super-secret-token-xyz"
        monkeypatch.setenv("VCS_AUTH_TOKEN", secret)

        # Make the server echo the token in its error payload.
        original_call = server.app.__call__

        def leaky_app(environ, start_response):
            if environ["PATH_INFO"] == "/refs" and environ["REQUEST_METHOD"] == "GET":
                body = json.dumps({"error": f"bad token: {secret}"}).encode()
                start_response("401 Unauthorized", [
                    ("Content-Type", "application/json"),
                    ("Content-Length", str(len(body))),
                ])
                return [body]
            return original_call(environ, start_response)

        server._httpd.set_app(leaky_app)

        client = server.client()
        with pytest.raises(RemoteError) as exc_info:
            client.fetch_refs()

        assert secret not in str(exc_info.value), (
            "Raw auth token leaked into RemoteError message!"
        )


# ---------------------------------------------------------------------------
# Full push simulation (negotiate → upload blobs → upload commit → update ref)
# ---------------------------------------------------------------------------

class TestFullPushSimulation:
    """
    Simulate the six-step push handshake end-to-end against a real HTTP server.
    This exercises every RemoteClient method in sequence, mirroring what
    vcs.remote.ops.push() does, but without the repo/store layer.
    """

    def test_full_push_handshake(self, server: _ServerContext) -> None:
        client = server.client()
        blob_hash = "b" * 64
        tree_hash = "t" * 62 + "00"
        commit_hash = "c" * 62 + "00"

        blob_data = b"print('hello')\n"
        tree_data = json.dumps({
            "type": "tree",
            "entries": [{"mode": "100644", "name": "hello.py", "object_hash": blob_hash}],
        }).encode()
        commit_data = {
            "hash": commit_hash,
            "tree_hash": tree_hash,
            "parent_hashes": [],
            "author": "Dev <dev@example.com>",
            "timestamp": "2026-02-22T00:00:00Z",
            "message": "push integration test",
        }

        # Step 1 — negotiate
        needed = client.negotiate_refs({"main": commit_hash})
        assert commit_hash in needed

        # Step 2 — upload blobs
        client.upload_blob(blob_hash, blob_data)
        client.upload_blob(tree_hash, tree_data)

        # Step 3 — upload commit metadata
        client.upload_commit(commit_data)

        # Step 4 — update ref
        client.update_ref("main", commit_hash)

        # Verify server state
        assert server.app.objects[blob_hash] == blob_data
        assert server.app.objects[tree_hash] == tree_data
        assert server.app.last_commit == commit_data
        assert server.app.refs["main"] == commit_hash

    def test_full_pull_simulation(self, server: _ServerContext) -> None:
        """Simulate the fetch side: fetch_refs → download blobs."""
        blob_hash = "d" * 64
        server.app.objects[blob_hash] = b"remote content"
        server.app.refs["main"] = blob_hash  # server has a "commit" at blob_hash

        client = server.client()
        refs = client.fetch_refs()
        assert refs["main"] == blob_hash

        data = client.download_blob(blob_hash)
        assert data == b"remote content"