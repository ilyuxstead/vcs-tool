"""
tests/unit/test_remote.py — remote protocol and ops unit tests.

Remote protocol tests use unittest.mock to avoid real network calls.
Remote ops tests mock RemoteClient to test the business logic in isolation.
"""

from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from vcs.remote.protocol import RemoteClient, _get_token, _redact, _headers
from vcs.remote.ops import add, list_all, push, fetch, pull
from vcs.repo.init import init_repo, resolve_head_commit, vcs_dir
from vcs.commit.stage import stage_files
from vcs.commit.snapshot import create_snapshot
from vcs.store.db import add_remote, open_db
from vcs.store.exceptions import AuthenticationError, RemoteError
from vcs.store.objects import ObjectStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_repo_with_commit(tmp_path: Path, author: str = "Dev <dev@test.com>") -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    init_repo(root)
    f = root / "main.py"
    f.write_text("print('hello')")
    stage_files([f], root)
    create_snapshot("Initial commit", author, root)
    return root


def _sha3(data: bytes) -> str:
    return hashlib.sha3_256(data).hexdigest()


def _make_remote_tree_blob(entries: list[dict] | None = None) -> tuple[str, bytes]:
    """Return (hash, raw) for a well-formed tree blob."""
    raw = json.dumps(
        {"type": "tree", "entries": entries or []},
        sort_keys=True,
    ).encode()
    return _sha3(raw), raw


def _make_remote_commit_blob(
    tree_hash: str,
    parent_hashes: list[str] | None = None,
    message: str = "remote commit",
    author: str = "Remote <r@example.com>",
    timestamp: str = "2026-01-01T00:00:00Z",
) -> tuple[str, bytes]:
    """Return (hash, raw) for a well-formed commit blob that _parse_commit_blob() accepts."""
    raw = json.dumps(
        {
            "type": "commit",
            "tree_hash": tree_hash,
            "parent_hashes": parent_hashes or [],
            "author": author,
            "timestamp": timestamp,
            "message": message,
        },
        sort_keys=True,
    ).encode()
    return _sha3(raw), raw


# ---------------------------------------------------------------------------
# Protocol — _get_token / _redact / _headers
# ---------------------------------------------------------------------------

class TestProtocolToken:
    def test_get_token_reads_env(self):
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "tok123"}):
            assert _get_token() == "tok123"

    def test_get_token_missing_raises(self):
        env = {k: v for k, v in os.environ.items() if k != "VCS_AUTH_TOKEN"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(AuthenticationError):
                _get_token()

    def test_redact_replaces_token(self):
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "secret"}):
            out = _redact("bearer secret in log")
            assert "secret" not in out
            assert "<REDACTED>" in out

    def test_headers_include_bearer(self):
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "mytoken"}):
            h = _headers()
            assert h["Authorization"] == "Bearer mytoken"

    def test_headers_include_extra(self):
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "t"}):
            h = _headers({"X-Custom": "value"})
            assert h["X-Custom"] == "value"


# ---------------------------------------------------------------------------
# Protocol — RemoteClient (mocked HTTP)
# ---------------------------------------------------------------------------

class TestRemoteClient:
    def _make_response(self, data: dict) -> MagicMock:
        resp = MagicMock()
        resp.read.return_value = json.dumps(data).encode()
        resp.__enter__ = lambda s: s
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    def _make_bytes_response(self, data: bytes) -> MagicMock:
        resp = MagicMock()
        resp.read.return_value = data
        resp.__enter__ = lambda s: s
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    def test_negotiate_refs_returns_needed(self):
        client = RemoteClient("https://example.com")
        resp = self._make_response({"need": ["abc123", "def456"]})
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "tok"}):
            with patch("urllib.request.urlopen", return_value=resp):
                needed = client.negotiate_refs({"main": "abc123"})
        assert needed == ["abc123", "def456"]

    def test_negotiate_refs_empty_need(self):
        client = RemoteClient("https://example.com")
        resp = self._make_response({"need": []})
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "tok"}):
            with patch("urllib.request.urlopen", return_value=resp):
                needed = client.negotiate_refs({"main": "abc123"})
        assert needed == []

    def test_upload_blob_sends_put(self):
        client = RemoteClient("https://example.com")
        resp = self._make_bytes_response(b"ok")
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "tok"}):
            with patch("urllib.request.urlopen", return_value=resp) as mock_open:
                client.upload_blob("abc123", b"blob data")
        req = mock_open.call_args[0][0]
        assert req.get_method() == "PUT"
        assert "abc123" in req.full_url

    def test_upload_commit_sends_post(self):
        client = RemoteClient("https://example.com")
        resp = self._make_response({})
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "tok"}):
            with patch("urllib.request.urlopen", return_value=resp) as mock_open:
                client.upload_commit({"hash": "abc", "message": "test"})
        req = mock_open.call_args[0][0]
        assert req.get_method() == "POST"

    def test_update_ref_success(self):
        client = RemoteClient("https://example.com")
        resp = self._make_response({"ok": True})
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "tok"}):
            with patch("urllib.request.urlopen", return_value=resp):
                client.update_ref("main", "abc123")  # should not raise

    def test_update_ref_rejected_raises(self):
        client = RemoteClient("https://example.com")
        resp = self._make_response({"ok": False})
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "tok"}):
            with patch("urllib.request.urlopen", return_value=resp):
                with pytest.raises(RemoteError, match="rejected"):
                    client.update_ref("main", "abc123")

    def test_fetch_refs_returns_dict(self):
        client = RemoteClient("https://example.com")
        resp = self._make_response({"main": "abc123"})
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "tok"}):
            with patch("urllib.request.urlopen", return_value=resp):
                refs = client.fetch_refs()
        assert refs == {"main": "abc123"}

    def test_download_blob_returns_bytes(self):
        client = RemoteClient("https://example.com")
        resp = self._make_bytes_response(b"blob content")
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "tok"}):
            with patch("urllib.request.urlopen", return_value=resp):
                data = client.download_blob("abc123")
        assert data == b"blob content"

    def test_http_error_raises_remote_error(self):
        client = RemoteClient("https://example.com")
        err = urllib.error.HTTPError(
            url="https://example.com/refs",
            code=403,
            msg="Forbidden",
            hdrs={},
            fp=BytesIO(b"forbidden"),
        )
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "tok"}):
            with patch("urllib.request.urlopen", side_effect=err):
                with pytest.raises(RemoteError, match="403"):
                    client.negotiate_refs({"main": "abc"})

    def test_url_error_raises_remote_error(self):
        client = RemoteClient("https://example.com")
        err = urllib.error.URLError(reason="Name resolution failed")
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "tok"}):
            with patch("urllib.request.urlopen", side_effect=err):
                with pytest.raises(RemoteError):
                    client.negotiate_refs({"main": "abc"})

    def test_token_not_in_error_message(self):
        """HTTP error messages must never contain the token."""
        client = RemoteClient("https://example.com")
        err = urllib.error.HTTPError(
            url="https://example.com/refs",
            code=401,
            msg="Unauthorized",
            hdrs={},
            fp=BytesIO(b"my-secret-token-in-body"),
        )
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "my-secret-token-in-body"}):
            with patch("urllib.request.urlopen", side_effect=err):
                with pytest.raises(RemoteError) as exc_info:
                    client.negotiate_refs({"main": "abc"})
        assert "my-secret-token-in-body" not in str(exc_info.value)

    def test_base_url_trailing_slash_stripped(self):
        client = RemoteClient("https://example.com/")
        assert client.base_url == "https://example.com"


# ---------------------------------------------------------------------------
# Remote ops — add / list_all
# ---------------------------------------------------------------------------

class TestRemoteOps:
    def test_add_and_list(self, tmp_path: Path):
        root = _make_repo_with_commit(tmp_path)
        add("origin", "https://example.com/repo", root)
        remotes = list_all(root)
        assert len(remotes) == 1
        assert remotes[0]["name"] == "origin"
        assert remotes[0]["url"] == "https://example.com/repo"

    def test_add_duplicate_raises(self, tmp_path: Path):
        root = _make_repo_with_commit(tmp_path)
        add("origin", "https://example.com/repo", root)
        with pytest.raises(RemoteError):
            add("origin", "https://other.com/repo", root)

    def test_list_empty(self, tmp_path: Path):
        root = _make_repo_with_commit(tmp_path)
        remotes = list_all(root)
        assert remotes == []


# ---------------------------------------------------------------------------
# Remote ops — push (mocked RemoteClient)
# ---------------------------------------------------------------------------

class TestRemotePush:
    def _mock_client(self, needed_hashes=None, remote_refs=None):
        m = MagicMock(spec=RemoteClient)
        m.negotiate_refs.return_value = needed_hashes or []
        # fetch_refs must return a real dict; push() uses it to seed
        # server_known_commits for the BFS stop condition.
        m.fetch_refs.return_value = remote_refs if remote_refs is not None else {}
        m.update_ref.return_value = None
        m.upload_blob.return_value = None
        m.upload_commit.return_value = None
        return m

    def test_push_success_no_blobs_needed(self, tmp_path: Path):
        root = _make_repo_with_commit(tmp_path)
        add("origin", "https://example.com", root)
        mock_client = self._mock_client(needed_hashes=[])
        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            result = push("origin", repo_root=root)
        assert result["blobs_uploaded"] == 0
        assert result["remote"] == "origin"
        mock_client.update_ref.assert_called_once()

    def test_push_uploads_needed_blobs(self, tmp_path: Path):
        root = _make_repo_with_commit(tmp_path)
        add("origin", "https://example.com", root)
        store = ObjectStore(vcs_dir(root) / "objects")
        all_hashes = store.all_hashes()
        # Empty remote: fetch_refs={} so no BFS boundary; server needs everything.
        mock_client = self._mock_client(needed_hashes=all_hashes, remote_refs={})
        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            result = push("origin", repo_root=root)
        # push() separates uploads into blobs / trees / commits.
        # The sum of all three must equal the total objects in the store.
        total_uploaded = (
            result["blobs_uploaded"]
            + result["trees_uploaded"]
            + result["commits_uploaded"]
        )
        assert total_uploaded == len(all_hashes), (
            f"Expected {len(all_hashes)} total uploads, got {total_uploaded} "
            f"(blobs={result['blobs_uploaded']}, trees={result['trees_uploaded']}, "
            f"commits={result['commits_uploaded']})"
        )

    def test_push_missing_remote_raises(self, tmp_path: Path):
        root = _make_repo_with_commit(tmp_path)
        with pytest.raises(RemoteError):
            push("nonexistent", repo_root=root)

    def test_push_detached_head_raises(self, tmp_path: Path):
        root = _make_repo_with_commit(tmp_path)
        add("origin", "https://example.com", root)
        # Force detached HEAD
        from vcs.repo.init import write_head
        commit_hash = resolve_head_commit(root)
        write_head(root, commit_hash)
        with patch("vcs.remote.ops.RemoteClient", return_value=self._mock_client()):
            with pytest.raises(RemoteError, match="detached"):
                push("origin", repo_root=root)

    def test_push_specific_branch(self, tmp_path: Path):
        root = _make_repo_with_commit(tmp_path)
        add("origin", "https://example.com", root)
        mock_client = self._mock_client()
        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            result = push("origin", branch_name="main", repo_root=root)
        assert result["branch"] == "main"


# ---------------------------------------------------------------------------
# Remote ops — fetch (mocked RemoteClient)
# ---------------------------------------------------------------------------

class TestRemoteFetch:
    def test_fetch_downloads_missing_blobs(self, tmp_path: Path):
        # fetch() calls _walk_and_ingest(), which downloads each ref tip and
        # immediately parses it as a commit via _parse_commit_blob().
        # Supplying raw bytes that aren't valid commit JSON causes:
        #   RemoteError: Corrupt commit object ...
        # We must provide a well-formed commit + tree blob pair.
        root = _make_repo_with_commit(tmp_path)
        add("origin", "https://example.com", root)

        tree_hash, tree_raw = _make_remote_tree_blob([])
        commit_hash, commit_raw = _make_remote_commit_blob(tree_hash, [])

        blob_map = {commit_hash: commit_raw, tree_hash: tree_raw}
        mock_client = MagicMock(spec=RemoteClient)
        mock_client.fetch_refs.return_value = {"main": commit_hash}
        mock_client.download_blob.side_effect = lambda h: blob_map[h]

        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            result = fetch("origin", repo_root=root)

        # Commit blob + tree blob were both downloaded.
        assert result["commits_fetched"] == 1
        assert result["blobs_downloaded"] >= 1
        assert mock_client.download_blob.call_count >= 1

    def test_fetch_skips_existing_blobs(self, tmp_path: Path):
        # _walk_and_ingest() guards on commit_exists(conn, hash) in SQLite,
        # not on whether the hash is present in the object store.  Passing a
        # raw file-blob hash (object store only, no SQLite row) caused the
        # guard to miss, download_blob to be called with a MagicMock default
        # return value, and store.write() to raise:
        #   TypeError: object supporting the buffer API required
        #
        # The correct sentinel is the HEAD commit hash, which IS in SQLite.
        root = _make_repo_with_commit(tmp_path)
        add("origin", "https://example.com", root)

        local_tip = resolve_head_commit(root)

        mock_client = MagicMock(spec=RemoteClient)
        mock_client.fetch_refs.return_value = {"main": local_tip}

        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            result = fetch("origin", repo_root=root)

        assert result["blobs_downloaded"] == 0
        assert result["commits_fetched"] == 0
        mock_client.download_blob.assert_not_called()

    def test_fetch_missing_remote_raises(self, tmp_path: Path):
        root = _make_repo_with_commit(tmp_path)
        with pytest.raises(RemoteError):
            fetch("nonexistent", repo_root=root)


# ---------------------------------------------------------------------------
# Remote ops — pull
# ---------------------------------------------------------------------------

class TestRemotePull:
    def test_pull_fetch_only(self, tmp_path: Path):
        root = _make_repo_with_commit(tmp_path)
        add("origin", "https://example.com", root)
        mock_client = MagicMock(spec=RemoteClient)
        mock_client.fetch_refs.return_value = {}
        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            result = pull("origin", repo_root=root, fetch_only=True)
        assert result["merged"] is False
        assert "blobs_downloaded" in result

    def test_pull_without_fetch_only(self, tmp_path: Path):
        root = _make_repo_with_commit(tmp_path)
        add("origin", "https://example.com", root)
        mock_client = MagicMock(spec=RemoteClient)
        mock_client.fetch_refs.return_value = {}
        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            result = pull("origin", repo_root=root)
        assert "merged" in result