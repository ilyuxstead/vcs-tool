"""
Append this class to tests/integration/test_cli_dispatch.py

It follows the exact style of the existing test classes (TestRepoInit,
TestRepoStatus, etc.) and uses the same run() / run_json() helpers that
are already defined at the top of that file.
"""

# -----------------------------------------------------------------------
# Add the following imports to the top of test_cli_dispatch.py if not
# already present (they should already be there):
#
#   from unittest.mock import patch, MagicMock
#   from vcs.remote.protocol import RemoteClient
# -----------------------------------------------------------------------

import json as _json
from pathlib import Path
from unittest.mock import MagicMock, patch

# Paste the constants + helper below *inside* the test file, or keep them
# local to the class using class-level attributes.

_FAKE_URL = "https://example.com/testrepo"
_BLOB_HASH = "b" * 64
_TREE_HASH = "t" * 64
_COMMIT_HASH = "c" * 64

_TREE_BLOB = _json.dumps({
    "type": "tree",
    "entries": [{"mode": "100644", "name": "hello.py", "object_hash": _BLOB_HASH}],
}).encode()
_COMMIT_BLOB = _json.dumps({
    "type": "commit",
    "hash": _COMMIT_HASH,
    "tree_hash": _TREE_HASH,
    "parent_hashes": [],
    "author": "Remote User <remote@test.local>",
    "timestamp": "2026-01-01T00:00:00Z",
    "message": "Initial remote commit",
}).encode()
_FILE_CONTENT = b"print('hello from remote')\n"


def _mock_client_for_clone(
    refs=None,
    blobs=None,
):
    """Build a RemoteClient mock wired with the default happy-path data."""
    if refs is None:
        refs = {"main": _COMMIT_HASH}
    if blobs is None:
        blobs = {
            _COMMIT_HASH: _COMMIT_BLOB,
            _TREE_HASH: _TREE_BLOB,
            _BLOB_HASH: _FILE_CONTENT,
        }
    m = MagicMock()
    m.fetch_refs.return_value = refs
    m.download_blob.side_effect = lambda h: blobs[h]
    return m


class TestRepoClone:
    """Integration tests for ``vcs repo.clone``."""

    # ------------------------------------------------------------------
    # Happy paths
    # ------------------------------------------------------------------

    def test_clone_success(self, tmp_path: Path):
        """Basic clone into an explicit destination succeeds (exit 0)."""
        dest = tmp_path / "cloned"
        client = _mock_client_for_clone()
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            code, out, _ = run(["repo.clone", _FAKE_URL, str(dest)])
        assert code == 0
        assert (dest / ".vcs").is_dir()

    def test_clone_success_message_contains_url(self, tmp_path: Path):
        """Success output mentions the URL that was cloned."""
        dest = tmp_path / "cloned"
        client = _mock_client_for_clone()
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            code, out, _ = run(["repo.clone", _FAKE_URL, str(dest)])
        assert code == 0
        assert "testrepo" in out or _FAKE_URL in out

    def test_clone_json_output(self, tmp_path: Path):
        """``--json`` flag produces a machine-readable success response."""
        dest = tmp_path / "cloned"
        client = _mock_client_for_clone()
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            code, data, _ = run_json(["repo.clone", _FAKE_URL, str(dest)])
        assert code == 0
        assert data["success"] is True

    def test_clone_writes_files(self, tmp_path: Path):
        """Working tree is reconstructed — expected file exists after clone."""
        dest = tmp_path / "cloned"
        client = _mock_client_for_clone()
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            run(["repo.clone", _FAKE_URL, str(dest)])
        assert (dest / "hello.py").exists()
        assert (dest / "hello.py").read_bytes() == _FILE_CONTENT

    def test_clone_registers_origin_remote(self, tmp_path: Path):
        """After clone, ``remote.list`` shows 'origin'."""
        dest = tmp_path / "cloned"
        client = _mock_client_for_clone()
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            run(["repo.clone", _FAKE_URL, str(dest)])
        code, out, _ = run(["remote.list"], dest)
        assert code == 0
        assert "origin" in out

    def test_clone_shallow_depth_flag(self, tmp_path: Path):
        """``--depth 1`` is accepted without error."""
        dest = tmp_path / "shallow"
        client = _mock_client_for_clone()
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            code, _, err = run(["repo.clone", "--depth", "1", _FAKE_URL, str(dest)])
        assert code == 0

    def test_clone_status_clean_after_clone(self, tmp_path: Path):
        """``repo.status`` reports a clean working tree immediately after clone."""
        dest = tmp_path / "cloned"
        client = _mock_client_for_clone()
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            run(["repo.clone", _FAKE_URL, str(dest)])
        code, out, _ = run(["repo.status"], dest)
        assert code == 0
        assert "clean" in out.lower()

    # ------------------------------------------------------------------
    # Error paths
    # ------------------------------------------------------------------

    def test_clone_network_failure_exits_1(self, tmp_path: Path):
        """A network error from the remote exits with code 1."""
        from vcs.store.exceptions import RemoteError
        dest = tmp_path / "fail"
        client = MagicMock()
        client.fetch_refs.side_effect = RemoteError("connection refused")
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            code, _, err = run(["repo.clone", _FAKE_URL, str(dest)])
        assert code == 1
        assert "Error" in err

    def test_clone_network_failure_json(self, tmp_path: Path):
        """A network error returns JSON with success=False when --json is set."""
        from vcs.store.exceptions import RemoteError
        dest = tmp_path / "fail"
        client = MagicMock()
        client.fetch_refs.side_effect = RemoteError("timeout")
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            code, data, _ = run_json(["repo.clone", _FAKE_URL, str(dest)])
        assert code == 1
        assert data["success"] is False

    def test_clone_dest_already_exists_exits_1(self, tmp_path: Path):
        """Cloning into an existing repo exits with code 1."""
        dest = tmp_path / "existing"
        run(["repo.init", str(dest)])  # pre-init destination
        client = _mock_client_for_clone()
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            code, _, err = run(["repo.clone", _FAKE_URL, str(dest)])
        assert code == 1
        assert "Error" in err