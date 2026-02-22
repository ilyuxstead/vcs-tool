"""
tests/unit/test_clone.py — unit tests for vcs.repo.clone.clone_repo().

All network I/O is mocked via unittest.mock so the tests run fully
offline.  Each test covers exactly one behaviour or error path, in the
Unix-philosophy spirit of small focused assertions.

Test matrix
-----------
TestCloneRepoSuccess
    test_clone_happy_path            — normal single-branch clone
    test_clone_creates_dest_dir      — dest directory is auto-created
    test_clone_default_dest_name     — dest derived from URL slug when omitted
    test_clone_wires_origin_remote   — "origin" remote is registered in new DB
    test_clone_writes_working_tree   — files land on disk after clone
    test_clone_sets_head             — HEAD points at the right branch
    test_clone_empty_repo            — server returns no refs → empty repo wired
    test_clone_depth_limits_chain    — depth=1 stops after one commit
    test_clone_multi_branch          — all branches created locally
    test_clone_returns_repo_root     — return value is the repo root Path

TestCloneRepoBlobHandling
    test_clone_skips_existing_blobs  — duplicate blobs not re-downloaded
    test_clone_reconstructs_index    — write_index called with correct mapping

TestCloneRepoErrors
    test_clone_fetch_refs_network_failure  — RemoteError on fetch_refs → CloneError
    test_clone_commit_download_failure     — RemoteError on blob download → CloneError
    test_clone_corrupt_commit_object       — JSON parse failure → CloneError
    test_clone_wrong_object_type           — tree blob returned for commit → CloneError
    test_clone_dest_already_exists         — RepositoryExistsError bubbles up
    test_clone_malformed_commit_dict       — missing 'hash' key → CloneError

TestCloneDispatchIntegration    (CLI-level, in test_cli_dispatch.py style)
    — see tests/integration/test_cli_dispatch.py::TestRepoClone
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from vcs.repo.clone import clone_repo, _resolve_dest, _fetch_commit_chain
from vcs.repo.init import init_repo, vcs_dir, find_repo_root
from vcs.store.db import open_db, get_remote, list_branches
from vcs.store.exceptions import CloneError, RemoteError, RepositoryExistsError
from vcs.remote.protocol import RemoteClient


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

FAKE_URL = "https://example.com/myrepo"

BLOB_HASH = "b" * 64
TREE_HASH = "t" * 64
COMMIT_HASH = "c" * 64

TREE_BLOB = json.dumps({
    "type": "tree",
    "entries": [
        {"mode": "100644", "name": "main.py", "object_hash": BLOB_HASH}
    ],
}).encode()

COMMIT_BLOB = json.dumps({
    "type": "commit",
    "hash": COMMIT_HASH,
    "tree_hash": TREE_HASH,
    "parent_hashes": [],
    "author": "Alice <alice@test.com>",
    "timestamp": "2026-01-01T00:00:00Z",
    "message": "Initial commit",
}).encode()

FILE_CONTENT = b"print('hello')\n"


def _make_mock_client(
    refs: dict[str, str] | None = None,
    blobs: dict[str, bytes] | None = None,
) -> MagicMock:
    """Return a RemoteClient mock wired with refs and blobs."""
    if refs is None:
        refs = {"main": COMMIT_HASH}
    if blobs is None:
        blobs = {
            COMMIT_HASH: COMMIT_BLOB,
            TREE_HASH: TREE_BLOB,
            BLOB_HASH: FILE_CONTENT,
        }

    client = MagicMock(spec=RemoteClient)
    client.fetch_refs.return_value = refs
    client.download_blob.side_effect = lambda h: blobs[h]
    return client


def _patch_client(client: MagicMock):
    """Context manager: patch RemoteClient constructor → client."""
    return patch("vcs.repo.clone.RemoteClient", return_value=client)


# ---------------------------------------------------------------------------
# Success paths
# ---------------------------------------------------------------------------

class TestCloneRepoSuccess:
    def test_clone_happy_path(self, tmp_path: Path):
        """A standard clone succeeds and returns a usable repo root."""
        dest = tmp_path / "clone"
        client = _make_mock_client()
        with _patch_client(client):
            root = clone_repo(FAKE_URL, dest)
        assert root == dest
        assert (dest / ".vcs").is_dir()

    def test_clone_creates_dest_dir(self, tmp_path: Path):
        """Destination directory is created automatically."""
        dest = tmp_path / "does" / "not" / "exist"
        client = _make_mock_client()
        with _patch_client(client):
            clone_repo(FAKE_URL, dest)
        assert dest.is_dir()

    def test_clone_default_dest_name(self, tmp_path: Path):
        """When dest is None, the directory name is derived from the URL slug."""
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            client = _make_mock_client()
            with _patch_client(client):
                root = clone_repo("https://example.com/myrepo")
            assert root.name == "myrepo"
        finally:
            os.chdir(old)

    def test_clone_default_dest_strips_vcs_suffix(self, tmp_path: Path):
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            client = _make_mock_client()
            with _patch_client(client):
                root = clone_repo("https://example.com/myrepo.vcs")
            assert root.name == "myrepo"
        finally:
            os.chdir(old)

    def test_clone_wires_origin_remote(self, tmp_path: Path):
        """After clone, the DB has an 'origin' remote with the correct URL."""
        dest = tmp_path / "clone"
        client = _make_mock_client()
        with _patch_client(client):
            clone_repo(FAKE_URL, dest)
        conn = open_db(vcs_dir(dest) / "vcs.db")
        try:
            remote = get_remote(conn, "origin")
        finally:
            conn.close()
        assert remote["url"] == FAKE_URL

    def test_clone_writes_working_tree(self, tmp_path: Path):
        """Files from the HEAD tree are written to the working directory."""
        dest = tmp_path / "clone"
        client = _make_mock_client()
        with _patch_client(client):
            clone_repo(FAKE_URL, dest)
        assert (dest / "main.py").exists()
        assert (dest / "main.py").read_bytes() == FILE_CONTENT

    def test_clone_sets_head(self, tmp_path: Path):
        """HEAD is written pointing at the default branch."""
        dest = tmp_path / "clone"
        client = _make_mock_client()
        with _patch_client(client):
            clone_repo(FAKE_URL, dest)
        head = (dest / ".vcs" / "HEAD").read_text(encoding="utf-8").strip()
        assert head == "ref: refs/branches/main"

    def test_clone_empty_repo(self, tmp_path: Path):
        """Cloning a server with no refs produces an empty but valid local repo."""
        dest = tmp_path / "empty"
        client = _make_mock_client(refs={}, blobs={})
        with _patch_client(client):
            root = clone_repo(FAKE_URL, dest)
        assert (root / ".vcs").is_dir()
        # No working tree files (nothing to check out)
        assert not list(root.glob("*.py"))

    def test_clone_depth_limits_chain(self, tmp_path: Path):
        """depth=1 stops the commit chain walk after a single commit."""
        dest = tmp_path / "shallow"
        parent_hash = "p" * 64
        parent_blob = json.dumps({
            "type": "commit",
            "hash": parent_hash,
            "tree_hash": TREE_HASH,
            "parent_hashes": [],
            "author": "Bob",
            "timestamp": "2025-01-01T00:00:00Z",
            "message": "Parent",
        }).encode()
        child_hash = "d" * 64
        child_blob = json.dumps({
            "type": "commit",
            "hash": child_hash,
            "tree_hash": TREE_HASH,
            "parent_hashes": [parent_hash],
            "author": "Alice",
            "timestamp": "2026-01-01T00:00:00Z",
            "message": "Child",
        }).encode()

        blobs = {
            child_hash: child_blob,
            parent_hash: parent_blob,
            TREE_HASH: TREE_BLOB,
            BLOB_HASH: FILE_CONTENT,
        }
        client = _make_mock_client(refs={"main": child_hash}, blobs=blobs)

        with _patch_client(client):
            clone_repo(FAKE_URL, dest, depth=1)

        # Only the tip commit (child) should have been downloaded.
        # download_blob should have been called for child + tree + blob,
        # but NOT for parent_hash.
        downloaded_hashes = {c.args[0] for c in client.download_blob.call_args_list}
        assert parent_hash not in downloaded_hashes
        assert child_hash in downloaded_hashes

    def test_clone_multi_branch(self, tmp_path: Path):
        """All remote branches are created in the local DB."""
        branch_a_hash = "a" * 64
        branch_b_hash = "e" * 64  # hex-safe
        commit_a = json.dumps({
            "type": "commit", "hash": branch_a_hash, "tree_hash": TREE_HASH,
            "parent_hashes": [], "author": "A", "timestamp": "2026-01-01T00:00:00Z",
            "message": "A",
        }).encode()
        commit_b = json.dumps({
            "type": "commit", "hash": branch_b_hash, "tree_hash": TREE_HASH,
            "parent_hashes": [], "author": "B", "timestamp": "2026-01-02T00:00:00Z",
            "message": "B",
        }).encode()
        blobs = {
            branch_a_hash: commit_a,
            branch_b_hash: commit_b,
            TREE_HASH: TREE_BLOB,
            BLOB_HASH: FILE_CONTENT,
        }
        refs = {"main": branch_a_hash, "feature": branch_b_hash}
        client = _make_mock_client(refs=refs, blobs=blobs)

        dest = tmp_path / "multi"
        with _patch_client(client):
            clone_repo(FAKE_URL, dest)

        conn = open_db(vcs_dir(dest) / "vcs.db")
        try:
            branches = {b["name"] for b in list_branches(conn)}
        finally:
            conn.close()

        assert "main" in branches
        assert "feature" in branches

    def test_clone_returns_repo_root(self, tmp_path: Path):
        """Return value is exactly the repo root Path (find_repo_root compatible)."""
        dest = tmp_path / "clone"
        client = _make_mock_client()
        with _patch_client(client):
            root = clone_repo(FAKE_URL, dest)
        assert find_repo_root(root) == root


# ---------------------------------------------------------------------------
# Blob handling details
# ---------------------------------------------------------------------------

class TestCloneRepoBlobHandling:
    def test_clone_skips_existing_blobs(self, tmp_path: Path):
        """Blobs already in the object store are not re-downloaded."""
        dest = tmp_path / "clone"
        client = _make_mock_client()
        with _patch_client(client):
            clone_repo(FAKE_URL, dest)

        # Clone again into a different dest but share object store? No —
        # instead verify that a second clone of the same repo only calls
        # download_blob for the commit and tree, not for a blob it already owns.
        dest2 = tmp_path / "clone2"
        client2 = _make_mock_client()

        # Pre-populate dest2's object store with the file blob
        dest2.mkdir(parents=True)
        init_repo(dest2)
        from vcs.store.objects import ObjectStore
        store2 = ObjectStore(vcs_dir(dest2) / "objects")
        store2.write(FILE_CONTENT)

        with _patch_client(client2):
            clone_repo(FAKE_URL, dest2)

        downloaded = {c.args[0] for c in client2.download_blob.call_args_list}
        # The file blob (BLOB_HASH) should NOT be downloaded again
        assert BLOB_HASH not in downloaded

    def test_clone_reconstructs_index(self, tmp_path: Path):
        """The staging index after clone matches the HEAD tree."""
        dest = tmp_path / "clone"
        client = _make_mock_client()
        with _patch_client(client):
            clone_repo(FAKE_URL, dest)

        from vcs.repo.status import read_index
        index = read_index(dest)
        assert "main.py" in index
        assert index["main.py"] == BLOB_HASH


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

class TestCloneRepoErrors:
    def test_clone_fetch_refs_network_failure(self, tmp_path: Path):
        """RemoteError during fetch_refs is wrapped as CloneError."""
        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.side_effect = RemoteError("connection refused")
        dest = tmp_path / "fail"
        with _patch_client(client):
            with pytest.raises(CloneError, match="Cannot reach remote"):
                clone_repo(FAKE_URL, dest)

    def test_clone_commit_download_failure(self, tmp_path: Path):
        """RemoteError during commit blob download is wrapped as CloneError."""
        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.return_value = {"main": COMMIT_HASH}
        client.download_blob.side_effect = RemoteError("404 not found")
        dest = tmp_path / "fail"
        with _patch_client(client):
            with pytest.raises(CloneError, match="Failed to download commit"):
                clone_repo(FAKE_URL, dest)

    def test_clone_corrupt_commit_object(self, tmp_path: Path):
        """Unparseable JSON for a commit blob raises CloneError."""
        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.return_value = {"main": COMMIT_HASH}
        client.download_blob.return_value = b"not json {"
        dest = tmp_path / "fail"
        with _patch_client(client):
            with pytest.raises(CloneError, match="Corrupt commit object"):
                clone_repo(FAKE_URL, dest)

    def test_clone_wrong_object_type_for_commit(self, tmp_path: Path):
        """A blob with type != 'commit' at the tip position raises CloneError."""
        wrong_blob = json.dumps({"type": "tree", "entries": []}).encode()
        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.return_value = {"main": COMMIT_HASH}
        client.download_blob.return_value = wrong_blob
        dest = tmp_path / "fail"
        with _patch_client(client):
            with pytest.raises(CloneError, match="Expected commit object"):
                clone_repo(FAKE_URL, dest)

    def test_clone_dest_already_exists(self, tmp_path: Path):
        """RepositoryExistsError bubbles up if dest already has a .vcs dir."""
        dest = tmp_path / "existing"
        init_repo(dest)
        client = _make_mock_client()
        with _patch_client(client):
            with pytest.raises(RepositoryExistsError):
                clone_repo(FAKE_URL, dest)

    def test_clone_malformed_commit_dict(self, tmp_path: Path):
        """A commit blob missing the 'hash' key raises CloneError."""
        bad_commit = json.dumps({
            "type": "commit",
            # 'hash' deliberately omitted
            "tree_hash": TREE_HASH,
            "parent_hashes": [],
            "author": "X",
            "timestamp": "2026-01-01T00:00:00Z",
            "message": "bad",
        }).encode()
        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.return_value = {"main": COMMIT_HASH}
        client.download_blob.return_value = bad_commit
        dest = tmp_path / "fail"
        with _patch_client(client):
            with pytest.raises(CloneError, match="Malformed commit object"):
                clone_repo(FAKE_URL, dest)


# ---------------------------------------------------------------------------
# Internal helper unit tests
# ---------------------------------------------------------------------------

class TestResolveDestHelper:
    def test_explicit_dest_returned_resolved(self, tmp_path: Path):
        dest = tmp_path / "mydir"
        assert _resolve_dest("https://example.com/repo", dest) == dest.resolve()

    def test_derives_slug_from_url(self, tmp_path: Path):
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = _resolve_dest("https://example.com/cool-project", None)
            assert result.name == "cool-project"
        finally:
            os.chdir(old)

    def test_fallback_for_empty_slug(self, tmp_path: Path):
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = _resolve_dest("https://example.com/", None)
            assert result.name == "cloned_repo"
        finally:
            os.chdir(old)


class TestFetchCommitChainHelper:
    def test_single_commit_no_parents(self):
        client = MagicMock(spec=RemoteClient)
        client.download_blob.return_value = COMMIT_BLOB
        chain = _fetch_commit_chain(client, COMMIT_HASH, depth=None)
        assert len(chain) == 1
        assert chain[0]["hash"] == COMMIT_HASH

    def test_depth_one_stops_at_tip(self):
        parent_hash = "p" * 64
        child_blob = json.dumps({
            "type": "commit", "hash": "d" * 64, "tree_hash": TREE_HASH,
            "parent_hashes": [parent_hash], "author": "X",
            "timestamp": "2026-01-01T00:00:00Z", "message": "child",
        }).encode()
        client = MagicMock(spec=RemoteClient)
        client.download_blob.return_value = child_blob
        chain = _fetch_commit_chain(client, "d" * 64, depth=1)
        assert len(chain) == 1
        # Parent must NOT have been fetched
        client.download_blob.assert_called_once_with("d" * 64)

    def test_network_failure_raises_clone_error(self):
        client = MagicMock(spec=RemoteClient)
        client.download_blob.side_effect = RemoteError("timeout")
        with pytest.raises(CloneError):
            _fetch_commit_chain(client, COMMIT_HASH, depth=None)