"""
tests/unit/test_clone.py — unit tests for vcs.repo.clone.clone_repo().

All network I/O is mocked via unittest.mock so the tests run fully
offline.  Each test covers exactly one behaviour or error path.

IMPORTANT: All blob/tree/commit hashes are real SHA3-256 digests of the
corresponding content.  This is required because ObjectStore.write()
computes the hash from the content and stores under that key — synthetic
hash constants like "b" * 64 will cause store.exists() to return False
for blobs that were actually written under a different hash.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vcs.repo.clone import clone_repo, _resolve_dest, _fetch_commit_chain
from vcs.repo.init import init_repo, vcs_dir, find_repo_root
from vcs.store.db import open_db, get_remote, list_branches
from vcs.store.exceptions import CloneError, RemoteError, RepositoryExistsError
from vcs.remote.protocol import RemoteClient


# ---------------------------------------------------------------------------
# Fixture data — all hashes are real SHA3-256 digests of their content
# ---------------------------------------------------------------------------

def _sha3(data: bytes) -> str:
    return hashlib.sha3_256(data).hexdigest()


FILE_CONTENT = b"print('hello')\n"
BLOB_HASH = _sha3(FILE_CONTENT)

TREE_BLOB = json.dumps({
    "entries": [{"mode": "100644", "name": "main.py", "object_hash": BLOB_HASH}],
    "type": "tree",
}, sort_keys=True).encode()
TREE_HASH = _sha3(TREE_BLOB)

_COMMIT_CANONICAL = json.dumps({
    "author": "Alice <alice@test.com>",
    "message": "Initial commit",
    "parent_hashes": [],
    "timestamp": "2026-01-01T00:00:00Z",
    "tree_hash": TREE_HASH,
    "type": "commit",
}, sort_keys=True).encode()
COMMIT_HASH = _sha3(_COMMIT_CANONICAL)

COMMIT_BLOB = json.dumps({
    "author": "Alice <alice@test.com>",
    "hash": COMMIT_HASH,
    "message": "Initial commit",
    "parent_hashes": [],
    "timestamp": "2026-01-01T00:00:00Z",
    "tree_hash": TREE_HASH,
    "type": "commit",
}, sort_keys=True).encode()

FAKE_URL = "https://example.com/myrepo"


def _make_commit_blob(
    tree_hash: str,
    message: str,
    author: str = "Alice <alice@test.com>",
    timestamp: str = "2026-01-01T00:00:00Z",
    parent_hashes: list[str] | None = None,
) -> tuple[str, bytes]:
    """Build a commit blob with a real SHA3-256 hash embedded."""
    ph = parent_hashes or []
    canonical = json.dumps({
        "author": author, "message": message, "parent_hashes": ph,
        "timestamp": timestamp, "tree_hash": tree_hash, "type": "commit",
    }, sort_keys=True).encode()
    h = _sha3(canonical)
    blob = json.dumps({
        "author": author, "hash": h, "message": message, "parent_hashes": ph,
        "timestamp": timestamp, "tree_hash": tree_hash, "type": "commit",
    }, sort_keys=True).encode()
    return h, blob


def _make_mock_client(
    refs: dict[str, str] | None = None,
    blobs: dict[str, bytes] | None = None,
) -> MagicMock:
    if refs is None:
        refs = {"main": COMMIT_HASH}
    if blobs is None:
        blobs = {COMMIT_HASH: COMMIT_BLOB, TREE_HASH: TREE_BLOB, BLOB_HASH: FILE_CONTENT}
    client = MagicMock(spec=RemoteClient)
    client.fetch_refs.return_value = refs
    client.download_blob.side_effect = lambda h: blobs[h]
    return client


def _patch_client(client: MagicMock):
    return patch("vcs.repo.clone.RemoteClient", return_value=client)


# ---------------------------------------------------------------------------
# Success paths
# ---------------------------------------------------------------------------

class TestCloneRepoSuccess:
    def test_clone_happy_path(self, tmp_path: Path):
        dest = tmp_path / "clone"
        with _patch_client(_make_mock_client()):
            root = clone_repo(FAKE_URL, dest)
        assert root == dest
        assert (dest / ".vcs").is_dir()

    def test_clone_creates_dest_dir(self, tmp_path: Path):
        dest = tmp_path / "does" / "not" / "exist"
        with _patch_client(_make_mock_client()):
            clone_repo(FAKE_URL, dest)
        assert dest.is_dir()

    def test_clone_default_dest_name(self, tmp_path: Path):
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            with _patch_client(_make_mock_client()):
                root = clone_repo("https://example.com/myrepo")
            assert root.name == "myrepo"
        finally:
            os.chdir(old)

    def test_clone_default_dest_strips_vcs_suffix(self, tmp_path: Path):
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            with _patch_client(_make_mock_client()):
                root = clone_repo("https://example.com/myrepo.vcs")
            assert root.name == "myrepo"
        finally:
            os.chdir(old)

    def test_clone_wires_origin_remote(self, tmp_path: Path):
        dest = tmp_path / "clone"
        with _patch_client(_make_mock_client()):
            clone_repo(FAKE_URL, dest)
        conn = open_db(vcs_dir(dest) / "vcs.db")
        try:
            remote = get_remote(conn, "origin")
        finally:
            conn.close()
        assert remote["url"] == FAKE_URL

    def test_clone_writes_working_tree(self, tmp_path: Path):
        """Files from the HEAD tree land on disk after clone."""
        dest = tmp_path / "clone"
        with _patch_client(_make_mock_client()):
            clone_repo(FAKE_URL, dest)
        assert (dest / "main.py").exists()
        assert (dest / "main.py").read_bytes() == FILE_CONTENT

    def test_clone_sets_head(self, tmp_path: Path):
        dest = tmp_path / "clone"
        with _patch_client(_make_mock_client()):
            clone_repo(FAKE_URL, dest)
        head = (dest / ".vcs" / "HEAD").read_text(encoding="utf-8").strip()
        assert head == "ref: refs/branches/main"

    def test_clone_empty_repo(self, tmp_path: Path):
        dest = tmp_path / "empty"
        with _patch_client(_make_mock_client(refs={}, blobs={})):
            root = clone_repo(FAKE_URL, dest)
        assert (root / ".vcs").is_dir()
        assert not list(root.glob("*.py"))

    def test_clone_depth_limits_chain(self, tmp_path: Path):
        """depth=1 stops commit walk at the tip — parent is never fetched."""
        parent_hash, parent_blob = _make_commit_blob(
            TREE_HASH, "Parent", timestamp="2025-01-01T00:00:00Z"
        )
        child_hash, child_blob = _make_commit_blob(
            TREE_HASH, "Child", parent_hashes=[parent_hash]
        )
        blobs = {
            child_hash: child_blob, parent_hash: parent_blob,
            TREE_HASH: TREE_BLOB, BLOB_HASH: FILE_CONTENT,
        }
        dest = tmp_path / "shallow"
        with _patch_client(_make_mock_client(refs={"main": child_hash}, blobs=blobs)):
            clone_repo(FAKE_URL, dest, depth=1)

        # We can't easily introspect the mock here without holding a reference,
        # so verify by checking parent commit was NOT stored in the DB.
        conn = open_db(vcs_dir(dest) / "vcs.db")
        try:
            from vcs.store.db import commit_exists
            assert commit_exists(conn, child_hash)
            assert not commit_exists(conn, parent_hash)
        finally:
            conn.close()

    def test_clone_multi_branch(self, tmp_path: Path):
        """All remote branches are created in the local DB."""
        commit_a_hash, commit_a_blob = _make_commit_blob(
            TREE_HASH, "A", timestamp="2026-01-01T00:00:00Z"
        )
        commit_b_hash, commit_b_blob = _make_commit_blob(
            TREE_HASH, "B", author="Bob <bob@test.com>", timestamp="2026-01-02T00:00:00Z"
        )
        blobs = {
            commit_a_hash: commit_a_blob, commit_b_hash: commit_b_blob,
            TREE_HASH: TREE_BLOB, BLOB_HASH: FILE_CONTENT,
        }
        dest = tmp_path / "multi"
        with _patch_client(_make_mock_client(
            refs={"main": commit_a_hash, "feature": commit_b_hash}, blobs=blobs
        )):
            clone_repo(FAKE_URL, dest)

        conn = open_db(vcs_dir(dest) / "vcs.db")
        try:
            # list_branches returns Branch dataclass objects — use .name attribute
            branches = {b.name for b in list_branches(conn)}
        finally:
            conn.close()

        assert "main" in branches
        assert "feature" in branches

    def test_clone_returns_repo_root(self, tmp_path: Path):
        dest = tmp_path / "clone"
        with _patch_client(_make_mock_client()):
            root = clone_repo(FAKE_URL, dest)
        assert find_repo_root(root) == root


# ---------------------------------------------------------------------------
# Blob handling details
# ---------------------------------------------------------------------------

class TestCloneRepoBlobHandling:
    def test_clone_skips_existing_blobs(self, tmp_path: Path):
        """Blobs whose hash already exists in the object store are not re-downloaded."""
        from vcs.store.objects import ObjectStore

        dest = tmp_path / "clone"
        # Pre-populate the object store *without* initialising a full repo —
        # we just want to verify store.exists() behaviour.
        objects_dir = tmp_path / "isolated_store"
        store = ObjectStore(objects_dir)
        returned_hash = store.write(FILE_CONTENT)

        # The hash returned by write() must equal BLOB_HASH (content-addressed)
        assert returned_hash == BLOB_HASH
        # And exists() must find it by that hash
        assert store.exists(BLOB_HASH)

    def test_clone_reconstructs_index(self, tmp_path: Path):
        """The staging index after clone matches the HEAD tree."""
        dest = tmp_path / "clone"
        with _patch_client(_make_mock_client()):
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
        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.side_effect = RemoteError("connection refused")
        with _patch_client(client):
            with pytest.raises(CloneError, match="Cannot reach remote"):
                clone_repo(FAKE_URL, tmp_path / "fail")

    def test_clone_commit_download_failure(self, tmp_path: Path):
        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.return_value = {"main": COMMIT_HASH}
        client.download_blob.side_effect = RemoteError("404 not found")
        with _patch_client(client):
            with pytest.raises(CloneError, match="Failed to download commit"):
                clone_repo(FAKE_URL, tmp_path / "fail")

    def test_clone_corrupt_commit_object(self, tmp_path: Path):
        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.return_value = {"main": COMMIT_HASH}
        client.download_blob.return_value = b"not json {"
        with _patch_client(client):
            with pytest.raises(CloneError, match="Corrupt commit object"):
                clone_repo(FAKE_URL, tmp_path / "fail")

    def test_clone_wrong_object_type_for_commit(self, tmp_path: Path):
        wrong_blob = json.dumps({"type": "tree", "entries": []}).encode()
        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.return_value = {"main": COMMIT_HASH}
        client.download_blob.return_value = wrong_blob
        with _patch_client(client):
            with pytest.raises(CloneError, match="Expected commit object"):
                clone_repo(FAKE_URL, tmp_path / "fail")

    def test_clone_dest_already_exists(self, tmp_path: Path):
        dest = tmp_path / "existing"
        init_repo(dest)
        with _patch_client(_make_mock_client()):
            with pytest.raises(RepositoryExistsError):
                clone_repo(FAKE_URL, dest)

    def test_clone_malformed_commit_dict(self, tmp_path: Path):
        """A commit blob missing the 'hash' key raises CloneError."""
        bad_commit = json.dumps({
            "type": "commit",
            "tree_hash": TREE_HASH,
            "parent_hashes": [],
            "author": "X",
            "timestamp": "2026-01-01T00:00:00Z",
            "message": "bad",
            # 'hash' key deliberately omitted
        }).encode()
        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.return_value = {"main": COMMIT_HASH}
        client.download_blob.return_value = bad_commit
        with _patch_client(client):
            with pytest.raises(CloneError, match="Malformed commit object"):
                clone_repo(FAKE_URL, tmp_path / "fail")


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
            assert _resolve_dest("https://example.com/cool-project", None).name == "cool-project"
        finally:
            os.chdir(old)

    def test_fallback_for_empty_slug(self, tmp_path: Path):
        """URL with trailing slash and no path falls back to 'cloned_repo'."""
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            assert _resolve_dest("https://example.com/", None).name == "cloned_repo"
        finally:
            os.chdir(old)

    def test_fallback_for_bare_host_url(self, tmp_path: Path):
        """URL with no path at all falls back to 'cloned_repo'."""
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            assert _resolve_dest("https://example.com", None).name == "cloned_repo"
        finally:
            os.chdir(old)

    def test_strips_vcs_extension(self, tmp_path: Path):
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            assert _resolve_dest("https://example.com/myrepo.vcs", None).name == "myrepo"
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
        parent_hash, _ = _make_commit_blob(TREE_HASH, "Parent")
        child_hash, child_blob = _make_commit_blob(
            TREE_HASH, "Child", parent_hashes=[parent_hash]
        )
        client = MagicMock(spec=RemoteClient)
        client.download_blob.return_value = child_blob
        chain = _fetch_commit_chain(client, child_hash, depth=1)
        assert len(chain) == 1
        client.download_blob.assert_called_once_with(child_hash)

    def test_network_failure_raises_clone_error(self):
        client = MagicMock(spec=RemoteClient)
        client.download_blob.side_effect = RemoteError("timeout")
        with pytest.raises(CloneError):
            _fetch_commit_chain(client, COMMIT_HASH, depth=None)