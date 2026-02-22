"""
tests/unit/test_remote_fetch_metadata.py
─────────────────────────────────────────
Regression tests for the bug:

    "remote.fetch — blobs download correctly but commit metadata is never
     written back to SQLite.  After a fetch, history.log shows nothing from
     the remote.  The objects are orphaned bytes on disk."

Every test in this module asserts something about the SQLite state AFTER
fetch(), not just blob counts.  The existing test suite only checked that
download_blob was (or wasn't) called — it never verified that the commit
rows or tree rows actually landed in the database.

Tests are deliberately independent of the network: RemoteClient is always
patched.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from vcs.repo.init import init_repo, vcs_dir
from vcs.commit.stage import stage_files
from vcs.commit.snapshot import create_snapshot
from vcs.store.db import (
    add_remote,
    branch_exists,
    commit_exists,
    get_branch,
    get_commit,
    get_tree,
    open_db,
)
from vcs.store.exceptions import RemoteError
from vcs.store.objects import ObjectStore
from vcs.remote.ops import fetch, _parse_commit_blob, _parse_tree_blob, _walk_and_ingest
from vcs.remote.protocol import RemoteClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

AUTHOR = "Dev <dev@test.com>"


def _sha3(data: bytes) -> str:
    return hashlib.sha3_256(data).hexdigest()


def _make_blob(content: bytes) -> tuple[str, bytes]:
    """Return (hash, raw) for an arbitrary file blob."""
    return _sha3(content), content


def _make_tree_blob(entries: list[dict]) -> tuple[str, bytes]:
    """Return (hash, raw) for a tree blob."""
    raw = json.dumps(
        {"type": "tree", "entries": entries},
        sort_keys=True,
    ).encode()
    return _sha3(raw), raw


def _make_commit_blob(
    tree_hash: str,
    parent_hashes: list[str],
    author: str = AUTHOR,
    message: str = "test commit",
    timestamp: str = "2026-01-01T00:00:00Z",
) -> tuple[str, bytes]:
    """
    Return (hash, raw) for a commit blob.

    Raw bytes are canonical JSON WITHOUT an embedded hash field, matching
    Commit.canonical_bytes().  _parse_commit_blob uses the lookup key
    (hex_hash) as the authoritative hash, so sha3(raw) is the value that
    ends up in SQLite — the same key used in blob_map and fetch_refs.
    """
    raw = json.dumps(
        {
            "type": "commit",
            "tree_hash": tree_hash,
            "parent_hashes": parent_hashes,
            "author": author,
            "timestamp": timestamp,
            "message": message,
        },
        sort_keys=True,
    ).encode()
    return _sha3(raw), raw


def _make_repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    init_repo(root)
    f = root / "main.py"
    f.write_text("print('hello')")
    stage_files([f], root)
    create_snapshot("Initial commit", AUTHOR, root)
    return root


def _add_origin(root: Path) -> None:
    conn = open_db(vcs_dir(root) / "vcs.db")
    try:
        add_remote(conn, "origin", "https://example.com")
    finally:
        conn.close()


def _build_mock_client(blob_map: dict[str, bytes]) -> MagicMock:
    """
    Build a mock RemoteClient whose download_blob returns from blob_map.
    fetch_refs is NOT set here — callers should set it per-test.
    """
    mock = MagicMock(spec=RemoteClient)
    mock.download_blob.side_effect = lambda h: blob_map[h]
    return mock


# ---------------------------------------------------------------------------
# Unit tests: _parse_commit_blob
# ---------------------------------------------------------------------------

class TestParseCommitBlob:
    def test_happy_path(self):
        file_hash = "a" * 64
        tree_hash, tree_raw = _make_tree_blob([
            {"mode": "100644", "name": "f.py", "object_hash": file_hash}
        ])
        commit_hash, commit_raw = _make_commit_blob(tree_hash, [])
        commit = _parse_commit_blob(commit_raw, commit_hash)
        assert commit.tree_hash == tree_hash
        assert commit.parent_hashes == ()
        assert commit.author == AUTHOR

    def test_wrong_type_raises(self):
        raw = json.dumps({"type": "tree", "entries": []}).encode()
        with pytest.raises(RemoteError, match="Expected commit"):
            _parse_commit_blob(raw, "a" * 64)

    def test_corrupt_json_raises(self):
        with pytest.raises(RemoteError, match="Corrupt commit"):
            _parse_commit_blob(b"not json", "b" * 64)

    def test_missing_field_raises(self):
        raw = json.dumps({"type": "commit", "author": "x"}).encode()
        with pytest.raises(RemoteError, match="missing field"):
            _parse_commit_blob(raw, "c" * 64)


# ---------------------------------------------------------------------------
# Unit tests: _parse_tree_blob
# ---------------------------------------------------------------------------

class TestParseTreeBlob:
    def test_happy_path(self):
        file_hash = "d" * 64
        tree_hash, tree_raw = _make_tree_blob([
            {"mode": "100644", "name": "README.md", "object_hash": file_hash}
        ])
        tree = _parse_tree_blob(tree_raw, tree_hash)
        assert len(tree.entries) == 1
        assert tree.entries[0].name == "README.md"

    def test_wrong_type_raises(self):
        raw = json.dumps({"type": "commit", "hash": "x" * 64}).encode()
        with pytest.raises(RemoteError, match="Expected tree"):
            _parse_tree_blob(raw, "e" * 64)

    def test_empty_entries_ok(self):
        tree_hash, tree_raw = _make_tree_blob([])
        tree = _parse_tree_blob(tree_raw, tree_hash)
        assert tree.entries == ()


# ---------------------------------------------------------------------------
# Integration tests: fetch() writes commit metadata to SQLite
# ---------------------------------------------------------------------------

class TestFetchWritesCommitMetadata:
    """
    Core regression suite for the bug.  Every test opens the local SQLite db
    after fetch() and asserts rows exist.
    """

    def _db(self, root: Path):
        return open_db(vcs_dir(root) / "vcs.db")

    def test_single_commit_written_to_sqlite(self, tmp_path):
        """Fetching one remote commit must produce a row in the commits table."""
        root = _make_repo(tmp_path)
        _add_origin(root)

        file_hash, file_raw = _make_blob(b"hello remote")
        tree_hash, tree_raw = _make_tree_blob([
            {"mode": "100644", "name": "hello.py", "object_hash": file_hash}
        ])
        commit_hash, commit_raw = _make_commit_blob(tree_hash, [])

        blob_map = {
            commit_hash: commit_raw,
            tree_hash: tree_raw,
            file_hash: file_raw,
        }
        mock_client = _build_mock_client(blob_map)
        mock_client.fetch_refs.return_value = {"main": commit_hash}

        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            result = fetch("origin", repo_root=root)

        assert result["commits_fetched"] == 1

        conn = self._db(root)
        try:
            assert commit_exists(conn, commit_hash), (
                "Commit was not written to SQLite — history.log would show nothing"
            )
        finally:
            conn.close()

    def test_commit_chain_all_written_to_sqlite(self, tmp_path):
        """A two-commit chain must produce two rows — BFS must walk parents."""
        root = _make_repo(tmp_path)
        _add_origin(root)

        # Build: root_commit ← child_commit
        file_hash, file_raw = _make_blob(b"v1")
        tree_hash, tree_raw = _make_tree_blob([
            {"mode": "100644", "name": "v.py", "object_hash": file_hash}
        ])
        root_commit_hash, root_commit_raw = _make_commit_blob(
            tree_hash, [], message="root"
        )
        child_commit_hash, child_commit_raw = _make_commit_blob(
            tree_hash, [root_commit_hash], message="child"
        )

        blob_map = {
            child_commit_hash: child_commit_raw,
            root_commit_hash: root_commit_raw,
            tree_hash: tree_raw,
            file_hash: file_raw,
        }
        mock_client = _build_mock_client(blob_map)
        mock_client.fetch_refs.return_value = {"main": child_commit_hash}

        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            result = fetch("origin", repo_root=root)

        assert result["commits_fetched"] == 2

        conn = self._db(root)
        try:
            assert commit_exists(conn, root_commit_hash), "Root commit missing from SQLite"
            assert commit_exists(conn, child_commit_hash), "Child commit missing from SQLite"
        finally:
            conn.close()

    def test_tree_rows_written_to_sqlite(self, tmp_path):
        """Tree metadata must also land in the trees table, not just commits."""
        root = _make_repo(tmp_path)
        _add_origin(root)

        file_hash, file_raw = _make_blob(b"tree test")
        tree_hash, tree_raw = _make_tree_blob([
            {"mode": "100644", "name": "tree_test.py", "object_hash": file_hash}
        ])
        commit_hash, commit_raw = _make_commit_blob(tree_hash, [])

        blob_map = {
            commit_hash: commit_raw,
            tree_hash: tree_raw,
            file_hash: file_raw,
        }
        mock_client = _build_mock_client(blob_map)
        mock_client.fetch_refs.return_value = {"main": commit_hash}

        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            fetch("origin", repo_root=root)

        conn = self._db(root)
        try:
            tree = get_tree(conn, tree_hash)
            assert len(tree.entries) == 1
            assert tree.entries[0].name == "tree_test.py"
        finally:
            conn.close()

    def test_branch_pointer_updated_after_fetch(self, tmp_path):
        """
        After fetch the branch tip must point at the remote tip so that
        history.log can walk the branch.
        """
        root = _make_repo(tmp_path)
        _add_origin(root)

        file_hash, file_raw = _make_blob(b"branch ptr")
        tree_hash, tree_raw = _make_tree_blob([
            {"mode": "100644", "name": "b.py", "object_hash": file_hash}
        ])
        commit_hash, commit_raw = _make_commit_blob(tree_hash, [])

        blob_map = {commit_hash: commit_raw, tree_hash: tree_raw, file_hash: file_raw}
        mock_client = _build_mock_client(blob_map)
        mock_client.fetch_refs.return_value = {"feature": commit_hash}

        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            fetch("origin", repo_root=root)

        conn = self._db(root)
        try:
            assert branch_exists(conn, "feature")
            branch = get_branch(conn, "feature")
            assert branch.tip_hash == commit_hash
        finally:
            conn.close()

    def test_already_known_commits_not_re_downloaded(self, tmp_path):
        """
        A commit already in SQLite must not trigger another download_blob call.
        This guards against exponential re-fetches on large histories.
        """
        root = _make_repo(tmp_path)
        _add_origin(root)

        # The local repo already has one commit.  Pretend the remote tip IS
        # that local commit (e.g. no-op fetch).
        conn = open_db(vcs_dir(root) / "vcs.db")
        try:
            from vcs.repo.init import resolve_head_commit
            local_tip = resolve_head_commit(root)
        finally:
            conn.close()

        mock_client = MagicMock(spec=RemoteClient)
        mock_client.fetch_refs.return_value = {"main": local_tip}

        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            result = fetch("origin", repo_root=root)

        # No new commits should have been fetched
        assert result["commits_fetched"] == 0
        mock_client.download_blob.assert_not_called()

    def test_fetch_multiple_branches(self, tmp_path):
        """Each branch's commits must be persisted independently."""
        root = _make_repo(tmp_path)
        _add_origin(root)

        file_hash, file_raw = _make_blob(b"multi branch")
        tree_hash, tree_raw = _make_tree_blob([
            {"mode": "100644", "name": "m.py", "object_hash": file_hash}
        ])
        main_commit_hash, main_commit_raw = _make_commit_blob(
            tree_hash, [], message="main tip"
        )
        dev_commit_hash, dev_commit_raw = _make_commit_blob(
            tree_hash, [], message="dev tip", timestamp="2026-01-02T00:00:00Z"
        )

        blob_map = {
            main_commit_hash: main_commit_raw,
            dev_commit_hash: dev_commit_raw,
            tree_hash: tree_raw,
            file_hash: file_raw,
        }
        mock_client = _build_mock_client(blob_map)
        mock_client.fetch_refs.return_value = {
            "main": main_commit_hash,
            "dev": dev_commit_hash,
        }

        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            result = fetch("origin", repo_root=root)

        assert result["commits_fetched"] == 2

        conn = self._db(root)
        try:
            assert commit_exists(conn, main_commit_hash)
            assert commit_exists(conn, dev_commit_hash)
            assert branch_exists(conn, "main")
            assert branch_exists(conn, "dev")
        finally:
            conn.close()

    def test_corrupt_commit_blob_raises_remote_error(self, tmp_path):
        """A malformed blob from the server must raise RemoteError, not crash."""
        root = _make_repo(tmp_path)
        _add_origin(root)

        bad_hash = "f" * 64
        mock_client = MagicMock(spec=RemoteClient)
        mock_client.fetch_refs.return_value = {"main": bad_hash}
        mock_client.download_blob.return_value = b"not json at all"

        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            with pytest.raises(RemoteError, match="Corrupt commit"):
                fetch("origin", repo_root=root)

    def test_missing_remote_raises(self, tmp_path):
        """Fetching from an unregistered remote must raise RemoteError."""
        root = _make_repo(tmp_path)
        with pytest.raises(RemoteError):
            fetch("nonexistent", repo_root=root)

    def test_return_dict_has_expected_keys(self, tmp_path):
        """Result dict must contain both legacy and new keys."""
        root = _make_repo(tmp_path)
        _add_origin(root)

        mock_client = MagicMock(spec=RemoteClient)
        mock_client.fetch_refs.return_value = {}

        with patch("vcs.remote.ops.RemoteClient", return_value=mock_client):
            result = fetch("origin", repo_root=root)

        for key in ("remote", "refs", "blobs_downloaded", "commits_fetched", "blobs_fetched"):
            assert key in result, f"Missing key {key!r} in fetch() result"


# ---------------------------------------------------------------------------
# _walk_and_ingest unit tests (lower-level, use a real store + db)
# ---------------------------------------------------------------------------

class TestWalkAndIngest:
    def test_shared_tree_downloaded_once(self, tmp_path):
        """
        When two commits share the same tree the tree blob must only be
        downloaded once, not twice.
        """
        root = tmp_path / "repo"
        root.mkdir()
        init_repo(root)
        dot_vcs = vcs_dir(root)
        conn = open_db(dot_vcs / "vcs.db")
        store = ObjectStore(dot_vcs / "objects")

        file_hash, file_raw = _make_blob(b"shared")
        tree_hash, tree_raw = _make_tree_blob([
            {"mode": "100644", "name": "s.py", "object_hash": file_hash}
        ])
        c1_hash, c1_raw = _make_commit_blob(tree_hash, [], message="c1")
        c2_hash, c2_raw = _make_commit_blob(
            tree_hash, [c1_hash], message="c2", timestamp="2026-01-02T00:00:00Z"
        )

        blob_map = {
            c2_hash: c2_raw,
            c1_hash: c1_raw,
            tree_hash: tree_raw,
            file_hash: file_raw,
        }
        mock_client = MagicMock(spec=RemoteClient)
        mock_client.download_blob.side_effect = lambda h: blob_map[h]

        try:
            _walk_and_ingest(mock_client, store, conn, c2_hash)
        finally:
            conn.close()

        # tree_hash should be downloaded exactly once
        tree_calls = [c for c in mock_client.download_blob.call_args_list
                      if c.args[0] == tree_hash]
        assert len(tree_calls) == 1, (
            f"Tree blob downloaded {len(tree_calls)} times, expected 1"
        )