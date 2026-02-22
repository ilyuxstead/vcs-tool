"""
tests/unit/test_remote_push_dag.py
===================================
Unit tests for the corrected remote.push() DAG walk.

Covers:
  • Single commit (root) — still works (regression guard).
  • Two-commit linear chain — ancestor is uploaded, not just tip.
  • Server already has the root — only the new commit is uploaded.
  • Detached HEAD raises RemoteError.
  • Missing remote raises RemoteError.
  • Blobs already known to server are not re-uploaded.
  • Return dict contains correct counts.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from vcs.remote.ops import push, add
from vcs.remote.protocol import RemoteClient
from vcs.repo.init import vcs_dir
from vcs.store.db import (
    insert_commit,
    insert_tree,
    open_db,
    update_branch_tip,
    create_branch,
)
from vcs.store.models import Commit, Tree, TreeEntry
from vcs.store.objects import ObjectStore
from vcs.store.exceptions import RemoteError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sha(data: bytes) -> str:
    return hashlib.sha3_256(data).hexdigest()


def _make_file_blob(content: bytes) -> tuple[str, bytes]:
    h = _sha(content)
    return h, content


def _make_tree(entries: list[dict]) -> tuple[str, bytes]:
    raw = json.dumps({"type": "tree", "entries": entries}).encode()
    h = _sha(raw)
    return h, raw


def _make_commit(tree_hash: str, parents: list[str], msg: str, ts: str | None = None) -> tuple[str, bytes]:
    ts = ts or "2026-01-01T00:00:00Z"
    payload = {
        "type": "commit",
        "tree_hash": tree_hash,
        "parent_hashes": parents,
        "author": "test <t@t.com>",
        "timestamp": ts,
        "message": msg,
    }
    raw = json.dumps(payload).encode()
    h = _sha(raw)
    return h, raw


def _make_repo_with_chain(tmp_path: Path, num_commits: int = 1) -> tuple[Path, list[str]]:
    """
    Create a minimal repo with *num_commits* in a linear chain.

    Returns (repo_root, [commit_hash_oldest, ..., commit_hash_tip]).
    """
    from vcs.repo.init import init_repo
    root = tmp_path / "repo"
    root.mkdir()
    init_repo(root)

    dot_vcs = vcs_dir(root)
    conn = open_db(dot_vcs / "vcs.db")
    store = ObjectStore(dot_vcs / "objects")

    file_hash, file_data = _make_file_blob(b"hello world")
    store.write(file_hash, file_data)

    tree_hash, tree_raw = _make_tree([
        {"mode": "100644", "name": "hello.txt", "object_hash": file_hash}
    ])
    store.write(tree_hash, tree_raw)
    insert_tree(conn, Tree(
        hash=tree_hash,
        entries=(TreeEntry(mode="100644", name="hello.txt", object_hash=file_hash),),
    ))

    commit_hashes: list[str] = []
    parents: list[str] = []
    for i in range(num_commits):
        ch, _ = _make_commit(tree_hash, parents, f"commit {i}",
                              ts=f"2026-01-{i+1:02d}T00:00:00Z")
        insert_commit(conn, Commit(
            hash=ch,
            tree_hash=tree_hash,
            parent_hashes=tuple(parents),
            author="test <t@t.com>",
            timestamp=f"2026-01-{i+1:02d}T00:00:00Z",
            message=f"commit {i}",
        ))
        commit_hashes.append(ch)
        parents = [ch]

    tip = commit_hashes[-1]
    if len(commit_hashes) == 1:
        # init_repo creates a "main" branch; just advance it
        update_branch_tip(conn, "main", tip)
    else:
        update_branch_tip(conn, "main", tip)

    conn.close()
    return root, commit_hashes


def _mock_client(needed: list[str] | None = None) -> MagicMock:
    m = MagicMock(spec=RemoteClient)
    m.negotiate_refs.return_value = needed if needed is not None else []
    m.upload_blob.return_value = None
    m.upload_commit.return_value = None
    m.update_ref.return_value = None
    return m


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPushDagWalk:

    def test_single_root_commit_uploads_commit_and_tree(self, tmp_path: Path):
        """A single-commit repo: commit blob + tree blob must be uploaded."""
        root, hashes = _make_repo_with_chain(tmp_path, num_commits=1)
        add("origin", "https://example.com", root)

        # Server needs everything (empty remote).
        tip = hashes[0]
        dot_vcs = vcs_dir(root)
        conn = open_db(dot_vcs / "vcs.db")
        store = ObjectStore(dot_vcs / "objects")
        all_hashes = store.all_hashes()
        conn.close()

        mock = _mock_client(needed=all_hashes)

        with patch("vcs.remote.ops.RemoteClient", return_value=mock):
            result = push("origin", repo_root=root)

        assert result["tip_hash"] == tip
        assert result["commits_uploaded"] >= 1
        assert result["trees_uploaded"] >= 1
        mock.update_ref.assert_called_once_with("main", tip)

    def test_two_commit_chain_uploads_both_commits(self, tmp_path: Path):
        """A two-commit chain to an empty remote: both commits must be uploaded."""
        root, hashes = _make_repo_with_chain(tmp_path, num_commits=2)
        add("origin", "https://example.com", root)

        dot_vcs = vcs_dir(root)
        store = ObjectStore(dot_vcs / "objects")
        all_hashes = store.all_hashes()

        # Server needs every object (empty).
        mock = _mock_client(needed=all_hashes)

        with patch("vcs.remote.ops.RemoteClient", return_value=mock):
            result = push("origin", repo_root=root)

        # Both commits must have been uploaded (one via upload_commit, one via upload_blob).
        assert result["commits_uploaded"] == 2, (
            f"Expected 2 commits uploaded, got {result['commits_uploaded']}"
        )
        mock.update_ref.assert_called_once_with("main", hashes[-1])

    def test_incremental_push_skips_known_ancestor(self, tmp_path: Path):
        """
        Server already has the first commit.
        Only the second (new) commit and any new objects should be uploaded.
        """
        root, hashes = _make_repo_with_chain(tmp_path, num_commits=2)
        add("origin", "https://example.com", root)

        old_commit_hash = hashes[0]
        new_commit_hash = hashes[1]

        dot_vcs = vcs_dir(root)
        store = ObjectStore(dot_vcs / "objects")

        # Server knows the old commit; needs only new commit-related blobs.
        # negotiate_refs returns hashes of objects server doesn't have.
        # The old commit hash is NOT in needed → treated as server_known.
        # New commit hash IS in needed → must be uploaded.
        mock = _mock_client(needed=[new_commit_hash])

        with patch("vcs.remote.ops.RemoteClient", return_value=mock):
            result = push("origin", repo_root=root)

        # Only the new tip commit should have been uploaded as a commit.
        assert result["commits_uploaded"] == 1, (
            f"Expected 1 commit uploaded, got {result['commits_uploaded']}"
        )
        # upload_blob should NOT have been called with the old commit hash.
        upload_blob_hashes = {c.args[0] for c in mock.upload_blob.call_args_list}
        assert old_commit_hash not in upload_blob_hashes, (
            "Old commit hash was re-uploaded despite server already having it."
        )

    def test_blobs_already_known_not_reuploaded(self, tmp_path: Path):
        """Server already has all file blobs — blobs_uploaded must be 0."""
        root, hashes = _make_repo_with_chain(tmp_path, num_commits=1)
        add("origin", "https://example.com", root)

        tip = hashes[0]
        # Server only needs the commit itself, not any file blobs.
        mock = _mock_client(needed=[tip])

        with patch("vcs.remote.ops.RemoteClient", return_value=mock):
            result = push("origin", repo_root=root)

        assert result["blobs_uploaded"] == 0, (
            f"Expected 0 file blobs uploaded, got {result['blobs_uploaded']}"
        )

    def test_detached_head_raises(self, tmp_path: Path):
        """Pushing in detached HEAD state must raise RemoteError."""
        root, _ = _make_repo_with_chain(tmp_path, num_commits=1)
        add("origin", "https://example.com", root)

        with patch("vcs.remote.ops.current_branch", return_value=None):
            with pytest.raises(RemoteError, match="detached HEAD"):
                push("origin", repo_root=root)

    def test_missing_remote_raises(self, tmp_path: Path):
        """Pushing to a remote that is not registered must raise RemoteError."""
        root, _ = _make_repo_with_chain(tmp_path, num_commits=1)
        with pytest.raises(RemoteError):
            push("nonexistent", repo_root=root)

    def test_return_dict_has_expected_keys(self, tmp_path: Path):
        """Return dict must contain branch, remote, tip_hash, and upload counts."""
        root, hashes = _make_repo_with_chain(tmp_path, num_commits=1)
        add("origin", "https://example.com", root)
        mock = _mock_client(needed=[])

        with patch("vcs.remote.ops.RemoteClient", return_value=mock):
            result = push("origin", repo_root=root)

        for key in ("branch", "remote", "tip_hash",
                    "commits_uploaded", "trees_uploaded", "blobs_uploaded"):
            assert key in result, f"Missing key {key!r} in push() result"

    def test_update_ref_called_last(self, tmp_path: Path):
        """update_ref must be the last remote call — after all uploads."""
        root, hashes = _make_repo_with_chain(tmp_path, num_commits=1)
        add("origin", "https://example.com", root)

        call_order: list[str] = []

        mock = MagicMock(spec=RemoteClient)
        mock.negotiate_refs.return_value = []
        mock.upload_blob.side_effect = lambda *a, **kw: call_order.append("upload_blob")
        mock.upload_commit.side_effect = lambda *a, **kw: call_order.append("upload_commit")
        mock.update_ref.side_effect = lambda *a, **kw: call_order.append("update_ref")

        with patch("vcs.remote.ops.RemoteClient", return_value=mock):
            push("origin", repo_root=root)

        if call_order:
            assert call_order[-1] == "update_ref", (
                f"update_ref was not the last call; order was: {call_order}"
            )
        else:
            # No uploads needed — update_ref should still have been called.
            mock.update_ref.assert_called_once()