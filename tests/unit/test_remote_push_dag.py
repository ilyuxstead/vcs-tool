"""
tests/unit/test_remote_push_dag.py
===================================
Unit tests for the corrected remote.push() DAG walk.

Covers:
  * Single commit (root) -- still works (regression guard).
  * Two-commit linear chain -- ancestor is uploaded, not just tip.
  * Server already has the root -- only the new commit is uploaded.
  * Detached HEAD raises RemoteError.
  * Missing remote raises RemoteError.
  * Blobs already known to server are not re-uploaded.
  * Return dict contains correct counts.
  * update_ref is the final remote call.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vcs.commit.snapshot import create_snapshot
from vcs.commit.stage import stage_files
from vcs.remote.ops import add, push
from vcs.remote.protocol import RemoteClient
from vcs.repo.init import init_repo, vcs_dir
from vcs.store.exceptions import RemoteError
from vcs.store.objects import ObjectStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_repo_with_chain(tmp_path: Path, num_commits: int = 1) -> tuple[Path, list[str]]:
    """
    Create a repo with *num_commits* real commits via the public VCS API.

    Uses init_repo -> stage_files -> create_snapshot so that the branch
    pointer, tree rows, and object-store blobs are all produced through
    the same code paths as production.  Mirrors the pattern used in
    tests/unit/test_remote.py::_make_repo_with_commit.

    Returns (repo_root, [oldest_commit_hash, ..., tip_commit_hash]).
    """
    root = tmp_path / "repo"
    root.mkdir()
    init_repo(root)

    commit_hashes: list[str] = []
    for i in range(num_commits):
        f = root / f"file{i}.txt"
        f.write_text(f"content for commit {i}")
        stage_files([f], root)
        snapshot = create_snapshot(f"commit {i}", "test <t@t.com>", root)
        commit_hashes.append(snapshot.hash)

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
    """
    All tests in this class patch VCS_AUTH_TOKEN so that RemoteClient
    construction does not raise AuthenticationError before our mocked
    network calls are reached.  The token value is arbitrary -- the
    RemoteClient is always replaced by a MagicMock before any real HTTP
    request would be attempted.
    """

    @pytest.fixture(autouse=True)
    def _set_auth_token(self):
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "test-token"}):
            yield

    def test_single_root_commit_uploads_commit_and_tree(self, tmp_path: Path):
        """A single-commit repo: at least one commit and one tree must be uploaded."""
        root, hashes = _make_repo_with_chain(tmp_path, num_commits=1)
        add("origin", "https://example.com", root)
        tip = hashes[0]

        store = ObjectStore(vcs_dir(root) / "objects")
        mock = _mock_client(needed=store.all_hashes())

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

        store = ObjectStore(vcs_dir(root) / "objects")
        mock = _mock_client(needed=store.all_hashes())

        with patch("vcs.remote.ops.RemoteClient", return_value=mock):
            result = push("origin", repo_root=root)

        assert result["commits_uploaded"] == 2, (
            f"Expected 2 commits uploaded, got {result['commits_uploaded']}"
        )
        mock.update_ref.assert_called_once_with("main", hashes[-1])

    def test_incremental_push_skips_known_ancestor(self, tmp_path: Path):
        """
        Server already has the first commit.
        Only the tip (second) commit should be uploaded.
        """
        root, hashes = _make_repo_with_chain(tmp_path, num_commits=2)
        add("origin", "https://example.com", root)

        old_commit_hash = hashes[0]
        new_commit_hash = hashes[1]

        # Server only needs the new tip; ancestor is implicitly known.
        mock = _mock_client(needed=[new_commit_hash])

        with patch("vcs.remote.ops.RemoteClient", return_value=mock):
            result = push("origin", repo_root=root)

        assert result["commits_uploaded"] == 1, (
            f"Expected 1 commit uploaded, got {result['commits_uploaded']}"
        )
        upload_blob_hashes = {c.args[0] for c in mock.upload_blob.call_args_list}
        assert old_commit_hash not in upload_blob_hashes, (
            "Old (already-known) commit was re-uploaded to the server."
        )

    def test_blobs_already_known_not_reuploaded(self, tmp_path: Path):
        """Server already has all file blobs -- blobs_uploaded must be 0."""
        root, hashes = _make_repo_with_chain(tmp_path, num_commits=1)
        add("origin", "https://example.com", root)
        tip = hashes[0]

        # Server only needs the commit object itself, not the file blobs.
        mock = _mock_client(needed=[tip])

        with patch("vcs.remote.ops.RemoteClient", return_value=mock):
            result = push("origin", repo_root=root)

        assert result["blobs_uploaded"] == 0, (
            f"Expected 0 file blobs re-uploaded, got {result['blobs_uploaded']}"
        )

    def test_detached_head_raises(self, tmp_path: Path):
        """Pushing in detached HEAD state must raise RemoteError."""
        root, _ = _make_repo_with_chain(tmp_path, num_commits=1)
        add("origin", "https://example.com", root)

        with patch("vcs.remote.ops.current_branch", return_value=None):
            with pytest.raises(RemoteError, match="detached HEAD"):
                push("origin", repo_root=root)

    def test_missing_remote_raises(self, tmp_path: Path):
        """Pushing to an unregistered remote must raise RemoteError."""
        root, _ = _make_repo_with_chain(tmp_path, num_commits=1)
        with pytest.raises(RemoteError):
            push("nonexistent", repo_root=root)

    def test_return_dict_has_expected_keys(self, tmp_path: Path):
        """Return dict must contain branch, remote, tip_hash, and all upload counts."""
        root, _ = _make_repo_with_chain(tmp_path, num_commits=1)
        add("origin", "https://example.com", root)
        mock = _mock_client(needed=[])

        with patch("vcs.remote.ops.RemoteClient", return_value=mock):
            result = push("origin", repo_root=root)

        for key in ("branch", "remote", "tip_hash",
                    "commits_uploaded", "trees_uploaded", "blobs_uploaded"):
            assert key in result, f"Missing key {key!r} in push() return value"

    def test_update_ref_called_last(self, tmp_path: Path):
        """update_ref must be called after all upload_blob / upload_commit calls."""
        root, _ = _make_repo_with_chain(tmp_path, num_commits=1)
        add("origin", "https://example.com", root)

        call_order: list[str] = []

        mock = MagicMock(spec=RemoteClient)
        mock.negotiate_refs.return_value = []
        mock.upload_blob.side_effect = lambda *a, **kw: call_order.append("upload_blob")
        mock.upload_commit.side_effect = lambda *a, **kw: call_order.append("upload_commit")
        mock.update_ref.side_effect = lambda *a, **kw: call_order.append("update_ref")

        with patch("vcs.remote.ops.RemoteClient", return_value=mock):
            push("origin", repo_root=root)

        assert "update_ref" in call_order, "update_ref was never called"
        assert call_order[-1] == "update_ref", (
            f"update_ref was not the last call; order was: {call_order}"
        )