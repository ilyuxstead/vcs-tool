"""
tests/unit/test_remote_pull.py — unit tests for pull() merge integration.

Strategy
--------
* ``fetch()`` is patched via ``RemoteClient`` (same approach as the existing
  test_remote.py suite) so no real network calls are made.
* ``merge_branch()`` is patched at its CANONICAL location:
  ``"vcs.branch.ops.merge_branch"``.

  Why not ``"vcs.remote.ops.merge_branch"``?
  pull() uses a deferred ``import vcs.branch.ops as _branch_ops`` to break
  a circular import, then calls ``_branch_ops.merge_branch()``.  Because
  the call goes through the module object at call time, patching the function
  on the module itself — ``vcs.branch.ops.merge_branch`` — is intercepted
  correctly.  Patching ``vcs.remote.ops.merge_branch`` would fail with
  AttributeError because that name never exists on the remote.ops module.

* End-to-end tests exercise the real merge path with no mocks on merge_branch.

Coverage targets
----------------
* fetch_only=True → merged=False, merge_branch never called
* No remote refs after fetch → merged=False, merge_branch never called
* Successful merge → merged=True, merge_commit in result, fetch keys present
* author kwarg forwarded to merge_branch
* merge message contains remote name
* Old placeholder 'note' key absent from result
* Explicit branch_name used as merge source
* branch_name=None falls back to current branch
* Detached HEAD without branch_name → VCSError raised
* Explicit branch_name bypasses detached HEAD guard
* MergeConflictError propagated with pull_fetch_result attached
* MergeConflictError does not produce merged=True
* End-to-end: merged file visible in working tree after pull
* End-to-end: fetch_only leaves working tree unchanged
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vcs.branch.ops import create as create_branch
from vcs.branch.ops import switch as switch_branch
from vcs.commit.snapshot import create_snapshot
from vcs.commit.stage import stage_files
from vcs.remote.ops import add, pull
from vcs.remote.protocol import RemoteClient
from vcs.repo.init import init_repo, resolve_head_commit, write_head
from vcs.store.exceptions import MergeConflictError, VCSError


# ---------------------------------------------------------------------------
# Module-level constants and helpers
# ---------------------------------------------------------------------------

AUTHOR = "Test User <test@example.com>"

# Patch target — the function on the module where it is defined.
# pull() does: import vcs.branch.ops as _branch_ops; _branch_ops.merge_branch(...)
# Patching the attribute on the module object intercepts every call through it.
_MERGE_PATCH = "vcs.branch.ops.merge_branch"
_CLIENT_PATCH = "vcs.remote.ops.RemoteClient"


def _make_repo(tmp_path: Path, author: str = AUTHOR) -> Path:
    """Initialise a repo with one commit on 'main' and register 'origin'."""
    root = tmp_path / "repo"
    root.mkdir()
    init_repo(root)
    f = root / "readme.txt"
    f.write_text("hello")
    stage_files([f], root)
    create_snapshot("Initial commit", author, root)
    add("origin", "https://example.com", root)
    return root


def _mock_client(refs: dict[str, str] | None = None) -> MagicMock:
    """Return a RemoteClient mock whose fetch_refs returns *refs*."""
    m = MagicMock(spec=RemoteClient)
    m.fetch_refs.return_value = refs if refs is not None else {}
    return m


def _detach(root: Path) -> None:
    """Put the repo into detached HEAD state."""
    commit_hash = resolve_head_commit(root)
    assert commit_hash is not None, "need at least one commit before detaching"
    write_head(root, commit_hash)


# ---------------------------------------------------------------------------
# fetch_only=True — merge must never be invoked
# ---------------------------------------------------------------------------

class TestPullFetchOnly:
    def test_fetch_only_returns_merged_false(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch(_CLIENT_PATCH, return_value=_mock_client({})):
            result = pull("origin", repo_root=root, fetch_only=True)
        assert result["merged"] is False

    def test_fetch_only_does_not_call_merge_branch(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch(_CLIENT_PATCH, return_value=_mock_client({})):
            with patch(_MERGE_PATCH) as mock_merge:
                pull("origin", repo_root=root, fetch_only=True)
        mock_merge.assert_not_called()

    def test_fetch_only_result_contains_fetch_keys(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch(_CLIENT_PATCH, return_value=_mock_client({})):
            result = pull("origin", repo_root=root, fetch_only=True)
        for key in ("remote", "refs", "blobs_downloaded"):
            assert key in result, f"Missing fetch key {key!r}"


# ---------------------------------------------------------------------------
# Empty refs — nothing to merge
# ---------------------------------------------------------------------------

class TestPullEmptyRefs:
    def test_empty_refs_returns_merged_false(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch(_CLIENT_PATCH, return_value=_mock_client({})):
            result = pull("origin", repo_root=root, author=AUTHOR)
        assert result["merged"] is False

    def test_empty_refs_does_not_call_merge_branch(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch(_CLIENT_PATCH, return_value=_mock_client({})):
            with patch(_MERGE_PATCH) as mock_merge:
                pull("origin", repo_root=root, author=AUTHOR)
        mock_merge.assert_not_called()


# ---------------------------------------------------------------------------
# Successful merge
# ---------------------------------------------------------------------------

_FAKE_MERGE_HASH = "m" * 64


class TestPullMergeSuccess:
    """
    Helper: _do_pull() runs pull() with RemoteClient and merge_branch both
    mocked, stores the merge mock on self for assertion in follow-up tests.
    """

    def _do_pull(self, root: Path, **kwargs) -> dict:
        with patch(_CLIENT_PATCH, return_value=_mock_client({"main": "a" * 64})):
            with patch(_MERGE_PATCH, return_value=_FAKE_MERGE_HASH) as mock_merge:
                result = pull("origin", repo_root=root, author=AUTHOR, **kwargs)
                self._mock_merge = mock_merge  # available after context exits
        return result

    def test_merged_is_true(self, tmp_path: Path):
        result = self._do_pull(_make_repo(tmp_path))
        assert result["merged"] is True

    def test_merge_commit_hash_in_result(self, tmp_path: Path):
        result = self._do_pull(_make_repo(tmp_path))
        assert result["merge_commit"] == _FAKE_MERGE_HASH

    def test_fetch_keys_present_in_result(self, tmp_path: Path):
        result = self._do_pull(_make_repo(tmp_path))
        for key in ("remote", "refs", "blobs_downloaded", "commits_fetched", "blobs_fetched"):
            assert key in result, f"Missing key {key!r}"

    def test_author_forwarded_to_merge_branch(self, tmp_path: Path):
        self._do_pull(_make_repo(tmp_path))
        # merge_branch is called with author as a keyword argument
        assert self._mock_merge.call_args.kwargs.get("author") == AUTHOR

    def test_merge_message_references_remote(self, tmp_path: Path):
        self._do_pull(_make_repo(tmp_path))
        msg: str = self._mock_merge.call_args.kwargs.get("message", "")
        assert "origin" in msg

    def test_no_note_placeholder_in_result(self, tmp_path: Path):
        """Old placeholder 'note' key must not appear in a wired result."""
        result = self._do_pull(_make_repo(tmp_path))
        assert "note" not in result


# ---------------------------------------------------------------------------
# branch_name kwarg — source branch selection
# ---------------------------------------------------------------------------

class TestPullBranchNameKwarg:
    def test_explicit_branch_name_used_as_source(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch(_CLIENT_PATCH, return_value=_mock_client({"feature": "b" * 64})):
            with patch(_MERGE_PATCH, return_value="c" * 64) as mock_merge:
                pull("origin", branch_name="feature", repo_root=root, author=AUTHOR)
        # source_name is the first positional argument to merge_branch
        assert mock_merge.call_args.args[0] == "feature"

    def test_branch_name_fallback_to_current_branch(self, tmp_path: Path):
        """When branch_name is None, current branch ('main') is used."""
        root = _make_repo(tmp_path)
        with patch(_CLIENT_PATCH, return_value=_mock_client({"main": "d" * 64})):
            with patch(_MERGE_PATCH, return_value="e" * 64) as mock_merge:
                pull("origin", repo_root=root, author=AUTHOR)
        assert mock_merge.call_args.args[0] == "main"


# ---------------------------------------------------------------------------
# Detached HEAD
# ---------------------------------------------------------------------------

class TestPullDetachedHead:
    def test_detached_head_raises(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        _detach(root)
        with patch(_CLIENT_PATCH, return_value=_mock_client({"main": "f" * 64})):
            with pytest.raises(VCSError, match="detached HEAD"):
                pull("origin", repo_root=root, author=AUTHOR)

    def test_explicit_branch_name_bypasses_detached_check(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        _detach(root)
        with patch(_CLIENT_PATCH, return_value=_mock_client({"feature": "g" * 64})):
            with patch(_MERGE_PATCH, return_value="h" * 64):
                result = pull(
                    "origin", branch_name="feature", repo_root=root, author=AUTHOR
                )
        assert result["merged"] is True


# ---------------------------------------------------------------------------
# MergeConflictError propagation
# ---------------------------------------------------------------------------

class TestPullMergeConflict:
    @staticmethod
    def _conflict() -> MergeConflictError:
        return MergeConflictError(
            "Conflict in file.txt",
            conflicted_files=["file.txt"],
        )

    def test_merge_conflict_is_raised(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch(_CLIENT_PATCH, return_value=_mock_client({"main": "a" * 64})):
            with patch(_MERGE_PATCH, side_effect=self._conflict()):
                with pytest.raises(MergeConflictError):
                    pull("origin", repo_root=root, author=AUTHOR)

    def test_merge_conflict_has_pull_fetch_result(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch(_CLIENT_PATCH, return_value=_mock_client({"main": "a" * 64})):
            with patch(_MERGE_PATCH, side_effect=self._conflict()):
                with pytest.raises(MergeConflictError) as exc_info:
                    pull("origin", repo_root=root, author=AUTHOR)
        assert hasattr(exc_info.value, "pull_fetch_result")
        assert "remote" in exc_info.value.pull_fetch_result  # type: ignore[attr-defined]

    def test_merge_conflict_does_not_produce_merged_true(self, tmp_path: Path):
        """Exception must propagate — result must never have merged=True."""
        root = _make_repo(tmp_path)
        captured: dict = {}
        with patch(_CLIENT_PATCH, return_value=_mock_client({"main": "a" * 64})):
            with patch(_MERGE_PATCH, side_effect=self._conflict()):
                try:
                    captured = pull("origin", repo_root=root, author=AUTHOR)
                except MergeConflictError:
                    pass
        assert captured.get("merged") is not True


# ---------------------------------------------------------------------------
# End-to-end wiring (real merge_branch — no mock on it)
# ---------------------------------------------------------------------------

class TestPullEndToEnd:
    """
    Exercises the full pull() → fetch() → merge_branch() call chain.
    fetch() is patched at the ops level so no real network is needed;
    merge_branch() runs for real against a locally-prepared branch.
    """

    def test_pull_merges_new_file_from_remote(self, tmp_path: Path):
        root = _make_repo(tmp_path)

        # Simulate a remote branch by creating a real local branch with an
        # extra file, then switching back to main.
        create_branch("remote-main", root)
        switch_branch("remote-main", root)
        remote_file = root / "remote_only.txt"
        remote_file.write_text("from remote")
        stage_files([remote_file], root)
        remote_tip = create_snapshot("Remote commit", AUTHOR, root).hash
        switch_branch("main", root)

        # Patch fetch() to report that 'remote-main' was updated to remote_tip.
        fake_fetch = {
            "remote": "origin",
            "refs": {"remote-main": remote_tip},
            "blobs_downloaded": 0,
            "commits_fetched": 1,
            "blobs_fetched": 0,
        }
        with patch("vcs.remote.ops.fetch", return_value=fake_fetch):
            result = pull(
                "origin",
                branch_name="remote-main",
                repo_root=root,
                author=AUTHOR,
            )

        assert result["merged"] is True
        assert "merge_commit" in result
        assert (root / "remote_only.txt").exists()
        assert (root / "remote_only.txt").read_text() == "from remote"

    def test_pull_fetch_only_leaves_working_tree_unchanged(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        before = {f.name for f in root.iterdir() if f.is_file()}

        with patch(_CLIENT_PATCH, return_value=_mock_client({})):
            pull("origin", repo_root=root, fetch_only=True)

        after = {f.name for f in root.iterdir() if f.is_file()}
        assert after == before