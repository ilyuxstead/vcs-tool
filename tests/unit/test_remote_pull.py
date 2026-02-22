"""
tests/unit/test_remote_pull.py — unit tests for pull() merge integration.

Strategy
--------
* ``fetch()`` is patched via ``RemoteClient`` (same approach as the existing
  test_remote.py suite) so no real network calls are made.
* ``merge_branch()`` is patched to isolate pull() logic from the branch/merge
  subsystem; the branch-layer already has its own test suite.
* A small number of tests exercise the real merge path end-to-end using a
  fully-initialised tmp repo so we verify the wiring, not just the mock.

Coverage targets
----------------
* fetch_only=True → merged=False, merge_branch never called
* No remote refs after fetch → merged=False (nothing to merge)
* Successful merge → merged=True, merge_commit present in result
* Detached HEAD without branch_name → VCSError raised
* MergeConflictError propagated with pull_fetch_result attached
* branch_name kwarg selects correct source branch
* author kwarg forwarded to merge_branch
* Return dict always contains fetch keys
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from vcs.branch.ops import create as create_branch
from vcs.branch.ops import switch as switch_branch
from vcs.commit.snapshot import create_snapshot
from vcs.commit.stage import stage_files
from vcs.remote.ops import add, pull
from vcs.remote.protocol import RemoteClient
from vcs.repo.init import init_repo, vcs_dir, write_head, resolve_head_commit
from vcs.store.db import add_remote, open_db
from vcs.store.exceptions import MergeConflictError, VCSError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

AUTHOR = "Test User <test@example.com>"
_FAKE_FETCH_RESULT = {
    "remote": "origin",
    "refs": {"main": "a" * 64},
    "blobs_downloaded": 0,
    "commits_fetched": 0,
    "blobs_fetched": 0,
}
_FAKE_FETCH_RESULT_EMPTY_REFS = {**_FAKE_FETCH_RESULT, "refs": {}}


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


# ---------------------------------------------------------------------------
# fetch_only=True — merge must never be invoked
# ---------------------------------------------------------------------------

class TestPullFetchOnly:
    def test_fetch_only_returns_merged_false(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch("vcs.remote.ops.RemoteClient", return_value=_mock_client({})):
            result = pull("origin", repo_root=root, fetch_only=True)
        assert result["merged"] is False

    def test_fetch_only_does_not_call_merge_branch(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch("vcs.remote.ops.RemoteClient", return_value=_mock_client({})):
            with patch("vcs.remote.ops.merge_branch") as mock_merge:
                pull("origin", repo_root=root, fetch_only=True)
        mock_merge.assert_not_called()

    def test_fetch_only_result_contains_fetch_keys(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch("vcs.remote.ops.RemoteClient", return_value=_mock_client({})):
            result = pull("origin", repo_root=root, fetch_only=True)
        for key in ("remote", "refs", "blobs_downloaded"):
            assert key in result, f"Missing fetch key {key!r} in fetch_only result"


# ---------------------------------------------------------------------------
# Empty refs — nothing to merge, merged=False
# ---------------------------------------------------------------------------

class TestPullEmptyRefs:
    def test_empty_refs_returns_merged_false(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch("vcs.remote.ops.RemoteClient", return_value=_mock_client({})):
            result = pull("origin", repo_root=root, author=AUTHOR)
        assert result["merged"] is False

    def test_empty_refs_does_not_call_merge_branch(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch("vcs.remote.ops.RemoteClient", return_value=_mock_client({})):
            with patch("vcs.remote.ops.merge_branch") as mock_merge:
                pull("origin", repo_root=root, author=AUTHOR)
        mock_merge.assert_not_called()


# ---------------------------------------------------------------------------
# Successful merge — merged=True, merge_commit present
# ---------------------------------------------------------------------------

class TestPullMergeSuccess:
    _MERGE_HASH = "m" * 64

    def _do_pull(self, root: Path, **kwargs) -> dict:
        with patch("vcs.remote.ops.RemoteClient",
                   return_value=_mock_client({"main": "a" * 64})):
            with patch("vcs.remote.ops.merge_branch",
                       return_value=self._MERGE_HASH) as mock_merge:
                result = pull("origin", repo_root=root, author=AUTHOR, **kwargs)
                self._mock_merge = mock_merge
        return result

    def test_merged_is_true(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        result = self._do_pull(root)
        assert result["merged"] is True

    def test_merge_commit_hash_in_result(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        result = self._do_pull(root)
        assert result["merge_commit"] == self._MERGE_HASH

    def test_fetch_keys_present_in_result(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        result = self._do_pull(root)
        for key in ("remote", "refs", "blobs_downloaded", "commits_fetched", "blobs_fetched"):
            assert key in result, f"Missing key {key!r}"

    def test_author_forwarded_to_merge_branch(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        self._do_pull(root)
        _, kwargs = self._mock_merge.call_args
        assert kwargs.get("author") == AUTHOR or self._mock_merge.call_args[0][1] == AUTHOR

    def test_merge_message_references_remote(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        self._do_pull(root)
        call_kwargs = self._mock_merge.call_args
        # message is passed as keyword arg
        msg: str = call_kwargs.kwargs.get("message", "")
        assert "origin" in msg

    def test_no_note_placeholder_in_result(self, tmp_path: Path):
        """The old placeholder 'note' key must not appear in a wired pull."""
        root = _make_repo(tmp_path)
        result = self._do_pull(root)
        assert "note" not in result


# ---------------------------------------------------------------------------
# branch_name kwarg — explicit source branch selection
# ---------------------------------------------------------------------------

class TestPullBranchNameKwarg:
    def test_explicit_branch_name_used_as_source(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch("vcs.remote.ops.RemoteClient",
                   return_value=_mock_client({"feature": "b" * 64})):
            with patch("vcs.remote.ops.merge_branch",
                       return_value="c" * 64) as mock_merge:
                pull("origin", branch_name="feature", repo_root=root, author=AUTHOR)
        # source_name positional arg must be the explicit branch
        assert mock_merge.call_args[0][0] == "feature"

    def test_branch_name_fallback_to_current_branch(self, tmp_path: Path):
        """When branch_name is None, current branch ('main') is used."""
        root = _make_repo(tmp_path)
        with patch("vcs.remote.ops.RemoteClient",
                   return_value=_mock_client({"main": "d" * 64})):
            with patch("vcs.remote.ops.merge_branch",
                       return_value="e" * 64) as mock_merge:
                pull("origin", repo_root=root, author=AUTHOR)
        assert mock_merge.call_args[0][0] == "main"


# ---------------------------------------------------------------------------
# Detached HEAD — must raise VCSError when no branch_name provided
# ---------------------------------------------------------------------------

class TestPullDetachedHead:
    @staticmethod
    def _detach(root: Path) -> None:
        """Write a bare commit hash to HEAD, putting the repo in detached state."""
        commit_hash = resolve_head_commit(root)
        assert commit_hash is not None, "repo must have at least one commit before detaching"
        write_head(root, commit_hash)

    def test_detached_head_raises(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        self._detach(root)
        with patch("vcs.remote.ops.RemoteClient",
                   return_value=_mock_client({"main": "f" * 64})):
            with pytest.raises(VCSError, match="detached HEAD"):
                pull("origin", repo_root=root, author=AUTHOR)

    def test_explicit_branch_name_bypasses_detached_check(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        self._detach(root)  # detach
        with patch("vcs.remote.ops.RemoteClient",
                   return_value=_mock_client({"feature": "g" * 64})):
            with patch("vcs.remote.ops.merge_branch", return_value="h" * 64):
                # Should not raise — branch_name overrides detached check
                result = pull("origin", branch_name="feature",
                              repo_root=root, author=AUTHOR)
        assert result["merged"] is True


# ---------------------------------------------------------------------------
# MergeConflictError — propagated with pull_fetch_result attached
# ---------------------------------------------------------------------------

class TestPullMergeConflict:
    def _conflict_error(self) -> MergeConflictError:
        return MergeConflictError(
            "Conflict in file.txt",
            conflicted_files=["file.txt"],
        )

    def test_merge_conflict_is_raised(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch("vcs.remote.ops.RemoteClient",
                   return_value=_mock_client({"main": "a" * 64})):
            with patch("vcs.remote.ops.merge_branch",
                       side_effect=self._conflict_error()):
                with pytest.raises(MergeConflictError):
                    pull("origin", repo_root=root, author=AUTHOR)

    def test_merge_conflict_has_pull_fetch_result(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        with patch("vcs.remote.ops.RemoteClient",
                   return_value=_mock_client({"main": "a" * 64})):
            with patch("vcs.remote.ops.merge_branch",
                       side_effect=self._conflict_error()):
                with pytest.raises(MergeConflictError) as exc_info:
                    pull("origin", repo_root=root, author=AUTHOR)
        assert hasattr(exc_info.value, "pull_fetch_result")
        fetch_result = exc_info.value.pull_fetch_result  # type: ignore[attr-defined]
        assert "remote" in fetch_result

    def test_merge_conflict_merged_key_not_true(self, tmp_path: Path):
        """The exception must propagate — caller must not see merged=True."""
        root = _make_repo(tmp_path)
        with patch("vcs.remote.ops.RemoteClient",
                   return_value=_mock_client({"main": "a" * 64})):
            with patch("vcs.remote.ops.merge_branch",
                       side_effect=self._conflict_error()):
                result_captured = {}
                try:
                    result_captured = pull("origin", repo_root=root, author=AUTHOR)
                except MergeConflictError:
                    pass
        assert result_captured.get("merged") is not True


# ---------------------------------------------------------------------------
# End-to-end wiring test (real merge, no mocks on merge_branch)
#
# Two branches diverge locally; pull() merges the remote-tracking branch
# (which fetch already advanced) into the current branch.  This exercises
# the full call chain without a real remote.
# ---------------------------------------------------------------------------

class TestPullEndToEnd:
    """
    Simulate a pull by:
      1. Creating a diverged 'remote' branch locally.
      2. Calling pull() with fetch() mocked to return that branch tip.
      3. Verifying the working tree reflects the merge result.
    """

    def test_pull_merges_new_file_from_remote(self, tmp_path: Path):
        root = _make_repo(tmp_path)

        # Simulate remote branch: create a local branch with an extra file.
        create_branch("remote-main", root)
        switch_branch("remote-main", root)
        remote_file = root / "remote_only.txt"
        remote_file.write_text("from remote")
        stage_files([remote_file], root)
        remote_tip = create_snapshot("Remote commit", AUTHOR, root).hash

        # Switch back to main so pull merges 'remote-main' into 'main'.
        switch_branch("main", root)

        # Patch fetch to pretend 'main' was updated to the remote tip.
        fake_fetch_result = {
            "remote": "origin",
            "refs": {"remote-main": remote_tip},
            "blobs_downloaded": 0,
            "commits_fetched": 1,
            "blobs_fetched": 0,
        }
        with patch("vcs.remote.ops.fetch", return_value=fake_fetch_result):
            result = pull(
                "origin",
                branch_name="remote-main",
                repo_root=root,
                author=AUTHOR,
            )

        assert result["merged"] is True
        assert "merge_commit" in result
        # Merged file must be visible in the working tree
        assert (root / "remote_only.txt").exists()
        assert (root / "remote_only.txt").read_text() == "from remote"

    def test_pull_fetch_only_leaves_working_tree_unchanged(self, tmp_path: Path):
        root = _make_repo(tmp_path)
        original_files = set(f.name for f in root.iterdir() if f.is_file())

        with patch("vcs.remote.ops.RemoteClient", return_value=_mock_client({})):
            pull("origin", repo_root=root, fetch_only=True)

        current_files = set(f.name for f in root.iterdir() if f.is_file())
        assert current_files == original_files