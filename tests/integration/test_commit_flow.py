"""
tests/integration/test_commit_flow.py — stage + snapshot end-to-end tests.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from vcs.commit.snapshot import create_snapshot
from vcs.commit.stage import stage_files, stage_all
from vcs.commit.show import get_commit_detail
from vcs.history.log import log
from vcs.repo.init import resolve_head_commit
from vcs.repo.status import compute_status


def _write_and_stage(repo_root: Path, name: str, content: str) -> Path:
    f = repo_root / name
    f.write_text(content)
    stage_files([f], repo_root)
    return f


class TestCommitFlow:
    def test_full_commit_cycle(self, tmp_repo: Path, author: str):
        """Stage → snapshot → verify HEAD advanced."""
        _write_and_stage(tmp_repo, "README.md", "# VCS Project")
        commit = create_snapshot("Initial commit", author, tmp_repo)

        head = resolve_head_commit(tmp_repo)
        assert head == commit.hash

    def test_history_shows_commits(self, tmp_repo: Path, author: str):
        _write_and_stage(tmp_repo, "a.txt", "first")
        c1 = create_snapshot("First", author, tmp_repo)

        _write_and_stage(tmp_repo, "b.txt", "second")
        c2 = create_snapshot("Second", author, tmp_repo)

        commits = log(tmp_repo)
        hashes = [c.hash for c in commits]
        assert c2.hash in hashes
        assert c1.hash in hashes
        # Newest first
        assert hashes.index(c2.hash) < hashes.index(c1.hash)

    def test_status_clean_after_commit(self, tmp_repo: Path, author: str):
        _write_and_stage(tmp_repo, "clean.txt", "content")
        create_snapshot("commit", author, tmp_repo)
        status = compute_status(tmp_repo)
        # File should now be tracked; no staged, no modified
        assert status.staged_new == []
        assert status.staged_modified == []

    def test_status_shows_modified(self, tmp_repo: Path, author: str):
        f = _write_and_stage(tmp_repo, "modify.txt", "original")
        create_snapshot("original", author, tmp_repo)
        f.write_text("changed")
        status = compute_status(tmp_repo)
        assert "modify.txt" in status.modified

    def test_commit_show_detail(self, tmp_repo: Path, author: str):
        _write_and_stage(tmp_repo, "show_me.txt", "content")
        commit = create_snapshot("Show this", author, tmp_repo)
        detail = get_commit_detail(commit.hash, tmp_repo)
        assert detail["hash"] == commit.hash
        assert detail["message"] == "Show this"
        assert any(f["path"] == "show_me.txt" for f in detail["files"])

    def test_three_commit_chain(self, tmp_repo: Path, author: str):
        commits = []
        for i in range(3):
            _write_and_stage(tmp_repo, f"file{i}.txt", f"content {i}")
            c = create_snapshot(f"Commit {i}", author, tmp_repo)
            commits.append(c)

        # Verify parent chain
        assert commits[1].parent_hashes == (commits[0].hash,)
        assert commits[2].parent_hashes == (commits[1].hash,)

    def test_large_file_warning(self, tmp_repo: Path, author: str, capsys):
        from vcs.store.objects import LARGE_BLOB_THRESHOLD_BYTES
        large = tmp_repo / "large.bin"
        large.write_bytes(b"x" * (LARGE_BLOB_THRESHOLD_BYTES + 1))
        stage_files([large], tmp_repo)
        create_snapshot("Add large file", author, tmp_repo)
        captured = capsys.readouterr()
        assert "Warning" in captured.err
