"""
tests/unit/test_branch.py — branch create/switch/merge/delete tests.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from vcs.branch.ops import create, delete, list_all, switch, merge_branch
from vcs.commit.snapshot import create_snapshot
from vcs.commit.stage import stage_files
from vcs.store.exceptions import (
    BranchExistsError,
    BranchNotFoundError,
    MergeConflictError,
    VCSError,
)


def _make_commit(repo_root: Path, filename: str, content: str, author: str, msg: str):
    f = repo_root / filename
    f.write_text(content)
    stage_files([f], repo_root)
    return create_snapshot(msg, author, repo_root)


class TestBranchCreate:
    def test_create_branch(self, tmp_repo: Path, author: str):
        _make_commit(tmp_repo, "a.txt", "a", author, "init")
        b = create("feature", tmp_repo)
        assert b.name == "feature"

    def test_create_branch_at_hash(self, tmp_repo: Path, author: str):
        c = _make_commit(tmp_repo, "a.txt", "a", author, "init")
        b = create("v1", tmp_repo, at_hash=c.hash)
        assert b.tip_hash == c.hash

    def test_create_duplicate_raises(self, tmp_repo: Path, author: str):
        _make_commit(tmp_repo, "a.txt", "a", author, "init")
        create("feature", tmp_repo)
        with pytest.raises(BranchExistsError):
            create("feature", tmp_repo)

    def test_create_in_empty_repo_raises(self, tmp_repo: Path):
        with pytest.raises(VCSError):
            create("feature", tmp_repo)


class TestBranchList:
    def test_list_includes_default(self, tmp_repo: Path, author: str):
        _make_commit(tmp_repo, "a.txt", "a", author, "init")
        branches = list_all(tmp_repo)
        names = [b.name for b in branches]
        assert "main" in names

    def test_list_sorted(self, tmp_repo: Path, author: str):
        _make_commit(tmp_repo, "a.txt", "a", author, "init")
        for name in ["zzz", "aaa", "mmm"]:
            create(name, tmp_repo)
        branches = list_all(tmp_repo)
        names = [b.name for b in branches]
        assert names == sorted(names)


class TestBranchSwitch:
    def test_switch_restores_files(self, tmp_repo: Path, author: str):
        # Commit on main
        _make_commit(tmp_repo, "main_file.txt", "main content", author, "main commit")
        create("feature", tmp_repo)

        # Switch to feature, add a file
        switch("feature", tmp_repo)
        _make_commit(tmp_repo, "feature_file.txt", "feature content", author, "feature commit")

        # Switch back to main
        switch("main", tmp_repo)
        assert (tmp_repo / "main_file.txt").exists()

    def test_switch_missing_branch_raises(self, tmp_repo: Path, author: str):
        _make_commit(tmp_repo, "a.txt", "a", author, "init")
        with pytest.raises(BranchNotFoundError):
            switch("nonexistent", tmp_repo)


class TestBranchDelete:
    def test_delete_branch(self, tmp_repo: Path, author: str):
        _make_commit(tmp_repo, "a.txt", "a", author, "init")
        create("feature", tmp_repo)
        delete("feature", tmp_repo)
        names = [b.name for b in list_all(tmp_repo)]
        assert "feature" not in names

    def test_delete_active_branch_raises(self, tmp_repo: Path, author: str):
        _make_commit(tmp_repo, "a.txt", "a", author, "init")
        with pytest.raises(VCSError, match="currently active"):
            delete("main", tmp_repo)

    def test_delete_missing_raises(self, tmp_repo: Path, author: str):
        _make_commit(tmp_repo, "a.txt", "a", author, "init")
        with pytest.raises(BranchNotFoundError):
            delete("ghost", tmp_repo)


class TestBranchMerge:
    def test_merge_no_conflict(self, tmp_repo: Path, author: str):
        # main: file_a
        _make_commit(tmp_repo, "file_a.txt", "content A", author, "add A")
        create("feature", tmp_repo)
        switch("feature", tmp_repo)

        # feature: add file_b (no overlap with main)
        _make_commit(tmp_repo, "file_b.txt", "content B", author, "add B")
        switch("main", tmp_repo)

        merge_hash = merge_branch("feature", author, repo_root=tmp_repo)
        assert len(merge_hash) == 64
        # Both files should now exist on main
        assert (tmp_repo / "file_a.txt").exists()
        assert (tmp_repo / "file_b.txt").exists()

    def test_merge_produces_two_parents(self, tmp_repo: Path, author: str):
        from vcs.repo.init import resolve_head_commit
        from vcs.store.db import get_commit, open_db
        from vcs.repo.init import vcs_dir

        _make_commit(tmp_repo, "a.txt", "a", author, "init")
        create("feature", tmp_repo)
        switch("feature", tmp_repo)
        _make_commit(tmp_repo, "b.txt", "b", author, "feature commit")
        switch("main", tmp_repo)

        merge_hash = merge_branch("feature", author, repo_root=tmp_repo)
        conn = open_db(vcs_dir(tmp_repo) / "vcs.db")
        try:
            merge_commit = get_commit(conn, merge_hash)
            assert len(merge_commit.parent_hashes) == 2
        finally:
            conn.close()

    def test_merge_conflict_raises(self, tmp_repo: Path, author: str):
        # Both branches modify the same file differently
        _make_commit(tmp_repo, "shared.txt", "original", author, "init")
        create("feature", tmp_repo)

        # Modify on main
        (tmp_repo / "shared.txt").write_text("main change")
        stage_files([tmp_repo / "shared.txt"], tmp_repo)
        create_snapshot("main change", author, tmp_repo)

        switch("feature", tmp_repo)
        (tmp_repo / "shared.txt").write_text("feature change")
        stage_files([tmp_repo / "shared.txt"], tmp_repo)
        create_snapshot("feature change", author, tmp_repo)

        switch("main", tmp_repo)
        with pytest.raises(MergeConflictError):
            merge_branch("feature", author, repo_root=tmp_repo)
