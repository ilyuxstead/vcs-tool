"""
tests/unit/test_gaps.py — targeted tests for remaining coverage gaps.

Covers: store/db list_commits, commit/show edge cases, repo/status
advanced scenarios, repo/init edge cases.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from vcs.commit.show import get_commit_detail
from vcs.commit.snapshot import create_snapshot
from vcs.commit.stage import stage_files
from vcs.repo.init import (
    find_repo_root,
    init_repo,
    read_head,
    resolve_head_commit,
    vcs_dir,
    write_head,
)
from vcs.repo.status import (
    WorkingTreeStatus,
    compute_status,
    read_index,
    write_index,
    _hash_file,
    _is_ignored,
    _load_ignore_patterns,
)
from vcs.store.db import (
    get_commit,
    insert_commit,
    list_commits,
    open_db,
)
from vcs.store.exceptions import CommitNotFoundError, ObjectNotFoundError
from vcs.store.models import Commit


# ---------------------------------------------------------------------------
# store/db — list_commits
# ---------------------------------------------------------------------------

class TestListCommits:
    def _make_commit(self, hash_: str, parent: str | None = None, author: str = "Alice", ts: str = "2026-01-01T00:00:00Z") -> Commit:
        return Commit(
            hash=hash_,
            tree_hash="b" * 64,
            parent_hashes=(parent,) if parent else (),
            author=author,
            timestamp=ts,
            message=f"commit {hash_[:4]}",
        )

    def test_list_all_commits(self, db_conn):
        for i, h in enumerate(["a" * 64, "b" * 64, "c" * 64]):
            insert_commit(db_conn, self._make_commit(h))
        commits = list_commits(db_conn)
        assert len(commits) == 3

    def test_list_with_limit(self, db_conn):
        for h in ["a" * 64, "b" * 64, "c" * 64]:
            insert_commit(db_conn, self._make_commit(h))
        commits = list_commits(db_conn, limit=2)
        assert len(commits) == 2

    def test_list_from_branch_tip(self, db_conn):
        c1 = self._make_commit("1" * 64)
        c2 = self._make_commit("2" * 64, parent="1" * 64, ts="2026-01-02T00:00:00Z")
        c3 = self._make_commit("3" * 64, parent="2" * 64, ts="2026-01-03T00:00:00Z")
        for c in [c1, c2, c3]:
            insert_commit(db_conn, c)
        commits = list_commits(db_conn, branch_tip="3" * 64)
        hashes = {c.hash for c in commits}
        assert "1" * 64 in hashes
        assert "2" * 64 in hashes
        assert "3" * 64 in hashes

    def test_list_author_filter(self, db_conn):
        insert_commit(db_conn, self._make_commit("a" * 64, author="Alice"))
        insert_commit(db_conn, self._make_commit("b" * 64, author="Bob"))
        commits = list_commits(db_conn, author="Alice")
        assert all("Alice" in c.author for c in commits)

    def test_list_empty_db(self, db_conn):
        commits = list_commits(db_conn)
        assert commits == []


# ---------------------------------------------------------------------------
# commit/show — edge cases
# ---------------------------------------------------------------------------

class TestCommitShow:
    def test_show_root_commit_no_parents(self, tmp_repo: Path, author: str):
        f = tmp_repo / "a.txt"
        f.write_text("content")
        stage_files([f], tmp_repo)
        c = create_snapshot("Root commit", author, tmp_repo)
        detail = get_commit_detail(c.hash, tmp_repo)
        assert detail["parent_hashes"] == []
        assert any(f["path"] == "a.txt" for f in detail["files"])
        assert detail["files"][0]["status"] == "added"

    def test_show_second_commit_shows_modified(self, tmp_repo: Path, author: str):
        f = tmp_repo / "a.txt"
        f.write_text("v1")
        stage_files([f], tmp_repo)
        create_snapshot("v1", author, tmp_repo)
        f.write_text("v2")
        stage_files([f], tmp_repo)
        c2 = create_snapshot("v2", author, tmp_repo)
        detail = get_commit_detail(c2.hash, tmp_repo)
        a_entry = next(e for e in detail["files"] if e["path"] == "a.txt")
        assert a_entry["status"] == "modified"

    def test_show_missing_commit_raises(self, tmp_repo: Path):
        with pytest.raises(CommitNotFoundError):
            get_commit_detail("0" * 64, tmp_repo)


# ---------------------------------------------------------------------------
# repo/status — advanced scenarios
# ---------------------------------------------------------------------------

class TestRepoStatus:
    def test_hash_file(self, tmp_path: Path):
        import hashlib
        f = tmp_path / "test.txt"
        f.write_text("content")
        h = _hash_file(f)
        expected = hashlib.sha3_256(b"content").hexdigest()
        assert h == expected

    def test_ignore_always_ignores_vcs(self):
        assert _is_ignored(".vcs/objects/abc", []) is True
        assert _is_ignored(".vcs/config.toml", []) is True

    def test_ignore_pattern_match(self):
        assert _is_ignored("build/output.o", ["*.o"]) is True
        assert _is_ignored("build/output.py", ["*.o"]) is False

    def test_ignore_filename_match(self):
        assert _is_ignored("deep/nested/file.pyc", ["*.pyc"]) is True

    def test_load_ignore_patterns_empty(self, tmp_path: Path):
        root = tmp_path / "repo"
        root.mkdir()
        patterns = _load_ignore_patterns(root)
        assert patterns == []

    def test_load_ignore_patterns_from_file(self, tmp_path: Path):
        root = tmp_path / "repo"
        root.mkdir()
        (root / ".vcsignore").write_text("*.pyc\n# comment\nbuild/\n")
        patterns = _load_ignore_patterns(root)
        assert "*.pyc" in patterns
        assert "build/" in patterns
        assert "# comment" not in patterns

    def test_status_deleted_tracked_file(self, tmp_repo: Path, author: str):
        f = tmp_repo / "todelete.txt"
        f.write_text("will be deleted")
        stage_files([f], tmp_repo)
        create_snapshot("add file", author, tmp_repo)
        f.unlink()
        status = compute_status(tmp_repo)
        assert "todelete.txt" in status.deleted

    def test_status_staged_deleted(self, tmp_repo: Path, author: str):
        f = tmp_repo / "staged_del.txt"
        f.write_text("content")
        stage_files([f], tmp_repo)
        create_snapshot("add", author, tmp_repo)
        # Stage it, then delete the file
        stage_files([f], tmp_repo)
        f.unlink()
        status = compute_status(tmp_repo)
        assert "staged_del.txt" in status.staged_deleted

    def test_write_and_read_index(self, tmp_repo: Path):
        index = {"a.txt": "a" * 64, "b.txt": "b" * 64}
        write_index(tmp_repo, index)
        loaded = read_index(tmp_repo)
        assert loaded == index

    def test_read_index_empty_if_no_file(self, tmp_repo: Path):
        index = read_index(tmp_repo)
        assert index == {}

    def test_status_is_clean_property(self):
        s = WorkingTreeStatus()
        assert s.is_clean is True
        s.untracked.append("x.txt")
        assert s.is_clean is False


# ---------------------------------------------------------------------------
# repo/init — edge cases
# ---------------------------------------------------------------------------

class TestRepoInitEdgeCases:
    def test_write_and_read_head(self, tmp_path: Path):
        root = tmp_path / "repo"
        root.mkdir()
        init_repo(root)
        write_head(root, "ref: refs/branches/feature")
        assert read_head(root) == "ref: refs/branches/feature"

    def test_resolve_head_detached(self, tmp_path: Path, author: str = "Dev <dev@test.com>"):
        root = tmp_path / "repo"
        root.mkdir()
        init_repo(root)
        f = root / "a.txt"
        f.write_text("a")
        stage_files([f], root)
        c = create_snapshot("commit", author, root)
        # Force detached HEAD
        write_head(root, c.hash)
        resolved = resolve_head_commit(root)
        assert resolved == c.hash

    def test_find_repo_at_exact_root(self, tmp_path: Path):
        root = tmp_path / "repo"
        root.mkdir()
        init_repo(root)
        found = find_repo_root(root)
        assert found == root


# ---------------------------------------------------------------------------
# store/objects — atomic write failure cleanup
# ---------------------------------------------------------------------------

class TestObjectStoreAtomicWrite:
    def test_partial_write_cleaned_up(self, tmp_path: Path):
        """If atomic write fails, no .tmp file is left behind."""
        from vcs.store.objects import ObjectStore
        store = ObjectStore(tmp_path / "objects")

        import os
        original_replace = os.replace

        def failing_replace(src, dst):
            raise OSError("disk full")

        with pytest.raises(OSError):
            with pytest.MonkeyPatch().context() as mp:
                mp.setattr(os, "replace", failing_replace)
                store.write(b"some data")

        tmp_files = list((tmp_path / "objects").rglob("*.tmp*"))
        assert tmp_files == []
