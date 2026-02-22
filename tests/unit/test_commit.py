"""
tests/unit/test_commit.py — commit creation unit tests.

Covers: stage, snapshot, show, immutability guards.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from vcs.commit.snapshot import create_snapshot, reject_amend
from vcs.commit.stage import stage_files, stage_all, unstage_files
from vcs.repo.init import init_repo
from vcs.store.exceptions import ImmutabilityViolationError, StagingError


class TestStage:
    def test_stage_single_file(self, tmp_repo: Path):
        f = tmp_repo / "hello.txt"
        f.write_text("hello")
        staged = stage_files([f], tmp_repo)
        assert staged == ["hello.txt"]

    def test_stage_missing_file_raises(self, tmp_repo: Path):
        with pytest.raises(StagingError):
            stage_files([tmp_repo / "ghost.txt"], tmp_repo)

    def test_stage_directory_raises(self, tmp_repo: Path):
        d = tmp_repo / "subdir"
        d.mkdir()
        with pytest.raises(StagingError):
            stage_files([d], tmp_repo)

    def test_stage_multiple(self, tmp_repo: Path):
        files = []
        for i in range(3):
            f = tmp_repo / f"file{i}.txt"
            f.write_text(f"content {i}")
            files.append(f)
        staged = stage_files(files, tmp_repo)
        assert len(staged) == 3

    def test_stage_all(self, tmp_repo: Path):
        for i in range(4):
            (tmp_repo / f"f{i}.py").write_text(f"# {i}")
        staged = stage_all(tmp_repo)
        assert len(staged) == 4

    def test_unstage_file(self, tmp_repo: Path):
        f = tmp_repo / "a.txt"
        f.write_text("content")
        stage_files([f], tmp_repo)
        removed = unstage_files([f], tmp_repo)
        assert removed == ["a.txt"]

    def test_unstage_missing_from_index_raises(self, tmp_repo: Path):
        f = tmp_repo / "not_staged.txt"
        f.write_text("x")
        with pytest.raises(StagingError):
            unstage_files([f], tmp_repo)


class TestSnapshot:
    def test_create_root_commit(self, tmp_repo: Path, author: str):
        f = tmp_repo / "main.py"
        f.write_text("print('hello')")
        stage_files([f], tmp_repo)
        commit = create_snapshot("Initial commit", author, tmp_repo)
        assert len(commit.hash) == 64
        assert commit.parent_hashes == ()
        assert commit.author == author
        assert commit.message == "Initial commit"

    def test_second_commit_has_parent(self, tmp_repo: Path, author: str):
        f = tmp_repo / "a.txt"
        f.write_text("v1")
        stage_files([f], tmp_repo)
        c1 = create_snapshot("First", author, tmp_repo)

        f.write_text("v2")
        stage_files([f], tmp_repo)
        c2 = create_snapshot("Second", author, tmp_repo)

        assert c2.parent_hashes == (c1.hash,)

    def test_empty_staging_raises(self, tmp_repo: Path, author: str):
        with pytest.raises(StagingError):
            create_snapshot("Empty", author, tmp_repo)

    def test_empty_message_raises(self, tmp_repo: Path, author: str):
        f = tmp_repo / "x.txt"
        f.write_text("x")
        stage_files([f], tmp_repo)
        with pytest.raises(StagingError):
            create_snapshot("", author, tmp_repo)

    def test_staging_cleared_after_commit(self, tmp_repo: Path, author: str):
        from vcs.repo.status import read_index
        f = tmp_repo / "a.txt"
        f.write_text("content")
        stage_files([f], tmp_repo)
        create_snapshot("Commit", author, tmp_repo)
        index = read_index(tmp_repo)
        assert index == {}

    def test_commit_hash_deterministic_with_fixed_timestamp(self, tmp_repo: Path, author: str):
        f = tmp_repo / "det.txt"
        f.write_text("same")
        stage_files([f], tmp_repo)
        c1 = create_snapshot("msg", author, tmp_repo, timestamp="2026-01-01T00:00:00Z")

        # Re-stage same content in a fresh repo and verify identical hash
        with tempfile.TemporaryDirectory() as td:
            root2 = Path(td).resolve() / "r2"  # resolve() handles macOS /private symlink
            root2.mkdir()
            init_repo(root2)
            f2 = root2 / "det.txt"
            f2.write_text("same")
            stage_files([f2], root2)
            c2 = create_snapshot("msg", author, root2, timestamp="2026-01-01T00:00:00Z")
            assert c1.hash == c2.hash

    def test_reject_amend(self):
        with pytest.raises(ImmutabilityViolationError):
            reject_amend()
