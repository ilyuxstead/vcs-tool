"""
tests/integration/test_repo_init.py — full repo initialisation workflow.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from vcs.repo.init import (
    find_repo_root,
    init_repo,
    read_head,
    vcs_dir,
    current_branch,
)
from vcs.store.exceptions import RepositoryExistsError, RepositoryNotFoundError


class TestInitRepo:
    def test_creates_vcs_dir(self, tmp_path: Path):
        root = tmp_path / "proj"
        root.mkdir()
        dot_vcs = init_repo(root)
        assert dot_vcs.is_dir()
        assert (dot_vcs / "objects").is_dir()
        assert (dot_vcs / "refs" / "branches").is_dir()
        assert (dot_vcs / "config.toml").is_file()
        assert (dot_vcs / "vcs.db").is_file()
        assert (dot_vcs / "HEAD").is_file()

    def test_head_points_to_main(self, tmp_path: Path):
        root = tmp_path / "proj"
        root.mkdir()
        init_repo(root)
        head = read_head(root)
        assert head == "ref: refs/branches/main"

    def test_double_init_raises(self, tmp_path: Path):
        root = tmp_path / "proj"
        root.mkdir()
        init_repo(root)
        with pytest.raises(RepositoryExistsError):
            init_repo(root)

    def test_find_repo_root_from_subdir(self, tmp_path: Path):
        root = tmp_path / "proj"
        root.mkdir()
        init_repo(root)
        subdir = root / "src" / "lib"
        subdir.mkdir(parents=True)
        found = find_repo_root(subdir)
        assert found == root

    def test_find_repo_root_not_found_raises(self, tmp_path: Path):
        empty = tmp_path / "empty"
        empty.mkdir()
        with pytest.raises(RepositoryNotFoundError):
            find_repo_root(empty)

    def test_current_branch_initially_main(self, tmp_path: Path):
        root = tmp_path / "proj"
        root.mkdir()
        init_repo(root)
        assert current_branch(root) == "main"
