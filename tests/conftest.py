"""
tests/conftest.py — shared fixtures and Hypothesis strategies.

All tests that need a real repository should use the ``tmp_repo``
fixture which initialises a fresh VCS repository in a temporary
directory and changes the working directory to it.
"""

from __future__ import annotations

import os
import string
from pathlib import Path

import pytest
from hypothesis import strategies as st

# ---------------------------------------------------------------------------
# Hypothesis strategies (Section 8.3 of the SRS)
# ---------------------------------------------------------------------------

arbitrary_blob = st.binary(min_size=0, max_size=65536)

valid_branch_name = st.text(
    alphabet=string.ascii_letters + "-_",
    min_size=1,
    max_size=64,
)

commit_message = st.text(min_size=1, max_size=1000)

file_path_components = st.text(
    alphabet=string.ascii_letters + string.digits + "-_.",
    min_size=1,
    max_size=20,
)
file_path = st.lists(file_path_components, min_size=1, max_size=5)


# ---------------------------------------------------------------------------
# Repository fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_repo(tmp_path: Path) -> Path:
    """
    Create a fresh VCS repository in *tmp_path* and return the repo root.

    The working directory is changed to the repo root for the duration
    of the test and restored afterward.
    """
    from vcs.repo.init import init_repo

    repo_root = tmp_path / "test_repo"
    repo_root.mkdir()
    init_repo(repo_root)
    old_cwd = os.getcwd()
    os.chdir(repo_root)
    yield repo_root
    os.chdir(old_cwd)


@pytest.fixture
def object_store(tmp_path: Path):
    """A fresh ObjectStore backed by a temp directory."""
    from vcs.store.objects import ObjectStore
    return ObjectStore(tmp_path / "objects")


@pytest.fixture
def db_conn(tmp_path: Path):
    """An open SQLite connection with the VCS schema applied."""
    from vcs.store.db import open_db
    conn = open_db(tmp_path / "vcs.db")
    yield conn
    conn.close()


@pytest.fixture
def author() -> str:
    return "Test User <test@vcs.local>"
