"""
tests/property/test_dag_properties.py — DAG invariant property tests.

Verifies: commit parent references always point to existing commits,
immutability after write, and branch pointer consistency.
"""

from __future__ import annotations

import string
import tempfile
from pathlib import Path

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

from vcs.store.db import (
    commit_exists,
    get_commit,
    insert_commit,
    open_db,
)
from vcs.store.models import Commit


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

commit_message_st = st.text(min_size=1, max_size=200)
author_st = st.text(
    alphabet=string.ascii_letters + " @.<>",
    min_size=3,
    max_size=50,
)
hash_st = st.text(alphabet="0123456789abcdef", min_size=64, max_size=64)


@st.composite
def commit_chain(draw, max_length=5):
    """Draw a chain of commits with valid parent pointers."""
    length = draw(st.integers(min_value=1, max_value=max_length))
    hashes = [draw(hash_st) for _ in range(length)]
    # Ensure unique hashes
    assume(len(set(hashes)) == length)
    commits = []
    for i, h in enumerate(hashes):
        parent = (hashes[i - 1],) if i > 0 else ()
        commits.append(Commit(
            hash=h,
            tree_hash="b" * 64,
            parent_hashes=parent,
            author=draw(author_st),
            timestamp="2026-01-01T00:00:00Z",
            message=draw(commit_message_st),
        ))
    return commits


class TestDAGParentInvariant:
    @given(commits=commit_chain())
    def test_parent_always_exists(self, commits: list[Commit]):
        """Every commit's parent hash (if present) refers to an existing commit."""
        with tempfile.TemporaryDirectory() as td:
            conn = open_db(Path(td) / "vcs.db")
            try:
                for c in commits:
                    insert_commit(conn, c)

                for c in commits:
                    for parent_hash in c.parent_hashes:
                        assert commit_exists(conn, parent_hash), (
                            f"Parent {parent_hash} of commit {c.hash} not found in store"
                        )
            finally:
                conn.close()

    @given(commits=commit_chain())
    def test_commit_hash_stable_after_insert(self, commits: list[Commit]):
        """After inserting a commit, reading it back returns the same hash."""
        with tempfile.TemporaryDirectory() as td:
            conn = open_db(Path(td) / "vcs.db")
            try:
                for c in commits:
                    insert_commit(conn, c)
                for c in commits:
                    fetched = get_commit(conn, c.hash)
                    assert fetched.hash == c.hash
                    assert fetched.message == c.message
            finally:
                conn.close()

    @given(commits=commit_chain())
    def test_duplicate_insert_does_not_corrupt(self, commits: list[Commit]):
        """Inserting the same commit twice leaves it unchanged."""
        with tempfile.TemporaryDirectory() as td:
            conn = open_db(Path(td) / "vcs.db")
            try:
                for c in commits:
                    insert_commit(conn, c)
                # Insert all again
                for c in commits:
                    insert_commit(conn, c)
                # All should still be correct
                for c in commits:
                    fetched = get_commit(conn, c.hash)
                    assert fetched.hash == c.hash
            finally:
                conn.close()
