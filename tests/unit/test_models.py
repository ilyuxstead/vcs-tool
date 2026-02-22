"""
tests/unit/test_models.py — store/models.py unit tests.

Covers serialisation, canonical_bytes determinism, and round-trip fidelity
for Commit, Tree, TreeEntry, Branch, and Tag.
"""

from __future__ import annotations

import json

import pytest

from vcs.store.models import Branch, Commit, Tag, Tree, TreeEntry


def _commit(**kwargs) -> Commit:
    defaults = dict(
        hash="a" * 64,
        tree_hash="b" * 64,
        parent_hashes=(),
        author="Alice <a@test.com>",
        timestamp="2026-01-01T00:00:00Z",
        message="Initial commit",
    )
    defaults.update(kwargs)
    return Commit(**defaults)


def _tree(**kwargs) -> Tree:
    defaults = dict(
        hash="c" * 64,
        entries=(
            TreeEntry(mode="100644", name="README.md", object_hash="d" * 64),
        ),
    )
    defaults.update(kwargs)
    return Tree(**defaults)


class TestCommitModel:
    def test_to_dict_roundtrip(self):
        c = _commit()
        d = c.to_dict()
        c2 = Commit.from_dict(d)
        assert c2.hash == c.hash
        assert c2.author == c.author
        assert c2.parent_hashes == c.parent_hashes
        assert c2.message == c.message

    def test_to_dict_parent_hashes_as_list(self):
        c = _commit(parent_hashes=("1" * 64,))
        d = c.to_dict()
        assert isinstance(d["parent_hashes"], list)

    def test_from_dict_with_parents(self):
        d = {
            "hash": "a" * 64,
            "tree_hash": "b" * 64,
            "parent_hashes": ["1" * 64, "2" * 64],
            "author": "Bob",
            "timestamp": "2026-01-02T00:00:00Z",
            "message": "Merge",
        }
        c = Commit.from_dict(d)
        assert c.parent_hashes == ("1" * 64, "2" * 64)

    def test_canonical_bytes_is_bytes(self):
        c = _commit()
        result = c.canonical_bytes()
        assert isinstance(result, bytes)

    def test_canonical_bytes_deterministic(self):
        c = _commit()
        assert c.canonical_bytes() == c.canonical_bytes()

    def test_canonical_bytes_different_commits_differ(self):
        c1 = _commit(message="first")
        c2 = _commit(message="second")
        assert c1.canonical_bytes() != c2.canonical_bytes()

    def test_canonical_bytes_is_valid_json(self):
        c = _commit()
        parsed = json.loads(c.canonical_bytes())
        assert parsed["type"] == "commit"
        assert parsed["message"] == "Initial commit"

    def test_from_dict_missing_parent_hashes_defaults_empty(self):
        d = {
            "hash": "a" * 64,
            "tree_hash": "b" * 64,
            "author": "X",
            "timestamp": "2026-01-01T00:00:00Z",
            "message": "root",
        }
        c = Commit.from_dict(d)
        assert c.parent_hashes == ()


class TestTreeModel:
    def test_to_dict_roundtrip(self):
        t = _tree()
        d = t.to_dict()
        t2 = Tree.from_dict(d)
        assert t2.hash == t.hash
        assert len(t2.entries) == 1
        assert t2.entries[0].name == "README.md"

    def test_canonical_bytes_sorted_by_name(self):
        t = Tree(
            hash="x" * 64,
            entries=(
                TreeEntry(mode="100644", name="z_file.txt", object_hash="1" * 64),
                TreeEntry(mode="100644", name="a_file.txt", object_hash="2" * 64),
            ),
        )
        parsed = json.loads(t.canonical_bytes())
        names = [e["name"] for e in parsed["entries"]]
        assert names == sorted(names)

    def test_canonical_bytes_deterministic(self):
        t = _tree()
        assert t.canonical_bytes() == t.canonical_bytes()

    def test_canonical_bytes_is_valid_json(self):
        t = _tree()
        parsed = json.loads(t.canonical_bytes())
        assert parsed["type"] == "tree"
        assert isinstance(parsed["entries"], list)

    def test_empty_tree(self):
        t = Tree(hash="e" * 64, entries=())
        d = t.to_dict()
        t2 = Tree.from_dict(d)
        assert t2.entries == ()

    def test_from_dict_empty_entries(self):
        d = {"hash": "a" * 64, "entries": []}
        t = Tree.from_dict(d)
        assert t.entries == ()


class TestBranchModel:
    def test_create(self):
        b = Branch(name="main", tip_hash="a" * 64)
        assert b.name == "main"
        assert b.tip_hash == "a" * 64

    def test_mutable(self):
        b = Branch(name="main", tip_hash="a" * 64)
        b.tip_hash = "b" * 64
        assert b.tip_hash == "b" * 64


class TestTagModel:
    def test_create_minimal(self):
        t = Tag(name="v1.0", target_hash="a" * 64)
        assert t.name == "v1.0"
        assert t.tagger == ""
        assert t.message == ""

    def test_create_annotated(self):
        t = Tag(
            name="v2.0",
            target_hash="b" * 64,
            tagger="CI Bot",
            timestamp="2026-06-01T00:00:00Z",
            message="Release 2.0",
        )
        assert t.tagger == "CI Bot"
        assert t.message == "Release 2.0"
