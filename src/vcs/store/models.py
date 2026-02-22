"""
vcs.store.models — immutable dataclasses representing VCS objects.

All objects are identified by their SHA3-256 hash.  The hash is always
computed from the object's canonical serialisation and stored alongside it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Tree entry
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TreeEntry:
    """One entry in a tree: a file name mapped to its blob hash."""

    mode: str          # e.g. "100644" (regular file), "100755" (executable)
    name: str          # filename (no path separators)
    object_hash: str   # SHA3-256 hex of the blob or sub-tree


# ---------------------------------------------------------------------------
# Core immutable objects
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Tree:
    """
    A directory snapshot: maps filenames to blob hashes.

    ``hash`` is the SHA3-256 of the canonical JSON serialisation of
    ``entries`` (sorted by name for determinism).
    """

    hash: str
    entries: tuple[TreeEntry, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "hash": self.hash,
            "entries": [
                {"mode": e.mode, "name": e.name, "object_hash": e.object_hash}
                for e in self.entries
            ],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Tree":
        entries = tuple(
            TreeEntry(mode=e["mode"], name=e["name"], object_hash=e["object_hash"])
            for e in data.get("entries", [])
        )
        return cls(hash=data["hash"], entries=entries)

    def canonical_bytes(self) -> bytes:
        """Deterministic serialisation used as the pre-image for hashing."""
        payload = [
            {"mode": e.mode, "name": e.name, "object_hash": e.object_hash}
            for e in sorted(self.entries, key=lambda e: e.name)
        ]
        return json.dumps({"type": "tree", "entries": payload}, sort_keys=True).encode()


@dataclass(frozen=True)
class Commit:
    """
    An immutable commit record.

    ``hash`` is the SHA3-256 of the canonical JSON serialisation of all
    other fields.  ``parent_hashes`` is an empty tuple for the root commit
    and a two-element tuple for a merge commit.
    """

    hash: str
    tree_hash: str
    parent_hashes: tuple[str, ...]   # () root | (p,) normal | (p1, p2) merge
    author: str
    timestamp: str                   # ISO-8601 UTC e.g. "2026-02-21T10:30:00Z"
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "hash": self.hash,
            "tree_hash": self.tree_hash,
            "parent_hashes": list(self.parent_hashes),
            "author": self.author,
            "timestamp": self.timestamp,
            "message": self.message,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Commit":
        return cls(
            hash=data["hash"],
            tree_hash=data["tree_hash"],
            parent_hashes=tuple(data.get("parent_hashes", [])),
            author=data["author"],
            timestamp=data["timestamp"],
            message=data["message"],
        )

    def canonical_bytes(self) -> bytes:
        """Deterministic serialisation used as the pre-image for hashing."""
        payload = {
            "type": "commit",
            "tree_hash": self.tree_hash,
            "parent_hashes": sorted(self.parent_hashes),
            "author": self.author,
            "timestamp": self.timestamp,
            "message": self.message,
        }
        return json.dumps(payload, sort_keys=True).encode()


# ---------------------------------------------------------------------------
# Mutable pointers
# ---------------------------------------------------------------------------

@dataclass
class Branch:
    """A mutable named pointer to a commit hash."""

    name: str
    tip_hash: str


@dataclass
class Tag:
    """An immutable named pointer (annotated or lightweight) to a commit hash."""

    name: str
    target_hash: str
    tagger: str = ""
    timestamp: str = ""
    message: str = ""
