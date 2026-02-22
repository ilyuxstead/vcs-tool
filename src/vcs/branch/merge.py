"""
vcs.branch.merge — three-way merge implementation.

Merge strategy: three-way only (no fast-forward).  Every merge produces
a new merge commit with two parent hashes.  Conflicts are reported
per-file and the user must resolve them manually (FR-BR-03, FR-BR-04).
"""

from __future__ import annotations

import difflib
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

from vcs.repo.init import (
    current_branch,
    find_repo_root,
    resolve_head_commit,
    vcs_dir,
)
from vcs.repo.status import write_index
from vcs.store.db import (
    get_branch,
    get_commit,
    get_tree,
    insert_commit,
    insert_tree,
    open_db,
    update_branch_tip,
)
from vcs.store.exceptions import BranchNotFoundError, CommitNotFoundError, MergeConflictError
from vcs.store.models import Commit, Tree, TreeEntry
from vcs.store.objects import ObjectStore


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha3_256(data).hexdigest()


# ---------------------------------------------------------------------------
# Common ancestor (LCA) — simple BFS
# ---------------------------------------------------------------------------

def _find_lca(conn, hash_a: str, hash_b: str) -> str | None:
    """
    Find the lowest common ancestor of two commits via BFS.

    Returns *None* if the histories are completely disjoint.
    """
    from collections import deque

    ancestors_a: set[str] = set()
    queue: deque[str] = deque([hash_a])
    while queue:
        h = queue.popleft()
        if h in ancestors_a:
            continue
        ancestors_a.add(h)
        try:
            c = get_commit(conn, h)
            queue.extend(c.parent_hashes)
        except CommitNotFoundError:
            pass

    queue = deque([hash_b])
    visited: set[str] = set()
    while queue:
        h = queue.popleft()
        if h in visited:
            continue
        visited.add(h)
        if h in ancestors_a:
            return h
        try:
            c = get_commit(conn, h)
            queue.extend(c.parent_hashes)
        except CommitNotFoundError:
            pass
    return None


# ---------------------------------------------------------------------------
# Three-way merge
# ---------------------------------------------------------------------------

class _MergeResult(NamedTuple):
    merged: dict[str, bytes]       # path → merged content
    conflicts: list[str]            # paths with unresolvable conflicts


def _merge_text(base: str, ours: str, theirs: str) -> tuple[str, bool]:
    """
    Perform a simple three-way text merge.

    Returns (merged_text, had_conflict).  Conflict markers use the
    standard ``<<<<<<<`` / ``=======`` / ``>>>>>>>`` format.
    """
    base_lines = base.splitlines(keepends=True)
    ours_lines = ours.splitlines(keepends=True)
    theirs_lines = theirs.splitlines(keepends=True)

    # Use difflib SequenceMatcher to detect changes from base
    matcher_ours = difflib.SequenceMatcher(None, base_lines, ours_lines)
    matcher_theirs = difflib.SequenceMatcher(None, base_lines, theirs_lines)

    opcodes_ours = matcher_ours.get_opcodes()
    opcodes_theirs = matcher_theirs.get_opcodes()

    # Simple line-level three-way: if only one side changed, take it.
    # If both changed differently, emit conflict markers.
    ours_changed = ours != base
    theirs_changed = theirs != base
    had_conflict = False

    if not ours_changed:
        return theirs, False
    if not theirs_changed:
        return ours, False

    if ours == theirs:
        return ours, False

    # Both sides changed differently — emit conflict markers
    had_conflict = True
    merged = (
        "<<<<<<< ours\n"
        + ours
        + "=======\n"
        + theirs
        + ">>>>>>> theirs\n"
    )
    return merged, had_conflict


def three_way_merge(
    base_blobs: dict[str, bytes],
    ours_blobs: dict[str, bytes],
    theirs_blobs: dict[str, bytes],
) -> _MergeResult:
    """Merge three file sets, returning merged content and conflict list."""
    all_paths = sorted(set(base_blobs) | set(ours_blobs) | set(theirs_blobs))
    merged: dict[str, bytes] = {}
    conflicts: list[str] = []

    for path in all_paths:
        base_data = base_blobs.get(path, b"")
        ours_data = ours_blobs.get(path, b"")
        theirs_data = theirs_blobs.get(path, b"")

        # File deleted on one side
        if ours_data == b"" and theirs_data != b"":
            if base_data == ours_data:
                # We deleted it, they kept it — take deletion
                continue
            else:
                merged[path] = theirs_data
                continue
        if theirs_data == b"" and ours_data != b"":
            if base_data == theirs_data:
                merged[path] = ours_data
                continue
            else:
                continue

        base_text = base_data.decode("utf-8", errors="replace")
        ours_text = ours_data.decode("utf-8", errors="replace")
        theirs_text = theirs_data.decode("utf-8", errors="replace")

        merged_text, had_conflict = _merge_text(base_text, ours_text, theirs_text)
        if had_conflict:
            conflicts.append(path)
        merged[path] = merged_text.encode("utf-8")

    return _MergeResult(merged=merged, conflicts=conflicts)
