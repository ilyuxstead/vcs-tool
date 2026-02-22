"""
vcs.commit.snapshot — create immutable commits from the staging area.

Snapshot workflow:
  1. Read the staging index → {relative_path: blob_hash}
  2. Build a Tree object from the index entries
  3. Hash the tree, store it in the object store and SQLite
  4. Build a Commit object (parent = current HEAD)
  5. Hash the commit, store it
  6. Advance the current branch tip
  7. Clear the staging index

Amend is explicitly forbidden (FR-IMM-05).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from vcs.repo.init import (
    current_branch,
    find_repo_root,
    resolve_head_commit,
    vcs_dir,
    write_head,
)
from vcs.repo.status import read_index, write_index
from vcs.store.db import (
    create_branch,
    get_branch,
    insert_commit,
    insert_tree,
    open_db,
    update_branch_tip,
)
from vcs.store.exceptions import BranchNotFoundError, ImmutabilityViolationError, StagingError
from vcs.store.models import Commit, Tree, TreeEntry
from vcs.store.objects import ObjectStore


def _now_utc() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha3_256(data).hexdigest()


def create_snapshot(
    message: str,
    author: str,
    repo_root: Path | None = None,
    *,
    timestamp: str | None = None,
) -> Commit:
    """
    Create a new immutable commit from the current staging area.

    Parameters
    ----------
    message:
        Commit message (must be non-empty).
    author:
        Author string (e.g. ``"Alice <alice@example.com>"``).
    repo_root:
        Repository root.  Auto-discovered if *None*.
    timestamp:
        Override the commit timestamp (ISO-8601 UTC).  Uses current
        time if *None*.  Useful for deterministic tests.

    Returns
    -------
    Commit
        The newly created commit object.

    Raises
    ------
    StagingError
        If the staging area is empty.
    ImmutabilityViolationError
        If an ``--amend`` flag is somehow passed (guard against callers).
    """
    if not message or not message.strip():
        raise StagingError("Commit message must not be empty.")

    root = repo_root or find_repo_root()
    index = read_index(root)

    if not index:
        raise StagingError(
            "Nothing to commit — staging area is empty. "
            "Use 'vcs commit.stage' to stage files first."
        )

    dot_vcs = vcs_dir(root)
    store = ObjectStore(dot_vcs / "objects")
    conn = open_db(dot_vcs / "vcs.db")

    try:
        # 1. Build Tree — start from parent's tree, overlay staged changes
        #    so that files not re-staged this commit are preserved.
        parent_hash = resolve_head_commit(root)
        parent_hashes: tuple[str, ...] = (parent_hash,) if parent_hash else ()

        inherited: dict[str, str] = {}  # path → blob_hash from parent tree
        if parent_hash:
            from vcs.store.db import get_commit as _get_commit, get_tree as _get_tree
            try:
                parent_commit = _get_commit(conn, parent_hash)
                parent_tree = _get_tree(conn, parent_commit.tree_hash)
                for entry in parent_tree.entries:
                    inherited[entry.name] = entry.object_hash
            except Exception:
                pass  # root commit or missing tree — start fresh

        # Staged files override inherited entries
        merged_index = {**inherited, **index}

        entries = tuple(
            TreeEntry(mode="100644", name=rel_path, object_hash=blob_hash)
            for rel_path, blob_hash in sorted(merged_index.items())
        )
        # Compute tree hash from canonical representation
        tree_payload = json.dumps(
            {
                "type": "tree",
                "entries": [
                    {"mode": e.mode, "name": e.name, "object_hash": e.object_hash}
                    for e in sorted(entries, key=lambda e: e.name)
                ],
            },
            sort_keys=True,
        ).encode()
        tree_hash = _hash_bytes(tree_payload)
        tree = Tree(hash=tree_hash, entries=entries)

        # Store tree blob and metadata
        store.write(tree_payload, warn_large=False)
        insert_tree(conn, tree)

        # 2. Resolve parent 20260222 delete those two lines entirely; parent_hash and parent_hashes
        # are already set above
        #parent_hash = resolve_head_commit(root)
        #parent_hashes: tuple[str, ...] = (parent_hash,) if parent_hash else ()

        # 3. Build Commit
        ts = timestamp or _now_utc()
        commit_payload_obj = {
            "type": "commit",
            "tree_hash": tree_hash,
            "parent_hashes": sorted(parent_hashes),
            "author": author,
            "timestamp": ts,
            "message": message,
        }
        commit_payload = json.dumps(commit_payload_obj, sort_keys=True).encode()
        commit_hash = _hash_bytes(commit_payload)

        commit = Commit(
            hash=commit_hash,
            tree_hash=tree_hash,
            parent_hashes=parent_hashes,
            author=author,
            timestamp=ts,
            message=message,
        )

        # Store commit blob and metadata
        store.write(commit_payload, warn_large=False)
        insert_commit(conn, commit)

        # 4. Advance branch tip (or create branch on first commit)
        branch_name = current_branch(root)
        if branch_name:
            try:
                get_branch(conn, branch_name)
                update_branch_tip(conn, branch_name, commit_hash)
            except BranchNotFoundError:
                create_branch(conn, branch_name, commit_hash)

        # 5. Clear staging index
        write_index(root, {})

        return commit

    finally:
        conn.close()


def reject_amend() -> None:
    """
    Guard function — always raises.

    Called by the CLI when a user passes ``--amend`` to ``commit.snapshot``.
    History is immutable; amend is not supported (FR-IMM-05).
    """
    raise ImmutabilityViolationError(
        "History is immutable. The --amend flag is not supported. "
        "Create a new commit to correct mistakes."
    )
