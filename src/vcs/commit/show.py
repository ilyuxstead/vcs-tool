"""
vcs.commit.show — display commit metadata and its diff.
"""

from __future__ import annotations

from pathlib import Path

from vcs.repo.init import find_repo_root, vcs_dir
from vcs.store.db import get_commit, get_tree, open_db
from vcs.store.exceptions import CommitNotFoundError
from vcs.store.models import Commit, Tree
from vcs.store.objects import ObjectStore


def get_commit_detail(hex_hash: str, repo_root: Path | None = None) -> dict:
    """
    Return a dict with commit metadata and a per-file diff summary.

    Returned structure::

        {
            "hash": str,
            "author": str,
            "timestamp": str,
            "message": str,
            "parent_hashes": [str, ...],
            "tree_hash": str,
            "files": [
                {
                    "path": str,
                    "status": "added" | "modified" | "deleted",
                    "old_hash": str | None,
                    "new_hash": str | None,
                }
            ]
        }
    """
    root = repo_root or find_repo_root()
    dot_vcs = vcs_dir(root)
    conn = open_db(dot_vcs / "vcs.db")
    store = ObjectStore(dot_vcs / "objects")

    try:
        commit = get_commit(conn, hex_hash)
        current_tree = get_tree(conn, commit.tree_hash)

        # Parent tree (may be empty for root commit)
        parent_entries: dict[str, str] = {}
        if commit.parent_hashes:
            parent_commit = get_commit(conn, commit.parent_hashes[0])
            parent_tree = get_tree(conn, parent_commit.tree_hash)
            parent_entries = {e.name: e.object_hash for e in parent_tree.entries}

        current_entries = {e.name: e.object_hash for e in current_tree.entries}

        files = []
        all_paths = sorted(set(parent_entries) | set(current_entries))
        for path in all_paths:
            old_hash = parent_entries.get(path)
            new_hash = current_entries.get(path)
            if old_hash is None:
                status = "added"
            elif new_hash is None:
                status = "deleted"
            else:
                status = "modified" if old_hash != new_hash else "unchanged"

            if status != "unchanged":
                files.append({
                    "path": path,
                    "status": status,
                    "old_hash": old_hash,
                    "new_hash": new_hash,
                })

        return {
            "hash": commit.hash,
            "author": commit.author,
            "timestamp": commit.timestamp,
            "message": commit.message,
            "parent_hashes": list(commit.parent_hashes),
            "tree_hash": commit.tree_hash,
            "files": files,
        }
    finally:
        conn.close()
