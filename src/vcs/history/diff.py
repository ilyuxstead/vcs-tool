"""
vcs.history.diff — produce diffs between commits or the working tree.

Uses Python stdlib ``difflib`` for unified diff generation.
No external dependencies.
"""

from __future__ import annotations

import difflib
from pathlib import Path

from vcs.repo.init import find_repo_root, resolve_head_commit, vcs_dir
from vcs.store.db import get_commit, get_tree, open_db
from vcs.store.objects import ObjectStore


def diff(
    hash_a: str | None,
    hash_b: str | None,
    repo_root: Path | None = None,
    *,
    stat: bool = False,
    name_only: bool = False,
) -> list[dict]:
    """Public alias for diff_commits; satisfies the vcs.history.diff.diff contract."""
    return diff_commits(hash_a, hash_b, repo_root, stat=stat, name_only=name_only)

def _get_tree_blobs(
    conn, store: ObjectStore, tree_hash: str
) -> dict[str, bytes]:
    """Return {filename: content_bytes} for a tree."""
    tree = get_tree(conn, tree_hash)
    result: dict[str, bytes] = {}
    for entry in tree.entries:
        if store.exists(entry.object_hash):
            result[entry.name] = store.read(entry.object_hash)
    return result


def _unified_diff(
    old_text: str,
    new_text: str,
    from_file: str,
    to_file: str,
) -> list[str]:
    """Return unified diff lines (no trailing newline on each line)."""
    return list(difflib.unified_diff(
        old_text.splitlines(keepends=True),
        new_text.splitlines(keepends=True),
        fromfile=from_file,
        tofile=to_file,
    ))


def diff_commits(
    hash_a: str | None,
    hash_b: str | None,
    repo_root: Path | None = None,
    *,
    stat: bool = False,
    name_only: bool = False,
) -> list[dict]:
    """
    Diff two commits (or a commit against the working tree).

    Parameters
    ----------
    hash_a:
        Older commit hash.  If *None*, diff from an empty tree.
    hash_b:
        Newer commit hash.  If *None*, diff against the working tree.
    stat:
        If *True*, include +/- line count statistics only.
    name_only:
        If *True*, return file names only (no diff content).

    Returns
    -------
    list[dict]
        One entry per changed file::

            {
                "path": str,
                "status": "added" | "modified" | "deleted",
                "lines": [str, ...],   # unified diff lines
                "added": int,
                "removed": int,
            }
    """
    root = repo_root or find_repo_root()
    dot_vcs = vcs_dir(root)
    conn = open_db(dot_vcs / "vcs.db")
    store = ObjectStore(dot_vcs / "objects")

    try:
        # Resolve old blobs
        old_blobs: dict[str, bytes] = {}
        if hash_a:
            commit_a = get_commit(conn, hash_a)
            old_blobs = _get_tree_blobs(conn, store, commit_a.tree_hash)

        # Resolve new blobs
        new_blobs: dict[str, bytes] = {}
        if hash_b:
            commit_b = get_commit(conn, hash_b)
            new_blobs = _get_tree_blobs(conn, store, commit_b.tree_hash)
        else:
            # Working tree
            for abs_path in root.rglob("*"):
                if abs_path.is_file() and ".vcs" not in abs_path.parts:
                    rel = abs_path.relative_to(root).as_posix()
                    new_blobs[rel] = abs_path.read_bytes()

        all_paths = sorted(set(old_blobs) | set(new_blobs))
        results: list[dict] = []

        for path in all_paths:
            old_data = old_blobs.get(path)
            new_data = new_blobs.get(path)

            if old_data == new_data:
                continue

            if old_data is None:
                status = "added"
            elif new_data is None:
                status = "deleted"
            else:
                status = "modified"

            entry: dict = {"path": path, "status": status, "lines": [], "added": 0, "removed": 0}

            if not name_only:
                old_text = old_data.decode("utf-8", errors="replace") if old_data else ""
                new_text = new_data.decode("utf-8", errors="replace") if new_data else ""
                diff_lines = _unified_diff(
                    old_text, new_text,
                    from_file=f"a/{path}",
                    to_file=f"b/{path}",
                )
                entry["lines"] = diff_lines
                entry["added"] = sum(1 for l in diff_lines if l.startswith("+") and not l.startswith("+++"))
                entry["removed"] = sum(1 for l in diff_lines if l.startswith("-") and not l.startswith("---"))

            results.append(entry)

        return results

    finally:
        conn.close()
