"""
vcs.history.log — commit history traversal.

Walks the DAG from a tip commit upward through parent pointers,
yielding commits in reverse-chronological order.

Performance target: history.log --limit 100 in under 500ms (NFR-02).
"""

from __future__ import annotations

from collections import deque
from pathlib import Path

from vcs.repo.init import find_repo_root, resolve_head_commit, vcs_dir
from vcs.store.db import get_branch, get_commit, open_db
from vcs.store.exceptions import BranchNotFoundError, CommitNotFoundError
from vcs.store.models import Commit


def log(
    repo_root: Path | None = None,
    *,
    branch: str | None = None,
    limit: int | None = None,
    author: str | None = None,
) -> list[Commit]:
    """
    Return commits reachable from the given branch tip (or HEAD).

    Traversal is BFS from the tip, following parent pointers.  Results
    are returned newest-first (by timestamp).

    Parameters
    ----------
    branch:
        Branch name to start from.  Defaults to the current branch.
    limit:
        Maximum number of commits to return.
    author:
        Filter to commits whose author string contains *author*
        (case-insensitive substring match).
    """
    root = repo_root or find_repo_root()
    dot_vcs = vcs_dir(root)
    conn = open_db(dot_vcs / "vcs.db")

    try:
        # Resolve starting commit hash
        if branch:
            try:
                b = get_branch(conn, branch)
                start_hash: str | None = b.tip_hash
            except BranchNotFoundError:
                raise BranchNotFoundError(
                    f"Branch {branch!r} does not exist."
                )
        else:
            start_hash = resolve_head_commit(root)

        if start_hash is None:
            return []  # No commits yet

        # BFS walk — avoids recursion depth issues on deep histories
        results: list[Commit] = []
        visited: set[str] = set()
        queue: deque[str] = deque([start_hash])

        while queue:
            if limit and len(results) >= limit:
                break

            current_hash = queue.popleft()
            if current_hash in visited:
                continue
            visited.add(current_hash)

            try:
                commit = get_commit(conn, current_hash)
            except CommitNotFoundError:
                continue  # Dangling reference — skip gracefully

            if author and author.lower() not in commit.author.lower():
                # Still traverse parents even if this commit is filtered
                pass
            else:
                results.append(commit)

            for parent_hash in commit.parent_hashes:
                if parent_hash not in visited:
                    queue.append(parent_hash)

        # Sort by timestamp descending for stable output
        results.sort(key=lambda c: c.timestamp, reverse=True)
        return results[:limit] if limit else results

    finally:
        conn.close()
