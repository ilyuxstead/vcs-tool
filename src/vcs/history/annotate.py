"""
vcs.history.annotate — per-line commit attribution (blame).

Walks the commit history for a file and attributes each line to the
commit that last introduced it.  Uses a simple "last-write-wins" approach
by walking history from newest to oldest.
"""

from __future__ import annotations

from pathlib import Path

from vcs.repo.init import find_repo_root, resolve_head_commit, vcs_dir
from vcs.store.db import get_commit, get_tree, open_db
from vcs.store.models import Commit
from vcs.store.objects import ObjectStore
from vcs.history.log import log


def annotate(
    file_path: str,
    repo_root: Path | None = None,
) -> list[dict]:
    """
    Return per-line attribution for *file_path*.

    Each entry in the returned list corresponds to one line::

        {
            "line_number": int,       # 1-based
            "commit_hash": str,       # short hash (8 chars)
            "author": str,
            "timestamp": str,
            "content": str,           # line content (no trailing newline)
        }

    Lines are attributed to the most recent commit that changed them.
    """
    root = repo_root or find_repo_root()
    dot_vcs = vcs_dir(root)
    store = ObjectStore(dot_vcs / "objects")
    conn = open_db(dot_vcs / "vcs.db")

    try:
        commits = log(root)
        if not commits:
            return []

        # Build attribution map: line_content → earliest attribution
        # We walk newest → oldest; first time we see content for a line, we keep it
        # Simple O(n*m) approach; sufficient for Phase 1
        current_lines: list[str] | None = None
        attribution: list[dict] | None = None

        for commit in commits:
            tree = get_tree(conn, commit.tree_hash)
            entry = next((e for e in tree.entries if e.name == file_path), None)
            if entry is None:
                continue

            if not store.exists(entry.object_hash):
                continue

            data = store.read(entry.object_hash)
            lines = data.decode("utf-8", errors="replace").splitlines()

            if attribution is None:
                # First (newest) commit that has this file — initialise
                attribution = [
                    {
                        "line_number": i + 1,
                        "commit_hash": commit.hash[:8],
                        "author": commit.author,
                        "timestamp": commit.timestamp,
                        "content": line,
                    }
                    for i, line in enumerate(lines)
                ]
                current_lines = lines
            else:
                # Walking newest → oldest: if a line in this older commit matches
                # the current line content, the line existed at least as far back
                # as this commit — attribute it here (it may go back further still).
                # If the content differs, the line was introduced in a newer commit,
                # so leave the attribution pointing at that newer commit.
                if len(lines) == len(current_lines):
                    for i, (old_line, cur_line) in enumerate(zip(lines, current_lines)):
                        if old_line == cur_line and i < len(attribution):
                            attribution[i].update({
                                "commit_hash": commit.hash[:8],
                                "author": commit.author,
                                "timestamp": commit.timestamp,
                            })
                # Update our reference snapshot for the next (older) iteration
                current_lines = lines

        return attribution or []

    finally:
        conn.close()
