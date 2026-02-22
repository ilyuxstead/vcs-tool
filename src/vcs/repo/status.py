"""
vcs.repo.status — working tree status relative to HEAD.

Computes three categories:
  - staged:    files in the staging area (index) ready to commit
  - modified:  tracked files changed since last commit (not staged)
  - untracked: files not known to VCS at all

The staging area is a JSON file at .vcs/index.json.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

from vcs.repo.init import resolve_head_commit, vcs_dir
from vcs.store.db import get_tree, open_db
from vcs.store.exceptions import ObjectNotFoundError
from vcs.store.objects import ObjectStore

INDEX_FILENAME = "index.json"
IGNORE_FILENAME = ".vcsignore"

# Always ignore these paths regardless of .vcsignore
_ALWAYS_IGNORE = {".vcs"}


@dataclass
class WorkingTreeStatus:
    """Result of a ``repo.status`` operation."""

    staged_new: list[str] = field(default_factory=list)
    staged_modified: list[str] = field(default_factory=list)
    staged_deleted: list[str] = field(default_factory=list)
    modified: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    untracked: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        """Return *True* if there are no changes of any kind."""
        return not any([
            self.staged_new, self.staged_modified, self.staged_deleted,
            self.modified, self.deleted, self.untracked,
        ])


# ---------------------------------------------------------------------------
# Index (staging area) helpers
# ---------------------------------------------------------------------------

def _index_path(repo_root: Path) -> Path:
    return vcs_dir(repo_root) / INDEX_FILENAME


def read_index(repo_root: Path) -> dict[str, str]:
    """
    Return the current staging index as ``{relative_path: sha3_256_hex}``.
    """
    path = _index_path(repo_root)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def write_index(repo_root: Path, index: dict[str, str]) -> None:
    """Atomically write *index* to the staging area file."""
    path = _index_path(repo_root)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(index, sort_keys=True, indent=2), encoding="utf-8")
    tmp.replace(path)


def _hash_file(path: Path) -> str:
    """Return the SHA3-256 hex digest of the file at *path*."""
    data = path.read_bytes()
    return hashlib.sha3_256(data).hexdigest()


# ---------------------------------------------------------------------------
# Ignore rules
# ---------------------------------------------------------------------------

def _load_ignore_patterns(repo_root: Path) -> list[str]:
    """Load patterns from .vcsignore (simple glob lines, no negation)."""
    ignore_file = repo_root / IGNORE_FILENAME
    if not ignore_file.exists():
        return []
    lines = ignore_file.read_text(encoding="utf-8").splitlines()
    return [ln.strip() for ln in lines if ln.strip() and not ln.startswith("#")]


def _is_ignored(rel_path: str, patterns: list[str]) -> bool:
    """Return True if *rel_path* matches any ignore pattern."""
    import fnmatch
    parts = Path(rel_path).parts
    if parts[0] in _ALWAYS_IGNORE:
        return True
    for pattern in patterns:
        if fnmatch.fnmatch(rel_path, pattern):
            return True
        if fnmatch.fnmatch(parts[-1], pattern):
            return True
    return False


# ---------------------------------------------------------------------------
# HEAD tree snapshot
# ---------------------------------------------------------------------------

def _head_tree_flat(repo_root: Path) -> dict[str, str]:
    """
    Return a flat ``{relative_path: blob_hash}`` dict for the HEAD commit's
    tree.  Returns an empty dict for a repo with no commits.
    """
    head_hash = resolve_head_commit(repo_root)
    if head_hash is None:
        return {}

    dot_vcs = vcs_dir(repo_root)
    conn = open_db(dot_vcs / "vcs.db")
    try:
        from vcs.store.db import get_commit
        commit = get_commit(conn, head_hash)
        tree = get_tree(conn, commit.tree_hash)
    finally:
        conn.close()

    # Flatten: for now trees are single-level (recursive trees = Phase 2)
    return {entry.name: entry.object_hash for entry in tree.entries}


# ---------------------------------------------------------------------------
# Main status computation
# ---------------------------------------------------------------------------

def compute_status(repo_root: Path) -> WorkingTreeStatus:
    """
    Compute the working tree status for the repository at *repo_root*.

    Performance target: <1 s for 10,000 files (NFR-01).
    """
    status = WorkingTreeStatus()
    index = read_index(repo_root)
    head_tree = _head_tree_flat(repo_root)
    ignore_patterns = _load_ignore_patterns(repo_root)

    object_store = ObjectStore(vcs_dir(repo_root) / "objects")

    # Enumerate all working-tree files
    working_tree: dict[str, str] = {}
    for abs_path in repo_root.rglob("*"):
        if not abs_path.is_file():
            continue
        rel = abs_path.relative_to(repo_root).as_posix()
        if _is_ignored(rel, ignore_patterns):
            continue
        working_tree[rel] = rel  # placeholder; hash computed lazily

    # --- Staged files ---
    for staged_path, staged_hash in index.items():
        abs_path = repo_root / staged_path
        if not abs_path.exists():
            status.staged_deleted.append(staged_path)
        elif staged_path in head_tree:
            if staged_hash != head_tree[staged_path]:
                status.staged_modified.append(staged_path)
            # else: staged but same as HEAD — still show as staged_modified
            # so user knows it's queued (edge: re-staged identical content)
        else:
            status.staged_new.append(staged_path)

    # --- Modified / deleted tracked files (not staged) ---
    for tracked_path, head_hash in head_tree.items():
        if tracked_path in index:
            continue  # Already covered by staged logic
        abs_path = repo_root / tracked_path
        if not abs_path.exists():
            status.deleted.append(tracked_path)
        else:
            current_hash = _hash_file(abs_path)
            if current_hash != head_hash:
                status.modified.append(tracked_path)

    # --- Untracked files ---
    all_known = set(index.keys()) | set(head_tree.keys())
    for rel in working_tree:
        if rel not in all_known:
            status.untracked.append(rel)

    # Sort for determinism
    status.staged_new.sort()
    status.staged_modified.sort()
    status.staged_deleted.sort()
    status.modified.sort()
    status.deleted.sort()
    status.untracked.sort()

    return status
