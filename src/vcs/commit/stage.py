"""
vcs.commit.stage — stage and unstage files.

Staging hashes the file content, writes the blob to the object store,
and records the mapping in .vcs/index.json.

Unstaging removes the path from the index (the blob remains in the
object store — blobs are never deleted).
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from vcs.repo.status import read_index, write_index
from vcs.repo.init import find_repo_root, vcs_dir
from vcs.store.objects import ObjectStore
from vcs.store.exceptions import StagingError


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha3_256(data).hexdigest()


def stage_files(paths: list[Path], repo_root: Path | None = None) -> list[str]:
    """
    Stage one or more files.

    Reads each file's content, writes it to the object store, and
    records the path → hash mapping in the index.

    Parameters
    ----------
    paths:
        Absolute or relative paths to files to stage.
    repo_root:
        Repository root.  Auto-discovered if *None*.

    Returns
    -------
    list[str]
        Relative (POSIX) paths of the files that were staged.

    Raises
    ------
    StagingError
        If a path does not exist or points to a directory.
    """
    root = repo_root or find_repo_root()
    store = ObjectStore(vcs_dir(root) / "objects")
    index = read_index(root)

    staged: list[str] = []
    for p in paths:
        abs_p = Path(p) if Path(p).is_absolute() else root / p
        abs_p = abs_p.resolve()
        root_resolved = root.resolve()  # handles macOS /private/var symlink

        if not abs_p.exists():
            raise StagingError(f"Path does not exist: {p}")
        if abs_p.is_dir():
            raise StagingError(f"Cannot stage a directory directly: {p}")

        data = abs_p.read_bytes()
        blob_hash = store.write(data)
        rel = abs_p.relative_to(root_resolved).as_posix()
        index[rel] = blob_hash
        staged.append(rel)

    write_index(root, index)
    return staged


def stage_all(repo_root: Path | None = None) -> list[str]:
    """
    Stage all modified and untracked files (equivalent to ``--all``).

    Uses :py:func:`vcs.repo.status.compute_status` to find eligible paths.
    """
    from vcs.repo.status import compute_status

    root = repo_root or find_repo_root()
    status = compute_status(root)

    candidates = (
        status.staged_new       # already staged but re-stage for refresh
        + status.staged_modified
        + status.modified
        + status.untracked
    )

    paths = [root / rel for rel in candidates]
    if not paths:
        return []
    return stage_files(paths, repo_root=root)


def unstage_files(paths: list[Path], repo_root: Path | None = None) -> list[str]:
    """
    Remove files from the staging index.

    The blob objects remain in the store — they are never deleted.

    Returns
    -------
    list[str]
        Relative paths that were removed from the index.

    Raises
    ------
    StagingError
        If a path was not in the index.
    """
    root = repo_root or find_repo_root()
    index = read_index(root)

    removed: list[str] = []
    for p in paths:
        abs_p = Path(p) if Path(p).is_absolute() else root / p
        rel = abs_p.resolve().relative_to(root.resolve()).as_posix()

        if rel not in index:
            raise StagingError(f"Path {rel!r} is not in the staging index.")
        del index[rel]
        removed.append(rel)

    write_index(root, index)
    return removed
