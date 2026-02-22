"""
vcs.repo.init — repository initialisation and discovery.

A repository lives in a ``.vcs/`` directory at the root of the working
tree.  :py:func:`find_repo_root` walks upward from a starting path to
locate the nearest ``.vcs/`` directory.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from vcs.store.db import open_db, create_branch
from vcs.store.exceptions import RepositoryExistsError, RepositoryNotFoundError
from vcs.repo.config import write_config

VCS_DIR = ".vcs"
DEFAULT_BRANCH = "main"


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def find_repo_root(start: Path | None = None) -> Path:
    """
    Walk upward from *start* (default: ``Path.cwd()``) looking for ``.vcs/``.

    Returns the **working-tree root** (parent of ``.vcs/``).
    Raises :py:exc:`RepositoryNotFoundError` if none is found.
    """
    current = (start or Path.cwd()).resolve()
    for directory in [current, *current.parents]:
        candidate = directory / VCS_DIR
        if candidate.is_dir():
            return directory
    raise RepositoryNotFoundError(
        "Not a VCS repository (or any parent up to the filesystem root). "
        f"Searched from: {current}"
    )


def vcs_dir(repo_root: Path) -> Path:
    """Return the ``.vcs/`` directory path for a given repo root."""
    return repo_root / VCS_DIR


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

def init_repo(path: Path, *, bare: bool = False) -> Path:
    """
    Initialise a new VCS repository at *path*.

    Parameters
    ----------
    path:
        The working-tree root directory.  Created if it does not exist.
    bare:
        If *True*, create a bare repository (no working tree).
        Currently a flag placeholder for future use.

    Returns
    -------
    Path
        The path to the newly created ``.vcs/`` directory.

    Raises
    ------
    RepositoryExistsError
        If *path* already contains a ``.vcs/`` directory.
    """
    dot_vcs = path / VCS_DIR
    if dot_vcs.exists():
        raise RepositoryExistsError(
            f"Repository already exists at {dot_vcs}."
        )

    # Create directory layout
    path.mkdir(parents=True, exist_ok=True)
    dot_vcs.mkdir()
    (dot_vcs / "objects").mkdir()
    (dot_vcs / "refs" / "branches").mkdir(parents=True)
    (dot_vcs / "refs" / "tags").mkdir(parents=True)

    # Write initial config
    config_path = dot_vcs / "config.toml"
    write_config(config_path, {
        "core": {
            "bare": bare,
            "default_branch": DEFAULT_BRANCH,
        }
    })

    # Initialise SQLite schema
    db_path = dot_vcs / "vcs.db"
    conn = open_db(db_path)
    conn.close()

    # Write HEAD pointing to default branch (no commit yet)
    head_path = dot_vcs / "HEAD"
    head_path.write_text(f"ref: refs/branches/{DEFAULT_BRANCH}\n", encoding="utf-8")

    return dot_vcs


# ---------------------------------------------------------------------------
# HEAD helpers
# ---------------------------------------------------------------------------

def read_head(repo_root: Path) -> str:
    """
    Return the current HEAD reference string.

    Returns either ``"ref: refs/branches/<name>"`` or a bare commit hash
    (detached HEAD).
    """
    head_path = vcs_dir(repo_root) / "HEAD"
    if not head_path.exists():
        raise RepositoryNotFoundError(f"HEAD not found in {vcs_dir(repo_root)}.")
    return head_path.read_text(encoding="utf-8").strip()


def write_head(repo_root: Path, ref: str) -> None:
    """Write *ref* to HEAD."""
    head_path = vcs_dir(repo_root) / "HEAD"
    head_path.write_text(ref + "\n", encoding="utf-8")


def current_branch(repo_root: Path) -> str | None:
    """
    Return the name of the current branch, or *None* if HEAD is detached.
    """
    head = read_head(repo_root)
    if head.startswith("ref: refs/branches/"):
        return head.removeprefix("ref: refs/branches/")
    return None


def resolve_head_commit(repo_root: Path) -> str | None:
    """
    Resolve HEAD to a commit hash.

    Returns *None* if the repository has no commits yet.
    """
    from vcs.store.db import get_branch
    from vcs.store.exceptions import BranchNotFoundError

    head = read_head(repo_root)

    if head.startswith("ref: refs/branches/"):
        branch_name = head.removeprefix("ref: refs/branches/")
        db_path = vcs_dir(repo_root) / "vcs.db"
        conn = open_db(db_path)
        try:
            branch = get_branch(conn, branch_name)
            return branch.tip_hash
        except BranchNotFoundError:
            return None  # Branch exists in HEAD but has no commits yet
        finally:
            conn.close()
    else:
        # Detached HEAD — the value is the commit hash itself
        return head if head else None
