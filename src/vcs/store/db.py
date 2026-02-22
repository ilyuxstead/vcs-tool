"""
vcs.store.db — SQLite metadata layer.

All schema migrations are applied in :py:func:`open_db` so callers
never need to manage schema versions manually.

Immutability contract (FR-IMM-03):
  - commits and trees tables have NO UPDATE / DELETE statements issued
    by any application code.
  - branches is the only table with mutable rows.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .exceptions import (
    BranchExistsError,
    BranchNotFoundError,
    CommitNotFoundError,
    TagExistsError,
    TagNotFoundError,
)
from .models import Branch, Commit, Tag, Tree, TreeEntry

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_DDL = """\
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS commits (
    hash        TEXT PRIMARY KEY,
    tree_hash   TEXT NOT NULL,
    parent_hashes TEXT NOT NULL,   -- JSON array of strings
    author      TEXT NOT NULL,
    timestamp   TEXT NOT NULL,
    message     TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS trees (
    hash    TEXT PRIMARY KEY,
    entries TEXT NOT NULL           -- JSON array of {mode, name, object_hash}
) STRICT;

CREATE TABLE IF NOT EXISTS branches (
    name        TEXT PRIMARY KEY,
    tip_hash    TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS remotes (
    name        TEXT PRIMARY KEY,
    url         TEXT NOT NULL,
    last_sync   TEXT NOT NULL DEFAULT ''
) STRICT;

CREATE TABLE IF NOT EXISTS tags (
    name        TEXT PRIMARY KEY,
    target_hash TEXT NOT NULL,
    tagger      TEXT NOT NULL DEFAULT '',
    timestamp   TEXT NOT NULL DEFAULT '',
    message     TEXT NOT NULL DEFAULT ''
) STRICT;
"""


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------

def open_db(db_path: Path) -> sqlite3.Connection:
    """
    Open (or create) the SQLite database at *db_path* and apply DDL.

    Returns a :py:class:`sqlite3.Connection` with ``row_factory`` set to
    :py:class:`sqlite3.Row` for dict-style column access.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_DDL)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Commits
# ---------------------------------------------------------------------------

def insert_commit(conn: sqlite3.Connection, commit: Commit) -> None:
    """Insert *commit* into the commits table (append-only, never updated)."""
    import json
    conn.execute(
        "INSERT OR IGNORE INTO commits "
        "(hash, tree_hash, parent_hashes, author, timestamp, message) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            commit.hash,
            commit.tree_hash,
            json.dumps(list(commit.parent_hashes)),
            commit.author,
            commit.timestamp,
            commit.message,
        ),
    )
    conn.commit()


def get_commit(conn: sqlite3.Connection, hex_hash: str) -> Commit:
    """
    Fetch a commit by hash.

    Raises :py:exc:`CommitNotFoundError` if not found.
    """
    import json
    row = conn.execute(
        "SELECT hash, tree_hash, parent_hashes, author, timestamp, message "
        "FROM commits WHERE hash = ?",
        (hex_hash,),
    ).fetchone()
    if row is None:
        raise CommitNotFoundError(f"Commit {hex_hash!r} not found.")
    return Commit(
        hash=row["hash"],
        tree_hash=row["tree_hash"],
        parent_hashes=tuple(json.loads(row["parent_hashes"])),
        author=row["author"],
        timestamp=row["timestamp"],
        message=row["message"],
    )


def commit_exists(conn: sqlite3.Connection, hex_hash: str) -> bool:
    """Return *True* if a commit with *hex_hash* is present."""
    row = conn.execute(
        "SELECT 1 FROM commits WHERE hash = ?", (hex_hash,)
    ).fetchone()
    return row is not None


def list_commits(
    conn: sqlite3.Connection,
    *,
    branch_tip: str | None = None,
    limit: int | None = None,
    author: str | None = None,
) -> list[Commit]:
    """
    Return commits in reverse-chronological order.

    If *branch_tip* is given, only commits reachable from that tip are
    returned (via a recursive CTE DAG walk).
    """
    import json

    if branch_tip is not None:
        # Recursive CTE: walk the DAG from tip upward through parents.
        sql = """\
WITH RECURSIVE ancestors(hash) AS (
    SELECT ? AS hash
    UNION
    SELECT json_each.value
    FROM ancestors
    JOIN commits ON commits.hash = ancestors.hash,
         json_each(commits.parent_hashes)
)
SELECT c.hash, c.tree_hash, c.parent_hashes, c.author, c.timestamp, c.message
FROM commits c
JOIN ancestors a ON c.hash = a.hash
"""
        params: list[Any] = [branch_tip]
    else:
        sql = "SELECT hash, tree_hash, parent_hashes, author, timestamp, message FROM commits"
        params = []

    if author:
        sql += " WHERE c.author LIKE ?" if branch_tip else " WHERE author LIKE ?"
        params.append(f"%{author}%")

    sql += " ORDER BY timestamp DESC"

    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [
        Commit(
            hash=r["hash"],
            tree_hash=r["tree_hash"],
            parent_hashes=tuple(json.loads(r["parent_hashes"])),
            author=r["author"],
            timestamp=r["timestamp"],
            message=r["message"],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Trees
# ---------------------------------------------------------------------------

def insert_tree(conn: sqlite3.Connection, tree: Tree) -> None:
    """Insert *tree* into the trees table (append-only)."""
    import json
    entries_json = json.dumps(
        [
            {"mode": e.mode, "name": e.name, "object_hash": e.object_hash}
            for e in tree.entries
        ]
    )
    conn.execute(
        "INSERT OR IGNORE INTO trees (hash, entries) VALUES (?, ?)",
        (tree.hash, entries_json),
    )
    conn.commit()


def get_tree(conn: sqlite3.Connection, hex_hash: str) -> Tree:
    """Fetch a tree by hash."""
    import json
    row = conn.execute(
        "SELECT hash, entries FROM trees WHERE hash = ?", (hex_hash,)
    ).fetchone()
    if row is None:
        from .exceptions import ObjectNotFoundError
        raise ObjectNotFoundError(f"Tree {hex_hash!r} not found.")
    entries = tuple(
        TreeEntry(mode=e["mode"], name=e["name"], object_hash=e["object_hash"])
        for e in json.loads(row["entries"])
    )
    return Tree(hash=row["hash"], entries=entries)


# ---------------------------------------------------------------------------
# Branches
# ---------------------------------------------------------------------------

def create_branch(conn: sqlite3.Connection, name: str, tip_hash: str) -> Branch:
    """
    Create a new branch pointer.

    Raises :py:exc:`BranchExistsError` if *name* is already taken.
    """
    existing = conn.execute(
        "SELECT 1 FROM branches WHERE name = ?", (name,)
    ).fetchone()
    if existing:
        raise BranchExistsError(f"Branch {name!r} already exists.")
    conn.execute(
        "INSERT INTO branches (name, tip_hash) VALUES (?, ?)", (name, tip_hash)
    )
    conn.commit()
    return Branch(name=name, tip_hash=tip_hash)


def get_branch(conn: sqlite3.Connection, name: str) -> Branch:
    """
    Fetch a branch by name.

    Raises :py:exc:`BranchNotFoundError` if not found.
    """
    row = conn.execute(
        "SELECT name, tip_hash FROM branches WHERE name = ?", (name,)
    ).fetchone()
    if row is None:
        raise BranchNotFoundError(f"Branch {name!r} not found.")
    return Branch(name=row["name"], tip_hash=row["tip_hash"])


def update_branch_tip(conn: sqlite3.Connection, name: str, tip_hash: str) -> None:
    """
    Advance the tip pointer of an existing branch.

    Raises :py:exc:`BranchNotFoundError` if *name* does not exist.
    """
    result = conn.execute(
        "UPDATE branches SET tip_hash = ? WHERE name = ?", (tip_hash, name)
    )
    conn.commit()
    if result.rowcount == 0:
        raise BranchNotFoundError(f"Branch {name!r} not found.")


def delete_branch(conn: sqlite3.Connection, name: str) -> None:
    """Delete a branch pointer (does not alter history)."""
    result = conn.execute("DELETE FROM branches WHERE name = ?", (name,))
    conn.commit()
    if result.rowcount == 0:
        raise BranchNotFoundError(f"Branch {name!r} not found.")


def list_branches(conn: sqlite3.Connection) -> list[Branch]:
    """Return all branches sorted by name."""
    rows = conn.execute(
        "SELECT name, tip_hash FROM branches ORDER BY name"
    ).fetchall()
    return [Branch(name=r["name"], tip_hash=r["tip_hash"]) for r in rows]


def branch_exists(conn: sqlite3.Connection, name: str) -> bool:
    """Return *True* if a branch with *name* exists."""
    return conn.execute(
        "SELECT 1 FROM branches WHERE name = ?", (name,)
    ).fetchone() is not None


# ---------------------------------------------------------------------------
# Remotes
# ---------------------------------------------------------------------------

def add_remote(conn: sqlite3.Connection, name: str, url: str) -> None:
    """Register a remote. Raises if *name* already exists."""
    existing = conn.execute(
        "SELECT 1 FROM remotes WHERE name = ?", (name,)
    ).fetchone()
    if existing:
        from .exceptions import RemoteError
        raise RemoteError(f"Remote {name!r} already exists.")
    conn.execute(
        "INSERT INTO remotes (name, url) VALUES (?, ?)", (name, url)
    )
    conn.commit()


def list_remotes(conn: sqlite3.Connection) -> list[dict]:
    """Return all remotes as plain dicts."""
    rows = conn.execute(
        "SELECT name, url, last_sync FROM remotes ORDER BY name"
    ).fetchall()
    return [dict(r) for r in rows]


def get_remote(conn: sqlite3.Connection, name: str) -> dict:
    """Fetch a remote by name."""
    row = conn.execute(
        "SELECT name, url, last_sync FROM remotes WHERE name = ?", (name,)
    ).fetchone()
    if row is None:
        from .exceptions import RemoteError
        raise RemoteError(f"Remote {name!r} not found.")
    return dict(row)


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def create_tag(
    conn: sqlite3.Connection,
    name: str,
    target_hash: str,
    tagger: str = "",
    timestamp: str = "",
    message: str = "",
) -> Tag:
    """
    Create an immutable tag.

    Raises :py:exc:`TagExistsError` if *name* is already used.
    """
    existing = conn.execute(
        "SELECT 1 FROM tags WHERE name = ?", (name,)
    ).fetchone()
    if existing:
        raise TagExistsError(f"Tag {name!r} already exists.")
    conn.execute(
        "INSERT INTO tags (name, target_hash, tagger, timestamp, message) "
        "VALUES (?, ?, ?, ?, ?)",
        (name, target_hash, tagger, timestamp, message),
    )
    conn.commit()
    return Tag(
        name=name,
        target_hash=target_hash,
        tagger=tagger,
        timestamp=timestamp,
        message=message,
    )


def list_tags(conn: sqlite3.Connection) -> list[Tag]:
    """Return all tags sorted by name."""
    rows = conn.execute(
        "SELECT name, target_hash, tagger, timestamp, message "
        "FROM tags ORDER BY name"
    ).fetchall()
    return [
        Tag(
            name=r["name"],
            target_hash=r["target_hash"],
            tagger=r["tagger"],
            timestamp=r["timestamp"],
            message=r["message"],
        )
        for r in rows
    ]
