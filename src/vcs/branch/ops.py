"""
vcs.branch.ops — branch create, list, switch, delete operations.

Branches are mutable named pointers (FR-BR-01).  Switching a branch
updates HEAD and restores the working tree to match (FR-BR-02).
"""

from __future__ import annotations

import shutil
from pathlib import Path

from vcs.repo.init import (
    current_branch,
    find_repo_root,
    resolve_head_commit,
    vcs_dir,
    write_head,
)
from vcs.repo.status import write_index
from vcs.store.db import (
    branch_exists,
    create_branch,
    delete_branch,
    get_branch,
    get_commit,
    get_tree,
    list_branches,
    open_db,
)
from vcs.store.exceptions import (
    BranchExistsError,
    BranchNotFoundError,
    CommitNotFoundError,
)
from vcs.store.models import Branch
from vcs.store.objects import ObjectStore


def create(
    name: str,
    repo_root: Path | None = None,
    *,
    at_hash: str | None = None,
) -> Branch:
    """
    Create a new branch pointing to *at_hash* or the current HEAD commit.

    Raises :py:exc:`BranchExistsError` if the name is taken.
    Raises :py:exc:`CommitNotFoundError` if *at_hash* does not exist.
    """
    root = repo_root or find_repo_root()
    dot_vcs = vcs_dir(root)
    conn = open_db(dot_vcs / "vcs.db")

    try:
        tip = at_hash or resolve_head_commit(root)
        if tip is None:
            from vcs.store.exceptions import VCSError
            raise VCSError("Cannot create a branch in an empty repository (no commits yet).")
        # Validate the hash exists
        get_commit(conn, tip)
        return create_branch(conn, name, tip)
    finally:
        conn.close()


def list_all(repo_root: Path | None = None) -> list[Branch]:
    """Return all branches, sorted by name."""
    root = repo_root or find_repo_root()
    dot_vcs = vcs_dir(root)
    conn = open_db(dot_vcs / "vcs.db")
    try:
        return list_branches(conn)
    finally:
        conn.close()


def switch(name: str, repo_root: Path | None = None) -> None:
    """
    Switch the working tree to branch *name*.

    Updates HEAD, restores tracked files from the target branch's tree,
    and rewrites the staging index to match the new HEAD.

    Raises :py:exc:`BranchNotFoundError` if *name* does not exist.
    """
    root = repo_root or find_repo_root()
    dot_vcs = vcs_dir(root)
    conn = open_db(dot_vcs / "vcs.db")
    store = ObjectStore(dot_vcs / "objects")

    try:
        branch = get_branch(conn, name)

        # Restore working tree from the branch's tree
        commit = get_commit(conn, branch.tip_hash)
        tree = get_tree(conn, commit.tree_hash)

        # Write out each file from the tree
        new_index: dict[str, str] = {}
        for entry in tree.entries:
            dest = root / entry.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            if store.exists(entry.object_hash):
                dest.write_bytes(store.read(entry.object_hash))
            new_index[entry.name] = entry.object_hash

        # Update HEAD and index
        write_head(root, f"ref: refs/branches/{name}")
        write_index(root, new_index)

    finally:
        conn.close()


def delete(name: str, repo_root: Path | None = None) -> None:
    """
    Delete a branch pointer.  History is not affected.

    Raises :py:exc:`BranchNotFoundError` if *name* does not exist.
    """
    root = repo_root or find_repo_root()
    active = current_branch(root)
    if active == name:
        from vcs.store.exceptions import VCSError
        raise VCSError(f"Cannot delete the currently active branch {name!r}. Switch to another branch first.")

    dot_vcs = vcs_dir(root)
    conn = open_db(dot_vcs / "vcs.db")
    try:
        delete_branch(conn, name)
    finally:
        conn.close()


def merge_branch(
    source_name: str,
    author: str,
    message: str | None = None,
    repo_root: Path | None = None,
) -> str:
    """
    Merge *source_name* into the current branch using a three-way merge.

    Always produces a merge commit — no fast-forward (FR-BR-03).

    Returns the new merge commit hash.

    Raises :py:exc:`MergeConflictError` if conflicts are found.
    """
    import hashlib
    import json
    from datetime import datetime, timezone

    from vcs.branch.merge import three_way_merge, _find_lca
    from vcs.store.db import insert_commit, insert_tree, update_branch_tip
    from vcs.store.exceptions import MergeConflictError
    from vcs.store.models import Commit, Tree, TreeEntry

    root = repo_root or find_repo_root()
    dot_vcs = vcs_dir(root)
    conn = open_db(dot_vcs / "vcs.db")
    store = ObjectStore(dot_vcs / "objects")

    try:
        target_name = current_branch(root)
        if not target_name:
            from vcs.store.exceptions import VCSError
            raise VCSError("Cannot merge in detached HEAD state.")

        target_branch = get_branch(conn, target_name)
        source_branch = get_branch(conn, source_name)

        ours_hash = target_branch.tip_hash
        theirs_hash = source_branch.tip_hash

        # Find LCA
        lca_hash = _find_lca(conn, ours_hash, theirs_hash)

        # Resolve tree blobs
        def _tree_blobs(commit_hash: str) -> dict[str, bytes]:
            c = get_commit(conn, commit_hash)
            t = get_tree(conn, c.tree_hash)
            return {
                e.name: store.read(e.object_hash)
                for e in t.entries
                if store.exists(e.object_hash)
            }

        base_blobs = _tree_blobs(lca_hash) if lca_hash else {}
        ours_blobs = _tree_blobs(ours_hash)
        theirs_blobs = _tree_blobs(theirs_hash)

        result = three_way_merge(base_blobs, ours_blobs, theirs_blobs)

        if result.conflicts:
            # Write conflict markers to working tree so user can resolve
            for path, content in result.merged.items():
                dest = root / path
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(content)
            raise MergeConflictError(
                f"Merge conflict in {len(result.conflicts)} file(s): "
                + ", ".join(result.conflicts)
                + ". Resolve conflicts, then run 'vcs commit.snapshot'.",
                conflicted_files=result.conflicts,
            )

        # No conflicts — write merged tree, write working tree, create merge commit
        merged_entries = []
        new_index: dict[str, str] = {}
        for path, content in sorted(result.merged.items()):
            blob_hash = store.write(content)
            merged_entries.append(
                TreeEntry(mode="100644", name=path, object_hash=blob_hash)
            )
            dest = root / path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content)
            new_index[path] = blob_hash

        # Build tree
        tree_payload = json.dumps(
            {
                "type": "tree",
                "entries": [
                    {"mode": e.mode, "name": e.name, "object_hash": e.object_hash}
                    for e in sorted(merged_entries, key=lambda e: e.name)
                ],
            },
            sort_keys=True,
        ).encode()
        tree_hash = hashlib.sha3_256(tree_payload).hexdigest()
        tree = Tree(hash=tree_hash, entries=tuple(merged_entries))
        store.write(tree_payload, warn_large=False)
        insert_tree(conn, tree)

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        commit_msg = message or f"Merge branch '{source_name}' into '{target_name}'"
        commit_payload = json.dumps(
            {
                "type": "commit",
                "tree_hash": tree_hash,
                "parent_hashes": sorted([ours_hash, theirs_hash]),
                "author": author,
                "timestamp": ts,
                "message": commit_msg,
            },
            sort_keys=True,
        ).encode()
        commit_hash = hashlib.sha3_256(commit_payload).hexdigest()
        merge_commit = Commit(
            hash=commit_hash,
            tree_hash=tree_hash,
            parent_hashes=(ours_hash, theirs_hash),
            author=author,
            timestamp=ts,
            message=commit_msg,
        )
        store.write(commit_payload, warn_large=False)
        insert_commit(conn, merge_commit)
        update_branch_tip(conn, target_name, commit_hash)

        # Clear staging index — merged state is now HEAD
        write_index(root, {})

        return commit_hash

    finally:
        conn.close()
