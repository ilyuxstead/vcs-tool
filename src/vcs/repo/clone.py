"""
vcs.repo.clone — clone a remote repository into a local directory.

Single responsibility: given a URL, destination path, and optional depth,
produce a fully functional local repository that mirrors the remote state.

Algorithm
---------
1.  Probe the remote via ``RemoteClient.fetch_refs()`` to obtain branch tips.
2.  ``init_repo()`` the destination directory.
3.  Register the origin remote in the new repo's database.
4.  Download every blob the remote advertises (respecting ``depth`` when given).
5.  For each branch tip, download the commit metadata and reconstruct the tree.
6.  Write all branch pointers locally; point HEAD at the default branch.
7.  Reconstruct the working tree for HEAD.

Raises
------
CloneError
    Any unrecoverable failure during cloning (wraps the underlying cause).
RepositoryExistsError
    Raised by ``init_repo`` if the destination already contains a ``.vcs/``
    directory — bubbles up unchanged so callers can surface it cleanly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from vcs.repo.init import DEFAULT_BRANCH, init_repo, vcs_dir, write_head
from vcs.store.db import (
    add_remote,
    create_branch,
    insert_commit,
    insert_tree,
    open_db,
    update_branch_tip,
)
from vcs.store.exceptions import CloneError, RemoteError
from vcs.store.models import Commit, Tree, TreeEntry
from vcs.store.objects import ObjectStore
from vcs.remote.protocol import RemoteClient


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def clone_repo(
    url: str,
    dest: Path | None = None,
    *,
    depth: int | None = None,
) -> Path:
    """
    Clone the repository at *url* into *dest*.

    Parameters
    ----------
    url:
        HTTP(S) URL of the remote VCS server (e.g. ``https://example.com/myrepo``).
    dest:
        Local destination directory.  Defaults to the last path component of
        *url* with ``.vcs`` stripped (mirrors ``git clone`` behaviour).
        Created if it does not exist.
    depth:
        Shallow-clone depth.  ``None`` (default) fetches the full history.
        When specified only the most recent *depth* commits per branch are
        requested from the remote.  The remote must support the depth hint;
        unsupported remotes silently ignore it and return full history.

    Returns
    -------
    Path
        The root of the newly created local repository.

    Raises
    ------
    CloneError
        If the remote is unreachable, returns no refs, or the download fails.
    RepositoryExistsError
        If *dest* already contains a ``.vcs/`` directory.
    """
    dest = _resolve_dest(url, dest)

    client = RemoteClient(url)

    # ------------------------------------------------------------------ #
    # 1. Discover refs                                                     #
    # ------------------------------------------------------------------ #
    try:
        remote_refs: dict[str, str] = client.fetch_refs()
    except RemoteError as exc:
        raise CloneError(f"Cannot reach remote {url!r}: {exc}") from exc

    if not remote_refs:
        # An empty repository is valid — init locally and wire remote.
        dot_vcs = init_repo(dest)
        conn = open_db(dot_vcs / "vcs.db")
        try:
            add_remote(conn, "origin", url)
        finally:
            conn.close()
        return dest

    # ------------------------------------------------------------------ #
    # 2. Initialise local repository                                       #
    # ------------------------------------------------------------------ #
    dot_vcs = init_repo(dest)
    store = ObjectStore(dot_vcs / "objects")
    conn = open_db(dot_vcs / "vcs.db")

    try:
        # ------------------------------------------------------------------ #
        # 3. Register origin remote                                            #
        # ------------------------------------------------------------------ #
        add_remote(conn, "origin", url)

        # ------------------------------------------------------------------ #
        # 4 & 5. Download objects + reconstruct commit / tree metadata         #
        # ------------------------------------------------------------------ #
        blobs_downloaded = 0
        commits_downloaded = 0

        for branch_name, tip_hash in remote_refs.items():
            commit_chain = _fetch_commit_chain(client, tip_hash, depth=depth)

            for commit_data in commit_chain:
                commits_downloaded += 1

                # Store commit metadata
                commit = _commit_from_dict(commit_data)
                insert_commit(conn, commit)

                # Fetch tree and all its blobs
                try:
                    tree_data = client.download_blob(commit.tree_hash)
                except RemoteError as exc:
                    raise CloneError(
                        f"Failed to download tree {commit.tree_hash[:12]} "
                        f"for commit {commit.hash[:12]}: {exc}"
                    ) from exc

                store.write(tree_data, warn_large=False)

                tree = _tree_from_blob(tree_data, commit.tree_hash)
                insert_tree(conn, tree)

                for entry in tree.entries:
                    if store.exists(entry.object_hash):
                        continue
                    try:
                        blob = client.download_blob(entry.object_hash)
                    except RemoteError as exc:
                        raise CloneError(
                            f"Failed to download blob {entry.object_hash[:12]} "
                            f"({entry.name}): {exc}"
                        ) from exc
                    store.write(blob)
                    blobs_downloaded += 1

        # ------------------------------------------------------------------ #
        # 6. Write branch pointers                                             #
        # ------------------------------------------------------------------ #
        default_branch: str = (
            DEFAULT_BRANCH if DEFAULT_BRANCH in remote_refs else next(iter(remote_refs))
        )

        for branch_name, tip_hash in remote_refs.items():
            create_branch(conn, branch_name, tip_hash)

        # ------------------------------------------------------------------ #
        # 7. Reconstruct working tree for HEAD branch                          #
        # ------------------------------------------------------------------ #
        head_tip = remote_refs[default_branch]
        _reconstruct_working_tree(dest, dot_vcs, store, conn, head_tip)

        # Point HEAD at the default branch
        write_head(dest, f"ref: refs/branches/{default_branch}")

    except CloneError:
        raise
    except Exception as exc:
        raise CloneError(f"Clone failed with unexpected error: {exc}") from exc
    finally:
        conn.close()

    return dest


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _resolve_dest(url: str, dest: Path | None) -> Path:
    """Derive the local destination path from the URL when not specified."""
    if dest is not None:
        return dest.resolve()
    # Strip trailing slashes, take last component, drop .vcs suffix
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    if slug.endswith(".vcs"):
        slug = slug[:-4]
    if not slug:
        slug = "cloned_repo"
    return Path.cwd() / slug


def _fetch_commit_chain(
    client: RemoteClient,
    tip_hash: str,
    *,
    depth: int | None,
) -> list[dict]:
    """
    Walk the commit graph from *tip_hash* backwards through parent links.

    The remote is expected to serve commit metadata via ``download_blob``.
    Stops after *depth* commits when shallow cloning is requested.

    Returns a list of commit dicts in traversal order (tip → root).
    """
    visited: set[str] = set()
    chain: list[dict] = []
    queue: list[str] = [tip_hash]

    while queue:
        if depth is not None and len(chain) >= depth:
            break

        current_hash = queue.pop(0)
        if current_hash in visited:
            continue
        visited.add(current_hash)

        try:
            raw = client.download_blob(current_hash)
        except RemoteError as exc:
            raise CloneError(
                f"Failed to download commit {current_hash[:12]}: {exc}"
            ) from exc

        try:
            commit_data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise CloneError(
                f"Corrupt commit object {current_hash[:12]}: {exc}"
            ) from exc

        if commit_data.get("type") != "commit":
            raise CloneError(
                f"Expected commit object at {current_hash[:12]}, "
                f"got {commit_data.get('type')!r}."
            )

        chain.append(commit_data)

        for parent_hash in commit_data.get("parent_hashes", []):
            if parent_hash not in visited:
                queue.append(parent_hash)

    return chain


def _commit_from_dict(data: dict) -> Commit:
    """Deserialise a commit dict downloaded from the remote."""
    try:
        return Commit(
            hash=data["hash"],
            tree_hash=data["tree_hash"],
            parent_hashes=tuple(data.get("parent_hashes", [])),
            author=data.get("author", ""),
            timestamp=data.get("timestamp", ""),
            message=data.get("message", ""),
        )
    except KeyError as exc:
        raise CloneError(f"Malformed commit object — missing field {exc}.") from exc


def _tree_from_blob(raw: bytes, expected_hash: str) -> Tree:
    """Deserialise a tree blob downloaded from the remote."""
    try:
        data = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CloneError(f"Corrupt tree object {expected_hash[:12]}: {exc}") from exc

    if data.get("type") != "tree":
        raise CloneError(
            f"Expected tree object at {expected_hash[:12]}, "
            f"got {data.get('type')!r}."
        )

    entries = tuple(
        TreeEntry(
            mode=e.get("mode", "100644"),
            name=e["name"],
            object_hash=e["object_hash"],
        )
        for e in data.get("entries", [])
    )
    return Tree(hash=expected_hash, entries=entries)


def _reconstruct_working_tree(
    repo_root: Path,
    dot_vcs: Path,
    store: ObjectStore,
    conn,
    tip_hash: str,
) -> None:
    """
    Write all files from the HEAD commit's tree into the working directory.

    Skips files whose blobs are absent from the local object store
    (shouldn't happen after a successful fetch, but is defensive).
    """
    from vcs.store.db import get_commit, get_tree
    from vcs.repo.status import write_index

    commit = get_commit(conn, tip_hash)
    tree = get_tree(conn, commit.tree_hash)

    index: dict[str, str] = {}

    for entry in tree.entries:
        dest_path = repo_root / entry.name
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        if not store.exists(entry.object_hash):
            # Should never happen after a clean download; skip gracefully.
            continue

        dest_path.write_bytes(store.read(entry.object_hash))
        index[entry.name] = entry.object_hash

    # Sync the staging index so `repo.status` sees a clean state immediately.
    write_index(repo_root, index)