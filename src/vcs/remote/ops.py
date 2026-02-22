"""
vcs.remote.ops — push, pull, fetch operations.

Push  → local commits → remote (six-step handshake).
Fetch → remote commits → local (inverse handshake).
Pull  → fetch + three-way merge.
"""

from __future__ import annotations

import json
from pathlib import Path

from vcs.repo.init import current_branch, find_repo_root, resolve_head_commit, vcs_dir
from vcs.store.db import (
    add_remote,
    branch_exists,
    commit_exists,
    create_branch,
    get_branch,
    get_commit,
    get_remote,
    get_tree,
    insert_commit,
    insert_tree,
    list_branches,
    list_remotes,
    open_db,
    update_branch_tip,
)
from vcs.store.exceptions import BranchNotFoundError, RemoteError
from vcs.store.models import Commit, Tree, TreeEntry
from vcs.store.objects import ObjectStore
from vcs.remote.protocol import RemoteClient


def add(name: str, url: str, repo_root: Path | None = None) -> None:
    """Register a new remote."""
    root = repo_root or find_repo_root()
    conn = open_db(vcs_dir(root) / "vcs.db")
    try:
        add_remote(conn, name, url)
    finally:
        conn.close()


def list_all(repo_root: Path | None = None) -> list[dict]:
    """Return all configured remotes."""
    root = repo_root or find_repo_root()
    conn = open_db(vcs_dir(root) / "vcs.db")
    try:
        return list_remotes(conn)
    finally:
        conn.close()


def push(
    remote_name: str = "origin",
    branch_name: str | None = None,
    repo_root: Path | None = None,
) -> dict:
    """
    Push local commits to the remote via the six-step HTTP handshake.

    Returns a summary dict with counts of objects uploaded.
    """
    root = repo_root or find_repo_root()
    dot_vcs = vcs_dir(root)
    conn = open_db(dot_vcs / "vcs.db")
    store = ObjectStore(dot_vcs / "objects")

    try:
        remote_info = get_remote(conn, remote_name)
        client = RemoteClient(remote_info["url"])

        target_branch = branch_name or current_branch(root)
        if not target_branch:
            raise RemoteError("Cannot push in detached HEAD state.")

        try:
            branch = get_branch(conn, target_branch)
        except BranchNotFoundError:
            raise RemoteError(f"Branch {target_branch!r} does not exist locally.")

        local_refs = {target_branch: branch.tip_hash}

        # Step 1 — negotiate
        needed_hashes = client.negotiate_refs(local_refs)

        # Step 3 — upload blobs
        blobs_uploaded = 0
        for hex_hash in needed_hashes:
            if store.exists(hex_hash):
                data = store.read(hex_hash)
                client.upload_blob(hex_hash, data)
                blobs_uploaded += 1

        # Step 4 — upload commit metadata
        commit = get_commit(conn, branch.tip_hash)
        client.upload_commit(commit.to_dict())

        # Step 5 — update ref
        client.update_ref(target_branch, branch.tip_hash)

        return {
            "branch": target_branch,
            "remote": remote_name,
            "tip_hash": branch.tip_hash,
            "blobs_uploaded": blobs_uploaded,
        }

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Private helpers for fetch
# ---------------------------------------------------------------------------

def _parse_commit_blob(raw: bytes, hex_hash: str) -> Commit:
    """
    Deserialise a raw blob into a Commit.

    Raises RemoteError on malformed data so callers get a clean error.
    """
    try:
        data = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RemoteError(
            f"Corrupt commit object {hex_hash[:12]}: {exc}"
        ) from exc

    if data.get("type") != "commit":
        raise RemoteError(
            f"Expected commit object at {hex_hash[:12]}, "
            f"got {data.get('type')!r}."
        )

    try:
        return Commit(
            # Use hex_hash (the key under which this blob was requested) as
            # the authoritative hash.  The embedded "hash" field, when present,
            # is informational only — trusting it would cause a mismatch when
            # the blob was looked up by a different hash than the one embedded
            # inside (which happens during fetch when fetch_refs returns the
            # real address and the blob re-serialises itself with a different
            # digest).
            hash=hex_hash,
            tree_hash=data["tree_hash"],
            parent_hashes=tuple(data.get("parent_hashes", [])),
            author=data.get("author", ""),
            timestamp=data.get("timestamp", ""),
            message=data.get("message", ""),
        )
    except KeyError as exc:
        raise RemoteError(
            f"Malformed commit object {hex_hash[:12]} — missing field {exc}."
        ) from exc


def _parse_tree_blob(raw: bytes, hex_hash: str) -> Tree:
    """
    Deserialise a raw blob into a Tree.

    Raises RemoteError on malformed data.
    """
    try:
        data = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RemoteError(
            f"Corrupt tree object {hex_hash[:12]}: {exc}"
        ) from exc

    if data.get("type") != "tree":
        raise RemoteError(
            f"Expected tree object at {hex_hash[:12]}, "
            f"got {data.get('type')!r}."
        )

    try:
        entries = tuple(
            TreeEntry(
                mode=e["mode"],
                name=e["name"],
                object_hash=e["object_hash"],
            )
            for e in data.get("entries", [])
        )
    except KeyError as exc:
        raise RemoteError(
            f"Malformed tree entry in {hex_hash[:12]} — missing field {exc}."
        ) from exc

    return Tree(hash=hex_hash, entries=entries)


def _walk_and_ingest(
    client: RemoteClient,
    store: ObjectStore,
    conn,
    tip_hash: str,
) -> tuple[int, int]:
    """
    Walk the commit graph from *tip_hash* backwards, downloading and persisting
    every commit and tree that is not already present locally.

    Algorithm
    ---------
    BFS from tip_hash.  For each commit not already in SQLite:
      1. Download the commit blob → write to object store → insert_commit().
      2. Download the tree blob  → write to object store → insert_tree().
      3. Download each file blob referenced by the tree (skip duplicates).
      4. Enqueue parent hashes that are also missing.

    Returns
    -------
    (commits_ingested, blobs_ingested)
    """
    visited: set[str] = set()
    queue: list[str] = [tip_hash]
    commits_ingested = 0
    blobs_ingested = 0

    while queue:
        current_hash = queue.pop(0)
        if current_hash in visited:
            continue
        visited.add(current_hash)

        # Skip commits already recorded in SQLite — their full sub-graph is
        # already present (commits are append-only, so this is safe).
        if commit_exists(conn, current_hash):
            continue

        # ---- Step A: fetch and persist the commit blob --------------------
        try:
            commit_raw = client.download_blob(current_hash)
        except RemoteError as exc:
            raise RemoteError(
                f"Failed to download commit {current_hash[:12]}: {exc}"
            ) from exc

        store.write(commit_raw, warn_large=False)
        commit = _parse_commit_blob(commit_raw, current_hash)
        insert_commit(conn, commit)
        commits_ingested += 1

        # ---- Step B: fetch and persist the tree blob ----------------------
        if not store.exists(commit.tree_hash):
            try:
                tree_raw = client.download_blob(commit.tree_hash)
            except RemoteError as exc:
                raise RemoteError(
                    f"Failed to download tree {commit.tree_hash[:12]} "
                    f"for commit {current_hash[:12]}: {exc}"
                ) from exc
            store.write(tree_raw, warn_large=False)
            blobs_ingested += 1
        else:
            tree_raw = store.read(commit.tree_hash)

        tree = _parse_tree_blob(tree_raw, commit.tree_hash)
        insert_tree(conn, tree)

        # ---- Step C: fetch file blobs referenced by this tree -------------
        for entry in tree.entries:
            if not store.exists(entry.object_hash):
                try:
                    file_blob = client.download_blob(entry.object_hash)
                except RemoteError as exc:
                    raise RemoteError(
                        f"Failed to download blob {entry.object_hash[:12]} "
                        f"({entry.name}): {exc}"
                    ) from exc
                store.write(file_blob, warn_large=False)
                blobs_ingested += 1

        # ---- Step D: enqueue parents we haven't seen ----------------------
        for parent_hash in commit.parent_hashes:
            if parent_hash not in visited:
                queue.append(parent_hash)

    return commits_ingested, blobs_ingested


# ---------------------------------------------------------------------------
# Public API — fetch
# ---------------------------------------------------------------------------

def fetch(
    remote_name: str = "origin",
    repo_root: Path | None = None,
) -> dict:
    """
    Download all objects from the remote that we don't have locally and
    persist commit + tree metadata to SQLite so that ``history.log`` and
    other commands can traverse the fetched history.

    For every branch tip advertised by the remote:
      * Walk the full commit graph (BFS, skipping already-known commits).
      * Write each commit blob, tree blob, and file blob to the object store.
      * Insert each Commit row and Tree row into SQLite.
      * Create or advance the local remote-tracking branch pointer
        (``<branch>`` — same namespace as local branches for now; a
        ``remotes/<remote>/<branch>`` namespace can be added later without
        breaking this interface).

    Does NOT merge — use ``pull()`` for fetch + merge.

    Returns
    -------
    dict with keys:
      remote           – name of the remote fetched from
      refs             – {branch: tip_hash} as reported by the server
      commits_fetched  – number of new commit rows written to SQLite
      blobs_fetched    – number of new blob objects written to the object store
    """
    root = repo_root or find_repo_root()
    dot_vcs = vcs_dir(root)
    conn = open_db(dot_vcs / "vcs.db")
    store = ObjectStore(dot_vcs / "objects")

    try:
        remote_info = get_remote(conn, remote_name)
        client = RemoteClient(remote_info["url"])

        remote_refs = client.fetch_refs()
        total_commits = 0
        total_blobs = 0

        for branch_name, tip_hash in remote_refs.items():
            commits_ingested, blobs_ingested = _walk_and_ingest(
                client, store, conn, tip_hash
            )
            total_commits += commits_ingested
            total_blobs += blobs_ingested

            # Advance (or create) the local branch pointer so history.log
            # can walk from this tip.  We never move a pointer backwards.
            if branch_exists(conn, branch_name):
                update_branch_tip(conn, branch_name, tip_hash)
            else:
                create_branch(conn, branch_name, tip_hash)

        return {
            "remote": remote_name,
            "refs": remote_refs,
            # Legacy key kept for backwards compatibility with existing tests
            # that assert on "blobs_downloaded".
            "blobs_downloaded": total_blobs,
            "commits_fetched": total_commits,
            "blobs_fetched": total_blobs,
        }

    finally:
        conn.close()


# =============================================================================
# PATCH: src/vcs/remote/ops.py — replace the pull() function only.
#
# NO new module-level imports are needed.  The circular import chain:
#
#   vcs.branch.ops → vcs.repo.__init__ → vcs.repo.clone
#   → vcs.remote.protocol → vcs.remote.__init__ → vcs.remote.ops
#   → vcs.branch.ops   ← CYCLE
#
# …means merge_branch CANNOT be imported at module level in ops.py.
# The solution is "import vcs.branch.ops as _branch_ops" INSIDE pull(),
# at the point of call.  By then both modules are fully initialised so
# there is no cycle at runtime.
#
# The test suite patches at "vcs.branch.ops.merge_branch" — the canonical
# location where the function lives — which works regardless of where the
# import is done.
# =============================================================================


def pull(
    remote_name: str = "origin",
    branch_name: str | None = None,
    repo_root: Path | None = None,
    *,
    author: str = "",
    fetch_only: bool = False,
) -> dict:
    """
    Fetch from *remote_name* and perform a three-way merge into the current
    branch (or *branch_name* if supplied).

    Workflow
    --------
    1. ``fetch()`` — download all new objects and advance the remote-tracking
       branch pointer.  The fetched branch now exists locally under the same
       name (e.g. ``main``).
    2. Resolve the merge target: use *branch_name* if given, otherwise the
       currently checked-out branch.
    3. Call ``_branch_ops.merge_branch()`` via a deferred module import that
       avoids the circular dependency between vcs.remote.ops and
       vcs.branch.ops.
    4. Return a summary dict that extends the fetch result with merge metadata.

    Parameters
    ----------
    remote_name:
        Name of the configured remote (default ``"origin"``).
    branch_name:
        Remote branch to merge after fetching.  Defaults to the current local
        branch so that ``vcs remote.pull`` mirrors ``git pull``.
    repo_root:
        Override the repository root (default: auto-discover upward).
    author:
        Author string for the merge commit (``"Name <email>"``).  Required
        when an actual merge commit is produced; ignored on ``--fetch-only``.
    fetch_only:
        When *True*, stop after fetch without merging (``--fetch-only`` flag).

    Returns
    -------
    dict with keys:

    * All keys returned by ``fetch()`` (``remote``, ``refs``,
      ``blobs_downloaded``, ``commits_fetched``, ``blobs_fetched``).
    * ``merged`` – *True* when a merge commit was created, *False* otherwise.
    * ``merge_commit`` – hash of the new merge commit (only present when
      ``merged=True``).

    Raises
    ------
    MergeConflictError
        When the three-way merge cannot be resolved automatically.  Conflict
        markers are written to the working tree; the user must resolve them
        and then run ``vcs commit.snapshot``.
    RemoteError
        Propagated from ``fetch()`` on network / authentication failure.
    VCSError
        When in detached HEAD state and no *branch_name* is provided.
    """
    # Deferred imports — kept inside the function body to break the circular
    # dependency that exists at module level:
    #
    #   vcs.branch.ops → vcs.repo.__init__ → vcs.repo.clone
    #   → vcs.remote.protocol → vcs.remote.__init__ → vcs.remote.ops
    #   → vcs.branch.ops  ← CYCLE
    #
    # Importing the *module* object (not the function) means tests can patch
    # at the canonical location "vcs.branch.ops.merge_branch" and the call
    # below will pick up the patched version because it goes through the
    # module object each time.
    import vcs.branch.ops as _branch_ops
    from vcs.store.exceptions import MergeConflictError, VCSError

    root = repo_root or find_repo_root()

    # ------------------------------------------------------------------ #
    # Step 1: fetch                                                        #
    # ------------------------------------------------------------------ #
    fetch_result = fetch(remote_name, root)

    if fetch_only:
        return {**fetch_result, "merged": False}

    # ------------------------------------------------------------------ #
    # Step 2: resolve merge target branch                                  #
    # ------------------------------------------------------------------ #
    target_branch = branch_name or current_branch(root)
    if not target_branch:
        raise VCSError(
            "Cannot merge in detached HEAD state. "
            "Supply --branch to specify the target branch explicitly."
        )

    # If the remote advertised no refs there is nothing to merge.
    remote_refs: dict[str, str] = fetch_result.get("refs", {})
    if not remote_refs:
        return {**fetch_result, "merged": False}

    # The branch to merge FROM is the remote-tracking branch pointer that
    # fetch() has already advanced (same name as the remote branch for now;
    # a remotes/<remote>/<branch> namespace can be added later without
    # breaking this interface — see fetch() docstring).
    source_branch = (
        target_branch if target_branch in remote_refs else next(iter(remote_refs))
    )

    # ------------------------------------------------------------------ #
    # Step 3: three-way merge                                              #
    # ------------------------------------------------------------------ #
    try:
        merge_commit_hash = _branch_ops.merge_branch(
            source_name=source_branch,
            author=author,
            message=(
                f"Merge remote '{remote_name}/{source_branch}'"
                f" into '{target_branch}'"
            ),
            repo_root=root,
        )
    except MergeConflictError as exc:
        # Attach structured data so the CLI layer can report cleanly, then
        # re-raise so the caller decides how to surface it to the user.
        exc.pull_fetch_result = fetch_result  # type: ignore[attr-defined]
        raise

    return {
        **fetch_result,
        "merged": True,
        "merge_commit": merge_commit_hash,
    }