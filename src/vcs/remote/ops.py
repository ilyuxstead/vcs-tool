"""
vcs.remote.ops — push, pull, fetch operations.

Push  → local commits → remote (six-step handshake).
Fetch → remote commits → local (inverse handshake).
Pull  → fetch + three-way merge.
"""

from __future__ import annotations

from pathlib import Path

from vcs.repo.init import current_branch, find_repo_root, resolve_head_commit, vcs_dir
from vcs.store.db import (
    add_remote,
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
from vcs.store.models import Commit
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


def fetch(
    remote_name: str = "origin",
    repo_root: Path | None = None,
) -> dict:
    """
    Download all objects from the remote that we don't have locally.

    Does NOT merge — use pull() for fetch + merge.
    """
    root = repo_root or find_repo_root()
    dot_vcs = vcs_dir(root)
    conn = open_db(dot_vcs / "vcs.db")
    store = ObjectStore(dot_vcs / "objects")

    try:
        remote_info = get_remote(conn, remote_name)
        client = RemoteClient(remote_info["url"])

        remote_refs = client.fetch_refs()
        blobs_downloaded = 0

        for branch_name, tip_hash in remote_refs.items():
            if not store.exists(tip_hash):
                blob_data = client.download_blob(tip_hash)
                store.write(blob_data)
                blobs_downloaded += 1

        return {
            "remote": remote_name,
            "refs": remote_refs,
            "blobs_downloaded": blobs_downloaded,
        }

    finally:
        conn.close()


def pull(
    remote_name: str = "origin",
    branch_name: str | None = None,
    repo_root: Path | None = None,
    *,
    author: str = "",
    fetch_only: bool = False,
) -> dict:
    """
    Fetch from remote and perform a three-way merge.

    ``--fetch-only`` stops after fetch without merging.
    """
    fetch_result = fetch(remote_name, repo_root)

    if fetch_only:
        return {**fetch_result, "merged": False}

    # Merge not implemented without knowing remote branch tip locally
    # This is a placeholder — full pull merge requires commit graph
    # synchronisation which is wired in the integration tests.
    return {
        **fetch_result,
        "merged": False,
        "note": "Auto-merge after pull requires commit metadata sync (Phase 1 integration).",
    }
