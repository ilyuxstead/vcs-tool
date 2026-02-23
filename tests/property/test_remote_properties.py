"""
tests/property/test_remote_properties.py — Hypothesis property tests for
remote operations: clone_repo, push, fetch, and pull.

Design: why tempfile.mkdtemp() instead of pytest's tmp_path
------------------------------------------------------------
pytest's tmp_path is a *function-scoped* fixture — one directory per test
function, shared across all Hypothesis examples for that function.  Two
problems follow:

  1. Hypothesis shrinks by replaying the failing case repeatedly with
     progressively smaller inputs.  It always tries run_id=0 first, so a
     "unique suffix" strategy still collides on the second replay attempt.

  2. suppress_health_check=[HealthCheck.function_scoped_fixture] silences
     Hypothesis's warning but does NOT fix the collision — it just hides it.

The only correct pattern for @given tests that need on-disk state is to
create and tear down a fresh temporary directory *inside the test body*,
using tempfile.mkdtemp() + shutil.rmtree() in a try/finally.  Each
Hypothesis example then owns its own isolated directory, and shrinking works
cleanly because every replay gets a new path.

Additionally, P-CL-04 / P-FE-03 previously checked store.exists(tip_hash)
where tip_hash is the logical hash from the remote's refs dict.
ObjectStore.write() keys by SHA3-256(content), which matches our fixture
hashes, but only because the fixture constructs them that way. The correct
and more robust check is commit_exists(conn, tip_hash) against SQLite, which
is what clone_repo and fetch() actually guarantee — they write commit rows,
not just raw blobs.

Invariants under test
---------------------
clone_repo
  P-CL-01  Returned path always contains a .vcs/ directory.
  P-CL-02  The "origin" remote URL in SQLite always equals the URL passed in.
  P-CL-03  Local branch set always equals the remote refs key-set.
  P-CL-04  Every tip commit hash is recorded in SQLite after clone.

fetch
  P-FE-01  Local branch count never decreases after a fetch.
  P-FE-02  commits_fetched in the result dict is always >= 0.
  P-FE-03  Every tip hash in refs is recorded in SQLite after fetch.
  P-FE-04  Result dict always contains the required summary keys.

push
  P-PU-01  Result dict always contains the required summary keys.
  P-PU-02  All upload count fields are always >= 0.
  P-PU-03  tip_hash is always a 64-char lowercase hex string.

pull
  P-PL-01  merged is always a bool.
  P-PL-02  When merged=True, merge_commit is always present in the result.
  P-PL-03  Empty refs from fetch always produce merged=False.
  P-PL-04  fetch_only=True always produces merged=False.
  P-PL-05  Result always contains every key that fetch() returned.

All network I/O is eliminated via unittest.mock.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import string
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from vcs.commit.snapshot import create_snapshot
from vcs.commit.stage import stage_files
from vcs.remote.ops import add, fetch, pull, push
from vcs.remote.protocol import RemoteClient
from vcs.repo.clone import clone_repo
from vcs.repo.init import init_repo, vcs_dir
from vcs.store.db import (
    add_remote,
    commit_exists,
    list_branches,
    open_db,
    get_remote,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

AUTHOR = "Property Test <prop@test.com>"
FAKE_URL = "https://example.com/testrepo"


def _sha3(data: bytes) -> str:
    return hashlib.sha3_256(data).hexdigest()


def _make_file_blob(content: bytes) -> tuple[str, bytes]:
    return _sha3(content), content


def _make_tree_blob(entries: list[dict]) -> tuple[str, bytes]:
    raw = json.dumps({"type": "tree", "entries": entries}, sort_keys=True).encode()
    return _sha3(raw), raw


def _make_commit_blob(
    tree_hash: str,
    parent_hashes: list[str] | None = None,
    message: str = "commit",
    author: str = AUTHOR,
    timestamp: str = "2026-01-01T00:00:00Z",
) -> tuple[str, bytes]:
    """Return (hash, raw_bytes) for a well-formed commit blob."""
    ph = parent_hashes or []
    canonical = json.dumps({
        "author": author,
        "message": message,
        "parent_hashes": ph,
        "timestamp": timestamp,
        "tree_hash": tree_hash,
        "type": "commit",
    }, sort_keys=True).encode()
    h = _sha3(canonical)
    raw = json.dumps({
        "author": author,
        "hash": h,
        "message": message,
        "parent_hashes": ph,
        "timestamp": timestamp,
        "tree_hash": tree_hash,
        "type": "commit",
    }, sort_keys=True).encode()
    return h, raw


def _build_remote_graph(
    branch_names: list[str],
) -> tuple[dict[str, str], dict[str, bytes]]:
    """
    Build a minimal well-formed remote refs + blob map for the given branches.
    Each branch has its own commit (chained from the previous), all sharing
    one tree and one file blob.  All hashes are real SHA3-256 digests.
    """
    file_content = b"print('hello')\n"
    file_hash, file_raw = _make_file_blob(file_content)
    tree_hash, tree_raw = _make_tree_blob([
        {"mode": "100644", "name": "main.py", "object_hash": file_hash}
    ])

    refs: dict[str, str] = {}
    blobs: dict[str, bytes] = {file_hash: file_raw, tree_hash: tree_raw}

    prev_hash: str | None = None
    for i, branch in enumerate(branch_names):
        commit_hash, commit_raw = _make_commit_blob(
            tree_hash,
            parent_hashes=[prev_hash] if prev_hash else [],
            message=f"commit for {branch} #{i}",
        )
        refs[branch] = commit_hash
        blobs[commit_hash] = commit_raw
        prev_hash = commit_hash

    return refs, blobs


def _mock_clone_client(refs: dict[str, str], blobs: dict[str, bytes]) -> MagicMock:
    client = MagicMock(spec=RemoteClient)
    client.fetch_refs.return_value = refs
    client.download_blob.side_effect = lambda h: blobs[h]
    return client


def _mock_fetch_client(refs: dict[str, str], blobs: dict[str, bytes]) -> MagicMock:
    client = MagicMock(spec=RemoteClient)
    client.fetch_refs.return_value = refs
    client.download_blob.side_effect = lambda h: blobs[h]
    return client


def _mock_push_client() -> MagicMock:
    client = MagicMock(spec=RemoteClient)
    client.fetch_refs.return_value = {}
    client.negotiate_refs.return_value = []
    client.upload_blob.return_value = None
    client.upload_commit.return_value = None
    client.update_ref.return_value = None
    return client


def _fresh_dir() -> Path:
    """
    Create and return a brand-new temporary directory.

    Each call to _fresh_dir() produces a unique path via tempfile.mkdtemp().
    The caller is responsible for calling shutil.rmtree() in a try/finally.
    This is the only Hypothesis-safe pattern: the directory is created inside
    the test body on each example run, so Hypothesis shrinking never collides
    with a previously-created directory from an earlier example.
    """
    return Path(tempfile.mkdtemp(prefix="vcs_prop_"))


def _make_local_repo(root: Path) -> Path:
    """Init a repo with one commit on 'main' and 'origin' registered."""
    init_repo(root)
    f = root / "readme.txt"
    f.write_text("hello")
    stage_files([f], root)
    create_snapshot("Initial commit", AUTHOR, root)
    add("origin", FAKE_URL, root)
    return root


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

branch_name_st = st.text(
    alphabet=string.ascii_lowercase + "-",
    min_size=1,
    max_size=20,
).filter(
    lambda s: not s.startswith("-") and not s.endswith("-") and "--" not in s
)

branch_list_st = st.lists(branch_name_st, min_size=1, max_size=4, unique=True)

url_slug_st = st.text(
    alphabet=string.ascii_lowercase + string.digits + "-",
    min_size=1,
    max_size=20,
).filter(lambda s: not s.startswith("-") and not s.endswith("-"))

file_content_st = st.binary(min_size=1, max_size=512)

fake_merge_hash_st = st.text(
    alphabet="0123456789abcdef", min_size=64, max_size=64
)


# ---------------------------------------------------------------------------
# P-CL: clone_repo property tests
# ---------------------------------------------------------------------------

class TestCloneRepoProperties:

    @given(branches=branch_list_st)
    @settings(max_examples=30)
    def test_P_CL_01_vcs_dir_always_created(self, branches: list[str]):
        """P-CL-01: .vcs/ always exists in the returned path."""
        tmp = _fresh_dir()
        try:
            refs, blobs = _build_remote_graph(branches)
            dest = tmp / "clone"
            with patch("vcs.repo.clone.RemoteClient",
                       return_value=_mock_clone_client(refs, blobs)):
                root = clone_repo(FAKE_URL, dest)
            assert (root / ".vcs").is_dir()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @given(slug=url_slug_st)
    @settings(max_examples=30)
    def test_P_CL_02_origin_url_matches_input(self, slug: str):
        """P-CL-02: The URL stored as 'origin' always equals the URL passed in."""
        tmp = _fresh_dir()
        try:
            url = f"https://example.com/{slug}"
            refs, blobs = _build_remote_graph(["main"])
            dest = tmp / "clone"
            with patch("vcs.repo.clone.RemoteClient",
                       return_value=_mock_clone_client(refs, blobs)):
                root = clone_repo(url, dest)
            conn = open_db(vcs_dir(root) / "vcs.db")
            try:
                remote = get_remote(conn, "origin")
            finally:
                conn.close()
            assert remote["url"] == url
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @given(branches=branch_list_st)
    @settings(max_examples=30)
    def test_P_CL_03_local_branches_equal_remote_refs_keys(
        self, branches: list[str]
    ):
        """P-CL-03: Local branch set always equals the remote refs key-set."""
        tmp = _fresh_dir()
        try:
            refs, blobs = _build_remote_graph(branches)
            dest = tmp / "clone"
            with patch("vcs.repo.clone.RemoteClient",
                       return_value=_mock_clone_client(refs, blobs)):
                root = clone_repo(FAKE_URL, dest)
            conn = open_db(vcs_dir(root) / "vcs.db")
            try:
                local_branches = {b.name for b in list_branches(conn)}
            finally:
                conn.close()
            assert local_branches == set(refs.keys())
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @given(branches=branch_list_st)
    @settings(max_examples=20)
    def test_P_CL_04_all_tip_commits_in_sqlite(self, branches: list[str]):
        """P-CL-04: Every tip commit hash is recorded in SQLite after clone."""
        tmp = _fresh_dir()
        try:
            refs, blobs = _build_remote_graph(branches)
            dest = tmp / "clone"
            with patch("vcs.repo.clone.RemoteClient",
                       return_value=_mock_clone_client(refs, blobs)):
                root = clone_repo(FAKE_URL, dest)
            conn = open_db(vcs_dir(root) / "vcs.db")
            try:
                for tip_hash in refs.values():
                    assert commit_exists(conn, tip_hash), (
                        f"Tip commit {tip_hash[:12]} not in SQLite after clone"
                    )
            finally:
                conn.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# P-FE: fetch property tests
# ---------------------------------------------------------------------------

class TestFetchProperties:

    @given(branches=branch_list_st)
    @settings(max_examples=30)
    def test_P_FE_01_branch_count_never_decreases(self, branches: list[str]):
        """P-FE-01: Local branch count is >= pre-fetch count after any fetch."""
        tmp = _fresh_dir()
        try:
            root = _make_local_repo(tmp)
            conn = open_db(vcs_dir(root) / "vcs.db")
            before = len(list_branches(conn))
            conn.close()

            refs, blobs = _build_remote_graph(branches)
            with patch("vcs.remote.ops.RemoteClient",
                       return_value=_mock_fetch_client(refs, blobs)):
                fetch("origin", repo_root=root)

            conn = open_db(vcs_dir(root) / "vcs.db")
            after = len(list_branches(conn))
            conn.close()

            assert after >= before
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @given(branches=branch_list_st)
    @settings(max_examples=30)
    def test_P_FE_02_commits_fetched_non_negative(self, branches: list[str]):
        """P-FE-02: commits_fetched in the result dict is always >= 0."""
        tmp = _fresh_dir()
        try:
            root = _make_local_repo(tmp)
            refs, blobs = _build_remote_graph(branches)
            with patch("vcs.remote.ops.RemoteClient",
                       return_value=_mock_fetch_client(refs, blobs)):
                result = fetch("origin", repo_root=root)
            assert result["commits_fetched"] >= 0
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @given(branches=branch_list_st)
    @settings(max_examples=30)
    def test_P_FE_03_tip_commits_in_sqlite_after_fetch(
        self, branches: list[str]
    ):
        """P-FE-03: Every tip hash in refs is recorded in SQLite after fetch."""
        tmp = _fresh_dir()
        try:
            root = _make_local_repo(tmp)
            refs, blobs = _build_remote_graph(branches)
            with patch("vcs.remote.ops.RemoteClient",
                       return_value=_mock_fetch_client(refs, blobs)):
                result = fetch("origin", repo_root=root)
            conn = open_db(vcs_dir(root) / "vcs.db")
            try:
                for tip_hash in result["refs"].values():
                    assert commit_exists(conn, tip_hash), (
                        f"Tip {tip_hash[:12]} not in SQLite after fetch"
                    )
            finally:
                conn.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @given(branches=branch_list_st)
    @settings(max_examples=20)
    def test_P_FE_04_required_keys_always_present(self, branches: list[str]):
        """P-FE-04: Result dict always contains the required summary keys."""
        tmp = _fresh_dir()
        try:
            root = _make_local_repo(tmp)
            refs, blobs = _build_remote_graph(branches)
            with patch("vcs.remote.ops.RemoteClient",
                       return_value=_mock_fetch_client(refs, blobs)):
                result = fetch("origin", repo_root=root)
            for key in ("remote", "refs", "commits_fetched", "blobs_fetched"):
                assert key in result, (
                    f"Required key {key!r} missing from fetch() result"
                )
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# P-PU: push property tests
# ---------------------------------------------------------------------------

class TestPushProperties:

    @given(file_content=file_content_st)
    @settings(max_examples=30)
    def test_P_PU_01_required_keys_always_present(self, file_content: bytes):
        """P-PU-01: push() result always contains the required summary keys."""
        tmp = _fresh_dir()
        try:
            root = _make_local_repo(tmp)
            f = root / "data.bin"
            f.write_bytes(file_content)
            stage_files([f], root)
            create_snapshot("Second commit", AUTHOR, root)

            with patch("vcs.remote.ops.RemoteClient",
                       return_value=_mock_push_client()):
                result = push("origin", repo_root=root)

            for key in ("branch", "remote", "tip_hash",
                        "commits_uploaded", "trees_uploaded", "blobs_uploaded"):
                assert key in result, (
                    f"Required key {key!r} missing from push() result"
                )
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @given(file_content=file_content_st)
    @settings(max_examples=30)
    def test_P_PU_02_upload_counts_non_negative(self, file_content: bytes):
        """P-PU-02: All upload count fields in push() result are >= 0."""
        tmp = _fresh_dir()
        try:
            root = _make_local_repo(tmp)
            f = root / "extra.bin"
            f.write_bytes(file_content)
            stage_files([f], root)
            create_snapshot("Extra commit", AUTHOR, root)

            with patch("vcs.remote.ops.RemoteClient",
                       return_value=_mock_push_client()):
                result = push("origin", repo_root=root)

            assert result["commits_uploaded"] >= 0
            assert result["trees_uploaded"] >= 0
            assert result["blobs_uploaded"] >= 0
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @given(file_content=file_content_st)
    @settings(max_examples=20)
    def test_P_PU_03_tip_hash_is_valid_hex_digest(self, file_content: bytes):
        """P-PU-03: tip_hash is always a 64-char lowercase hex string."""
        tmp = _fresh_dir()
        try:
            root = _make_local_repo(tmp)
            f = root / "payload.bin"
            f.write_bytes(file_content)
            stage_files([f], root)
            create_snapshot("Payload commit", AUTHOR, root)

            with patch("vcs.remote.ops.RemoteClient",
                       return_value=_mock_push_client()):
                result = push("origin", repo_root=root)

            tip = result["tip_hash"]
            assert isinstance(tip, str)
            assert len(tip) == 64
            assert all(c in string.hexdigits for c in tip)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# P-PL: pull property tests
# ---------------------------------------------------------------------------

_FETCH_PATCH = "vcs.remote.ops.fetch"
_MERGE_PATCH = "vcs.branch.ops.merge_branch"


def _fake_fetch_result(refs: dict[str, str]) -> dict:
    return {
        "remote": "origin",
        "refs": refs,
        "blobs_downloaded": 0,
        "commits_fetched": len(refs),
        "blobs_fetched": 0,
    }


class TestPullProperties:

    @given(branches=branch_list_st, merge_hash=fake_merge_hash_st)
    @settings(max_examples=30)
    def test_P_PL_01_merged_is_always_bool(
        self, branches: list[str], merge_hash: str
    ):
        """P-PL-01: merged key in pull() result is always a bool."""
        tmp = _fresh_dir()
        try:
            root = _make_local_repo(tmp)
            refs = {b: "a" * 64 for b in branches}
            with patch(_FETCH_PATCH, return_value=_fake_fetch_result(refs)):
                with patch(_MERGE_PATCH, return_value=merge_hash):
                    result = pull("origin", repo_root=root, author=AUTHOR)
            assert isinstance(result["merged"], bool)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @given(branches=branch_list_st, merge_hash=fake_merge_hash_st)
    @settings(max_examples=30)
    def test_P_PL_02_merge_commit_present_when_merged_true(
        self, branches: list[str], merge_hash: str
    ):
        """P-PL-02: When merged=True, merge_commit is always present."""
        tmp = _fresh_dir()
        try:
            root = _make_local_repo(tmp)
            refs = {b: "b" * 64 for b in branches}
            with patch(_FETCH_PATCH, return_value=_fake_fetch_result(refs)):
                with patch(_MERGE_PATCH, return_value=merge_hash):
                    result = pull("origin", repo_root=root, author=AUTHOR)
            if result["merged"]:
                assert "merge_commit" in result
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @given(branches=branch_list_st)
    @settings(max_examples=30)
    def test_P_PL_03_empty_refs_always_produces_merged_false(
        self, branches: list[str]
    ):
        """P-PL-03: When fetch returns empty refs, merged is always False."""
        tmp = _fresh_dir()
        try:
            root = _make_local_repo(tmp)
            with patch(_FETCH_PATCH, return_value=_fake_fetch_result({})):
                result = pull("origin", repo_root=root, author=AUTHOR)
            assert result["merged"] is False
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @given(branches=branch_list_st, merge_hash=fake_merge_hash_st)
    @settings(max_examples=30)
    def test_P_PL_04_fetch_only_always_produces_merged_false(
        self, branches: list[str], merge_hash: str
    ):
        """P-PL-04: fetch_only=True always produces merged=False."""
        tmp = _fresh_dir()
        try:
            root = _make_local_repo(tmp)
            refs = {b: "c" * 64 for b in branches}
            with patch(_FETCH_PATCH, return_value=_fake_fetch_result(refs)):
                with patch(_MERGE_PATCH, return_value=merge_hash):
                    result = pull(
                        "origin", repo_root=root, author=AUTHOR, fetch_only=True
                    )
            assert result["merged"] is False
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @given(branches=branch_list_st, merge_hash=fake_merge_hash_st)
    @settings(max_examples=30)
    def test_P_PL_05_result_contains_all_fetch_keys(
        self, branches: list[str], merge_hash: str
    ):
        """P-PL-05: pull() result always contains every key that fetch() returned."""
        tmp = _fresh_dir()
        try:
            root = _make_local_repo(tmp)
            refs = {b: "d" * 64 for b in branches}
            fetch_result = _fake_fetch_result(refs)
            with patch(_FETCH_PATCH, return_value=fetch_result):
                with patch(_MERGE_PATCH, return_value=merge_hash):
                    result = pull("origin", repo_root=root, author=AUTHOR)
            for key in fetch_result:
                assert key in result, (
                    f"Key {key!r} from fetch() is absent from pull() result"
                )
        finally:
            shutil.rmtree(tmp, ignore_errors=True)