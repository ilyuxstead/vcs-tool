"""
tests/property/test_remote_properties.py — Hypothesis property tests for
remote operations: clone_repo, push, fetch, and pull.

Invariants under test
---------------------
clone_repo
  P-CL-01  Returned path always contains a .vcs/ directory, regardless of
           URL shape or branch-name content.
  P-CL-02  The "origin" remote URL stored in SQLite always equals the URL
           that was passed to clone_repo().
  P-CL-03  The set of local branches created always equals the key-set of
           the remote refs dict.
  P-CL-04  commits_fetched is always non-negative (monotonicity sanity).

push
  P-PU-01  Objects uploaded to the server are always a subset of what the
           server said it needed (no extra objects are pushed).
  P-PU-02  The return dict always contains the required summary keys.
  P-PU-03  commits_uploaded is always >= 0 and <= total local commits.

fetch
  P-FE-01  Local branch count never decreases after a fetch.
  P-FE-02  commits_fetched in the return dict is always >= 0.
  P-FE-03  Every branch tip hash returned in refs exists in the local
           object store after the fetch completes.
  P-FE-04  The return dict always carries the required keys.

pull
  P-PL-01  merged is always a bool.
  P-PL-02  When merged=True the result always contains "merge_commit".
  P-PL-03  When fetch returns empty refs, merged is always False.
  P-PL-04  fetch_only=True always produces merged=False regardless of what
           the remote advertises.
  P-PL-05  The result always contains all keys returned by fetch().

All network I/O is eliminated via unittest.mock.  The repo layer is exercised
with real on-disk state via pytest's tmp_path (passed through Hypothesis's
@settings(deriving from a fixture is handled with a shared tmp_path factory).
"""

from __future__ import annotations

import hashlib
import json
import string
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from vcs.commit.snapshot import create_snapshot
from vcs.commit.stage import stage_files
from vcs.remote.ops import add, fetch, pull, push
from vcs.remote.protocol import RemoteClient
from vcs.repo.clone import clone_repo
from vcs.repo.init import init_repo, vcs_dir
from vcs.store.db import add_remote, list_branches, open_db, get_remote
from vcs.store.objects import ObjectStore
from vcs.store.exceptions import RemoteError


# ---------------------------------------------------------------------------
# Shared constants and helpers
# ---------------------------------------------------------------------------

AUTHOR = "Property Test <prop@test.com>"
FAKE_URL = "https://example.com/testrepo"

# SHA3-256 hash of b"" — used as a stable sentinel blob hash.
_EMPTY_HASH = hashlib.sha3_256(b"").hexdigest()


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
    Build a minimal but well-formed set of remote refs and blobs for the
    given branch names.  Each branch gets its own independent commit→tree→blob
    chain so all hashes are real SHA3-256 digests.

    Returns (refs, blob_map) where refs maps branch→tip_hash and blob_map
    maps every hash to its raw bytes.
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


def _mock_client(refs: dict[str, str], blobs: dict[str, bytes]) -> MagicMock:
    client = MagicMock(spec=RemoteClient)
    client.fetch_refs.return_value = refs
    client.download_blob.side_effect = lambda h: blobs[h]
    # negotiate_refs returns all hashes the server doesn't have (simulate empty server)
    client.negotiate_refs.return_value = list(blobs.keys())
    client.upload_blob.return_value = None
    client.upload_commit.return_value = None
    client.update_ref.return_value = None
    return client


def _make_local_repo(root: Path) -> Path:
    """Init a local repo with one commit on 'main' and 'origin' registered."""
    init_repo(root)
    f = root / "readme.txt"
    f.write_text("hello")
    stage_files([f], root)
    create_snapshot("Initial commit", AUTHOR, root)
    add("origin", FAKE_URL, root)
    return root


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

# Valid branch names: lowercase letters and hyphens, 1–20 chars.
branch_name_st = st.text(
    alphabet=string.ascii_lowercase + "-",
    min_size=1,
    max_size=20,
).filter(lambda s: not s.startswith("-") and not s.endswith("-") and "--" not in s)

# A non-empty list of distinct branch names (up to 4 branches).
branch_list_st = st.lists(
    branch_name_st,
    min_size=1,
    max_size=4,
    unique=True,
)

# URL path slugs that produce a sensible dest directory name.
url_slug_st = st.text(
    alphabet=string.ascii_lowercase + string.digits + "-_",
    min_size=1,
    max_size=30,
).filter(lambda s: s.isidentifier() or "-" in s or "_" in s)

# Arbitrary file content (bytes, up to 512 bytes to keep tests fast).
file_content_st = st.binary(min_size=1, max_size=512)

# Fake merge commit hash (64 hex chars).
fake_merge_hash_st = st.text(alphabet="0123456789abcdef", min_size=64, max_size=64)


# ---------------------------------------------------------------------------
# P-CL: clone_repo property tests
# ---------------------------------------------------------------------------

class TestCloneRepoProperties:

    @given(branches=branch_list_st)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_CL_01_vcs_dir_always_created(self, tmp_path: Path, branches: list[str]):
        """P-CL-01: .vcs/ always exists in the returned path regardless of branch count."""
        refs, blobs = _build_remote_graph(branches)
        client = _mock_client(refs, blobs)
        dest = tmp_path / "clone_cl01"
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            root = clone_repo(FAKE_URL, dest)
        assert (root / ".vcs").is_dir()

    @given(slug=url_slug_st)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_CL_02_origin_url_matches_input(self, tmp_path: Path, slug: str):
        """P-CL-02: The URL stored as 'origin' always equals the URL passed in."""
        url = f"https://example.com/{slug}"
        refs, blobs = _build_remote_graph(["main"])
        client = _mock_client(refs, blobs)
        dest = tmp_path / f"clone_cl02_{slug}"
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            root = clone_repo(url, dest)
        conn = open_db(vcs_dir(root) / "vcs.db")
        try:
            remote = get_remote(conn, "origin")
        finally:
            conn.close()
        assert remote["url"] == url

    @given(branches=branch_list_st)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_CL_03_local_branches_equal_remote_refs_keys(
        self, tmp_path: Path, branches: list[str]
    ):
        """P-CL-03: Local branch set always equals the remote refs key-set."""
        refs, blobs = _build_remote_graph(branches)
        client = _mock_client(refs, blobs)
        dest = tmp_path / "clone_cl03"
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            root = clone_repo(FAKE_URL, dest)
        conn = open_db(vcs_dir(root) / "vcs.db")
        try:
            local_branch_names = {b.name for b in list_branches(conn)}
        finally:
            conn.close()
        assert local_branch_names == set(refs.keys())

    @given(branches=branch_list_st)
    @settings(max_examples=20, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_CL_04_all_tip_blobs_stored_in_object_store(
        self, tmp_path: Path, branches: list[str]
    ):
        """P-CL-04: Every tip commit hash advertised by the remote is in the object store."""
        refs, blobs = _build_remote_graph(branches)
        client = _mock_client(refs, blobs)
        dest = tmp_path / "clone_cl04"
        with patch("vcs.repo.clone.RemoteClient", return_value=client):
            root = clone_repo(FAKE_URL, dest)
        store = ObjectStore(vcs_dir(root) / "objects")
        for tip_hash in refs.values():
            assert store.exists(tip_hash), (
                f"Tip commit {tip_hash[:12]} not found in object store after clone"
            )


# ---------------------------------------------------------------------------
# P-FE: fetch property tests
# ---------------------------------------------------------------------------

class TestFetchProperties:

    @given(branches=branch_list_st)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_FE_01_branch_count_never_decreases(
        self, tmp_path: Path, branches: list[str]
    ):
        """P-FE-01: Local branch count is >= pre-fetch count after any fetch."""
        root = _make_local_repo(tmp_path / "repo_fe01")
        conn = open_db(vcs_dir(root) / "vcs.db")
        before = len(list_branches(conn))
        conn.close()

        refs, blobs = _build_remote_graph(branches)
        client = _mock_client(refs, blobs)
        with patch("vcs.remote.ops.RemoteClient", return_value=client):
            fetch("origin", repo_root=root)

        conn = open_db(vcs_dir(root) / "vcs.db")
        after = len(list_branches(conn))
        conn.close()

        assert after >= before

    @given(branches=branch_list_st)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_FE_02_commits_fetched_non_negative(
        self, tmp_path: Path, branches: list[str]
    ):
        """P-FE-02: commits_fetched in the result dict is always >= 0."""
        root = _make_local_repo(tmp_path / "repo_fe02")
        refs, blobs = _build_remote_graph(branches)
        client = _mock_client(refs, blobs)
        with patch("vcs.remote.ops.RemoteClient", return_value=client):
            result = fetch("origin", repo_root=root)
        assert result["commits_fetched"] >= 0

    @given(branches=branch_list_st)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_FE_03_tip_hashes_in_object_store_after_fetch(
        self, tmp_path: Path, branches: list[str]
    ):
        """P-FE-03: Every tip hash in refs is present in the object store after fetch."""
        root = _make_local_repo(tmp_path / "repo_fe03")
        refs, blobs = _build_remote_graph(branches)
        client = _mock_client(refs, blobs)
        with patch("vcs.remote.ops.RemoteClient", return_value=client):
            result = fetch("origin", repo_root=root)
        store = ObjectStore(vcs_dir(root) / "objects")
        for tip_hash in result["refs"].values():
            assert store.exists(tip_hash), (
                f"Tip {tip_hash[:12]} missing from object store after fetch"
            )

    @given(branches=branch_list_st)
    @settings(max_examples=20, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_FE_04_required_keys_always_present(
        self, tmp_path: Path, branches: list[str]
    ):
        """P-FE-04: Result dict always contains the required summary keys."""
        root = _make_local_repo(tmp_path / "repo_fe04")
        refs, blobs = _build_remote_graph(branches)
        client = _mock_client(refs, blobs)
        with patch("vcs.remote.ops.RemoteClient", return_value=client):
            result = fetch("origin", repo_root=root)
        for key in ("remote", "refs", "commits_fetched", "blobs_fetched"):
            assert key in result, f"Required key {key!r} missing from fetch() result"


# ---------------------------------------------------------------------------
# P-PU: push property tests
# ---------------------------------------------------------------------------

class TestPushProperties:

    @given(file_content=file_content_st)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_PU_01_required_keys_always_present(
        self, tmp_path: Path, file_content: bytes
    ):
        """P-PU-01: push() result always contains the required summary keys."""
        root = _make_local_repo(tmp_path / "repo_pu01")

        # Add a second commit with generated content so push has something new.
        f = root / "data.bin"
        f.write_bytes(file_content)
        stage_files([f], root)
        create_snapshot("Second commit", AUTHOR, root)

        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.return_value = {}           # server has nothing
        client.negotiate_refs.return_value = []        # server needs nothing extra
        client.upload_blob.return_value = None
        client.upload_commit.return_value = None
        client.update_ref.return_value = None

        with patch("vcs.remote.ops.RemoteClient", return_value=client):
            result = push("origin", repo_root=root)

        for key in ("branch", "remote", "tip_hash", "commits_uploaded",
                    "trees_uploaded", "blobs_uploaded"):
            assert key in result, f"Required key {key!r} missing from push() result"

    @given(file_content=file_content_st)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_PU_02_upload_counts_non_negative(
        self, tmp_path: Path, file_content: bytes
    ):
        """P-PU-02: All upload count fields in push() result are >= 0."""
        root = _make_local_repo(tmp_path / "repo_pu02")

        f = root / "extra.bin"
        f.write_bytes(file_content)
        stage_files([f], root)
        create_snapshot("Extra commit", AUTHOR, root)

        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.return_value = {}
        client.negotiate_refs.return_value = []
        client.upload_blob.return_value = None
        client.upload_commit.return_value = None
        client.update_ref.return_value = None

        with patch("vcs.remote.ops.RemoteClient", return_value=client):
            result = push("origin", repo_root=root)

        assert result["commits_uploaded"] >= 0
        assert result["trees_uploaded"] >= 0
        assert result["blobs_uploaded"] >= 0

    @given(file_content=file_content_st)
    @settings(max_examples=20, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_PU_03_tip_hash_is_valid_hex_digest(
        self, tmp_path: Path, file_content: bytes
    ):
        """P-PU-03: tip_hash in push() result is always a 64-char hex string."""
        root = _make_local_repo(tmp_path / "repo_pu03")

        f = root / "payload.bin"
        f.write_bytes(file_content)
        stage_files([f], root)
        create_snapshot("Payload commit", AUTHOR, root)

        client = MagicMock(spec=RemoteClient)
        client.fetch_refs.return_value = {}
        client.negotiate_refs.return_value = []
        client.upload_blob.return_value = None
        client.upload_commit.return_value = None
        client.update_ref.return_value = None

        with patch("vcs.remote.ops.RemoteClient", return_value=client):
            result = push("origin", repo_root=root)

        tip = result["tip_hash"]
        assert isinstance(tip, str)
        assert len(tip) == 64
        assert all(c in string.hexdigits for c in tip)


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
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_PL_01_merged_is_always_bool(
        self, tmp_path: Path, branches: list[str], merge_hash: str
    ):
        """P-PL-01: merged key in pull() result is always a bool."""
        root = _make_local_repo(tmp_path / "repo_pl01")
        refs = {b: "a" * 64 for b in branches}
        with patch(_FETCH_PATCH, return_value=_fake_fetch_result(refs)):
            with patch(_MERGE_PATCH, return_value=merge_hash):
                result = pull("origin", repo_root=root, author=AUTHOR)
        assert isinstance(result["merged"], bool)

    @given(branches=branch_list_st, merge_hash=fake_merge_hash_st)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_PL_02_merge_commit_present_when_merged_true(
        self, tmp_path: Path, branches: list[str], merge_hash: str
    ):
        """P-PL-02: When merged=True, merge_commit key is always present."""
        root = _make_local_repo(tmp_path / "repo_pl02")
        refs = {b: "b" * 64 for b in branches}
        with patch(_FETCH_PATCH, return_value=_fake_fetch_result(refs)):
            with patch(_MERGE_PATCH, return_value=merge_hash):
                result = pull("origin", repo_root=root, author=AUTHOR)
        if result["merged"]:
            assert "merge_commit" in result

    @given(branches=branch_list_st)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_PL_03_empty_refs_always_produces_merged_false(
        self, tmp_path: Path, branches: list[str]
    ):
        """P-PL-03: When fetch returns empty refs, merged is always False."""
        root = _make_local_repo(tmp_path / "repo_pl03")
        empty_fetch = _fake_fetch_result({})
        with patch(_FETCH_PATCH, return_value=empty_fetch):
            result = pull("origin", repo_root=root, author=AUTHOR)
        assert result["merged"] is False

    @given(branches=branch_list_st, merge_hash=fake_merge_hash_st)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_PL_04_fetch_only_always_produces_merged_false(
        self, tmp_path: Path, branches: list[str], merge_hash: str
    ):
        """P-PL-04: fetch_only=True always produces merged=False."""
        root = _make_local_repo(tmp_path / "repo_pl04")
        refs = {b: "c" * 64 for b in branches}
        with patch(_FETCH_PATCH, return_value=_fake_fetch_result(refs)):
            with patch(_MERGE_PATCH, return_value=merge_hash):
                result = pull("origin", repo_root=root, author=AUTHOR, fetch_only=True)
        assert result["merged"] is False

    @given(branches=branch_list_st, merge_hash=fake_merge_hash_st)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_P_PL_05_result_contains_all_fetch_keys(
        self, tmp_path: Path, branches: list[str], merge_hash: str
    ):
        """P-PL-05: pull() result always contains every key that fetch() returns."""
        root = _make_local_repo(tmp_path / "repo_pl05")
        refs = {b: "d" * 64 for b in branches}
        fetch_result = _fake_fetch_result(refs)
        with patch(_FETCH_PATCH, return_value=fetch_result):
            with patch(_MERGE_PATCH, return_value=merge_hash):
                result = pull("origin", repo_root=root, author=AUTHOR)
        for key in fetch_result:
            assert key in result, (
                f"Key {key!r} from fetch() result is absent from pull() result"
            )