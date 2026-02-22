"""
tests/integration/test_large_blob.py — 10 MB threshold warning tests.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from vcs.commit.stage import stage_files
from vcs.store.objects import LARGE_BLOB_THRESHOLD_BYTES, ObjectStore


class TestLargeBlob:
    def test_small_blob_no_warning(self, tmp_path: Path, capsys):
        store = ObjectStore(tmp_path / "objects")
        store.write(b"small content")
        assert "Warning" not in capsys.readouterr().err

    def test_exactly_threshold_no_warning(self, tmp_path: Path, capsys):
        store = ObjectStore(tmp_path / "objects")
        store.write(b"x" * LARGE_BLOB_THRESHOLD_BYTES)
        assert "Warning" not in capsys.readouterr().err

    def test_one_over_threshold_warns(self, tmp_path: Path, capsys):
        store = ObjectStore(tmp_path / "objects")
        store.write(b"x" * (LARGE_BLOB_THRESHOLD_BYTES + 1))
        err = capsys.readouterr().err
        assert "Warning" in err
        assert "10 MB" in err
        assert "Phase 4" in err

    def test_large_blob_stored_and_retrievable(self, tmp_path: Path, capsys):
        store = ObjectStore(tmp_path / "objects")
        large = b"y" * (LARGE_BLOB_THRESHOLD_BYTES + 1024)
        h = store.write(large, warn_large=False)
        assert store.read(h) == large

    def test_large_file_in_staging(self, tmp_repo: Path, capsys):
        large_file = tmp_repo / "large.bin"
        large_file.write_bytes(b"z" * (LARGE_BLOB_THRESHOLD_BYTES + 1))
        stage_files([large_file], tmp_repo)
        err = capsys.readouterr().err
        assert "Warning" in err
