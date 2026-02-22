"""
tests/unit/test_objects.py — ObjectStore unit tests.

Covers: write, read, exists, atomicity, corruption detection,
large-blob warning, and duplicate no-op behaviour.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import patch

import pytest

from vcs.store.exceptions import ObjectCorruptionError, ObjectNotFoundError
from vcs.store.objects import ObjectStore, LARGE_BLOB_THRESHOLD_BYTES, _compute_hash


class TestComputeHash:
    def test_deterministic(self):
        data = b"hello world"
        assert _compute_hash(data) == _compute_hash(data)

    def test_sha3_256(self):
        data = b"test"
        expected = hashlib.sha3_256(data).hexdigest()
        assert _compute_hash(data) == expected

    def test_empty_bytes(self):
        assert len(_compute_hash(b"")) == 64  # 256 bits / 4 bits per hex char

    def test_distinct_inputs_distinct_hashes(self):
        assert _compute_hash(b"a") != _compute_hash(b"b")


class TestObjectStoreWrite:
    def test_write_returns_hash(self, object_store: ObjectStore):
        data = b"hello vcs"
        h = object_store.write(data)
        assert h == _compute_hash(data)

    def test_write_creates_file(self, object_store: ObjectStore):
        data = b"file content"
        h = object_store.write(data)
        expected_path = object_store._dir / h[:2] / h[2:]
        assert expected_path.exists()

    def test_write_duplicate_is_noop(self, object_store: ObjectStore):
        data = b"duplicate"
        h1 = object_store.write(data)
        h2 = object_store.write(data)
        assert h1 == h2
        # File should exist exactly once
        path = object_store._dir / h1[:2] / h1[2:]
        assert path.exists()

    def test_write_large_blob_warns(self, object_store: ObjectStore, capsys):
        large = b"x" * (LARGE_BLOB_THRESHOLD_BYTES + 1)
        object_store.write(large)
        captured = capsys.readouterr()
        assert "Warning" in captured.err
        assert "10 MB" in captured.err
        assert "Phase 4" in captured.err

    def test_write_large_blob_no_warn_flag(self, object_store: ObjectStore, capsys):
        large = b"x" * (LARGE_BLOB_THRESHOLD_BYTES + 1)
        object_store.write(large, warn_large=False)
        captured = capsys.readouterr()
        assert "Warning" not in captured.err

    def test_write_at_threshold_no_warn(self, object_store: ObjectStore, capsys):
        at_threshold = b"x" * LARGE_BLOB_THRESHOLD_BYTES
        object_store.write(at_threshold)
        captured = capsys.readouterr()
        assert "Warning" not in captured.err


class TestObjectStoreRead:
    def test_roundtrip(self, object_store: ObjectStore):
        data = b"roundtrip data"
        h = object_store.write(data)
        assert object_store.read(h) == data

    def test_read_missing_raises(self, object_store: ObjectStore):
        with pytest.raises(ObjectNotFoundError):
            object_store.read("a" * 64)

    def test_read_corrupt_raises(self, object_store: ObjectStore):
        data = b"original"
        h = object_store.write(data)
        # Corrupt the stored file
        path = object_store._dir / h[:2] / h[2:]
        path.write_bytes(b"corrupted!!!")
        with pytest.raises(ObjectCorruptionError):
            object_store.read(h)

    def test_read_empty_blob(self, object_store: ObjectStore):
        h = object_store.write(b"")
        assert object_store.read(h) == b""


class TestObjectStoreExists:
    def test_exists_true(self, object_store: ObjectStore):
        h = object_store.write(b"exists")
        assert object_store.exists(h) is True

    def test_exists_false(self, object_store: ObjectStore):
        assert object_store.exists("f" * 64) is False


class TestObjectStoreAllHashes:
    def test_empty_store(self, object_store: ObjectStore):
        assert object_store.all_hashes() == []

    def test_multiple_objects(self, object_store: ObjectStore):
        hashes = [object_store.write(f"data{i}".encode()) for i in range(5)]
        all_h = object_store.all_hashes()
        assert set(all_h) == set(hashes)
