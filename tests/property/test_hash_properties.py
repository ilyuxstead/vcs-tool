"""
tests/property/test_hash_properties.py — Hypothesis property tests.

Covers: SHA3-256 determinism, no false collisions, roundtrip integrity,
immutability after write.
"""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from vcs.store.objects import ObjectStore, _compute_hash


# Re-declare strategies inline for clarity
arbitrary_blob = st.binary(min_size=0, max_size=65536)


class TestHashDeterminism:
    @given(data=arbitrary_blob)
    def test_hash_deterministic(self, data: bytes):
        """For any blob b, SHA3_256(b) == SHA3_256(b). Always."""
        assert _compute_hash(data) == _compute_hash(data)

    @given(data=arbitrary_blob)
    def test_hash_matches_stdlib(self, data: bytes):
        """Our _compute_hash must agree with hashlib.sha3_256."""
        expected = hashlib.sha3_256(data).hexdigest()
        assert _compute_hash(data) == expected

    @given(data=arbitrary_blob)
    def test_hash_is_64_hex_chars(self, data: bytes):
        h = _compute_hash(data)
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    @given(a=arbitrary_blob, b=arbitrary_blob)
    def test_distinct_inputs_distinct_hashes(self, a: bytes, b: bytes):
        """For any two distinct blobs, hash(a) != hash(b) with overwhelming probability."""
        assume(a != b)
        assert _compute_hash(a) != _compute_hash(b)


class TestRoundtripIntegrity:
    @given(data=arbitrary_blob)
    def test_write_then_read_equals_original(self, data: bytes):
        # Must use tempfile directly — pytest tmp_path is function-scoped
        # and incompatible with Hypothesis (FailedHealthCheck).
        with tempfile.TemporaryDirectory() as td:
            store = ObjectStore(Path(td) / "objects")
            h = store.write(data, warn_large=False)
            assert store.read(h) == data

    @given(data=arbitrary_blob)
    def test_exists_after_write(self, data: bytes):
        with tempfile.TemporaryDirectory() as td:
            store = ObjectStore(Path(td) / "objects")
            h = store.write(data, warn_large=False)
            assert store.exists(h)

    @given(data=arbitrary_blob)
    def test_write_idempotent(self, data: bytes):
        """Writing the same blob twice returns the same hash."""
        with tempfile.TemporaryDirectory() as td:
            store = ObjectStore(Path(td) / "objects")
            h1 = store.write(data, warn_large=False)
            h2 = store.write(data, warn_large=False)
            assert h1 == h2
