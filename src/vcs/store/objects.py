"""
vcs.store.objects — content-addressable blob storage.

Layout: .vcs/objects/<first-2-hex>/<remaining-hex>

All writes are atomic (write to a temp file, then os.rename).
Hash verification is performed on every read.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from .exceptions import ObjectCorruptionError, ObjectNotFoundError

# Blobs larger than this threshold trigger a stderr warning.
LARGE_BLOB_THRESHOLD_BYTES = 10 * 1024 * 1024  # 10 MB


def _compute_hash(data: bytes) -> str:
    """Return the SHA3-256 hex digest of *data*."""
    return hashlib.sha3_256(data).hexdigest()


def _object_path(objects_dir: Path, hex_hash: str) -> Path:
    """Derive the filesystem path for a given hash under *objects_dir*."""
    return objects_dir / hex_hash[:2] / hex_hash[2:]


class ObjectStore:
    """
    Low-level content-addressable storage for raw byte blobs.

    Callers are responsible for serialising higher-level objects (commits,
    trees) to bytes before calling :py:meth:`write`.
    """

    def __init__(self, objects_dir: Path) -> None:
        self._dir = objects_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write(self, data: bytes, *, warn_large: bool = True) -> str:
        """
        Write *data* to the object store.

        Returns the SHA3-256 hex hash.  If an object with the same hash
        already exists the call is a silent no-op (FR-OBJ-04).

        Parameters
        ----------
        data:
            Raw bytes to store.
        warn_large:
            If *True* (default) emit a warning to stderr for blobs that
            exceed :py:data:`LARGE_BLOB_THRESHOLD_BYTES` (FR-BLOB-02).
        """
        hex_hash = _compute_hash(data)
        dest = _object_path(self._dir, hex_hash)

        if dest.exists():
            return hex_hash  # Duplicate — silent no-op (FR-OBJ-04)

        if warn_large and len(data) > LARGE_BLOB_THRESHOLD_BYTES:
            import sys
            print(
                f"Warning: blob {hex_hash} exceeds 10 MB threshold. "
                "Chunked storage will be available in Phase 4.",
                file=sys.stderr,
            )

        dest.parent.mkdir(parents=True, exist_ok=True)
        self._atomic_write(dest, data)
        return hex_hash

    def read(self, hex_hash: str) -> bytes:
        """
        Read and verify a blob by its hash.

        Raises
        ------
        ObjectNotFoundError
            If no object with *hex_hash* exists.
        ObjectCorruptionError
            If the stored content does not match its filename hash (FR-IMM-04).
        """
        dest = _object_path(self._dir, hex_hash)
        if not dest.exists():
            raise ObjectNotFoundError(
                f"Object {hex_hash!r} not found in store.",
                error_code="OBJECT_NOT_FOUND",
            )

        data = dest.read_bytes()
        actual_hash = _compute_hash(data)
        if actual_hash != hex_hash:
            raise ObjectCorruptionError(
                f"Object {hex_hash!r} is corrupt: "
                f"expected hash {hex_hash!r}, got {actual_hash!r}.",
            )
        return data

    def exists(self, hex_hash: str) -> bool:
        """Return *True* if an object with *hex_hash* is present in the store."""
        return _object_path(self._dir, hex_hash).exists()

    def all_hashes(self) -> list[str]:
        """Return a list of all object hashes currently in the store."""
        hashes: list[str] = []
        for prefix_dir in self._dir.iterdir():
            if not prefix_dir.is_dir() or len(prefix_dir.name) != 2:
                continue
            for obj_file in prefix_dir.iterdir():
                if obj_file.is_file():
                    hashes.append(prefix_dir.name + obj_file.name)
        return hashes

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _atomic_write(dest: Path, data: bytes) -> None:
        """Write *data* to *dest* atomically via a temp file + rename."""
        fd, tmp_path = tempfile.mkstemp(dir=dest.parent, prefix=".tmp_")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
            os.replace(tmp_path, dest)
        except Exception:
            # Clean up temp file if anything goes wrong.
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
