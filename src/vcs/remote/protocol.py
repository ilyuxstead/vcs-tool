"""
vcs.remote.protocol — HTTP sync protocol implementation.

Wire format (Section 4.4 of the SRS):
  - Metadata: HTTP + JSON
  - Blob bodies: application/octet-stream (avoids ~33% base64 overhead)

Authentication: every request includes ``Authorization: Bearer <token>``
read from the ``VCS_AUTH_TOKEN`` environment variable.  The token is
NEVER logged or included in error messages (FR-REM-04, NFR-09).
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

from vcs.store.exceptions import AuthenticationError, RemoteError

_TOKEN_PLACEHOLDER = "<REDACTED>"


def _get_token() -> str:
    """
    Read the auth token from the environment.

    Raises :py:exc:`AuthenticationError` if the variable is not set.
    """
    token = os.environ.get("VCS_AUTH_TOKEN", "")
    if not token:
        raise AuthenticationError(
            "VCS_AUTH_TOKEN environment variable is not set. "
            "Set it to authenticate with the remote server."
        )
    return token


def _redact(text: str) -> str:
    """Replace any occurrence of the token with a placeholder in *text*."""
    token = os.environ.get("VCS_AUTH_TOKEN", "")
    if token and token in text:
        return text.replace(token, _TOKEN_PLACEHOLDER)
    return text


def _headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Build HTTP headers including the Bearer auth token."""
    token = _get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "vcs-tool/0.1.0",
    }
    if extra:
        headers.update(extra)
    return headers


def _request_json(
    method: str,
    url: str,
    payload: Any | None = None,
) -> Any:
    """
    Send a JSON request and return the parsed JSON response.

    Raises :py:exc:`RemoteError` on HTTP or network failures.
    Token is never logged.
    """
    body = json.dumps(payload).encode() if payload is not None else None
    headers = _headers({"Content-Type": "application/json", "Accept": "application/json"})

    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body_text = _redact(exc.read().decode(errors="replace"))
        raise RemoteError(
            f"HTTP {exc.code} from {_redact(url)}: {body_text}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RemoteError(f"Network error reaching {_redact(url)}: {exc.reason}") from exc


def _upload_blob(url: str, data: bytes) -> None:
    """
    Upload a single blob as application/octet-stream.
    """
    headers = _headers({"Content-Type": "application/octet-stream"})
    req = urllib.request.Request(url, data=data, headers=headers, method="PUT")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        raise RemoteError(f"Blob upload failed HTTP {exc.code}: {_redact(exc.read().decode())}") from exc
    except urllib.error.URLError as exc:
        raise RemoteError(f"Blob upload network error: {exc.reason}") from exc


def _download_blob(url: str) -> bytes:
    """Download a blob from *url*, returning raw bytes."""
    headers = _headers({"Accept": "application/octet-stream"})
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        raise RemoteError(f"Blob download failed HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise RemoteError(f"Blob download network error: {exc.reason}") from exc


# ---------------------------------------------------------------------------
# High-level push / pull primitives
# ---------------------------------------------------------------------------

class RemoteClient:
    """
    Stateless client for the VCS HTTP sync protocol.

    All methods map directly to the six-step handshake defined in
    Section 4.4 of the SRS.
    """

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    # Step 1 — Ref negotiation
    def negotiate_refs(self, local_refs: dict[str, str]) -> list[str]:
        """
        Send local branch tips; get back list of hashes the server needs.

        Parameters
        ----------
        local_refs:
            ``{"branch_name": "commit_hash", ...}``

        Returns
        -------
        list[str]
            Object hashes that the server does not have.
        """
        response = _request_json("POST", f"{self.base_url}/refs", {"refs": local_refs})
        return response.get("need", [])

    # Step 3 — Blob upload
    def upload_blob(self, hex_hash: str, data: bytes) -> None:
        """Upload a single blob object to the remote."""
        _upload_blob(f"{self.base_url}/objects/{hex_hash}", data)

    # Step 4 — Commit metadata upload
    def upload_commit(self, commit_data: dict) -> None:
        """Send commit + tree metadata after all blobs are confirmed."""
        _request_json("POST", f"{self.base_url}/commit", commit_data)

    # Step 5 — Ref update
    def update_ref(self, branch: str, commit_hash: str) -> None:
        """
        Request that the server advance a branch tip.

        The server rejects this if it has diverged (client must pull first).
        """
        response = _request_json(
            "POST",
            f"{self.base_url}/refs/update",
            {"branch": branch, "hash": commit_hash},
        )
        if not response.get("ok"):
            raise RemoteError(
                f"Server rejected ref update for branch {branch!r}. "
                "The remote has diverged — pull and merge first."
            )

    # Pull side — fetch ref list from server
    def fetch_refs(self) -> dict[str, str]:
        """Return the server's current branch tips."""
        return _request_json("GET", f"{self.base_url}/refs", None) or {}

    # Pull side — download a blob
    def download_blob(self, hex_hash: str) -> bytes:
        """Download a blob from the remote object store."""
        return _download_blob(f"{self.base_url}/objects/{hex_hash}")
