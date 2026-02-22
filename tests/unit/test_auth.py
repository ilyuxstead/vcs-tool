"""
tests/unit/test_auth.py — VCS_AUTH_TOKEN security tests.

Verifies that the token is never leaked into stdout, stderr, or
exception messages during normal CLI invocations (NFR-09, FR-REM-04).
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from vcs.remote.protocol import _get_token, _redact, RemoteClient
from vcs.store.exceptions import AuthenticationError


class TestGetToken:
    def test_reads_env_var(self):
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "secret123"}):
            assert _get_token() == "secret123"

    def test_missing_env_raises(self):
        env = {k: v for k, v in os.environ.items() if k != "VCS_AUTH_TOKEN"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(AuthenticationError):
                _get_token()

    def test_empty_token_raises(self):
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": ""}):
            with pytest.raises(AuthenticationError):
                _get_token()


class TestRedact:
    def test_redacts_token_from_text(self):
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "my-secret-token"}):
            result = _redact("Error: Authorization: Bearer my-secret-token failed")
            assert "my-secret-token" not in result
            assert "<REDACTED>" in result

    def test_no_token_no_change(self):
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": "secret"}):
            result = _redact("Normal log message with no token")
            assert result == "Normal log message with no token"

    def test_empty_env_no_change(self):
        env = {k: v for k, v in os.environ.items() if k != "VCS_AUTH_TOKEN"}
        with patch.dict(os.environ, env, clear=True):
            result = _redact("some text")
            assert result == "some text"


class TestTokenNotInOutput:
    """
    Verify that running CLI commands does not leak the token to stdout/stderr.
    These tests use a repo that is already initialised.
    """

    def test_status_no_token_leak(self, tmp_repo, capsys):
        secret = "super-secret-token-xyz"
        with patch.dict(os.environ, {"VCS_AUTH_TOKEN": secret}):
            from vcs.__main__ import main
            try:
                main(["repo.status"])
            except SystemExit:
                pass
            captured = capsys.readouterr()
            assert secret not in captured.out
            assert secret not in captured.err
