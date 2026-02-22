"""
tests/unit/test_output.py — cli/output.py unit tests.

Tests all output functions and formatting helpers for both human and
JSON output modes, with and without ANSI colour.
"""

from __future__ import annotations

import io
import json
import sys
from contextlib import redirect_stdout, redirect_stderr

import pytest

from vcs.cli.output import (
    format_commit,
    format_status,
    internal_error,
    print_output,
    success,
    user_error,
)
from vcs.repo.status import WorkingTreeStatus
from vcs.store.models import Commit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _commit(msg: str = "Test commit") -> Commit:
    return Commit(
        hash="a" * 64,
        tree_hash="b" * 64,
        parent_hashes=(),
        author="Alice <a@test.com>",
        timestamp="2026-01-01T00:00:00Z",
        message=msg,
    )


def capture_success(message: str, data=None, *, json_mode: bool = False) -> tuple[int, str]:
    buf = io.StringIO()
    code = 0
    with redirect_stdout(buf):
        try:
            success(message, data=data, json_mode=json_mode)
        except SystemExit as e:
            code = e.code
    return code, buf.getvalue()


def capture_user_error(message: str, error_code: str = "USER_ERROR", *, json_mode: bool = False) -> tuple[int, str]:
    buf = io.StringIO()
    code = 0
    with redirect_stderr(buf):
        try:
            user_error(message, error_code, json_mode=json_mode)
        except SystemExit as e:
            code = e.code
    return code, buf.getvalue()


def capture_internal_error(message: str, exc=None, *, json_mode: bool = False, verbose: bool = False) -> tuple[int, str]:
    buf = io.StringIO()
    code = 0
    with redirect_stderr(buf):
        try:
            internal_error(message, exc=exc, json_mode=json_mode, verbose=verbose)
        except SystemExit as e:
            code = e.code
    return code, buf.getvalue()


# ---------------------------------------------------------------------------
# success()
# ---------------------------------------------------------------------------

class TestSuccess:
    def test_exits_zero(self):
        code, _ = capture_success("all good")
        assert code == 0

    def test_prints_message(self):
        _, out = capture_success("hello world")
        assert "hello world" in out

    def test_json_mode_structure(self):
        _, out = capture_success("ok", data={"key": "val"}, json_mode=True)
        data = json.loads(out)
        assert data["success"] is True
        assert data["message"] == "ok"
        assert data["data"] == {"key": "val"}
        assert data["error_code"] is None

    def test_json_mode_no_data(self):
        _, out = capture_success("done", json_mode=True)
        data = json.loads(out)
        assert data["data"] is None


# ---------------------------------------------------------------------------
# user_error()
# ---------------------------------------------------------------------------

class TestUserError:
    def test_exits_one(self):
        code, _ = capture_user_error("bad input")
        assert code == 1

    def test_prints_to_stderr(self):
        _, err = capture_user_error("something wrong")
        assert "something wrong" in err

    def test_prefixes_error(self):
        _, err = capture_user_error("missing file")
        assert "Error" in err

    def test_json_mode_structure(self):
        _, err = capture_user_error("bad", "MY_CODE", json_mode=True)
        data = json.loads(err)
        assert data["success"] is False
        assert data["error_code"] == "MY_CODE"
        assert data["data"] is None

    def test_custom_error_code(self):
        _, err = capture_user_error("oops", "CUSTOM_CODE", json_mode=True)
        data = json.loads(err)
        assert data["error_code"] == "CUSTOM_CODE"


# ---------------------------------------------------------------------------
# internal_error()
# ---------------------------------------------------------------------------

class TestInternalError:
    def test_exits_two(self):
        code, _ = capture_internal_error("crash")
        assert code == 2

    def test_prints_to_stderr(self):
        _, err = capture_internal_error("unexpected")
        assert "unexpected" in err
        assert "Internal error" in err

    def test_json_mode_structure(self):
        _, err = capture_internal_error("boom", json_mode=True)
        data = json.loads(err)
        assert data["success"] is False
        assert data["error_code"] == "INTERNAL_ERROR"

    def test_verbose_with_exception(self, capsys):
        try:
            raise ValueError("original cause")
        except ValueError as exc:
            buf = io.StringIO()
            with redirect_stderr(buf):
                try:
                    internal_error("crash", exc=exc, verbose=True)
                except SystemExit:
                    pass
            err = buf.getvalue()
        # verbose mode should include traceback info
        assert "Internal error" in err

    def test_non_verbose_no_traceback(self):
        try:
            raise ValueError("hidden")
        except ValueError as exc:
            _, err = capture_internal_error("crash", exc=exc, verbose=False)
        assert "Traceback" not in err


# ---------------------------------------------------------------------------
# print_output()
# ---------------------------------------------------------------------------

class TestPrintOutput:
    def test_human_mode(self, capsys):
        print_output("hello")
        captured = capsys.readouterr()
        assert "hello" in captured.out

    def test_json_mode_with_data(self, capsys):
        print_output("msg", json_mode=True, data={"x": 1})
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["data"] == {"x": 1}

    def test_json_mode_no_data_falls_back(self, capsys):
        # json_mode=True but data=None → falls back to plain print
        print_output("fallback", json_mode=True, data=None)
        captured = capsys.readouterr()
        assert "fallback" in captured.out


# ---------------------------------------------------------------------------
# format_commit()
# ---------------------------------------------------------------------------

class TestFormatCommit:
    def test_contains_hash(self):
        out = format_commit(_commit(), color=False)
        assert "a" * 12 in out

    def test_contains_author(self):
        out = format_commit(_commit(), color=False)
        assert "Alice" in out

    def test_contains_message(self):
        out = format_commit(_commit("My message"), color=False)
        assert "My message" in out

    def test_short_mode_omits_message(self):
        out = format_commit(_commit("Should not appear"), color=False, short=True)
        assert "Should not appear" not in out

    def test_color_mode_includes_ansi(self):
        out = format_commit(_commit(), color=True)
        assert "\033[" in out

    def test_no_color_no_ansi(self):
        out = format_commit(_commit(), color=False)
        assert "\033[" not in out


# ---------------------------------------------------------------------------
# format_status()
# ---------------------------------------------------------------------------

class TestFormatStatus:
    def test_clean_repo(self):
        status = WorkingTreeStatus()
        out = format_status(status, color=False)
        assert "clean" in out

    def test_shows_staged_new(self):
        status = WorkingTreeStatus(staged_new=["new.txt"])
        out = format_status(status, color=False)
        assert "new.txt" in out
        assert "new file" in out

    def test_shows_staged_modified(self):
        status = WorkingTreeStatus(staged_modified=["mod.txt"])
        out = format_status(status, color=False)
        assert "mod.txt" in out
        assert "modified" in out

    def test_shows_staged_deleted(self):
        status = WorkingTreeStatus(staged_deleted=["del.txt"])
        out = format_status(status, color=False)
        assert "del.txt" in out
        assert "deleted" in out

    def test_shows_untracked(self):
        status = WorkingTreeStatus(untracked=["new_untracked.txt"])
        out = format_status(status, color=False)
        assert "new_untracked.txt" in out
        assert "Untracked" in out

    def test_shows_modified_unstaged(self):
        status = WorkingTreeStatus(modified=["changed.txt"])
        out = format_status(status, color=False)
        assert "changed.txt" in out

    def test_shows_deleted_unstaged(self):
        status = WorkingTreeStatus(deleted=["gone.txt"])
        out = format_status(status, color=False)
        assert "gone.txt" in out

    def test_color_mode_includes_ansi(self):
        status = WorkingTreeStatus(staged_new=["x.txt"])
        out = format_status(status, color=True)
        assert "\033[" in out
