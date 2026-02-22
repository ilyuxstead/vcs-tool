# tests/integration/conftest.py
"""
Shared helpers for all integration tests.

pytest auto-imports this file, so `run`, `run_json`, and `make_repo`
are available in every test module under tests/integration/ without
an explicit import.
"""

from __future__ import annotations

import io
import json
import os
from pathlib import Path

import pytest

from vcs.__main__ import main


def run(args: list[str], repo_root: Path | None = None) -> tuple[int, str, str]:
    """
    Run main() with *args*, capture stdout/stderr, return (exit_code, out, err).
    Injects --repo if repo_root is provided.
    """
    if repo_root:
        args = ["--repo", str(repo_root)] + args

    out_buf = io.StringIO()
    err_buf = io.StringIO()
    exit_code = 0
    with __import__("contextlib").redirect_stdout(out_buf), \
         __import__("contextlib").redirect_stderr(err_buf):
        try:
            main(args)
        except SystemExit as e:
            exit_code = e.code if isinstance(e.code, int) else 1
    return exit_code, out_buf.getvalue(), err_buf.getvalue()


def run_json(args: list[str], repo_root: Path | None = None) -> tuple[int, dict, str]:
    """Like run() but prepends --json and parses stdout or stderr as JSON."""
    code, out, err = run(["--json"] + args, repo_root)
    if out.strip():
        data = json.loads(out)
    elif err.strip():
        data = json.loads(err)
    else:
        data = {}
    return code, data, err


def make_repo(tmp_path: Path, author: str = "CLI Tester <cli@test.local>") -> Path:
    """Initialise a repo, make one commit, return the root."""
    root = tmp_path / "repo"
    root.mkdir()
    code, _, _ = run(["repo.init", str(root)])
    assert code == 0
    (root / "hello.py").write_text("print('hello')\n")
    run(["commit.stage", "hello.py"], root)
    run(["commit.snapshot", "-m", "Initial commit", "--author", author], root)
    return root