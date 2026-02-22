"""
vcs.cli.output — human-readable and JSON output formatting.

All commands must satisfy the error handling contract (Section 5.3):
  - Exit 0: success, output on stdout
  - Exit 1: user error, human-readable message on stderr
  - Exit 2: internal error, stderr includes traceback in --verbose mode
  - --json: always emit {success, message, data, error_code}
"""

from __future__ import annotations

import json
import sys
import traceback
from typing import Any


def _ansi(code: str, text: str, *, color: bool) -> str:
    if not color:
        return text
    return f"\033[{code}m{text}\033[0m"


def success(
    message: str,
    data: Any = None,
    *,
    json_mode: bool = False,
    color: bool = True,
) -> None:
    """Print a success message and exit 0."""
    if json_mode:
        print(json.dumps({"success": True, "message": message, "data": data, "error_code": None}))
    else:
        print(message)
    sys.exit(0)


def user_error(
    message: str,
    error_code: str = "USER_ERROR",
    *,
    json_mode: bool = False,
) -> None:
    """Print a user error to stderr and exit 1."""
    if json_mode:
        print(
            json.dumps({"success": False, "message": message, "data": None, "error_code": error_code}),
            file=sys.stderr,
        )
    else:
        print(f"Error: {message}", file=sys.stderr)
    sys.exit(1)


def internal_error(
    message: str,
    exc: Exception | None = None,
    *,
    json_mode: bool = False,
    verbose: bool = False,
) -> None:
    """Print an internal error to stderr and exit 2."""
    if json_mode:
        print(
            json.dumps({"success": False, "message": message, "data": None, "error_code": "INTERNAL_ERROR"}),
            file=sys.stderr,
        )
    else:
        print(f"Internal error: {message}", file=sys.stderr)
        if verbose and exc:
            traceback.print_exc(file=sys.stderr)
    sys.exit(2)


def print_output(text: str, *, json_mode: bool = False, data: Any = None) -> None:
    """Print human-readable text (or JSON data) to stdout without exiting."""
    if json_mode and data is not None:
        print(json.dumps({"success": True, "message": text, "data": data, "error_code": None}))
    else:
        print(text)


def format_commit(commit, *, color: bool = True, short: bool = False) -> str:
    """Format a single commit for display."""
    hash_str = _ansi("33", commit.hash[:12], color=color)  # yellow
    author_str = _ansi("32", commit.author, color=color)   # green
    lines = [
        f"commit {hash_str}",
        f"Author: {author_str}",
        f"Date:   {commit.timestamp}",
    ]
    if not short:
        lines.append("")
        lines.append(f"    {commit.message}")
        lines.append("")
    return "\n".join(lines)


def format_status(status, *, color: bool = True) -> str:
    """Format a WorkingTreeStatus for display."""
    lines = []

    def colored_path(path: str, code: str) -> str:
        return _ansi(code, path, color=color)

    if status.staged_new:
        lines.append("Changes to be committed (new files):")
        for p in status.staged_new:
            lines.append(f"        new file:   {colored_path(p, '32')}")
    if status.staged_modified:
        lines.append("Changes to be committed (modified):")
        for p in status.staged_modified:
            lines.append(f"        modified:   {colored_path(p, '32')}")
    if status.staged_deleted:
        lines.append("Changes to be committed (deleted):")
        for p in status.staged_deleted:
            lines.append(f"        deleted:    {colored_path(p, '32')}")
    if status.modified:
        lines.append("Changes not staged for commit:")
        for p in status.modified:
            lines.append(f"        modified:   {colored_path(p, '31')}")
    if status.deleted:
        lines.append("Changes not staged for commit (deleted):")
        for p in status.deleted:
            lines.append(f"        deleted:    {colored_path(p, '31')}")
    if status.untracked:
        lines.append("Untracked files:")
        for p in status.untracked:
            lines.append(f"        {colored_path(p, '31')}")
    if not lines:
        lines.append("nothing to commit, working tree clean")
    return "\n".join(lines)
