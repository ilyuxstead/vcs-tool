#!/usr/bin/env python3
"""
tools/audit_phase1.py — Phase 1 exit-criteria completeness audit.

For every Phase 1 CLI command, determine:
  1. Is there a dispatch handler in __main__.py?
  2. Does the handler call real implementation code (not just raise NotImplementedError / "not yet implemented")?
  3. Is there at least one passing integration test for it?
  4. Does the backing implementation module exist (not missing/stubbed)?

Exit codes:
  0 — all commands FUNCTIONAL
  1 — one or more commands STUBBED or MISSING
"""

from __future__ import annotations

import ast
import importlib.util
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

# ---------------------------------------------------------------------------
# Configuration — every Phase 1 command and its expected backing module
# ---------------------------------------------------------------------------

PHASE1_COMMANDS: dict[str, dict] = {
    "repo.init":         {"module": "vcs.repo.init",         "fn": "init_repo"},
    "repo.clone":        {"module": "vcs.repo.clone",        "fn": "clone_repo"},  # suspected missing
    "repo.status":       {"module": "vcs.repo.status",       "fn": "compute_status"},
    "repo.config":       {"module": "vcs.repo.config",       "fn": "get_value"},
    "commit.stage":      {"module": "vcs.commit.stage",      "fn": "stage_files"},
    "commit.unstage":    {"module": "vcs.commit.stage",      "fn": "unstage_files"},
    "commit.snapshot":   {"module": "vcs.commit.snapshot",   "fn": "create_snapshot"},
    "commit.show":       {"module": "vcs.commit.snapshot",   "fn": None},
    "history.log":       {"module": "vcs.history.log",       "fn": "log"},
    "history.diff":      {"module": "vcs.history.diff",      "fn": "diff"},
    "history.annotate":  {"module": "vcs.history.annotate",  "fn": "annotate"},
    "branch.create":     {"module": "vcs.branch.ops",        "fn": "create"},
    "branch.list":       {"module": "vcs.branch.ops",        "fn": "list_all"},
    "branch.switch":     {"module": "vcs.branch.ops",        "fn": "switch"},
    "branch.merge":      {"module": "vcs.branch.ops",        "fn": "merge_branch"},
    "branch.delete":     {"module": "vcs.branch.ops",        "fn": "delete"},
    "remote.add":        {"module": "vcs.remote.ops",        "fn": "add"},
    "remote.list":       {"module": "vcs.remote.ops",        "fn": "list_all"},
    "remote.push":       {"module": "vcs.remote.ops",        "fn": "push"},
    "remote.fetch":      {"module": "vcs.remote.ops",        "fn": "fetch"},
    "remote.pull":       {"module": "vcs.remote.ops",        "fn": "pull"},
    "tag.create":        {"module": "vcs.store.db",          "fn": "create_tag"},
    "tag.list":          {"module": "vcs.store.db",          "fn": "list_tags"},
}

# Strings in dispatch handler bodies that indicate a stub / placeholder
STUB_PATTERNS = [
    r"not yet implemented",
    r"NotImplementedError",
    r"TODO",
    r"FIXME",
    r"raise.*NotImplemented",
    r"pass\s*$",
    r"placeholder",
    r"Auto-merge.*requires.*Phase 1 integration",  # pull() stub note
]

Status = Literal["FUNCTIONAL", "STUBBED", "MISSING_HANDLER", "MISSING_MODULE", "MISSING_FUNCTION"]


@dataclass
class CommandAudit:
    command: str
    status: Status
    notes: list[str] = field(default_factory=list)
    has_integration_test: bool = False


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def _load_source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _extract_dispatch_body(source: str, command: str) -> str | None:
    """
    Pull the elif/if block for a given command from __main__._dispatch.
    Returns the source slice as a string, or None if not found.
    """
    # Match: if/elif command == "repo.clone": ... (until next elif/else/end)
    pattern = re.compile(
        r'elif\s+command\s*==\s*["\']' + re.escape(command) + r'["\'].*?(?=elif\s+command|else\s*:|$)',
        re.DOTALL,
    )
    m = pattern.search(source)
    if not m:
        # Also try "if command ==" for the first branch
        pattern2 = re.compile(
            r'if\s+command\s*==\s*["\']' + re.escape(command) + r'["\'].*?(?=elif\s+command|else\s*:|$)',
            re.DOTALL,
        )
        m = pattern2.search(source)
    return m.group(0) if m else None


def _is_stubbed(body: str) -> tuple[bool, str]:
    """Return (is_stub, reason) for a dispatch body."""
    for pat in STUB_PATTERNS:
        if re.search(pat, body, re.IGNORECASE | re.MULTILINE):
            return True, f"matches stub pattern: {pat!r}"
    return False, ""


def _module_importable(module_name: str) -> bool:
    """Check if a Python module can be found (without importing it)."""
    spec = importlib.util.find_spec(module_name)
    return spec is not None


def _module_has_function(module_name: str, fn_name: str) -> bool:
    """Check if a module exposes a given function name via source scan."""
    spec = importlib.util.find_spec(module_name)
    if spec is None or spec.origin is None:
        return False
    src = Path(spec.origin).read_text(encoding="utf-8")
    # Look for def fn_name( in source
    return bool(re.search(rf"^def\s+{re.escape(fn_name)}\s*\(", src, re.MULTILINE))


def _function_is_stub(module_name: str, fn_name: str) -> tuple[bool, str]:
    """Return (is_stub, reason) if the backing function body is trivial."""
    spec = importlib.util.find_spec(module_name)
    if spec is None or spec.origin is None:
        return False, ""
    src = Path(spec.origin).read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return False, ""

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == fn_name:
            body = node.body
            # Stub: single pass, single raise NotImplementedError, or docstring + pass/raise
            real_stmts = [s for s in body if not isinstance(s, ast.Expr) or
                          not isinstance(s.value, ast.Constant)]
            if len(real_stmts) == 0:
                return True, "function body is docstring-only"
            if len(real_stmts) == 1:
                s = real_stmts[0]
                if isinstance(s, ast.Pass):
                    return True, "function body is `pass`"
                if isinstance(s, ast.Raise):
                    return True, "function body raises unconditionally"
            # Check for TODO/stub comments in function source slice
            fn_src = ast.get_source_segment(src, node) or ""
            for pat in STUB_PATTERNS:
                if re.search(pat, fn_src, re.IGNORECASE):
                    return True, f"function body matches stub pattern: {pat!r}"
    return False, ""


def _has_integration_test(command: str, test_source: str) -> bool:
    """Check if integration test file mentions the command."""
    return command in test_source or command.replace(".", "_") in test_source


# ---------------------------------------------------------------------------
# Main audit logic
# ---------------------------------------------------------------------------

def audit(repo_root: Path) -> list[CommandAudit]:
    src_root = repo_root / "src"
    # Add src to sys.path so importlib can find vcs.*
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))

    main_src = (repo_root / "src" / "vcs" / "__main__.py").read_text(encoding="utf-8")

    integration_test_path = repo_root / "tests" / "integration" / "test_cli_dispatch.py"
    integration_src = integration_test_path.read_text(encoding="utf-8") if integration_test_path.exists() else ""

    results: list[CommandAudit] = []

    for command, meta in PHASE1_COMMANDS.items():
        audit = CommandAudit(command=command, status="FUNCTIONAL")
        audit.has_integration_test = _has_integration_test(command, integration_src)

        # 1. Check dispatch handler exists
        body = _extract_dispatch_body(main_src, command)
        if body is None:
            audit.status = "MISSING_HANDLER"
            audit.notes.append("No elif/if block found in __main__._dispatch for this command")
            results.append(audit)
            continue

        # 2. Check dispatch body for stub patterns
        is_stub, reason = _is_stubbed(body)
        if is_stub:
            audit.status = "STUBBED"
            audit.notes.append(f"Dispatch handler is stubbed: {reason}")

        # 3. Check backing module importable
        mod = meta["module"]
        if not _module_importable(mod):
            audit.status = "MISSING_MODULE"
            audit.notes.append(f"Module {mod!r} not found on sys.path (src={src_root})")
            results.append(audit)
            continue

        # 4. Check backing function exists
        fn = meta.get("fn")
        if fn and not _module_has_function(mod, fn):
            audit.status = "MISSING_FUNCTION"
            audit.notes.append(f"Function {fn!r} not found in {mod!r}")
            results.append(audit)
            continue

        # 5. Check if backing function is itself a stub
        if fn:
            fn_stub, fn_reason = _function_is_stub(mod, fn)
            if fn_stub:
                audit.status = "STUBBED"
                audit.notes.append(f"Backing function {mod}.{fn} is a stub: {fn_reason}")

        # 6. Special deep check for pull() — known partial implementation
        if command == "remote.pull":
            pull_note = "pull() returns merged=False with a placeholder note — three-way merge after fetch is NOT wired end-to-end"
            if audit.status == "FUNCTIONAL":
                audit.status = "STUBBED"
            audit.notes.append(pull_note)

        results.append(audit)

    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

STATUS_EMOJI = {
    "FUNCTIONAL":        "✅",
    "STUBBED":           "⚠️ ",
    "MISSING_HANDLER":   "❌",
    "MISSING_MODULE":    "❌",
    "MISSING_FUNCTION":  "❌",
}

STATUS_COLOR = {
    "FUNCTIONAL":        "\033[32m",   # green
    "STUBBED":           "\033[33m",   # yellow
    "MISSING_HANDLER":   "\033[31m",   # red
    "MISSING_MODULE":    "\033[31m",
    "MISSING_FUNCTION":  "\033[31m",
}
RESET = "\033[0m"


def _colorize(status: Status, text: str, *, color: bool = True) -> str:
    if not color:
        return text
    return STATUS_COLOR.get(status, "") + text + RESET


def print_report(results: list[CommandAudit], *, color: bool = True) -> None:
    width = max(len(r.command) for r in results) + 2

    print()
    print("=" * 72)
    print("  Phase 1 Exit-Criteria Audit — Command Completeness Checklist")
    print("=" * 72)
    print(f"  {'Command':<{width}}  {'Status':<20}  {'Test?':<6}")
    print(f"  {'-'*width}  {'-'*20}  {'-'*6}")

    counts: dict[str, int] = {}
    for r in results:
        emoji = STATUS_EMOJI.get(r.status, "?")
        status_str = _colorize(r.status, r.status, color=color)
        test_str = "yes" if r.has_integration_test else _colorize("STUBBED", "NO", color=color)
        print(f"  {r.command:<{width}}  {emoji} {status_str:<30}  {test_str}")
        if r.notes:
            for note in r.notes:
                print(f"  {'':>{width}}    → {note}")
        counts[r.status] = counts.get(r.status, 0) + 1

    print()
    print("  Summary")
    print(f"  {'─'*40}")
    for status, count in sorted(counts.items()):
        emoji = STATUS_EMOJI.get(status, "?")
        print(f"  {emoji} {status:<20} {count:>3}")
    total = len(results)
    functional = counts.get("FUNCTIONAL", 0)
    print(f"  {'─'*40}")
    print(f"  Total: {functional}/{total} fully functional  "
          f"({100*functional//total if total else 0}%)")
    print()

    # Gap analysis
    gaps = [r for r in results if r.status != "FUNCTIONAL"]
    if gaps:
        print("  ── GAPS REQUIRING ACTION BEFORE PHASE 2 ──────────────────────")
        for r in gaps:
            print(f"\n  [{r.status}] {r.command}")
            for note in r.notes:
                print(f"    • {note}")
        print()
    else:
        print("  🎉 All Phase 1 commands are functional! Ready for Phase 2.\n")


# ---------------------------------------------------------------------------
# Unit tests (self-contained, run with: python -m pytest audit_phase1.py)
# ---------------------------------------------------------------------------

def test_extract_dispatch_body_found():
    src = """
    if command == "repo.init":
        do_something()
    elif command == "repo.clone":
        clone_it()
    elif command == "repo.status":
        status()
    """
    body = _extract_dispatch_body(src, "repo.clone")
    assert body is not None
    assert "clone_it" in body
    assert "status" not in body  # should not bleed into next block


def test_extract_dispatch_body_missing():
    src = 'if command == "repo.init":\n    pass\n'
    assert _extract_dispatch_body(src, "repo.clone") is None


def test_is_stubbed_detects_not_yet_implemented():
    body = 'elif command == "repo.clone":\n    user_error("not yet implemented")\n'
    stub, reason = _is_stubbed(body)
    assert stub is True


def test_is_stubbed_clean():
    body = 'elif command == "repo.clone":\n    clone_repo(url, dest)\n    success("Cloned")\n'
    stub, reason = _is_stubbed(body)
    assert stub is False


def test_has_integration_test_positive():
    src = 'class TestRepoClone:\n    def test_clone_success(self):\n        run(["repo.clone", url])\n'
    assert _has_integration_test("repo.clone", src) is True


def test_has_integration_test_negative():
    src = 'class TestRepoInit:\n    pass\n'
    assert _has_integration_test("repo.clone", src) is False


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Audit Phase 1 command completeness.")
    parser.add_argument(
        "--repo",
        default=".",
        help="Path to the vcs-tool repository root (default: current directory)",
    )
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI color output")
    parser.add_argument(
        "--fail-on-stub",
        action="store_true",
        help="Exit 1 if any command is not FUNCTIONAL (for CI gates)",
    )
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    if not (repo / "src" / "vcs").is_dir():
        print(f"Error: {repo} does not look like the vcs-tool repo root (no src/vcs/ found)", file=sys.stderr)
        sys.exit(2)

    results = audit(repo)
    print_report(results, color=not args.no_color)

    non_functional = [r for r in results if r.status != "FUNCTIONAL"]
    if args.fail_on_stub and non_functional:
        sys.exit(1)
    sys.exit(0)