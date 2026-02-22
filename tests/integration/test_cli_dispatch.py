"""
tests/integration/test_cli_dispatch.py — end-to-end CLI dispatch coverage.

Drives main() for every noun.verb command to exercise __main__.py and
cli/output.py together. Uses --json for easy assertion on outputs.
"""

from __future__ import annotations

import io
import json
import os
import sys
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from unittest.mock import patch

import pytest

from vcs.__main__ import main

from tests.integration.conftest import run, run_json, make_repo
# ---------------------------------------------------------------------------
# repo.*
# ---------------------------------------------------------------------------

class TestRepoInit:
    def test_init_success(self, tmp_path: Path):
        path = tmp_path / "new_repo"
        code, out, _ = run(["repo.init", str(path)])
        assert code == 0
        assert (path / ".vcs").is_dir()

    def test_init_json(self, tmp_path: Path):
        path = tmp_path / "jr"
        code, data, _ = run_json(["repo.init", str(path)])
        assert code == 0
        assert data["success"] is True

    def test_init_bare_flag(self, tmp_path: Path):
        path = tmp_path / "bare_repo"
        code, _, _ = run(["repo.init", "--bare", str(path)])
        assert code == 0

    def test_init_double_fails(self, tmp_path: Path):
        path = tmp_path / "dup"
        run(["repo.init", str(path)])
        code, _, err = run(["repo.init", str(path)])
        assert code == 1
        assert "Error" in err

    def test_init_default_cwd(self, tmp_path: Path):
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            code, _, _ = run(["repo.init"])
            assert code == 0
        finally:
            os.chdir(old)


class TestRepoStatus:
    def test_status_clean(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, out, _ = run(["repo.status"], root)
        assert code == 0
        assert "clean" in out

    def test_status_untracked(self, tmp_path: Path):
        root = make_repo(tmp_path)
        (root / "new.txt").write_text("untracked")
        code, out, _ = run(["repo.status"], root)
        assert code == 0
        assert "new.txt" in out

    def test_status_json(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, data, _ = run_json(["repo.status"], root)
        assert code == 0
        assert data["success"] is True
        assert "clean" in data["data"]
        assert data["data"]["clean"] is True

    def test_status_staged_shown(self, tmp_path: Path):
        root = make_repo(tmp_path)
        (root / "staged.txt").write_text("staged")
        run(["commit.stage", "staged.txt"], root)
        code, out, _ = run(["repo.status"], root)
        assert code == 0
        assert "staged.txt" in out


class TestRepoConfig:
    def test_set_and_get(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, _, _ = run(["repo.config", "core.author", "New Author"], root)
        assert code == 0
        code, out, _ = run(["repo.config", "core.author"], root)
        assert code == 0
        assert "New Author" in out

    def test_get_json(self, tmp_path: Path):
        root = make_repo(tmp_path)
        run(["repo.config", "core.author", "JSON Author"], root)
        code, data, _ = run_json(["repo.config", "core.author"], root)
        assert code == 0
        assert data["data"] == "JSON Author"

    def test_set_boolean(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, _, _ = run(["repo.config", "core.bare", "true"], root)
        assert code == 0

    def test_set_integer(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, _, _ = run(["repo.config", "limits.max", "42"], root)
        assert code == 0

    def test_get_missing_key_fails(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, _, err = run(["repo.config", "no.such.key"], root)
        assert code == 1


# ---------------------------------------------------------------------------
# commit.*
# ---------------------------------------------------------------------------

class TestCommitStage:
    def test_stage_file(self, tmp_path: Path):
        root = make_repo(tmp_path)
        (root / "new.txt").write_text("new")
        code, out, _ = run(["commit.stage", "new.txt"], root)
        assert code == 0
        assert "new.txt" in out

    def test_stage_all(self, tmp_path: Path):
        root = make_repo(tmp_path)
        for i in range(3):
            (root / f"f{i}.txt").write_text(f"content {i}")
        code, out, _ = run(["commit.stage", "--all"], root)
        assert code == 0

    def test_stage_no_paths_fails(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, _, err = run(["commit.stage"], root)
        assert code == 1

    def test_stage_missing_file_fails(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, _, err = run(["commit.stage", "ghost.txt"], root)
        assert code == 1


class TestCommitUnstage:
    def test_unstage_file(self, tmp_path: Path):
        root = make_repo(tmp_path)
        (root / "u.txt").write_text("u")
        run(["commit.stage", "u.txt"], root)
        code, out, _ = run(["commit.unstage", "u.txt"], root)
        assert code == 0

    def test_unstage_not_staged_fails(self, tmp_path: Path):
        root = make_repo(tmp_path)
        (root / "x.txt").write_text("x")
        code, _, err = run(["commit.unstage", "x.txt"], root)
        assert code == 1


class TestCommitSnapshot:
    def test_snapshot_success(self, tmp_path: Path):
        root = make_repo(tmp_path)
        (root / "v2.txt").write_text("v2")
        run(["commit.stage", "v2.txt"], root)
        code, out, _ = run(["commit.snapshot", "-m", "Second commit", "--author", "A <a@b.com>"], root)
        assert code == 0

    def test_snapshot_json(self, tmp_path: Path):
        root = make_repo(tmp_path)
        (root / "v3.txt").write_text("v3")
        run(["commit.stage", "v3.txt"], root)
        code, data, _ = run_json(["commit.snapshot", "-m", "JSON commit", "--author", "A <a@b.com>"], root)
        assert code == 0
        assert data["success"] is True
        assert "hash" in data["data"]

    def test_snapshot_amend_rejected(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, _, err = run(["commit.snapshot", "-m", "bad", "--amend"], root)
        assert code == 1
        assert "immutable" in err.lower()

    def test_snapshot_empty_staging_fails(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, _, err = run(["commit.snapshot", "-m", "empty"], root)
        assert code == 1

    def test_snapshot_author_from_config(self, tmp_path: Path):
        root = make_repo(tmp_path)
        run(["repo.config", "core.author", "Config Author <cfg@test.com>"], root)
        (root / "cfg.txt").write_text("cfg")
        run(["commit.stage", "cfg.txt"], root)
        # No --author flag — should pick up from config
        code, out, _ = run(["commit.snapshot", "-m", "from config"], root)
        assert code == 0


class TestCommitShow:
    def test_show_commit(self, tmp_path: Path):
        root = make_repo(tmp_path)
        from vcs.repo.init import resolve_head_commit
        head = resolve_head_commit(root)
        code, out, _ = run(["commit.show", head], root)
        assert code == 0
        assert "Initial commit" in out

    def test_show_commit_json(self, tmp_path: Path):
        root = make_repo(tmp_path)
        from vcs.repo.init import resolve_head_commit
        head = resolve_head_commit(root)
        code, data, _ = run_json(["commit.show", head], root)
        assert code == 0
        assert data["data"]["message"] == "Initial commit"

    def test_show_commit_stat(self, tmp_path: Path):
        root = make_repo(tmp_path)
        from vcs.repo.init import resolve_head_commit
        head = resolve_head_commit(root)
        code, out, _ = run(["commit.show", "--stat", head], root)
        assert code == 0
        assert "file" in out

    def test_show_missing_hash_fails(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, _, err = run(["commit.show", "a" * 64], root)
        assert code == 1


# ---------------------------------------------------------------------------
# history.*
# ---------------------------------------------------------------------------

class TestHistoryLog:
    def test_log_shows_commit(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, out, _ = run(["history.log"], root)
        assert code == 0
        assert "Initial commit" in out

    def test_log_json(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, data, _ = run_json(["history.log"], root)
        assert code == 0
        assert len(data["data"]) == 1

    def test_log_limit(self, tmp_path: Path):
        root = make_repo(tmp_path)
        for i in range(4):
            (root / f"f{i}.txt").write_text(f"c{i}")
            run(["commit.stage", f"f{i}.txt"], root)
            run(["commit.snapshot", "-m", f"commit {i}", "--author", "A <a@b.com>"], root)
        code, data, _ = run_json(["history.log", "--limit", "2"], root)
        assert len(data["data"]) == 2

    def test_log_branch_filter(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, data, _ = run_json(["history.log", "--branch", "main"], root)
        assert code == 0

    def test_log_author_filter(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, data, _ = run_json(["history.log", "--author", "CLI Tester"], root)
        assert code == 0
        assert len(data["data"]) == 1


class TestHistoryDiff:
    def test_diff_head_vs_working(self, tmp_path: Path):
        root = make_repo(tmp_path)
        (root / "hello.py").write_text("print('modified')\n")
        from vcs.repo.init import resolve_head_commit
        head = resolve_head_commit(root)
        code, out, _ = run(["history.diff", head], root)
        assert code == 0

    def test_diff_json(self, tmp_path: Path):
        root = make_repo(tmp_path)
        (root / "hello.py").write_text("print('changed')\n")
        from vcs.repo.init import resolve_head_commit
        head = resolve_head_commit(root)
        code, data, _ = run_json(["history.diff", head], root)
        assert code == 0
        assert isinstance(data["data"], list)

    def test_diff_stat(self, tmp_path: Path):
        root = make_repo(tmp_path)
        (root / "hello.py").write_text("print('stat')\n")
        from vcs.repo.init import resolve_head_commit
        head = resolve_head_commit(root)
        code, out, _ = run(["history.diff", "--stat", head], root)
        assert code == 0

    def test_diff_name_only(self, tmp_path: Path):
        root = make_repo(tmp_path)
        (root / "hello.py").write_text("changed\n")
        from vcs.repo.init import resolve_head_commit
        head = resolve_head_commit(root)
        code, out, _ = run(["history.diff", "--name-only", head], root)
        assert code == 0
        assert "hello.py" in out

    def test_diff_two_commits(self, tmp_path: Path):
        root = make_repo(tmp_path)
        from vcs.repo.init import resolve_head_commit
        c1 = resolve_head_commit(root)
        (root / "hello.py").write_text("v2\n")
        run(["commit.stage", "hello.py"], root)
        run(["commit.snapshot", "-m", "v2", "--author", "A <a@b.com>"], root)
        c2 = resolve_head_commit(root)
        code, out, _ = run(["history.diff", c1, c2], root)
        assert code == 0

    def test_diff_no_args_defaults_to_head(self, tmp_path: Path):
        root = make_repo(tmp_path)
        (root / "hello.py").write_text("default diff\n")
        code, out, _ = run(["history.diff"], root)
        assert code == 0


class TestHistoryAnnotate:
    def test_annotate_file(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, out, _ = run(["history.annotate", "hello.py"], root)
        assert code == 0

    def test_annotate_json(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, data, _ = run_json(["history.annotate", "hello.py"], root)
        assert code == 0
        assert isinstance(data["data"], list)
        assert data["data"][0]["line_number"] == 1

    def test_annotate_shows_commit_hash(self, tmp_path: Path):
        root = make_repo(tmp_path)
        from vcs.repo.init import resolve_head_commit
        head = resolve_head_commit(root)
        code, out, _ = run(["history.annotate", "hello.py"], root)
        assert head[:8] in out


# ---------------------------------------------------------------------------
# branch.*
# ---------------------------------------------------------------------------

class TestBranchCLI:
    def test_create_branch(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, out, _ = run(["branch.create", "feature"], root)
        assert code == 0

    def test_list_branches(self, tmp_path: Path):
        root = make_repo(tmp_path)
        run(["branch.create", "feat"], root)
        code, out, _ = run(["branch.list"], root)
        assert code == 0
        assert "feat" in out
        assert "main" in out

    def test_list_branches_json(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, data, _ = run_json(["branch.list"], root)
        assert code == 0
        names = [b["name"] for b in data["data"]]
        assert "main" in names

    def test_switch_branch(self, tmp_path: Path):
        root = make_repo(tmp_path)
        run(["branch.create", "feat"], root)
        code, out, _ = run(["branch.switch", "feat"], root)
        assert code == 0

    def test_merge_branch(self, tmp_path: Path):
        root = make_repo(tmp_path)
        run(["branch.create", "feat"], root)
        run(["branch.switch", "feat"], root)
        (root / "feat.txt").write_text("feature")
        run(["commit.stage", "feat.txt"], root)
        run(["commit.snapshot", "-m", "feat commit", "--author", "A <a@b.com>"], root)
        run(["branch.switch", "main"], root)
        code, out, _ = run(["branch.merge", "feat", "--author", "A <a@b.com>"], root)
        assert code == 0

    def test_delete_branch(self, tmp_path: Path):
        root = make_repo(tmp_path)
        run(["branch.create", "to-delete"], root)
        code, out, _ = run(["branch.delete", "to-delete"], root)
        assert code == 0

    def test_delete_active_branch_fails(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, _, err = run(["branch.delete", "main"], root)
        assert code == 1

    def test_branch_create_at_hash(self, tmp_path: Path):
        root = make_repo(tmp_path)
        from vcs.repo.init import resolve_head_commit
        head = resolve_head_commit(root)
        code, out, _ = run(["branch.create", "at-hash", "--at", head], root)
        assert code == 0


# ---------------------------------------------------------------------------
# remote.*
# ---------------------------------------------------------------------------

class TestRemoteCLI:
    def test_add_remote(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, out, _ = run(["remote.add", "origin", "https://vcs.example.com/repo"], root)
        assert code == 0

    def test_list_remotes_empty(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, out, _ = run(["remote.list"], root)
        assert code == 0
        assert out.strip() == ""

    def test_list_remotes_json(self, tmp_path: Path):
        root = make_repo(tmp_path)
        run(["remote.add", "origin", "https://example.com/r"], root)
        code, data, _ = run_json(["remote.list"], root)
        assert code == 0
        assert data["data"][0]["name"] == "origin"

    def test_list_remotes_text(self, tmp_path: Path):
        root = make_repo(tmp_path)
        run(["remote.add", "upstream", "https://upstream.example.com/r"], root)
        code, out, _ = run(["remote.list"], root)
        assert "upstream" in out


# ---------------------------------------------------------------------------
# tag.*
# ---------------------------------------------------------------------------

class TestTagCLI:
    def test_create_tag(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, out, _ = run(["tag.create", "v1.0", "-m", "First release"], root)
        assert code == 0

    def test_list_tags(self, tmp_path: Path):
        root = make_repo(tmp_path)
        run(["tag.create", "v1.0"], root)
        code, out, _ = run(["tag.list"], root)
        assert code == 0
        assert "v1.0" in out

    def test_list_tags_json(self, tmp_path: Path):
        root = make_repo(tmp_path)
        run(["tag.create", "v2.0", "-m", "Release 2"], root)
        code, data, _ = run_json(["tag.list"], root)
        assert code == 0
        assert any(t["name"] == "v2.0" for t in data["data"])

    def test_tag_empty_repo_fails(self, tmp_path: Path):
        root = tmp_path / "empty"
        root.mkdir()
        run(["repo.init", str(root)])
        code, _, err = run(["tag.create", "v0"], root)
        assert code == 1

    def test_tag_with_tagger(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, out, _ = run(["tag.create", "v3.0", "--tagger", "Release Bot <bot@ci.com>"], root)
        assert code == 0

    def test_tag_at_specific_hash(self, tmp_path: Path):
        root = make_repo(tmp_path)
        from vcs.repo.init import resolve_head_commit
        head = resolve_head_commit(root)
        code, out, _ = run(["tag.create", "pinned", head], root)
        assert code == 0


# ---------------------------------------------------------------------------
# Error handling / exit codes
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_vcs_error_exits_1(self, tmp_path: Path):
        """VCSError subclasses map to exit code 1."""
        root = make_repo(tmp_path)
        code, _, err = run(["branch.switch", "nonexistent"], root)
        assert code == 1
        assert "Error" in err

    def test_vcs_error_json(self, tmp_path: Path):
        root = make_repo(tmp_path)
        code, data, _ = run_json(["branch.switch", "nonexistent"], root)
        assert code == 1
        assert data["success"] is False
        assert data["error_code"] is not None

    def test_not_a_repo_exits_1(self, tmp_path: Path):
        """Trying to run commands outside a repo exits 1."""
        empty = tmp_path / "empty"
        empty.mkdir()
        old = os.getcwd()
        os.chdir(empty)
        try:
            code, _, err = run(["repo.status"])
            assert code == 1
        finally:
            os.chdir(old)

    def test_internal_error_exits_2(self, tmp_path: Path):
        """Unexpected exceptions exit with code 2."""
        root = make_repo(tmp_path)
        with patch("vcs.__main__._dispatch", side_effect=RuntimeError("boom")):
            code, _, err = run(["repo.status"], root)
        assert code == 2
        assert "Internal error" in err

    def test_internal_error_verbose_shows_traceback(self, tmp_path: Path):
        root = make_repo(tmp_path)
        with patch("vcs.__main__._dispatch", side_effect=RuntimeError("boom")):
            code, _, err = run(["--verbose", "repo.status"], root)
        assert code == 2

    def test_internal_error_json(self, tmp_path: Path):
        root = make_repo(tmp_path)
        with patch("vcs.__main__._dispatch", side_effect=RuntimeError("boom")):
            code, data, _ = run_json(["repo.status"], root)
        assert code == 2
        assert data["success"] is False
        assert data["error_code"] == "INTERNAL_ERROR"
