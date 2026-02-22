"""
tests/unit/test_cli_parser.py — argument parsing edge case tests.
"""

from __future__ import annotations

import pytest

from vcs.cli.parser import parse, SUBPARSERS


class TestParserCommands:
    def test_all_phase1_commands_registered(self):
        expected = {
            "repo.init", "repo.clone", "repo.status", "repo.config",
            "commit.stage", "commit.unstage", "commit.snapshot", "commit.show",
            "history.log", "history.diff", "history.annotate",
            "branch.create", "branch.list", "branch.switch", "branch.merge", "branch.delete",
            "remote.add", "remote.list", "remote.push", "remote.pull", "remote.fetch",
            "tag.create", "tag.list",
        }
        assert expected.issubset(set(SUBPARSERS.keys()))

    def test_unknown_command_exits(self):
        with pytest.raises(SystemExit) as exc_info:
            parse(["totally.unknown"])
        assert exc_info.value.code != 0

    def test_repo_init_defaults(self):
        g, s = parse(["repo.init"])
        assert s.path == "."
        assert s.bare is False

    def test_repo_init_bare(self):
        g, s = parse(["repo.init", "--bare"])
        assert s.bare is True

    def test_repo_init_path(self):
        g, s = parse(["repo.init", "/tmp/myrepo"])
        assert s.path == "/tmp/myrepo"

    def test_commit_snapshot_requires_message(self):
        with pytest.raises(SystemExit):
            parse(["commit.snapshot"])

    def test_commit_snapshot_message(self):
        g, s = parse(["commit.snapshot", "-m", "my message"])
        assert s.message == "my message"

    def test_global_json_flag(self):
        g, s = parse(["--json", "repo.status"])
        assert g.json_mode is True

    def test_global_verbose_flag(self):
        g, s = parse(["-v", "repo.status"])
        assert g.verbose is True

    def test_global_no_color(self):
        g, s = parse(["--no-color", "repo.status"])
        assert g.no_color is True

    def test_global_repo_flag(self):
        g, s = parse(["--repo", "/my/repo", "repo.status"])
        assert g.repo == "/my/repo"

    def test_history_log_all_options(self):
        g, s = parse(["history.log", "--branch", "main", "--limit", "10", "--author", "Alice"])
        assert s.branch == "main"
        assert s.limit == 10
        assert s.author == "Alice"

    def test_branch_create_at(self):
        g, s = parse(["branch.create", "feature", "--at", "abc123"])
        assert s.name == "feature"
        assert s.at_hash == "abc123"

    def test_remote_push_defaults(self):
        g, s = parse(["remote.push"])
        assert s.remote == "origin"
        assert s.branch is None

    def test_tag_create_with_message(self):
        g, s = parse(["tag.create", "v1.0", "-m", "Release"])
        assert s.name == "v1.0"
        assert s.message == "Release"

    def test_history_diff_stat_flag(self):
        g, s = parse(["history.diff", "--stat"])
        assert s.stat is True

    def test_history_diff_name_only(self):
        g, s = parse(["history.diff", "--name-only"])
        assert s.name_only is True
