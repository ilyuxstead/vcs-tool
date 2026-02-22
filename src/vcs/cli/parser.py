"""
vcs.cli.parser — argument parsing for all ``vcs <noun>.<verb>`` commands.

The dispatcher parses the first positional argument as ``<noun>.<verb>``,
then delegates to the relevant sub-parser.
"""

from __future__ import annotations

import argparse
import sys


def _global_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="vcs",
        description="VCS — a modern version control system.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    p.add_argument("command", help="noun.verb command (e.g. repo.init, commit.stage)")
    p.add_argument("--repo", metavar="PATH", help="Override repository root path")
    p.add_argument("--verbose", "-v", action="store_true", help="Increase output verbosity")
    p.add_argument("--json", action="store_true", dest="json_mode", help="Output machine-readable JSON")
    p.add_argument("--no-color", action="store_true", help="Disable ANSI colour output")
    p.add_argument("--help", "-h", action="help", default=argparse.SUPPRESS)
    return p


# ---------------------------------------------------------------------------
# Sub-command parsers
# ---------------------------------------------------------------------------

def _make_subparsers() -> dict[str, argparse.ArgumentParser]:
    subs: dict[str, argparse.ArgumentParser] = {}

    # repo.init
    p = argparse.ArgumentParser(prog="vcs repo.init", description="Initialise a new repository.")
    p.add_argument("path", nargs="?", default=".", help="Directory to initialise (default: .)")
    p.add_argument("--bare", action="store_true", help="Create a bare repository (no working tree)")
    subs["repo.init"] = p

    # repo.clone
    p = argparse.ArgumentParser(prog="vcs repo.clone", description="Clone a remote repository.")
    p.add_argument("url", help="Remote URL to clone from")
    p.add_argument("dir", nargs="?", default=None, help="Destination directory")
    p.add_argument("--depth", type=int, metavar="N", help="Shallow clone depth")
    subs["repo.clone"] = p

    # repo.status
    p = argparse.ArgumentParser(prog="vcs repo.status", description="Show working tree status.")
    p.add_argument("--short", action="store_true", help="Compact output")
    subs["repo.status"] = p

    # repo.config
    p = argparse.ArgumentParser(prog="vcs repo.config", description="Get or set config values.")
    p.add_argument("key", help="Config key (dot-separated, e.g. core.author)")
    p.add_argument("value", nargs="?", default=None, help="Value to set (omit to get)")
    p.add_argument("--global", action="store_true", dest="global_", help="Target user-level config")
    subs["repo.config"] = p

    # commit.stage
    p = argparse.ArgumentParser(prog="vcs commit.stage", description="Stage file(s) for next commit.")
    p.add_argument("paths", nargs="*", help="Files to stage")
    p.add_argument("--all", action="store_true", dest="all_", help="Stage all modified/untracked files")
    subs["commit.stage"] = p

    # commit.unstage
    p = argparse.ArgumentParser(prog="vcs commit.unstage", description="Remove file(s) from staging.")
    p.add_argument("paths", nargs="+", help="Files to unstage")
    subs["commit.unstage"] = p

    # commit.snapshot
    p = argparse.ArgumentParser(prog="vcs commit.snapshot", description="Create a new commit.")
    p.add_argument("-m", "--message", required=True, help="Commit message")
    p.add_argument("--author", help="Author string (defaults to config core.author)")
    p.add_argument("--amend", action="store_true", help=argparse.SUPPRESS)  # always rejected
    p.add_argument("--timestamp", help=argparse.SUPPRESS)  # hidden; for tests
    subs["commit.snapshot"] = p

    # commit.show
    p = argparse.ArgumentParser(prog="vcs commit.show", description="Show commit details and diff.")
    p.add_argument("hash", help="Commit hash to show")
    p.add_argument("--stat", action="store_true", help="Show file stats summary only")
    subs["commit.show"] = p

    # history.log
    p = argparse.ArgumentParser(prog="vcs history.log", description="List commit history.")
    p.add_argument("--branch", "-b", metavar="BRANCH", help="Branch to show history for")
    p.add_argument("--limit", "-n", type=int, metavar="N", help="Maximum commits to show")
    p.add_argument("--author", metavar="AUTHOR", help="Filter by author substring")
    subs["history.log"] = p

    # history.diff
    p = argparse.ArgumentParser(prog="vcs history.diff", description="Show diff between commits or working tree.")
    p.add_argument("hash_a", nargs="?", default=None, help="Older commit hash (default: HEAD)")
    p.add_argument("hash_b", nargs="?", default=None, help="Newer commit hash (default: working tree)")
    p.add_argument("--stat", action="store_true", help="Show line count stats only")
    p.add_argument("--name-only", action="store_true", help="Show file names only")
    subs["history.diff"] = p

    # history.annotate
    p = argparse.ArgumentParser(prog="vcs history.annotate", description="Per-line commit attribution.")
    p.add_argument("file", help="File path to annotate")
    subs["history.annotate"] = p

    # branch.create
    p = argparse.ArgumentParser(prog="vcs branch.create", description="Create a new branch.")
    p.add_argument("name", help="Branch name")
    p.add_argument("--at", metavar="HASH", dest="at_hash", help="Create branch at specific commit hash")
    subs["branch.create"] = p

    # branch.list
    p = argparse.ArgumentParser(prog="vcs branch.list", description="List all branches.")
    p.add_argument("--remote", action="store_true", help="Include remote tracking branches")
    subs["branch.list"] = p

    # branch.switch
    p = argparse.ArgumentParser(prog="vcs branch.switch", description="Switch to a branch.")
    p.add_argument("name", help="Branch name to switch to")
    subs["branch.switch"] = p

    # branch.merge
    p = argparse.ArgumentParser(prog="vcs branch.merge", description="Merge a branch into current (three-way).")
    p.add_argument("name", help="Branch name to merge in")
    p.add_argument("-m", "--message", help="Custom merge commit message")
    p.add_argument("--author", help="Author string (defaults to config core.author)")
    subs["branch.merge"] = p

    # branch.delete
    p = argparse.ArgumentParser(prog="vcs branch.delete", description="Delete a branch pointer.")
    p.add_argument("name", help="Branch name to delete")
    subs["branch.delete"] = p

    # remote.add
    p = argparse.ArgumentParser(prog="vcs remote.add", description="Register a remote.")
    p.add_argument("name", help="Remote name (e.g. origin)")
    p.add_argument("url", help="Remote URL")
    subs["remote.add"] = p

    # remote.list
    p = argparse.ArgumentParser(prog="vcs remote.list", description="List configured remotes.")
    subs["remote.list"] = p

    # remote.push
    p = argparse.ArgumentParser(prog="vcs remote.push", description="Push commits to remote.")
    p.add_argument("remote", nargs="?", default="origin", help="Remote name (default: origin)")
    p.add_argument("branch", nargs="?", default=None, help="Branch name (default: current branch)")
    subs["remote.push"] = p

    # remote.pull
    p = argparse.ArgumentParser(prog="vcs remote.pull", description="Fetch and merge from remote.")
    p.add_argument("remote", nargs="?", default="origin", help="Remote name")
    p.add_argument("branch", nargs="?", default=None, help="Branch name")
    p.add_argument("--fetch-only", action="store_true", help="Download only, do not merge")
    p.add_argument("--author", help="Author for the merge commit")
    subs["remote.pull"] = p

    # remote.fetch
    p = argparse.ArgumentParser(prog="vcs remote.fetch", description="Download remote objects without merging.")
    p.add_argument("remote", nargs="?", default="origin", help="Remote name")
    subs["remote.fetch"] = p

    # tag.create
    p = argparse.ArgumentParser(prog="vcs tag.create", description="Create an immutable tag.")
    p.add_argument("name", help="Tag name")
    p.add_argument("hash", nargs="?", default=None, help="Commit hash to tag (default: HEAD)")
    p.add_argument("-m", "--message", default="", help="Annotated tag message")
    p.add_argument("--tagger", default="", help="Tagger identity string")
    subs["tag.create"] = p

    # tag.list
    p = argparse.ArgumentParser(prog="vcs tag.list", description="List all tags.")
    subs["tag.list"] = p

    return subs


SUBPARSERS = _make_subparsers()


def parse(argv: list[str] | None = None) -> tuple[argparse.Namespace, argparse.Namespace]:
    """
    Parse *argv* into a (global_args, sub_args) tuple.

    Raises :py:exc:`SystemExit` on parse errors (standard argparse behaviour).
    """
    argv = argv or sys.argv[1:]

    global_parser = _global_parser()
    # Parse only up to the command to get global flags
    global_ns, remainder = global_parser.parse_known_args(argv)

    command = global_ns.command
    sub_parser = SUBPARSERS.get(command)
    if sub_parser is None:
        global_parser.error(
            f"Unknown command {command!r}. "
            f"Available commands: {', '.join(sorted(SUBPARSERS))}"
        )

    sub_ns = sub_parser.parse_args(remainder)
    return global_ns, sub_ns
