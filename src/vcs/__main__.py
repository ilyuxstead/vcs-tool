"""
vcs.__main__ — thin dispatcher.

Parses the noun.verb command, resolves global flags, and delegates
to the appropriate sub-tool module.  All error handling is centralised
here; sub-tools raise exceptions and this module converts them to the
correct exit codes (Section 5.3 of the SRS).
"""

from __future__ import annotations

import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    """Entry point for the ``vcs`` executable."""
    from vcs.cli.parser import parse
    from vcs.cli.output import user_error, internal_error
    from vcs.store.exceptions import (
        VCSError,
        ImmutabilityViolationError,
        AuthenticationError,
    )

    try:
        global_ns, sub_ns = parse(argv)
    except SystemExit:
        raise  # argparse already printed the message

    json_mode: bool = global_ns.json_mode
    verbose: bool = global_ns.verbose
    color: bool = not global_ns.no_color
    repo_override: str | None = global_ns.repo
    repo_root: Path | None = Path(repo_override) if repo_override else None

    command: str = global_ns.command

    try:
        _dispatch(command, global_ns, sub_ns, repo_root, json_mode=json_mode, color=color)
    except ImmutabilityViolationError as exc:
        user_error(exc.message, error_code=exc.error_code, json_mode=json_mode)
    except VCSError as exc:
        user_error(exc.message, error_code=exc.error_code, json_mode=json_mode)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        internal_error(str(exc), exc=exc, json_mode=json_mode, verbose=verbose)


def _dispatch(
    command: str,
    global_ns,
    sub_ns,
    repo_root: Path | None,
    *,
    json_mode: bool,
    color: bool,
) -> None:
    """Route a parsed command to the appropriate handler."""
    from vcs.cli.output import success, user_error, print_output, format_commit, format_status

    # ------------------------------------------------------------------
    # repo.*
    # ------------------------------------------------------------------
    if command == "repo.init":
        from vcs.repo.init import init_repo
        path = Path(sub_ns.path).resolve()
        dot_vcs = init_repo(path, bare=sub_ns.bare)
        success(f"Initialised empty VCS repository in {dot_vcs}", json_mode=json_mode)

    elif command == "repo.status":
        from vcs.repo.init import find_repo_root
        from vcs.repo.status import compute_status
        root = repo_root or find_repo_root()
        status = compute_status(root)
        if json_mode:
            import json as _json
            print(_json.dumps({
                "success": True,
                "message": "status",
                "data": {
                    "staged_new": status.staged_new,
                    "staged_modified": status.staged_modified,
                    "staged_deleted": status.staged_deleted,
                    "modified": status.modified,
                    "deleted": status.deleted,
                    "untracked": status.untracked,
                    "clean": status.is_clean,
                },
                "error_code": None,
            }))
        else:
            print(format_status(status, color=color))

    elif command == "repo.config":
        from vcs.repo.init import find_repo_root, vcs_dir
        from vcs.repo.config import (
            read_config, get_value, set_value,
            USER_CONFIG_PATH, REPO_CONFIG_NAME,
        )
        if sub_ns.global_:
            config_path = USER_CONFIG_PATH
        else:
            root = repo_root or find_repo_root()
            config_path = vcs_dir(root) / REPO_CONFIG_NAME

        if sub_ns.value is None:
            cfg = read_config(config_path)
            val = get_value(cfg, sub_ns.key)
            if json_mode:
                import json as _json
                print(_json.dumps({"success": True, "message": str(val), "data": val, "error_code": None}))
            else:
                print(val)
        else:
            # Try to coerce value type
            raw = sub_ns.value
            val = raw
            if raw.lower() == "true":
                val = True
            elif raw.lower() == "false":
                val = False
            else:
                try:
                    val = int(raw)
                except ValueError:
                    pass
            set_value(config_path, sub_ns.key, val)
            success(f"Set {sub_ns.key} = {val}", json_mode=json_mode)

    # ------------------------------------------------------------------
    # commit.*
    # ------------------------------------------------------------------
    elif command == "commit.stage":
        from vcs.repo.init import find_repo_root
        from vcs.commit.stage import stage_files, stage_all
        root = repo_root or find_repo_root()
        if sub_ns.all_:
            staged = stage_all(root)
        else:
            if not sub_ns.paths:
                user_error("Specify file paths or use --all.", json_mode=json_mode)
            staged = stage_files([Path(p) for p in sub_ns.paths], root)
        success(f"Staged {len(staged)} file(s): {', '.join(staged)}", json_mode=json_mode)

    elif command == "commit.unstage":
        from vcs.repo.init import find_repo_root
        from vcs.commit.stage import unstage_files
        root = repo_root or find_repo_root()
        removed = unstage_files([Path(p) for p in sub_ns.paths], root)
        success(f"Unstaged {len(removed)} file(s).", json_mode=json_mode)

    elif command == "commit.snapshot":
        from vcs.repo.init import find_repo_root, vcs_dir
        from vcs.repo.config import read_config, get_value
        from vcs.commit.snapshot import create_snapshot, reject_amend

        if sub_ns.amend:
            reject_amend()

        root = repo_root or find_repo_root()

        # Resolve author from flag → config → fallback
        author = sub_ns.author
        if not author:
            cfg = read_config(vcs_dir(root) / "config.toml")
            try:
                author = get_value(cfg, "core.author")
            except Exception:
                author = "Unknown <unknown@vcs>"

        commit = create_snapshot(
            message=sub_ns.message,
            author=author,
            repo_root=root,
            timestamp=getattr(sub_ns, "timestamp", None),
        )
        success(
            f"[{commit.hash[:8]}] {commit.message[:60]}",
            data=commit.to_dict(),
            json_mode=json_mode,
        )

    elif command == "commit.show":
        from vcs.repo.init import find_repo_root
        from vcs.commit.show import get_commit_detail
        root = repo_root or find_repo_root()
        detail = get_commit_detail(sub_ns.hash, root)
        if json_mode:
            import json as _json
            print(_json.dumps({"success": True, "message": "commit", "data": detail, "error_code": None}))
        else:
            lines = [
                f"commit {detail['hash']}",
                f"Author: {detail['author']}",
                f"Date:   {detail['timestamp']}",
                f"Parents: {', '.join(detail['parent_hashes']) or 'none (root commit)'}",
                "",
                f"    {detail['message']}",
                "",
            ]
            if not sub_ns.stat:
                for f in detail["files"]:
                    lines.append(f"  {f['status']:10s}  {f['path']}")
            else:
                added = sum(1 for f in detail["files"] if f["status"] == "added")
                modified = sum(1 for f in detail["files"] if f["status"] == "modified")
                deleted = sum(1 for f in detail["files"] if f["status"] == "deleted")
                lines.append(f"  {len(detail['files'])} file(s) changed: {added} added, {modified} modified, {deleted} deleted")
            print("\n".join(lines))

    # ------------------------------------------------------------------
    # history.*
    # ------------------------------------------------------------------
    elif command == "history.log":
        from vcs.repo.init import find_repo_root
        from vcs.history.log import log
        root = repo_root or find_repo_root()
        commits = log(root, branch=sub_ns.branch, limit=sub_ns.limit, author=sub_ns.author)
        if json_mode:
            import json as _json
            print(_json.dumps({
                "success": True, "message": f"{len(commits)} commit(s)",
                "data": [c.to_dict() for c in commits], "error_code": None,
            }))
        else:
            for c in commits:
                print(format_commit(c, color=color))

    elif command == "history.diff":
        from vcs.repo.init import find_repo_root, resolve_head_commit
        from vcs.history.diff import diff_commits
        root = repo_root or find_repo_root()
        hash_a = sub_ns.hash_a
        hash_b = sub_ns.hash_b
        # Default hash_a to HEAD if neither is provided
        if hash_a is None and hash_b is None:
            hash_a = resolve_head_commit(root)

        results = diff_commits(hash_a, hash_b, root, stat=sub_ns.stat, name_only=sub_ns.name_only)
        if json_mode:
            import json as _json
            print(_json.dumps({"success": True, "message": "diff", "data": results, "error_code": None}))
        else:
            for entry in results:
                print(f"--- {entry['path']} ({entry['status']})")
                if not sub_ns.name_only:
                    print("".join(entry["lines"]), end="")
                    if sub_ns.stat:
                        print(f"  +{entry['added']} -{entry['removed']}")

    elif command == "history.annotate":
        from vcs.repo.init import find_repo_root
        from vcs.history.annotate import annotate
        root = repo_root or find_repo_root()
        lines = annotate(sub_ns.file, root)
        if json_mode:
            import json as _json
            print(_json.dumps({"success": True, "message": "annotate", "data": lines, "error_code": None}))
        else:
            for entry in lines:
                print(
                    f"{entry['commit_hash']} "
                    f"({entry['author']:<20s} {entry['timestamp']}) "
                    f"{entry['line_number']:>4d}: {entry['content']}"
                )

    # ------------------------------------------------------------------
    # branch.*
    # ------------------------------------------------------------------
    elif command == "branch.create":
        from vcs.repo.init import find_repo_root
        from vcs.branch.ops import create
        root = repo_root or find_repo_root()
        branch = create(sub_ns.name, root, at_hash=sub_ns.at_hash)
        success(f"Created branch {branch.name!r} at {branch.tip_hash[:8]}", json_mode=json_mode)

    elif command == "branch.list":
        from vcs.repo.init import find_repo_root, current_branch
        from vcs.branch.ops import list_all
        root = repo_root or find_repo_root()
        branches = list_all(root)
        active = current_branch(root)
        if json_mode:
            import json as _json
            print(_json.dumps({
                "success": True, "message": "branches",
                "data": [{"name": b.name, "tip_hash": b.tip_hash, "active": b.name == active} for b in branches],
                "error_code": None,
            }))
        else:
            for b in branches:
                marker = "* " if b.name == active else "  "
                print(f"{marker}{b.name}  {b.tip_hash[:8]}")

    elif command == "branch.switch":
        from vcs.repo.init import find_repo_root
        from vcs.branch.ops import switch
        root = repo_root or find_repo_root()
        switch(sub_ns.name, root)
        success(f"Switched to branch {sub_ns.name!r}", json_mode=json_mode)

    elif command == "branch.merge":
        from vcs.repo.init import find_repo_root, vcs_dir
        from vcs.repo.config import read_config, get_value
        from vcs.branch.ops import merge_branch
        root = repo_root or find_repo_root()
        author = sub_ns.author
        if not author:
            cfg = read_config(vcs_dir(root) / "config.toml")
            try:
                author = get_value(cfg, "core.author")
            except Exception:
                author = "Unknown <unknown@vcs>"
        merge_hash = merge_branch(sub_ns.name, author, message=sub_ns.message, repo_root=root)
        success(f"Merged {sub_ns.name!r} → merge commit {merge_hash[:8]}", json_mode=json_mode)

    elif command == "branch.delete":
        from vcs.repo.init import find_repo_root
        from vcs.branch.ops import delete
        root = repo_root or find_repo_root()
        delete(sub_ns.name, root)
        success(f"Deleted branch {sub_ns.name!r}", json_mode=json_mode)

    # ------------------------------------------------------------------
    # remote.*
    # ------------------------------------------------------------------
    elif command == "remote.add":
        from vcs.repo.init import find_repo_root
        from vcs.remote.ops import add
        root = repo_root or find_repo_root()
        add(sub_ns.name, sub_ns.url, root)
        success(f"Added remote {sub_ns.name!r} → {sub_ns.url}", json_mode=json_mode)

    elif command == "remote.list":
        from vcs.repo.init import find_repo_root
        from vcs.remote.ops import list_all
        root = repo_root or find_repo_root()
        remotes = list_all(root)
        if json_mode:
            import json as _json
            print(_json.dumps({"success": True, "message": "remotes", "data": remotes, "error_code": None}))
        else:
            for r in remotes:
                print(f"{r['name']}\t{r['url']}")

    elif command == "remote.push":
        from vcs.repo.init import find_repo_root
        from vcs.remote.ops import push
        root = repo_root or find_repo_root()
        result = push(sub_ns.remote, sub_ns.branch, root)
        success(f"Pushed {result['blobs_uploaded']} object(s) to {result['remote']}", json_mode=json_mode)

    elif command == "remote.fetch":
        from vcs.repo.init import find_repo_root
        from vcs.remote.ops import fetch
        root = repo_root or find_repo_root()
        result = fetch(sub_ns.remote, root)
        success(f"Fetched {result['blobs_downloaded']} object(s) from {result['remote']}", json_mode=json_mode)

    elif command == "remote.pull":
        from vcs.repo.init import find_repo_root
        from vcs.remote.ops import pull
        root = repo_root or find_repo_root()
        result = pull(
            sub_ns.remote,
            sub_ns.branch,
            root,
            fetch_only=sub_ns.fetch_only,
        )
        success(f"Pull complete. Fetched {result['blobs_downloaded']} object(s).", json_mode=json_mode)

    # ------------------------------------------------------------------
    # tag.*
    # ------------------------------------------------------------------
    elif command == "tag.create":
        from vcs.repo.init import find_repo_root, resolve_head_commit, vcs_dir
        from vcs.repo.config import read_config, get_value
        from vcs.store.db import create_tag, open_db
        root = repo_root or find_repo_root()
        target = sub_ns.hash or resolve_head_commit(root)
        if not target:
            user_error("No commit to tag — repository has no commits.", json_mode=json_mode)
        tagger = sub_ns.tagger
        if not tagger:
            cfg = read_config(vcs_dir(root) / "config.toml")
            try:
                tagger = get_value(cfg, "core.author")
            except Exception:
                tagger = ""
        from datetime import datetime, timezone
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        conn = open_db(vcs_dir(root) / "vcs.db")
        try:
            tag = create_tag(conn, sub_ns.name, target, tagger=tagger, timestamp=ts, message=sub_ns.message)
        finally:
            conn.close()
        success(f"Created tag {tag.name!r} → {tag.target_hash[:8]}", json_mode=json_mode)

    elif command == "tag.list":
        from vcs.repo.init import find_repo_root, vcs_dir
        from vcs.store.db import list_tags, open_db
        root = repo_root or find_repo_root()
        conn = open_db(vcs_dir(root) / "vcs.db")
        try:
            tags = list_tags(conn)
        finally:
            conn.close()
        if json_mode:
            import json as _json
            print(_json.dumps({
                "success": True, "message": "tags",
                "data": [{"name": t.name, "target_hash": t.target_hash, "message": t.message} for t in tags],
                "error_code": None,
            }))
        else:
            for t in tags:
                msg = f"  {t.message}" if t.message else ""
                print(f"{t.name}  {t.target_hash[:8]}{msg}")

    else:
        user_error(f"Command {command!r} is not yet implemented.", json_mode=json_mode)


if __name__ == "__main__":
    main()
