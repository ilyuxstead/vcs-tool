"""
tests/integration/test_branch_flow.py — branch + merge (3-way) end-to-end.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from vcs.branch.ops import create, switch, merge_branch, list_all
from vcs.commit.snapshot import create_snapshot
from vcs.commit.stage import stage_files
from vcs.history.log import log
from vcs.repo.init import current_branch, resolve_head_commit
from vcs.store.exceptions import MergeConflictError


def _commit(root: Path, fname: str, content: str, author: str, msg: str):
    f = root / fname
    f.write_text(content)
    stage_files([f], root)
    return create_snapshot(msg, author, root)


class TestBranchFlow:
    def test_create_switch_commit(self, tmp_repo: Path, author: str):
        _commit(tmp_repo, "base.txt", "base", author, "base")

        create("feature", tmp_repo)
        switch("feature", tmp_repo)
        assert current_branch(tmp_repo) == "feature"

        _commit(tmp_repo, "feature.txt", "feature work", author, "feature commit")

        switch("main", tmp_repo)
        assert current_branch(tmp_repo) == "main"

    def test_merge_creates_commit_with_two_parents(self, tmp_repo: Path, author: str):
        from vcs.store.db import get_commit, open_db
        from vcs.repo.init import vcs_dir

        _commit(tmp_repo, "base.txt", "base", author, "base")
        create("feature", tmp_repo)
        switch("feature", tmp_repo)
        _commit(tmp_repo, "new.txt", "new feature", author, "feature work")
        switch("main", tmp_repo)

        merge_hash = merge_branch("feature", author, repo_root=tmp_repo)
        conn = open_db(vcs_dir(tmp_repo) / "vcs.db")
        try:
            mc = get_commit(conn, merge_hash)
            assert len(mc.parent_hashes) == 2
        finally:
            conn.close()

    def test_merged_branch_files_visible_on_main(self, tmp_repo: Path, author: str):
        _commit(tmp_repo, "main.txt", "main", author, "main file")
        create("feat", tmp_repo)
        switch("feat", tmp_repo)
        _commit(tmp_repo, "feat.txt", "feature", author, "feature file")
        switch("main", tmp_repo)
        merge_branch("feat", author, repo_root=tmp_repo)

        assert (tmp_repo / "main.txt").exists()
        assert (tmp_repo / "feat.txt").exists()

    def test_conflict_writes_markers_to_working_tree(self, tmp_repo: Path, author: str):
        _commit(tmp_repo, "shared.txt", "original content\n", author, "initial")
        create("feat", tmp_repo)

        # Modify on main
        (tmp_repo / "shared.txt").write_text("main version\n")
        stage_files([tmp_repo / "shared.txt"], tmp_repo)
        create_snapshot("main edit", author, tmp_repo)

        # Modify differently on feat
        switch("feat", tmp_repo)
        (tmp_repo / "shared.txt").write_text("feature version\n")
        stage_files([tmp_repo / "shared.txt"], tmp_repo)
        create_snapshot("feat edit", author, tmp_repo)

        switch("main", tmp_repo)
        with pytest.raises(MergeConflictError) as exc_info:
            merge_branch("feat", author, repo_root=tmp_repo)

        assert "shared.txt" in exc_info.value.conflicted_files
        content = (tmp_repo / "shared.txt").read_text()
        assert "<<<<<<< ours" in content

    def test_history_log_on_branch(self, tmp_repo: Path, author: str):
        _commit(tmp_repo, "a.txt", "a", author, "main commit")
        create("feat", tmp_repo)
        switch("feat", tmp_repo)
        _commit(tmp_repo, "b.txt", "b", author, "feat commit")

        feat_log = log(tmp_repo, branch="feat")
        messages = [c.message for c in feat_log]
        assert "feat commit" in messages
        assert "main commit" in messages

    def test_no_fast_forward_always_merge_commit(self, tmp_repo: Path, author: str):
        """Even when the merge could be fast-forwarded, we always create a merge commit."""
        _commit(tmp_repo, "a.txt", "a", author, "init")
        create("feat", tmp_repo)
        switch("feat", tmp_repo)
        c_feat = _commit(tmp_repo, "b.txt", "b", author, "feat only")
        switch("main", tmp_repo)

        merge_hash = merge_branch("feat", author, repo_root=tmp_repo)
        # The merge hash must differ from the feature tip
        assert merge_hash != c_feat.hash
