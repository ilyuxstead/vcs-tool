"""vcs.history — DAG traversal: log, diff, annotate."""

from .log import log
from .diff import diff_commits
from .annotate import annotate

__all__ = ["log", "diff_commits", "annotate"]
