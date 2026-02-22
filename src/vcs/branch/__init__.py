"""vcs.branch — branch lifecycle: create, list, switch, merge, delete."""

from .ops import create, delete, list_all, merge_branch, switch
from .merge import three_way_merge

__all__ = ["create", "delete", "list_all", "merge_branch", "switch", "three_way_merge"]
