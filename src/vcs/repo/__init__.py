"""vcs.repo — repository lifecycle: init, clone, status, config."""

from .init import (
    DEFAULT_BRANCH,
    VCS_DIR,
    current_branch,
    find_repo_root,
    init_repo,
    read_head,
    resolve_head_commit,
    vcs_dir,
    write_head,
)
from .config import (
    read_config,
    resolve_config,
    get_value,
    set_value,
    write_config,
    USER_CONFIG_PATH,
    REPO_CONFIG_NAME,
)
from .status import (
    WorkingTreeStatus,
    compute_status,
    read_index,
    write_index,
)
from .clone import clone_repo
__all__ = [
    "DEFAULT_BRANCH",
    "VCS_DIR",
    "current_branch",
    "find_repo_root",
    "init_repo",
    "read_head",
    "resolve_head_commit",
    "vcs_dir",
    "write_head",
    "read_config",
    "resolve_config",
    "get_value",
    "set_value",
    "write_config",
    "USER_CONFIG_PATH",
    "REPO_CONFIG_NAME",
    "WorkingTreeStatus",
    "compute_status",
    "read_index",
    "write_index",
    "clone_repo",
]
