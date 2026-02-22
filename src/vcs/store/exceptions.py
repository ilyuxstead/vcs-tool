"""
vcs.store.exceptions — all domain-specific exception types.

Every exception carries a human-readable message suitable for display
on stderr and an optional error_code string for --json output.
"""


class VCSError(Exception):
    """Base class for all VCS errors."""

    error_code: str = "VCS_ERROR"

    def __init__(self, message: str, error_code: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        if error_code is not None:
            self.error_code = error_code


class ObjectCorruptionError(VCSError):
    """Raised when a stored object's hash does not match its content."""

    error_code = "OBJECT_CORRUPTION"


class ObjectNotFoundError(VCSError):
    """Raised when a requested object hash does not exist in the store."""

    error_code = "OBJECT_NOT_FOUND"


class RepositoryNotFoundError(VCSError):
    """Raised when no .vcs/ directory can be located in the path hierarchy."""

    error_code = "REPO_NOT_FOUND"


class RepositoryExistsError(VCSError):
    """Raised when attempting to init a repo that already exists."""

    error_code = "REPO_EXISTS"


class BranchNotFoundError(VCSError):
    """Raised when a named branch does not exist."""

    error_code = "BRANCH_NOT_FOUND"


class BranchExistsError(VCSError):
    """Raised when a branch name is already in use."""

    error_code = "BRANCH_EXISTS"


class CommitNotFoundError(VCSError):
    """Raised when a commit hash cannot be resolved."""

    error_code = "COMMIT_NOT_FOUND"


class MergeConflictError(VCSError):
    """Raised when a three-way merge produces unresolvable conflicts."""

    error_code = "MERGE_CONFLICT"

    def __init__(self, message: str, conflicted_files: list[str] | None = None) -> None:
        super().__init__(message)
        self.conflicted_files: list[str] = conflicted_files or []


class StagingError(VCSError):
    """Raised for errors in the staging area."""

    error_code = "STAGING_ERROR"


class ConfigError(VCSError):
    """Raised for configuration read/write errors."""

    error_code = "CONFIG_ERROR"


class RemoteError(VCSError):
    """Raised for remote sync errors (network, auth, divergence)."""

    error_code = "REMOTE_ERROR"


class AuthenticationError(RemoteError):
    """Raised when VCS_AUTH_TOKEN is missing or rejected."""

    error_code = "AUTH_ERROR"


class TagNotFoundError(VCSError):
    """Raised when a tag does not exist."""

    error_code = "TAG_NOT_FOUND"


class TagExistsError(VCSError):
    """Raised when a tag name is already in use (tags are immutable)."""

    error_code = "TAG_EXISTS"


class ImmutabilityViolationError(VCSError):
    """Raised when code attempts a forbidden mutation (amend, rebase, etc.)."""

    error_code = "IMMUTABILITY_VIOLATION"
