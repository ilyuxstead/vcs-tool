"""vcs.store — content-addressable object store and SQLite metadata layer."""

from .db import open_db
from .exceptions import (
    AuthenticationError,
    BranchExistsError,
    BranchNotFoundError,
    CommitNotFoundError,
    ConfigError,
    ImmutabilityViolationError,
    MergeConflictError,
    ObjectCorruptionError,
    ObjectNotFoundError,
    RemoteError,
    RepositoryExistsError,
    RepositoryNotFoundError,
    StagingError,
    TagExistsError,
    TagNotFoundError,
    VCSError,
)
from .models import Branch, Commit, Tag, Tree, TreeEntry
from .objects import ObjectStore

__all__ = [
    # Core store
    "ObjectStore",
    "open_db",
    # Models
    "Commit",
    "Tree",
    "TreeEntry",
    "Branch",
    "Tag",
    # Exceptions
    "VCSError",
    "ObjectCorruptionError",
    "ObjectNotFoundError",
    "RepositoryNotFoundError",
    "RepositoryExistsError",
    "BranchNotFoundError",
    "BranchExistsError",
    "CommitNotFoundError",
    "MergeConflictError",
    "StagingError",
    "ConfigError",
    "RemoteError",
    "AuthenticationError",
    "TagNotFoundError",
    "TagExistsError",
    "ImmutabilityViolationError",
]
