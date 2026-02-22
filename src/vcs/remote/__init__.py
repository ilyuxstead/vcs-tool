"""vcs.remote — remote management: add, list, push, pull, fetch."""

from .ops import add, fetch, list_all, pull, push
from .protocol import RemoteClient

__all__ = ["add", "fetch", "list_all", "pull", "push", "RemoteClient"]
