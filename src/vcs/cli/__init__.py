"""vcs.cli — argument parsing and output formatting."""

from .parser import parse, SUBPARSERS
from .output import success, user_error, internal_error, print_output

__all__ = ["parse", "SUBPARSERS", "success", "user_error", "internal_error", "print_output"]
