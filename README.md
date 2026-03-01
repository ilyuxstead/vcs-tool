# VCS — Version Control System

> A modern, Python-native version control system synthesizing the best of Git, Fossil, Mercurial, and SVN — while eliminating their respective pain points.

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Coverage: 94%](https://img.shields.io/badge/coverage-94%25-brightgreen.svg)]()
[![Phase 1: Complete](https://img.shields.io/badge/Phase%201-Complete-brightgreen.svg)]()
[![Tests: Passing](https://img.shields.io/badge/tests-passing-brightgreen.svg)]()

-----

## Table of Contents

- [Overview](#overview)
- [Design Philosophy](#design-philosophy)
- [Features](#features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
- [Configuration](#configuration)
- [Storage Model](#storage-model)
- [Security](#security)
- [Testing](#testing)
- [Phase Roadmap](#phase-roadmap)
- [Non-Functional Requirements](#non-functional-requirements)
- [Contributing](#contributing)

-----

## Overview

VCS is a new version control system built from the ground up in pure Python, targeting software teams ranging from solo developers to medium-sized organizations (up to 50 engineers). It prioritizes a clean mental model, an intuitive noun-verb CLI, immutable history integrity, and built-in project management tooling in later phases.

**Phase 1 is complete.** All 23 CLI commands are functional, the system has achieved **94% line coverage** and **93.53% branch coverage** (exceeding the 90% NFR floor), and all 16 Hypothesis property-based tests pass.

-----

## Design Philosophy

VCS is built around three core principles:

**Unix Philosophy first.** Each subsystem is a small, independently testable Python module that does exactly one thing well. They are composed via a thin dispatcher rather than a monolithic codebase. If you can’t explain what a module does in one sentence, it needs to be split.

**Test-first, always.** No function ships without a corresponding test. pytest and Hypothesis property-based testing are used from the first line of production code. Coverage is enforced at the build level — the pipeline fails below 90%.

**Immutability is non-negotiable.** All commits are append-only. History is never rewritten. Every artifact is content-addressed with SHA3-256 and integrity-checked on every read. There is no `--amend`, no rebase, no force-push.

-----

## Features

### Phase 1 (Complete)

- **Immutable, append-only commit history** — no rewrites, no surprises
- **Content-addressable object store** using SHA3-256 (FIPS 202, no length-extension vulnerability)
- **Hybrid storage model** — SQLite (WAL mode) for metadata, flat files for blob objects
- **Full distributed model** — complete history on every clone; central server is optional
- **Three-way merge only** — no fast-forward merges; the DAG always tells the truth
- **Shallow clone support** (`--depth N`) — flag accepted; full semantics deferred to Phase 4
- **TOML-based configuration** — repo-level (`.vcs/config.toml`) and user-level (`~/.config/vcs/config.toml`)
- **Auth token redaction** — `VCS_AUTH_TOKEN` is never emitted to stdout or stderr
- **Self-describing CLI** — every command has `--help`; every public function has a docstring
- **Zero external runtime dependencies** — stdlib + bundled SQLite only

### Planned

- **Phase 2:** Built-in issue tracking and wiki, cross-linked with commit hashes
- **Phase 3:** Read-only WSGI web interface for repo browsing, history, diffs, and wiki rendering
- **Phase 4:** Shell tab completion, man pages, chunked blob storage (>10 MB), `git-import` tooling, PyPI packaging

-----

## Architecture

### Decision Log (Key Choices)

|Decision          |Choice                                                      |Rationale                                                                              |
|------------------|------------------------------------------------------------|---------------------------------------------------------------------------------------|
|Distribution model|Hybrid (distributed + optional central)                     |Full offline capability; central server for team sync without single point of failure  |
|History model     |Immutable / append-only                                     |Eliminates rebase accidents, corruption, force-push disasters; Fossil-proven approach  |
|Metadata storage  |SQLite (WAL mode)                                           |Queryable, single-file backup, transactional, battle-tested                            |
|Blob storage      |Content-addressable flat files                              |Scales for large files; hash-verified integrity; Git-proven approach                   |
|CLI structure     |Noun-verb (`vcs object.action`)                             |Self-documenting; maps cleanly to subsystem modules                                    |
|Hash algorithm    |SHA3-256                                                    |FIPS 202; no length-extension vulnerability; `hashlib` stdlib; zero external dependency|
|Merge strategy    |Three-way merge only                                        |Forces an honest merge commit; DAG stays truthful; no fast-forward ambiguity           |
|Config format     |TOML (`tomllib` read / minimal manual serializer for writes)|`pyproject.toml` precedent; `tomllib` in stdlib 3.11+; no external dep                 |
|Wire format       |HTTP + JSON for metadata; raw octet-stream for blob bodies  |Avoids base64 overhead on blobs while keeping metadata human-readable                  |

-----

## Project Structure

```
vcs/                          # Root of repository
├── pyproject.toml            # Build metadata, dependencies, tool config
├── README.md
├── src/
│   └── vcs/                  # Main package
│       ├── __main__.py       # Entry point: vcs dispatcher
│       ├── _version.py
│       ├── repo/             # repo.* sub-tool
│       │   ├── __init__.py
│       │   ├── init.py
│       │   ├── clone.py
│       │   ├── status.py
│       │   └── config.py     # TOML read/write helpers
│       ├── commit/           # commit.* sub-tool
│       │   ├── __init__.py
│       │   ├── stage.py
│       │   └── snapshot.py   # includes commit.show
│       ├── history/          # history.* sub-tool
│       │   ├── __init__.py
│       │   ├── log.py
│       │   ├── diff.py
│       │   └── annotate.py
│       ├── branch/           # branch.* sub-tool
│       │   ├── __init__.py
│       │   ├── ops.py
│       │   └── merge.py      # Three-way merge only
│       ├── remote/           # remote.* sub-tool
│       │   ├── __init__.py
│       │   ├── ops.py
│       │   └── protocol.py   # HTTP handshake; auth header injection; token redaction
│       ├── store/            # Object store (shared low-level layer)
│       │   ├── __init__.py
│       │   ├── objects.py    # Blob read/write, SHA3-256 hash verification
│       │   ├── db.py         # SQLite connection, schema migration
│       │   ├── models.py     # Dataclasses: Commit, Tree, Branch, Tag
│       │   └── exceptions.py # Full exception taxonomy (17 types)
│       └── cli/              # CLI helpers
│           ├── __init__.py
│           ├── parser.py     # argparse configuration for all commands
│           └── output.py     # Formatting utilities
├── tests/
│   ├── unit/                 # Unit tests per module
│   ├── integration/          # End-to-end CLI integration tests
│   └── property/             # Hypothesis property-based tests
└── tools/
    └── audit_phase1.py       # Phase 1 exit-criteria completeness audit
```

-----

## Installation

**Requirements:** Python 3.11 or higher. No external runtime dependencies.

```bash
# From PyPI (Phase 4)
pip install vcs-tool

# From source
git clone https://github.com/your-org/vcs.git
cd vcs
pip install -e .
```

Once installed, the single entry point `vcs` is available on your PATH.

```bash
vcs --help
```

-----

## Quick Start

```bash
# Initialize a new repository
vcs repo.init

# Configure your identity
vcs repo.config core.author "Your Name <you@example.com>"

# Stage files and create your first commit
vcs commit.stage --all
vcs commit.snapshot -m "Initial commit"

# Check status
vcs repo.status

# View history
vcs history.log

# Create and switch to a feature branch
vcs branch.create feature/my-feature
vcs branch.switch feature/my-feature

# Work, stage, commit...
vcs commit.stage src/mymodule.py
vcs commit.snapshot -m "Add mymodule"

# Merge back to main
vcs branch.switch main
vcs branch.merge feature/my-feature

# Add a remote and push
vcs remote.add origin https://your-server/repo
vcs remote.push origin main
```

-----

## CLI Reference

VCS uses a strict `noun.verb` command structure. Every command accepts `--help`.

### Repository (`repo.*`)

|Command                        |Description                                                      |
|-------------------------------|-----------------------------------------------------------------|
|`vcs repo.init [path]`         |Initialize a new repository in the current directory or at `path`|
|`vcs repo.clone <url> [dest]`  |Clone a remote repository; accepts `--depth N` for shallow clones|
|`vcs repo.status`              |Show working tree status (staged, modified, untracked files)     |
|`vcs repo.config <key> [value]`|Get or set a config value; `--global` targets user-level config  |

### Commit (`commit.*`)

|Command                        |Description                                                                  |
|-------------------------------|-----------------------------------------------------------------------------|
|`vcs commit.stage <paths...>`  |Stage files for the next commit; `--all` stages everything modified/untracked|
|`vcs commit.unstage <paths...>`|Remove files from staging                                                    |
|`vcs commit.snapshot -m <msg>` |Create a new commit from staged files; `--author` overrides config           |
|`vcs commit.show <hash>`       |Show commit metadata and diff; `--stat` for summary only                     |


> **Note:** `--amend` is intentionally rejected. History is immutable.

### History (`history.*`)

|Command                             |Description                                                              |
|------------------------------------|-------------------------------------------------------------------------|
|`vcs history.log`                   |List commit history; `--branch`, `--limit`, `--author` filters available |
|`vcs history.diff [hash_a] [hash_b]`|Diff between two commits or working tree vs HEAD; `--stat`, `--name-only`|
|`vcs history.annotate <file>`       |Show per-line commit attribution (blame)                                 |

### Branch (`branch.*`)

|Command                   |Description                                                    |
|--------------------------|---------------------------------------------------------------|
|`vcs branch.create <name>`|Create a new branch at HEAD                                    |
|`vcs branch.list`         |List all branches                                              |
|`vcs branch.switch <name>`|Switch to a branch                                             |
|`vcs branch.merge <name>` |Merge a branch into the current branch (three-way merge always)|
|`vcs branch.delete <name>`|Delete a branch                                                |

### Remote (`remote.*`)

|Command                            |Description                             |
|-----------------------------------|----------------------------------------|
|`vcs remote.add <name> <url>`      |Register a remote                       |
|`vcs remote.list`                  |List configured remotes                 |
|`vcs remote.push <remote> <branch>`|Push a branch to a remote               |
|`vcs remote.fetch <remote>`        |Fetch refs from a remote without merging|
|`vcs remote.pull <remote> <branch>`|Fetch and merge from a remote           |

### Tags (`tag.*`)

|Command                       |Description                                |
|------------------------------|-------------------------------------------|
|`vcs tag.create <name> [hash]`|Create a tag at a commit (defaults to HEAD)|
|`vcs tag.list`                |List all tags                              |

-----

## Configuration

Configuration uses TOML format. VCS reads and merges two levels:

- **Repository-level:** `.vcs/config.toml` (repo root)
- **User-level:** `~/.config/vcs/config.toml`

Repository config takes precedence over user config.

```toml
# Example .vcs/config.toml
[core]
author = "G. Grasham <g@example.com>"

[remote.origin]
url = "https://your-server/repo"
```

Read and write config via the CLI:

```bash
vcs repo.config core.author                          # get
vcs repo.config core.author "Name <email>"           # set (repo-level)
vcs repo.config --global core.author "Name <email>"  # set (user-level)
```

Config is read using Python 3.11+ stdlib `tomllib`. Writes use a purpose-built minimal TOML serializer — no external dependency required.

-----

## Storage Model

VCS uses a hybrid storage model designed for integrity and performance:

**SQLite (WAL mode)** stores all structured metadata: commits, trees, branches, tags, the staging index, and remote refs. WAL mode ensures readers never block writers. All writes are atomic — either a SQLite transaction or a temp-then-rename file operation. There is no in-between state that can corrupt the repository.

**Content-addressable flat files** store blob objects. Every blob is named by its SHA3-256 hash. Blobs are verified on every read — corruption is surfaced immediately, never silently ignored.

Blobs exceeding 10 MB are accepted and stored inline, but VCS emits a warning to stderr. Chunked storage is planned for Phase 4.

```
.vcs/
├── config.toml
├── vcs.db              # SQLite: all metadata
└── objects/            # Content-addressable blobs
    ├── ab/
    │   └── ab3f...     # First 2 chars = directory, remainder = filename
    └── ...
```

-----

## Security

- No `eval()`, no `shell=True` in subprocess calls, no `pickle` for object serialization.
- `VCS_AUTH_TOKEN` is redacted from all log output, error messages, and URLs before surfacing. Redaction is enforced at the `protocol.py` layer via `_redact()` and verified by a Hypothesis property test.
- SHA3-256 is used for all content addressing. It has no length-extension vulnerability and is available in Python’s stdlib `hashlib` with no external dependency.

-----

## Testing

VCS is built test-first. The test suite has three layers:

**Unit tests** (`tests/unit/`) cover individual functions and modules in isolation using mocks and fixtures.

**Integration tests** (`tests/integration/`) exercise full end-to-end CLI workflows against real temporary repositories on disk.

**Property-based tests** (`tests/property/`) use Hypothesis to generate adversarial inputs and verify invariants such as token redaction, clone branch parity, fetch monotonicity, and push key presence.

### Running the Suite

```bash
# Run all tests with coverage
pytest --cov=src/vcs --cov-report=term-missing --cov-branch

# Run only unit tests
pytest tests/unit/

# Run only property tests
pytest tests/property/

# Run the Phase 1 completeness audit
python tools/audit_phase1.py
```

### Coverage

The coverage gate is enforced in `pyproject.toml` — the build fails below 90%.

```
Phase 1 exit coverage:
  Line coverage:    94.00%
  Branch coverage:  93.53%
  Hypothesis tests: 16/16 passing
  CLI commands:     23/23 functional
```

### Key Property-Based Invariants Tested

- **Token redaction** — no sequence of valid CLI invocations causes `VCS_AUTH_TOKEN` to appear in stdout or stderr
- **Clone branch parity** — after `repo.clone`, local branch names exactly equal remote ref keys
- **Fetch monotonicity** — after `remote.fetch`, branch count never decreases
- **Push key presence** — push response always contains all required protocol keys
- **Hash integrity** — any mutation of a blob body causes hash verification to fail on read

-----

## Phase Roadmap

|Phase                      |Scope                                                                                                                                                           |Status        |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
|**Phase 1 — Core VCS**     |Object store, SQLite schema, repo init/clone, commit stage/snapshot/show, history log/diff/annotate, branch CRUD + three-way merge, remote push/pull/fetch, tags|✅ **Complete**|
|**Phase 2 — Issues + Wiki**|`issue.*` and `wiki.*` commands; stored in SQLite; cross-linked with commit hashes; branch/merge.py coverage gap resolved                                       |🔲 Planned     |
|**Phase 3 — Web UI**       |Read-only WSGI interface for repo browsing, commit history, diff view, issue list, wiki rendering; deployable with gunicorn                                     |🔲 Planned     |
|**Phase 4 — Polish**       |Shell tab completion, man pages, performance profiling, chunked blob storage (>10 MB), `git-import`, full `--depth` shallow clone semantics, PyPI release       |🔲 Planned     |

-----

## Non-Functional Requirements

|ID    |Category     |Requirement                                                                                              |Status        |
|------|-------------|---------------------------------------------------------------------------------------------------------|--------------|
|NFR-01|Performance  |`repo.status` on 10,000 files completes in under 1 second                                                |✅ Met         |
|NFR-02|Performance  |`history.log --limit 100` completes in under 500ms regardless of history depth                           |✅ Met         |
|NFR-03|Reliability  |No data loss on interrupted operations; all writes are atomic                                            |✅ Met         |
|NFR-04|Correctness  |Object store hash verified on every read; corruption immediately surfaced                                |✅ Met         |
|NFR-05|Portability  |Linux and macOS supported; Windows is a stretch goal                                                     |✅ Met         |
|NFR-06|Packaging    |Installable via `pip install vcs-tool`; single entry point: `vcs`                                        |🔲 Phase 4     |
|NFR-07|Compatibility|Python 3.11+; no external runtime dependencies beyond stdlib + SQLite                                    |✅ Met         |
|NFR-08|Test Coverage|Minimum 90% line and branch coverage; all public API functions have at least one Hypothesis property test|✅ 94% / 93.53%|
|NFR-09|Security     |No `eval()`, no `shell=True`, no `pickle`; `VCS_AUTH_TOKEN` redacted from all output                     |✅ Met         |
|NFR-10|Documentation|Every CLI command has `--help`; every public Python function has a docstring                             |✅ Met         |

-----

## Contributing

Contributions are welcome. The bar is high by design.

**Before opening a PR:**

1. Every new function must have a docstring.
1. Every new function must have at least one unit test.
1. Property-based tests are required for any function that handles user-supplied strings, hashes, or network data.
1. Coverage must not drop below 90% line or branch. The CI pipeline will reject the PR automatically.
1. No `shell=True`. No `eval()`. No `pickle`. No external runtime dependencies without a very strong justification.
1. Run the full suite locally before pushing: `pytest --cov=src/vcs --cov-report=term-missing --cov-branch`

**Style:** Python 3.11+, type annotations on all public functions, `dataclasses` for data models, `argparse` for CLI, `tomllib` for config parsing.

-----

*VCS — SRS v0.3 · February 2026 · Phase 1 Complete*
