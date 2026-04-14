# Bidirectional Folder Sync — Design Document

> **Version:** 1.0  
> **Date:** April 14, 2026  
> A smart two-way sync engine that propagates creates, updates, and deletes between two folders — keeping both sides consistent while preserving new files on either end.

---

## Core Concepts

| Concept | Value | Notes |
|---|---|---|
| Sync model | State-based | Snapshot comparison, not filesystem watchers |
| State tracking | `.sync_state.json` | Persisted manifest of last-known state |
| Conflict strategy | Newest wins | With optional `.conflict` copy |
| Deletion safety | Trash folder | Soft-delete with configurable retention |

---

## Architecture

The engine uses a **three-point comparison** model. On every run it builds a fresh snapshot of both folders, then diffs each against the *previous state* to classify every file path as created, modified, deleted, or unchanged.

### 1. State File `.sync_state.json`

Stores a per-file record with path, size, mtime, and content hash (xxhash64 for speed). Lives alongside the script, not inside either folder. This is the "last known truth" — the baseline both sides are compared against.

### 2. Snapshot Builder

Walks both folder trees in parallel (using thread pools for I/O). Produces two maps: `{relative_path → FileRecord}`. Respects `.syncignore` glob patterns.

### 3. Diff Engine

Compares each snapshot against state file to produce two change-sets. Each file path gets a classification: `CREATED`, `MODIFIED`, `DELETED`, or `UNCHANGED`.

### 4. Reconciler

Merges the two change-sets using the decision matrix (see below). Produces an ordered action list: copies, deletes, conflict resolutions. Applies actions, then writes the new state file atomically.

---

## Decision Matrix

Every file path produces a pair of statuses (Folder A change, Folder B change). This matrix defines the action for each combination.

| Folder A | Folder B | Action | Rationale |
|---|---|---|---|
| **Created** | Absent | Copy A → B | New file, propagate |
| Absent | **Created** | Copy B → A | New file, propagate |
| **Modified** | Unchanged | Copy A → B | Only one side changed |
| Unchanged | **Modified** | Copy B → A | Only one side changed |
| **Modified** | **Modified** | ⚠ **Conflict** | Both changed — newest wins + `.conflict` copy |
| **Deleted** | Unchanged | Delete from B (→ trash) | Intentional delete, propagate |
| Unchanged | **Deleted** | Delete from A (→ trash) | Intentional delete, propagate |
| **Deleted** | **Modified** | Keep B's version, restore to A | Edit wins over delete |
| **Modified** | **Deleted** | Keep A's version, restore to B | Edit wins over delete |
| **Deleted** | **Deleted** | Remove from state | Both agree — purge record |
| **Created** | **Created** | ⚠ **Conflict** | Same path created both sides — compare content |

---

## Sync Phases

Each sync run executes these phases in strict order. Failures at any phase abort and leave state unchanged (atomic commit).

### Phase 1 — Lock Acquisition

Obtain an exclusive file lock (`.sync.lock`) to prevent concurrent runs. Timeout after 30s with clear error.

### Phase 2 — Snapshot Both Folders

Parallel tree walk with hash computation. Skip files matching `.syncignore`. Build `Map<path, FileRecord>` for each side.

### Phase 3 — Diff Against Saved State

For each file in the union of snapshot + state: classify as `CREATED` (in snapshot, not in state), `MODIFIED` (hash differs), `DELETED` (in state, not in snapshot), or `UNCHANGED`.

### Phase 4 — Reconcile & Plan Actions

Cross-reference the two change-sets using the decision matrix. Output an ordered action queue: copies first, then deletes, then conflict resolutions.

### Phase 5 — Execute & Commit

Apply file operations with atomic writes (write to temp, then rename). On success, write the merged snapshot as the new state file. On any failure, roll back and leave state untouched.

### Phase 6 — Report & Release Lock

Log summary (files copied, deleted, conflicts). Write structured JSON log for audit. Release lock file.

---

## CLI Interface

```bash
# Basic two-way sync
bisync --folder-a /path/to/a --folder-b /path/to/b

# Dry run — show what would happen
bisync --folder-a ./a --folder-b ./b --dry-run

# Keep conflict copies (default: newest wins silently)
bisync --folder-a ./a --folder-b ./b --on-conflict keep-both

# Disable delete propagation (never delete on sync)
bisync --folder-a ./a --folder-b ./b --no-delete

# Use trash folder with 7-day retention
bisync --folder-a ./a --folder-b ./b --trash-days 7

# Initial sync (no prior state — treat all files as new)
bisync --folder-a ./a --folder-b ./b --init

# Verbose logging
bisync --folder-a ./a --folder-b ./b -v
```

### Configuration Flags

| Flag | Default | Description |
|---|---|---|
| `--folder-a` | *required* | First sync folder path |
| `--folder-b` | *required* | Second sync folder path |
| `--state-file` | `.sync_state.json` | Path to the state persistence file |
| `--on-conflict` | `newest` | `newest` \| `keep-both` \| `skip` \| `ask` |
| `--no-delete` | `false` | Never propagate deletions |
| `--trash-days` | `30` | Days to keep trashed files (0 = permanent delete) |
| `--dry-run` | `false` | Preview actions without applying |
| `--init` | `false` | First run — build state from scratch |
| `--ignore` | `.syncignore` | Path to ignore patterns file |
| `-v` | `false` | Verbose output |

---

## Key Design Decisions

### Safety

- **Edit wins over delete** — If one side deletes a file while the other modifies it, the modified version survives and is restored to both sides. Data preservation over cleanup.
- **Soft deletes by default** — Propagated deletions move files to `.sync_trash/` with a timestamp prefix. Configurable retention period before permanent removal.

### Performance

- **Hash-based change detection** — Uses xxhash64 content hashing instead of mtime-only comparison. Catches renames, re-saves without changes, and clock drift between machines.
- **Fast-path via mtime pre-filter** — Only hashes files whose mtime or size changed since last state. For a 10k-file folder where 5 changed, this skips 9,995 hash computations.

### Robustness

- **Atomic state commits** — State file is written to a temp path and renamed. If the process crashes mid-sync, the old state is intact and the next run re-does the work cleanly.
- **First-run seeding** — On `--init`, both folders are scanned and merged (union of all files). No state file means nothing is classified as "deleted" — only creates and conflicts.

---

## Data Structures

### FileRecord

```json
{
  "path":     "docs/readme.md",
  "size":     4821,
  "mtime":    1713100800.0,
  "hash":     "a1b2c3d4e5f6a7b8",
  "is_dir":   false
}
```

### State File Structure

```json
{
  "version":    2,
  "last_sync":  "2026-04-14T10:30:00Z",
  "folder_a":   "/home/user/projects",
  "folder_b":   "/mnt/backup/projects",
  "files": {
    "docs/readme.md": { "path": "docs/readme.md", "size": 4821, "mtime": 1713100800.0, "hash": "a1b2c3d4e5f6a7b8", "is_dir": false },
    "src/main.py":    { "path": "src/main.py", "size": 1200, "mtime": 1713100500.0, "hash": "f8e7d6c5b4a39281", "is_dir": false }
  }
}
```

---

## Edge Cases Handled

| Scenario | Behavior |
|---|---|
| Symlinks | Followed by default. `--no-follow-symlinks` to skip. |
| Empty directories | Synced as entries in state. Created/deleted as needed. |
| Permission errors | Logged and skipped. Doesn't block other files. |
| File in use / locked | Retry up to 3× with 1s backoff, then skip with warning. |
| Path too long (>260 chars) | Warned on Windows. Handled natively on Linux/macOS. |
| Case-sensitive vs insensitive FS | Normalize paths to lowercase on case-insensitive systems. |
| Large files (>1 GB) | Streamed copy with progress. Chunked hashing (64 KB blocks). |
| Interrupted previous sync | Lock file detected → warn and offer `--force-unlock`. |
| Clock skew between machines | Hash comparison is authoritative. Mtime used only as fast-path filter. |