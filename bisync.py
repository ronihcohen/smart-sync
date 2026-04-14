#!/usr/bin/env python3
import os
import sys
import errno
import time
import json
import fcntl
import shutil
import fnmatch
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import xxhash

logger = logging.getLogger("bisync")

@dataclass
class FileRecord:
    path: str
    size: int
    mtime: float
    hash: str
    is_dir: bool

class SyncState:
    def __init__(self, state_file_path: Path):
        self.state_file_path = state_file_path
        self.version = 2
        self.last_sync = ""
        self.folder_a = ""
        self.folder_b = ""
        self.files = {}

    def load(self):
        if not self.state_file_path.exists():
            return
        try:
            with open(self.state_file_path, 'r') as f:
                data = json.load(f)
                self.version = data.get("version", 2)
                self.last_sync = data.get("last_sync", "")
                self.folder_a = data.get("folder_a", "")
                self.folder_b = data.get("folder_b", "")
                self.files = {}
                for k, v in data.get("files", {}).items():
                    self.files[k] = FileRecord(**v)
        except Exception as e:
            logger.error(f"Failed to load state file: {e}")
            sys.exit(1)

    def save(self):
        data = {
            "version": self.version,
            "last_sync": datetime.now(timezone.utc).isoformat(),
            "folder_a": self.folder_a,
            "folder_b": self.folder_b,
            "files": {k: asdict(v) for k, v in self.files.items()}
        }
        tmp_path = self.state_file_path.with_name(self.state_file_path.name + ".tmp")
        try:
            with open(tmp_path, 'w') as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_path, self.state_file_path)
            logger.debug("State saved.")
        except Exception as e:
            logger.error(f"Failed to save state file: {e}")
            if tmp_path.exists():
                tmp_path.unlink()
            sys.exit(1)

def hash_file(filepath: Path) -> str:
    h = xxhash.xxh64()
    with open(filepath, 'rb') as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

def is_ignored(path_str: str, ignores: list) -> bool:
    for pattern in ignores:
        if fnmatch.fnmatch(path_str, pattern) or fnmatch.fnmatch(os.path.basename(path_str), pattern):
            return True
    return False

class BiSyncEngine:
    def __init__(self, args):
        self.args = args
        self.folder_a = Path(args.folder_a).resolve()
        self.folder_b = Path(args.folder_b).resolve()
        self.state_file = Path(args.state_file).resolve()
        self.state = SyncState(self.state_file)
        self.lock_file = self.state_file.with_name(".sync.lock")
        self.lock_fd = None
        self.ignores = self.load_ignores(args.ignore)
        
        # Trash setup
        self.trash_a = self.folder_a / ".sync_trash"
        self.trash_b = self.folder_b / ".sync_trash"
        self.ignores.extend([".sync_trash", ".syncignore", ".sync.lock", self.state_file.name])

    def load_ignores(self, ignore_file: str) -> list:
        ignores = []
        path = Path(ignore_file)
        if path.exists():
            with open(path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        ignores.append(line)
        return ignores

    def run(self):
        logger.info(f"Starting bisync run...")
        logger.info(f"Folder A: {self.folder_a}")
        logger.info(f"Folder B: {self.folder_b}")
        
        self.phase1_lock()
        try:
            if self.args.init:
                logger.info("Init mode: starting fresh.")
                if self.state_file.exists():
                    self.state_file.unlink()
            else:
                self.state.load()
            
            self.state.folder_a = str(self.folder_a)
            self.state.folder_b = str(self.folder_b)

            logger.info("Phase 2: Snapshotting folders...")
            snap_a = self.phase2_snapshot(self.folder_a)
            snap_b = self.phase2_snapshot(self.folder_b)

            logger.info("Phase 3: Diffs against state...")
            diff_a = self.phase3_diff(snap_a)
            diff_b = self.phase3_diff(snap_b)

            logger.info("Phase 4: Reconciling change-sets...")
            actions = self.phase4_reconcile(snap_a, snap_b, diff_a, diff_b)
            
            logger.info(f"Phase 5: Execution plan - {len(actions)} actions")
            if not self.args.dry_run:
                self.phase5_execute(actions, snap_a, snap_b)
                self.state.save()
            else:
                logger.info("Dry run: skipping execution.")

            logger.info("Sync complete.")
        finally:
            self.phase6_release_lock()

    def phase1_lock(self):
        start_time = time.time()
        while True:
            try:
                self.lock_fd = os.open(self.lock_file, os.O_RDWR | os.O_CREAT | os.O_TRUNC)
                fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                logger.debug("Acquired lock.")
                break
            except (IOError, OSError) as e:
                if e.errno != errno.EAGAIN:
                    logger.error(f"Error acquiring lock: {e}")
                    sys.exit(1)
                if time.time() - start_time > 30:
                    logger.error("Timeout: Could not acquire lock file after 30s.")
                    sys.exit(1)
                time.sleep(1)

    def phase6_release_lock(self):
        if self.lock_fd is not None:
            fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
            os.close(self.lock_fd)
            try:
                self.lock_file.unlink()
            except Exception:
                pass
            logger.debug("Released lock.")

    def phase2_snapshot(self, folder: Path) -> dict:
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
            return {}

        snapshot = {}
        paths_to_hash = []

        # Find all files
        for root, dirs, files in os.walk(folder):
            # Exclude ignored dirs inplace
            dirs[:] = [d for d in dirs if not is_ignored(d, self.ignores)]
            
            for name in dirs + files:
                p = Path(root) / name
                rel_path = p.relative_to(folder).as_posix()
                if is_ignored(rel_path, self.ignores):
                    continue
                paths_to_hash.append((p, rel_path))

        def process_path(item):
            p, rel = item
            try:
                stat = p.stat()
                is_dir = p.is_dir()
                rec = FileRecord(
                    path=rel,
                    size=stat.st_size if not is_dir else 0,
                    mtime=stat.st_mtime,
                    hash="",
                    is_dir=is_dir
                )
                if not is_dir:
                    prev = self.state.files.get(rel)
                    if prev and not self.args.init and prev.size == rec.size and prev.mtime == rec.mtime:
                        rec.hash = prev.hash
                    else:
                        rec.hash = hash_file(p)
                return rel, rec
            except Exception as e:
                logger.warning(f"Failed to process {p}: {e}")
                return None

        with ThreadPoolExecutor() as executor:
            for result in executor.map(process_path, paths_to_hash):
                if result:
                    rel, rec = result
                    snapshot[rel] = rec
        
        return snapshot

    def phase3_diff(self, snapshot: dict) -> dict:
        changes = {}
        all_paths = set(snapshot.keys()) | set(self.state.files.keys())
        
        for p in all_paths:
            in_snap = p in snapshot
            in_state = p in self.state.files
            
            if in_snap and not in_state:
                changes[p] = "CREATED"
            elif not in_snap and in_state:
                changes[p] = "DELETED"
            elif in_snap and in_state:
                snap_rec = snapshot[p]
                state_rec = self.state.files[p]
                if snap_rec.hash != state_rec.hash or snap_rec.is_dir != state_rec.is_dir:
                    changes[p] = "MODIFIED"
                else:
                    changes[p] = "UNCHANGED"
        return changes

    def phase4_reconcile(self, snap_a: dict, snap_b: dict, diff_a: dict, diff_b: dict) -> list:
        actions = []
        all_paths = set(diff_a.keys()) | set(diff_b.keys())
        
        for p in all_paths:
            sa = diff_a.get(p, "UNCHANGED")
            sb = diff_b.get(p, "UNCHANGED")
            
            if sa == "UNCHANGED" and sb == "UNCHANGED":
                continue

            if sa == "CREATED" and sb in ("UNCHANGED", "DELETED"):
                actions.append(("copy", "a_to_b", p))
            elif sa in ("UNCHANGED", "DELETED") and sb == "CREATED":
                actions.append(("copy", "b_to_a", p))
            elif sa == "MODIFIED" and sb == "UNCHANGED":
                actions.append(("copy", "a_to_b", p))
            elif sa == "UNCHANGED" and sb == "MODIFIED":
                actions.append(("copy", "b_to_a", p))
            elif sa == "MODIFIED" and sb == "MODIFIED":
                if snap_a[p].hash == snap_b[p].hash:
                    # same modification manually on both sides
                    actions.append(("merge_state", p, "a"))
                else:
                    actions.append(("conflict", p))
            elif sa == "DELETED" and sb == "UNCHANGED":
                actions.append(("delete", "b", p))
            elif sa == "UNCHANGED" and sb == "DELETED":
                actions.append(("delete", "a", p))
            elif sa == "DELETED" and sb == "MODIFIED":
                actions.append(("copy", "b_to_a", p))
            elif sa == "MODIFIED" and sb == "DELETED":
                actions.append(("copy", "a_to_b", p))
            elif sa == "DELETED" and sb == "DELETED":
                actions.append(("remove_state", p))
            elif sa == "CREATED" and sb == "CREATED":
                if snap_a[p].hash == snap_b[p].hash:
                    actions.append(("merge_state", p, "a"))
                else:
                    actions.append(("conflict", p))

        # order actions: copies, then conflicts, then deletes, then metadata
        ordered = []
        for a in actions:
            if a[0] == "copy": ordered.append(a)
        for a in actions:
            if a[0] == "conflict": ordered.append(a)
        for a in actions:
            if a[0] == "delete": ordered.append(a)
        for a in actions:
            if a[0] in ("merge_state", "remove_state"): ordered.append(a)
            
        return ordered

    def safe_copy(self, src: Path, dst: Path, is_dir: bool):
        if is_dir:
            dst.mkdir(parents=True, exist_ok=True)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            tmp_dst = dst.with_name(dst.name + ".tmp")
            shutil.copy2(src, tmp_dst)
            os.replace(tmp_dst, dst)

    def trash(self, tgt: Path, folder_trash: Path, rel_path: str):
        if not tgt.exists():
            return
        if self.args.no_delete:
            logger.info(f"Skipping delete of {rel_path} (no-delete enabled)")
            return
        
        if self.args.trash_days > 0:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            trash_dst = folder_trash / ts / rel_path
            trash_dst.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(tgt), str(trash_dst))
            except Exception as e:
                logger.error(f"Failed to trash {tgt}: {e}")
        else:
            if tgt.is_dir():
                shutil.rmtree(tgt, ignore_errors=True)
            else:
                tgt.unlink(missing_ok=True)

    def phase5_execute(self, actions: list, snap_a: dict, snap_b: dict):
        for action in actions:
            cmd = action[0]

            if cmd == "copy":
                direction = action[1]
                p = action[2]
                if direction == "a_to_b":
                    src = self.folder_a / p
                    dst = self.folder_b / p
                    is_dir = snap_a[p].is_dir
                    logger.info(f"Copying {p} (A -> B)")
                    self.safe_copy(src, dst, is_dir)
                    self.state.files[p] = snap_a[p]
                elif direction == "b_to_a":
                    src = self.folder_b / p
                    dst = self.folder_a / p
                    is_dir = snap_b[p].is_dir
                    logger.info(f"Copying {p} (B -> A)")
                    self.safe_copy(src, dst, is_dir)
                    self.state.files[p] = snap_b[p]

            elif cmd == "delete":
                target = action[1]
                p = action[2]
                logger.info(f"Deleting {p} from {target.upper()}")
                if target == "a":
                    self.trash(self.folder_a / p, self.trash_a, p)
                else:
                    self.trash(self.folder_b / p, self.trash_b, p)
                # If deleted from one, it's deleted everywhere from state view
                if p in self.state.files:
                    del self.state.files[p]

            elif cmd == "conflict":
                p = action[1]
                logger.warning(f"Conflict detected for {p}")
                # default newest wins strategy
                ma = snap_a[p].mtime
                mb = snap_b[p].mtime
                if ma >= mb:
                    winner_folder = "a"
                    loser_folder = "b"
                    winner_path = self.folder_a / p
                    loser_path = self.folder_b / p
                    winner_snap = snap_a[p]
                else:
                    winner_folder = "b"
                    loser_folder = "a"
                    winner_path = self.folder_b / p
                    loser_path = self.folder_a / p
                    winner_snap = snap_b[p]
                
                logger.info(f"Conflict resolving: {winner_folder.upper()} is newer. Overwriting {loser_folder.upper()} with conflict copy.")
                conflict_name = loser_path.name + ".conflict"
                conflict_dst = loser_path.with_name(conflict_name)
                
                # rename the loser
                if loser_path.exists():
                    os.replace(loser_path, conflict_dst)
                
                # copy winner to loser
                self.safe_copy(winner_path, loser_path, winner_snap.is_dir)
                self.state.files[p] = winner_snap

            elif cmd == "remove_state":
                p = action[1]
                if p in self.state.files:
                    del self.state.files[p]
                    logger.debug(f"Removed {p} from state.")
            
            elif cmd == "merge_state":
                p = action[1]
                side = action[2]
                self.state.files[p] = snap_a[p] if side == 'a' else snap_b[p]

def main():
    parser = argparse.ArgumentParser(description="Bidirectional Folder Sync")
    parser.add_argument("--folder-a", required=True, help="First sync folder path")
    parser.add_argument("--folder-b", required=True, help="Second sync folder path")
    parser.add_argument("--state-file", default=".sync_state.json", help="Path to state file")
    parser.add_argument("--on-conflict", default="newest", choices=["newest", "keep-both", "skip", "ask"], help="Conflict strategy")
    parser.add_argument("--no-delete", action="store_true", help="Never propagate deletions")
    parser.add_argument("--trash-days", type=int, default=30, help="Days to keep trashed files")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without applying")
    parser.add_argument("--init", action="store_true", help="Initial sync")
    parser.add_argument("--ignore", default=".syncignore", help="Path to ignore patterns file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s [%(levelname)s] %(message)s")
    
    engine = BiSyncEngine(args)
    engine.run()

if __name__ == "__main__":
    main()
