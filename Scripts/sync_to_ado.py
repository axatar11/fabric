#!/usr/bin/env python3
"""Copy tested repo files into the Azure DevOps sibling clone (file sync only).

Default layout (override with --source / --target or env):

  Source (GitHub / agent):  C:\\spark-dev\\fabric\\fabric
  Target (ADO):             C:\\spark-dev\\CoE_Transformation_Framework

Does not run git, cherry-pick, or push — you commit and push to DevOps yourself.

Example (PowerShell):

  cd C:\\spark-dev\\fabric\\fabric
  python Scripts\\sync_to_ado.py --dry-run
  python Scripts\\sync_to_ado.py
  cd C:\\spark-dev\\CoE_Transformation_Framework
  git status
  git add -A
  git commit -m "Sync from fabric test repo"
  git push
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

DEFAULT_SOURCE = Path(r"C:\spark-dev\fabric\fabric")
DEFAULT_TARGET = Path(r"C:\spark-dev\CoE_Transformation_Framework")

# Directories copied recursively (relative to repo root).
SYNC_DIRS = ("Common", "Notebook", "Scripts")

# Root files copied when present.
SYNC_FILES = (
    "README.md",
    "requirements-local-spark.txt",
    ".gitignore",
    Path(".vscode") / "settings.json",
)

# Directory names skipped anywhere under a sync tree.
SKIP_DIR_NAMES = frozenset(
    {
        ".git",
        ".venv",
        "__pycache__",
        ".ipynb_checkpoints",
        ".fabric",
        "merge_staging",
        "medallion_cache",
    }
)

# File names skipped (basename match).
SKIP_FILE_NAMES = frozenset({"local_settings.py", "sync_to_ado.py"})


def _env_path(name: str) -> Path | None:
    raw = os.environ.get(name, "").strip()
    return Path(raw) if raw else None


def resolve_paths(source: Path | None, target: Path | None) -> tuple[Path, Path]:
    src = source or _env_path("MEDALLION_SYNC_SOURCE") or DEFAULT_SOURCE
    tgt = target or _env_path("MEDALLION_SYNC_TARGET") or DEFAULT_TARGET
    return src.resolve(), tgt.resolve()


def validate_roots(source: Path, target: Path) -> None:
    if not source.is_dir():
        raise SystemExit(f"Source folder not found: {source}")
    if not (source / "Common" / "bootstrap.py").is_file():
        raise SystemExit(
            f"Source does not look like this repo (missing Common/bootstrap.py): {source}"
        )
    if source.resolve() == target.resolve():
        raise SystemExit("Source and target must be different folders.")
    target.mkdir(parents=True, exist_ok=True)


def should_skip_dir(name: str) -> bool:
    return name in SKIP_DIR_NAMES


def should_skip_file(name: str) -> bool:
    return name in SKIP_FILE_NAMES


def sync_tree(rel_dir: str, source: Path, target: Path, *, dry_run: bool) -> int:
    """Copy directory tree; return number of files copied."""
    src_root = source / rel_dir
    if not src_root.is_dir():
        print(f"skip  (missing) {rel_dir}/")
        return 0
    files = 0
    for dirpath, dirnames, filenames in os.walk(src_root):
        dirnames[:] = [d for d in dirnames if not should_skip_dir(d)]
        rel_base = Path(dirpath).relative_to(source)
        for fname in filenames:
            if should_skip_file(fname):
                continue
            src_file = Path(dirpath) / fname
            dst_file = target / rel_base / fname
            if dry_run:
                print(f"COPY  {src_file} -> {dst_file}")
            else:
                dst_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_file, dst_file)
                print(f"copy  {dst_file.relative_to(target)}")
            files += 1
    return files


def sync_files(source: Path, target: Path, *, dry_run: bool) -> int:
    n = 0
    for rel in SYNC_FILES:
        rel_path = Path(rel)
        display = rel_path.as_posix()
        src = source / rel_path
        if not src.is_file():
            print(f"skip  (missing) {display}")
            continue
        dst = target / rel_path
        if dry_run:
            print(f"COPY  {src} -> {dst}")
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            print(f"copy  {display}")
        n += 1
    return n


def main() -> int:
    parser = argparse.ArgumentParser(
        description="File-sync medallion repo into Azure DevOps clone (no git operations)."
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=None,
        help=f"Test/GitHub repo root (default: {DEFAULT_SOURCE})",
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=None,
        help=f"ADO repo root (default: {DEFAULT_TARGET})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print paths only; do not write files.",
    )
    args = parser.parse_args()

    source, target = resolve_paths(args.source, args.target)
    validate_roots(source, target)

    print(f"Source: {source}")
    print(f"Target: {target}")
    if args.dry_run:
        print("Mode:   dry-run\n")
    else:
        print("Mode:   copy\n")

    total_files = 0
    for name in SYNC_DIRS:
        total_files += sync_tree(name, source, target, dry_run=args.dry_run)
    total_files += sync_files(source, target, dry_run=args.dry_run)

    print(f"\nDone — {total_files} file(s) {'would be ' if args.dry_run else ''}copied.")
    if not args.dry_run:
        print(f"Next: cd {target}")
        print("      git status")
        print("      git add -A && git commit -m \"Sync from fabric test repo\" && git push")
    return 0


if __name__ == "__main__":
    sys.exit(main())
