#!/usr/bin/env python3
"""Copy medallion files from the Azure DevOps clone into this repo (file sync only).

Opposite of ``sync_to_ado.py``. Does not run git pull, merge, or commit — refresh the
ADO folder yourself (``git pull`` in the DevOps clone), then run this script.

Default layout (override with ``--source`` / ``--target`` or env ``MEDALLION_SYNC_ADO``, ``MEDALLION_SYNC_FABRIC``):

  Source (ADO):             C:\\spark-dev\\CoE_Transformation_Framework
  Target (this repo):       parent of Scripts/ (where you run this from)

Does not overwrite local-only files (``local_settings.py``) or the sync helper scripts.

Example (PowerShell):

  cd C:\\spark-dev\\CoE_Transformation_Framework
  git pull

  cd C:\\Users\\you\\Desktop\\AI\\fabric
  python Scripts\\sync_from_ado.py --dry-run
  python Scripts\\sync_from_ado.py
  git status
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = Path(r"C:\spark-dev\CoE_Transformation_Framework")
DEFAULT_TARGET = _REPO_ROOT

SYNC_DIRS = ("Common", "Notebook", "Scripts")

SYNC_FILES = (
    "README.md",
    "requirements-local-spark.txt",
    ".gitignore",
    Path(".vscode") / "settings.json",
)

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

# Never clobber local secrets or repo-specific sync entrypoints.
SKIP_FILE_NAMES = frozenset(
    {
        "local_settings.py",
        "sync_to_ado.py",
        "sync_from_ado.py",
    }
)


def _env_path(name: str) -> Path | None:
    raw = os.environ.get(name, "").strip()
    return Path(raw) if raw else None


def resolve_paths(source: Path | None, target: Path | None) -> tuple[Path, Path]:
    # Use MEDALLION_SYNC_ADO / MEDALLION_SYNC_FABRIC (not SOURCE/TARGET — those match sync_to_ado semantics).
    src = source or _env_path("MEDALLION_SYNC_ADO") or DEFAULT_SOURCE
    tgt = target or _env_path("MEDALLION_SYNC_FABRIC") or DEFAULT_TARGET
    return src.resolve(), tgt.resolve()


def validate_roots(source: Path, target: Path) -> None:
    if not source.is_dir():
        raise SystemExit(f"ADO source folder not found: {source}")
    if not (source / "Common" / "bootstrap.py").is_file():
        raise SystemExit(
            f"Source does not look like the medallion repo (missing Common/bootstrap.py): {source}"
        )
    if source.resolve() == target.resolve():
        raise SystemExit("Source and target must be different folders.")
    target.mkdir(parents=True, exist_ok=True)
    if not target.is_dir():
        raise SystemExit(f"Target folder not found: {target}")


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
        description="File-sync Azure DevOps clone into this medallion repo (no git operations)."
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=None,
        help=f"ADO repo root after git pull (default: {DEFAULT_SOURCE})",
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=None,
        help=f"GitHub / agent repo root (default: {_REPO_ROOT})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print paths only; do not write files.",
    )
    args = parser.parse_args()

    source, target = resolve_paths(args.source, args.target)
    validate_roots(source, target)

    print(f"Source (ADO):  {source}")
    print(f"Target (repo): {target}")
    if args.dry_run:
        print("Mode:          dry-run\n")
    else:
        print("Mode:          copy\n")

    total_files = 0
    for name in SYNC_DIRS:
        total_files += sync_tree(name, source, target, dry_run=args.dry_run)
    total_files += sync_files(source, target, dry_run=args.dry_run)

    print(f"\nDone — {total_files} file(s) {'would be ' if args.dry_run else ''}copied.")
    if not args.dry_run:
        print(f"Next: cd {target}")
        print("      git status")
        print('      git add -A && git commit -m "Sync from ADO CoE repo"')
    return 0


if __name__ == "__main__":
    sys.exit(main())