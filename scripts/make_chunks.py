"""Create Slurm-friendly chunk manifests from raw JSON files."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from werewolf.io_utils import ensure_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--chunks-dir", required=True)
    parser.add_argument("--files-per-chunk", type=int, default=200)
    parser.add_argument("--include-empty", action="store_true")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    chunks_dir = ensure_dir(args.chunks_dir)

    files = sorted(data_dir.glob("*.json"))
    if not args.include_empty:
        files = [path for path in files if path.stat().st_size > 0]

    manifest_rows = []
    for chunk_id, start in enumerate(range(0, len(files), args.files_per_chunk)):
        chunk_files = files[start : start + args.files_per_chunk]
        chunk_path = chunks_dir / f"chunk_{chunk_id:05d}.txt"
        chunk_path.write_text("\n".join(str(path.resolve()) for path in chunk_files), encoding="utf-8")
        manifest_rows.append(
            {
                "chunk_id": chunk_id,
                "chunk_file": str(chunk_path.resolve()),
                "n_files": len(chunk_files),
                "total_bytes": sum(path.stat().st_size for path in chunk_files),
                "first_file": chunk_files[0].name if chunk_files else None,
                "last_file": chunk_files[-1].name if chunk_files else None,
            }
        )

    manifest = pd.DataFrame(manifest_rows)
    manifest.to_csv(chunks_dir / "chunk_manifest.csv", index=False)
    (chunks_dir / "chunk_count.txt").write_text(str(len(manifest_rows)), encoding="utf-8")
    print(f"Created {len(manifest_rows)} chunk manifests in {chunks_dir}")


if __name__ == "__main__":
    main()
