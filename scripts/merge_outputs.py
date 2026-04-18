"""Merge chunk-level outputs into one merged table per dataset."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from werewolf.config import TABLE_NAMES
from werewolf.io_utils import ensure_dir, write_frame


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunks-root", required=True)
    parser.add_argument("--merged-root", required=True)
    parser.add_argument("--write-format", choices=["parquet", "csv"], default="parquet")
    args = parser.parse_args()

    chunks_root = Path(args.chunks_root)
    merged_root = ensure_dir(args.merged_root)
    summary_rows = []

    for table_name in TABLE_NAMES:
        frames = []
        for chunk_dir in sorted(chunks_root.glob("chunk_*")):
            parquet_path = chunk_dir / f"{table_name}.parquet"
            csv_path = chunk_dir / f"{table_name}.csv.gz"
            if parquet_path.exists():
                frames.append(pd.read_parquet(parquet_path))
            elif csv_path.exists():
                frames.append(pd.read_csv(csv_path))
        if frames:
            merged = pd.concat(frames, ignore_index=True)
            write_frame(merged, root=merged_root, table_name=table_name, write_format=args.write_format)
            summary_rows.append({"table_name": table_name, "row_count": int(len(merged)), "n_chunks": len(frames)})
        else:
            summary_rows.append({"table_name": table_name, "row_count": 0, "n_chunks": 0})

    pd.DataFrame(summary_rows).to_csv(merged_root / "merge_summary.csv", index=False)
    print(f"Merged tables written to {merged_root}")


if __name__ == "__main__":
    main()
