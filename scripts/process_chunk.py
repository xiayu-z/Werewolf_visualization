"""Process one chunk manifest into chunk-level tables."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from werewolf.config import TABLE_NAMES
from werewolf.io_utils import ensure_dir, write_frame
from werewolf.parse import parse_game_file


def chunk_id_from_path(chunk_file: Path) -> str:
    digits = "".join(char for char in chunk_file.stem if char.isdigit())
    return digits or chunk_file.stem


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk-file", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--write-format", choices=["parquet", "csv"], default="parquet")
    args = parser.parse_args()

    chunk_file = Path(args.chunk_file)
    output_root = Path(args.output_root)
    chunk_id = chunk_id_from_path(chunk_file)
    chunk_dir = ensure_dir(output_root / f"chunk_{chunk_id}")

    table_rows: dict[str, list[dict]] = {table_name: [] for table_name in TABLE_NAMES}
    file_paths = [Path(line.strip()) for line in chunk_file.read_text(encoding="utf-8").splitlines() if line.strip()]

    for file_path in file_paths:
        parsed = parse_game_file(file_path)
        for table_name in TABLE_NAMES:
            table_rows[table_name].extend(parsed[table_name])

    summary_rows = []
    for table_name in TABLE_NAMES:
        df = pd.DataFrame(table_rows[table_name])
        summary_rows.append({"table_name": table_name, "row_count": int(len(df))})
        if not df.empty:
            write_frame(df, root=chunk_dir, table_name=table_name, write_format=args.write_format)

    pd.DataFrame(summary_rows).to_csv(chunk_dir / "chunk_summary.csv", index=False)
    with open(chunk_dir / "metadata.json", "w", encoding="utf-8") as handle:
        json.dump(
            {
                "chunk_id": chunk_id,
                "chunk_file": str(chunk_file.resolve()),
                "n_input_files": len(file_paths),
                "write_format": args.write_format,
            },
            handle,
            indent=2,
        )
    print(f"Finished chunk {chunk_id} with {len(file_paths)} input files")


if __name__ == "__main__":
    main()
