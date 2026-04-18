from __future__ import annotations

import argparse
from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.append(str(Path(__file__).resolve().parent))
    from common import DEFAULT_CHUNK_ROOT, DEFAULT_MERGED_ROOT, DEFAULT_OUTPUT_DIR, run_overview_statistics
else:
    from .common import DEFAULT_CHUNK_ROOT, DEFAULT_MERGED_ROOT, DEFAULT_OUTPUT_DIR, run_overview_statistics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate overview descriptive statistics for the Werewolf project."
    )
    parser.add_argument(
        "--merged-root",
        type=Path,
        default=DEFAULT_MERGED_ROOT,
        help="Directory containing merged parquet outputs.",
    )
    parser.add_argument(
        "--chunk-root",
        type=Path,
        default=DEFAULT_CHUNK_ROOT,
        help="Directory containing chunk-level CSV outputs used as a fallback.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where summary tables will be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_overview_statistics(
        merged_root=args.merged_root,
        chunk_root=args.chunk_root,
        output_dir=args.output_dir,
    )
    print(f"Overview statistics written to {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
