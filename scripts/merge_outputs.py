import argparse
from pathlib import Path
import pandas as pd


def write_df(df, out_path: Path, write_format: str):
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if write_format == "csv":
        df.to_csv(out_path.with_suffix(".csv"), index=False)
        print(f"Wrote {out_path.with_suffix('.csv')} shape={df.shape}")
    elif write_format == "parquet":
        df.to_parquet(out_path.with_suffix(".parquet"), index=False)
        print(f"Wrote {out_path.with_suffix('.parquet')} shape={df.shape}")
    else:
        raise ValueError(f"Unsupported write_format: {write_format}")


def merge_pattern(chunks_root: Path, pattern: str):
    files = sorted(chunks_root.glob(pattern))
    if not files:
        print(f"No files found for pattern: {pattern}")
        return pd.DataFrame()

    dfs = []
    for f in files:
        dfs.append(pd.read_csv(f))

    merged = pd.concat(dfs, ignore_index=True)
    print(f"Merged {len(files)} files for pattern {pattern}, shape={merged.shape}")
    return merged


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunks-root", required=True, help="Directory containing chunk outputs")
    parser.add_argument("--merged-root", required=True, help="Directory to write merged outputs")
    parser.add_argument("--write-format", choices=["csv", "parquet"], default="parquet")
    args = parser.parse_args()

    chunks_root = Path(args.chunks_root)
    merged_root = Path(args.merged_root)

    games = merge_pattern(chunks_root, "games_chunk_*.csv")
    players = merge_pattern(chunks_root, "players_chunk_*.csv")
    public_messages = merge_pattern(chunks_root, "public_messages_chunk_*.csv")
    events = merge_pattern(chunks_root, "events_chunk_*.csv")
    errors = merge_pattern(chunks_root, "errors_chunk_*.csv")

    write_df(games, merged_root / "games", args.write_format)
    write_df(players, merged_root / "players", args.write_format)
    write_df(public_messages, merged_root / "public_messages", args.write_format)
    write_df(events, merged_root / "events", args.write_format)
    write_df(errors, merged_root / "errors", args.write_format)


if __name__ == "__main__":
    main()
