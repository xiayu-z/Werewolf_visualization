import math
from pathlib import Path
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, help="Directory containing json files")
    parser.add_argument("--chunks-dir", required=True, help="Directory to write chunk manifest files")
    parser.add_argument("--files-per-chunk", type=int, default=200, help="How many files per chunk")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    chunks_dir = Path(args.chunks_dir)
    chunks_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(data_dir.glob("*.json"))
    n_files = len(files)

    if n_files == 0:
        raise RuntimeError(f"No json files found in {data_dir}")

    n_chunks = math.ceil(n_files / args.files_per_chunk)

    for chunk_idx in range(n_chunks):
        start = chunk_idx * args.files_per_chunk
        end = min((chunk_idx + 1) * args.files_per_chunk, n_files)
        chunk_files = files[start:end]

        out_path = chunks_dir / f"chunk_{chunk_idx:05d}.txt"
        with open(out_path, "w", encoding="utf-8") as f:
            for p in chunk_files:
                f.write(str(p.resolve()) + "\n")

    with open(chunks_dir / "chunk_count.txt", "w", encoding="utf-8") as f:
        f.write(str(n_chunks) + "\n")

    print(f"Found {n_files} files")
    print(f"Created {n_chunks} chunk manifests in {chunks_dir}")
    print(f"Files per chunk: {args.files_per_chunk}")

if __name__ == "__main__":
    main()
