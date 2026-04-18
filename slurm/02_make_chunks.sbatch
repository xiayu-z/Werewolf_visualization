#!/bin/bash
#SBATCH --job-name=ww-chunks
#SBATCH --output=/workspace/chen2744/Werewolf/logs/slurm/%x-%j.out
#SBATCH --time=00:20:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --partition=short

set -euo pipefail

PROJECT_ROOT=/workspace/chen2744/Werewolf
source "$PROJECT_ROOT/.venv/bin/activate"
export PYTHONPATH="$PROJECT_ROOT/src:$PROJECT_ROOT"

python "$PROJECT_ROOT/scripts/make_chunks.py" \
  --data-dir "$PROJECT_ROOT/data" \
  --chunks-dir "$PROJECT_ROOT/chunks" \
  --files-per-chunk 200

echo "Chunk count:"
cat "$PROJECT_ROOT/chunks/chunk_count.txt"

echo "First few chunk files:"
ls -lh "$PROJECT_ROOT/chunks" | head -20
