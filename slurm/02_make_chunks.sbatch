#!/bin/bash
#SBATCH --job-name=ww-chunks
#SBATCH --output=logs/slurm/%x-%j.out
#SBATCH --time=00:20:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G

set -euo pipefail

PROJECT_ROOT=${PROJECT_ROOT:-/workspace/chen2744/Werewolf}
mkdir -p "$PROJECT_ROOT/logs/slurm"
source "$PROJECT_ROOT/.venv/bin/activate"
export PYTHONPATH="$PROJECT_ROOT/src:$PROJECT_ROOT"

python "$PROJECT_ROOT/scripts/make_chunks.py" \
  --data-dir "$PROJECT_ROOT/data" \
  --chunks-dir "$PROJECT_ROOT/chunks" \
  --files-per-chunk 200

cat "$PROJECT_ROOT/chunks/chunk_count.txt"
