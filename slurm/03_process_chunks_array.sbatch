#!/bin/bash
#SBATCH --job-name=ww-array
#SBATCH --output=logs/slurm/%x-%A_%a.out
#SBATCH --time=02:30:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=24G

set -euo pipefail

PROJECT_ROOT=${PROJECT_ROOT:-/workspace/chen2744/Werewolf}
mkdir -p "$PROJECT_ROOT/logs/slurm"
source "$PROJECT_ROOT/.venv/bin/activate"
export PYTHONPATH="$PROJECT_ROOT/src:$PROJECT_ROOT"

CHUNK_FILE=$(printf "%s/chunks/chunk_%05d.txt" "$PROJECT_ROOT" "$SLURM_ARRAY_TASK_ID")

python "$PROJECT_ROOT/scripts/process_chunk.py" \
  --chunk-file "$CHUNK_FILE" \
  --output-root "$PROJECT_ROOT/processed/chunks" \
  --write-format parquet
