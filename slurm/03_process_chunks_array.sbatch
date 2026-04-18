#!/bin/bash
#SBATCH --job-name=ww-array
#SBATCH --output=/workspace/chen2744/Werewolf/logs/slurm/%x-%A_%a.out
#SBATCH --time=02:30:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --partition=long
#SBATCH --array=0-7

set -euo pipefail

PROJECT_ROOT=/workspace/chen2744/Werewolf
source "$PROJECT_ROOT/.venv/bin/activate"
export PYTHONPATH="$PROJECT_ROOT/src:$PROJECT_ROOT"

CHUNK_ID=$(printf "%05d" ${SLURM_ARRAY_TASK_ID})
CHUNK_FILE="$PROJECT_ROOT/chunks/chunk_${CHUNK_ID}.txt"

python "$PROJECT_ROOT/scripts/process_chunk.py" \
  --chunk_file "$CHUNK_FILE" \
  --chunk_id "$CHUNK_ID" \
  --output_dir "$PROJECT_ROOT/outputs/chunk_results"
