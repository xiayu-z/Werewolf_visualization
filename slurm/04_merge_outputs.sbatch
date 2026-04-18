#!/bin/bash
#SBATCH --job-name=ww-merge
#SBATCH --output=/workspace/chen2744/Werewolf/logs/slurm/%x-%j.out
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --partition=long

set -euo pipefail

PROJECT_ROOT=/workspace/chen2744/Werewolf
source "$PROJECT_ROOT/.venv/bin/activate"
export PYTHONPATH="$PROJECT_ROOT/src:$PROJECT_ROOT"

python "$PROJECT_ROOT/scripts/merge_outputs.py" \
  --chunks-root "$PROJECT_ROOT/outputs/chunk_results" \
  --merged-root "$PROJECT_ROOT/outputs/merged" \
  --write-format parquet
