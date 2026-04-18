#!/bin/bash
#SBATCH --job-name=ww-merge
#SBATCH --output=logs/slurm/%x-%j.out
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=24G

set -euo pipefail

PROJECT_ROOT=${PROJECT_ROOT:-/workspace/chen2744/Werewolf}
mkdir -p "$PROJECT_ROOT/logs/slurm"
source "$PROJECT_ROOT/.venv/bin/activate"
export PYTHONPATH="$PROJECT_ROOT/src:$PROJECT_ROOT"

python "$PROJECT_ROOT/scripts/merge_outputs.py" \
  --chunks-root "$PROJECT_ROOT/processed/chunks" \
  --merged-root "$PROJECT_ROOT/processed/merged" \
  --write-format parquet
