#!/bin/bash
#SBATCH --job-name=ww-analysis
#SBATCH --output=logs/slurm/%x-%j.out
#SBATCH --time=03:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=24G

set -euo pipefail

PROJECT_ROOT=${PROJECT_ROOT:-/workspace/chen2744/Werewolf}
mkdir -p "$PROJECT_ROOT/logs/slurm"
source "$PROJECT_ROOT/.venv/bin/activate"
export PYTHONPATH="$PROJECT_ROOT/src:$PROJECT_ROOT"

python "$PROJECT_ROOT/scripts/run_all_analyses.py" \
  --processed-root "$PROJECT_ROOT/processed/merged" \
  --output-root "$PROJECT_ROOT/outputs"
