#!/bin/bash
#SBATCH --job-name=ww-env
#SBATCH --output=logs/slurm/%x-%j.out
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G

set -euo pipefail

PROJECT_ROOT=${PROJECT_ROOT:-/workspace/chen2744/Werewolf}
mkdir -p "$PROJECT_ROOT/logs/slurm"

python3 -m venv "$PROJECT_ROOT/.venv"
source "$PROJECT_ROOT/.venv/bin/activate"

python -m pip install --upgrade pip
python -m pip install -r "$PROJECT_ROOT/requirements.txt"
