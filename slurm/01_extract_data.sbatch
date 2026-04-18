#!/bin/bash
#SBATCH --job-name=ww-extract
#SBATCH --output=logs/slurm/%x-%j.out
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --partition=short

set -euo pipefail

PROJECT_ROOT=${PROJECT_ROOT:-/workspace/chen2744/Werewolf}
mkdir -p "$PROJECT_ROOT/logs/slurm" "$PROJECT_ROOT/processed" "$PROJECT_ROOT/outputs" "$PROJECT_ROOT/chunks"

if compgen -G "$PROJECT_ROOT/data/*.json" > /dev/null; then
  echo "data/ already exists and contains JSON files. Skipping extraction."
else
  tar -xf "$PROJECT_ROOT/data.tar" -C "$PROJECT_ROOT"
fi

find "$PROJECT_ROOT/data" -maxdepth 1 -name '*.json' | wc -l
