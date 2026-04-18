#!/bin/bash
#SBATCH --job-name=ww-extract
#SBATCH --output=/workspace/chen2744/Werewolf/logs/slurm/%x-%j.out
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition=short

set -x

PROJECT_ROOT=/workspace/chen2744/Werewolf

pwd
ls -ld "$PROJECT_ROOT"
ls -ld "$PROJECT_ROOT/data"
ls -ld "$PROJECT_ROOT/logs"
ls -ld "$PROJECT_ROOT/logs/slurm"
find "$PROJECT_ROOT/data" -maxdepth 1 -name '*.json' | head
find "$PROJECT_ROOT/data" -maxdepth 1 -name '*.json' | wc -l
