"""Run all analysis stages in a fixed order."""

from __future__ import annotations

import argparse

from analysis import descriptive_analysis, modeling, role_analysis, speech_analysis, vote_analysis


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--processed-root", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args()

    descriptive_analysis.run(processed_root=args.processed_root, output_root=args.output_root)
    vote_analysis.run(processed_root=args.processed_root, output_root=args.output_root)
    speech_analysis.run(processed_root=args.processed_root, output_root=args.output_root)
    role_analysis.run(processed_root=args.processed_root, output_root=args.output_root)
    modeling.run(processed_root=args.processed_root, output_root=args.output_root)


if __name__ == "__main__":
    main()
