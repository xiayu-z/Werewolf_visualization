from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

if __package__ in {None, ""}:
    import sys

    sys.path.append(str(Path(__file__).resolve().parent))
    from common import DEFAULT_OUTPUT_DIR, resolve_output_dirs
else:
    from .common import DEFAULT_OUTPUT_DIR, resolve_output_dirs


def load_summary_tables(output_dir: Path) -> dict[str, pd.DataFrame]:
    table_dir, _ = resolve_output_dirs(output_dir)
    filenames = {
        "winner_team_counts": "winner_team_counts.csv",
        "game_length_counts": "game_length_counts.csv",
        "role_counts": "role_counts.csv",
        "role_survival_rates": "role_survival_rates.csv",
        "messages_per_game_distribution": "messages_per_game_distribution.csv",
        "event_type_counts": "event_type_counts.csv",
    }

    missing = [name for name, filename in filenames.items() if not (table_dir / filename).exists()]
    if missing:
        missing_text = ", ".join(missing)
        raise FileNotFoundError(
            "Missing summary tables for plotting. Run 01_overview_stats.py first. "
            f"Missing: {missing_text}"
        )

    return {
        name: pd.read_csv(table_dir / filename)
        for name, filename in filenames.items()
    }


def _set_plot_style() -> None:
    sns.set_theme(style="whitegrid")
    plt.rcParams["figure.dpi"] = 140
    plt.rcParams["savefig.bbox"] = "tight"


def _save_barplot(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: Path,
    color: str,
) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=df, x=x, y=y, color=color, ax=ax)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    for patch in ax.patches:
        height = patch.get_height()
        ax.annotate(
            f"{height:.0f}",
            (patch.get_x() + patch.get_width() / 2, height),
            ha="center",
            va="bottom",
            fontsize=9,
            xytext=(0, 4),
            textcoords="offset points",
        )
    fig.savefig(output_path)
    plt.close(fig)


def _save_histogram(
    series: pd.Series,
    *,
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: Path,
    color: str,
) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    bin_start = int(series.min())
    bin_stop = int(series.max()) + 2
    sns.histplot(series, bins=range(bin_start, bin_stop), color=color, edgecolor="white", ax=ax)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    fig.savefig(output_path)
    plt.close(fig)


def generate_plots(output_dir: Path, *, top_n_events: int = 12) -> Path:
    _set_plot_style()
    table_dir, plot_dir = resolve_output_dirs(output_dir)
    tables = load_summary_tables(output_dir)

    _save_barplot(
        tables["winner_team_counts"],
        x="winner_team",
        y="count",
        title="Winner Team Distribution",
        xlabel="Winner team",
        ylabel="Games",
        output_path=plot_dir / "winner_team_bar.png",
        color="#4C78A8",
    )

    _save_barplot(
        tables["game_length_counts"],
        x="last_day",
        y="count",
        title="Game Length Distribution",
        xlabel="Last day",
        ylabel="Games",
        output_path=plot_dir / "game_length_bar.png",
        color="#F58518",
    )

    _save_barplot(
        tables["role_counts"],
        x="role",
        y="count",
        title="Player Role Distribution",
        xlabel="Role",
        ylabel="Players",
        output_path=plot_dir / "role_count_bar.png",
        color="#54A24B",
    )

    _save_barplot(
        tables["role_survival_rates"],
        x="role",
        y="survival_rate",
        title="Role Survival Rate",
        xlabel="Role",
        ylabel="Survival rate",
        output_path=plot_dir / "role_survival_rate_bar.png",
        color="#E45756",
    )

    _save_histogram(
        tables["messages_per_game_distribution"]["message_count"],
        title="Public Messages per Game",
        xlabel="Messages per game",
        ylabel="Games",
        output_path=plot_dir / "messages_per_game_hist.png",
        color="#72B7B2",
    )

    top_events = tables["event_type_counts"].head(top_n_events)
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=top_events, x="count", y="event_name", color="#B279A2", ax=ax)
    ax.set_title(f"Top {len(top_events)} Event Types")
    ax.set_xlabel("Count")
    ax.set_ylabel("Event type")
    fig.savefig(plot_dir / "top_event_types_bar.png")
    plt.close(fig)

    print(f"Overview plots written to {plot_dir.resolve()}")
    return plot_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate overview plots for the Werewolf descriptive analysis."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory containing overview summary tables and receiving plots.",
    )
    parser.add_argument(
        "--top-n-events",
        type=int,
        default=12,
        help="How many event types to include in the event frequency bar chart.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    generate_plots(args.output_dir, top_n_events=args.top_n_events)


if __name__ == "__main__":
    main()
