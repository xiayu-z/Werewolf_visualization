"""Descriptive statistics and overview visualizations."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from analysis.common import build_player_feature_table, load_tables, prepare_output_dirs, setup_plot_style


def run(processed_root: str | Path, output_root: str | Path) -> None:
    setup_plot_style()
    tables = load_tables(processed_root)
    games = tables["games"]
    players = tables["players"]
    votes = tables["votes"]
    speeches = tables["speeches"]
    night_actions = tables["night_actions"]
    errors = tables["errors"]

    feature_table = build_player_feature_table(players, votes, speeches, night_actions)
    tables_dir, figures_dir = prepare_output_dirs(output_root, "01_descriptive")

    overview = pd.DataFrame(
        [
            {"metric": "games_parsed", "value": int(len(games))},
            {"metric": "player_game_rows", "value": int(len(players))},
            {"metric": "public_speeches", "value": int(len(speeches))},
            {"metric": "votes", "value": int(len(votes))},
            {"metric": "night_actions", "value": int(len(night_actions))},
            {"metric": "error_rows", "value": int(len(errors))},
            {"metric": "mean_last_day", "value": float(games["last_day"].mean()) if not games.empty else 0.0},
            {
                "metric": "mean_public_messages_per_player_game",
                "value": float(feature_table["public_message_count"].mean()) if not feature_table.empty else 0.0,
            },
        ]
    )
    overview.to_csv(tables_dir / "dataset_overview.csv", index=False)

    role_summary = (
        feature_table.groupby("role", observed=False, as_index=False)
        .agg(
            player_games=("player_id", "size"),
            win_rate=("won", "mean"),
            mean_survival_days=("survival_days", "mean"),
            mean_public_messages=("public_message_count", "mean"),
            mean_vote_accuracy=("vote_accuracy", "mean"),
        )
        .sort_values("role")
    )
    role_summary.to_csv(tables_dir / "role_summary.csv", index=False)

    winner_team_summary = (
        games.groupby("winner_team", as_index=False)
        .agg(game_count=("episode_id", "size"), mean_last_day=("last_day", "mean"))
        .sort_values("game_count", ascending=False)
    )
    winner_team_summary.to_csv(tables_dir / "winner_team_summary.csv", index=False)

    with open(tables_dir / "overview.md", "w", encoding="utf-8") as handle:
        handle.write("# Descriptive Overview\n\n")
        for row in overview.itertuples(index=False):
            handle.write(f"- {row.metric}: {row.value}\n")

    if not role_summary.empty:
        fig, ax = plt.subplots(figsize=(9, 5))
        sns.barplot(data=role_summary, x="role", y="win_rate", ax=ax, color="#5677A4")
        ax.set_title("Win Rate by Role")
        ax.set_xlabel("Role")
        ax.set_ylabel("Win rate")
        fig.savefig(figures_dir / "role_win_rate.png")
        plt.close(fig)

    if not games.empty:
        fig, ax = plt.subplots(figsize=(9, 5))
        sns.histplot(data=games, x="last_day", bins=range(int(games["last_day"].min()), int(games["last_day"].max()) + 2), ax=ax, color="#C97B63")
        ax.set_title("Distribution of Game Length")
        ax.set_xlabel("Last day index")
        ax.set_ylabel("Game count")
        fig.savefig(figures_dir / "game_length_distribution.png")
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(8, 5))
        sns.barplot(data=winner_team_summary, x="winner_team", y="game_count", ax=ax, color="#6B9E78")
        ax.set_title("Winning Team Counts")
        ax.set_xlabel("Winning team")
        ax.set_ylabel("Games")
        fig.savefig(figures_dir / "winning_team_counts.png")
        plt.close(fig)

    if not feature_table.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.boxplot(data=feature_table, x="role", y="public_message_count", ax=ax, color="#D9B26F")
        ax.set_title("Public Message Count by Role")
        ax.set_xlabel("Role")
        ax.set_ylabel("Public messages per player-game")
        fig.savefig(figures_dir / "public_messages_by_role.png")
        plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--processed-root", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args()
    run(processed_root=args.processed_root, output_root=args.output_root)


if __name__ == "__main__":
    main()
