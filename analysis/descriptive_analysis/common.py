from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Iterable

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MERGED_ROOT = REPO_ROOT.parent / "download" / "download" / "outputs" / "outputs" / "merged"
DEFAULT_CHUNK_ROOT = REPO_ROOT.parent / "download" / "download" / "outputs" / "outputs" / "chunk_results"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
DEFAULT_TABLE_DIR = DEFAULT_OUTPUT_DIR / "tables"
DEFAULT_PLOT_DIR = DEFAULT_OUTPUT_DIR / "plots"


def resolve_output_dirs(output_dir: Path | str | None = None) -> tuple[Path, Path]:
    base_dir = Path(output_dir) if output_dir is not None else DEFAULT_OUTPUT_DIR
    table_dir = base_dir / "tables"
    plot_dir = base_dir / "plots"
    table_dir.mkdir(parents=True, exist_ok=True)
    plot_dir.mkdir(parents=True, exist_ok=True)
    return table_dir, plot_dir


def _normalize_text_value(value: object, *, unknown_label: str = "Unknown") -> str:
    if pd.isna(value):
        return unknown_label

    text = str(value).strip()
    return text if text else unknown_label


def _normalize_bool_value(value: object) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n", ""}:
        return False
    return False


def _read_chunk_csvs(
    chunk_root: Path,
    pattern: str,
    *,
    usecols: list[str] | None = None,
) -> pd.DataFrame:
    files = sorted(chunk_root.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No chunk CSV files found for pattern: {pattern}")

    frames = [pd.read_csv(path, usecols=usecols) for path in files]
    return pd.concat(frames, ignore_index=True)


def load_table(
    name: str,
    *,
    merged_root: Path | str = DEFAULT_MERGED_ROOT,
    chunk_root: Path | str = DEFAULT_CHUNK_ROOT,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    merged_root = Path(merged_root)
    chunk_root = Path(chunk_root)

    parquet_path = merged_root / f"{name}.parquet"
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path, columns=columns)
        except Exception as exc:
            if not chunk_root.exists():
                raise RuntimeError(
                    f"Failed to read parquet file {parquet_path} and no CSV fallback is available."
                ) from exc

    merged_csv_path = merged_root / f"{name}.csv"
    if merged_csv_path.exists():
        return pd.read_csv(merged_csv_path, usecols=columns)

    pattern = f"{name}_chunk_*.csv"
    return _read_chunk_csvs(chunk_root, pattern, usecols=columns)


def load_event_name_counts(
    *,
    merged_root: Path | str = DEFAULT_MERGED_ROOT,
    chunk_root: Path | str = DEFAULT_CHUNK_ROOT,
    chunksize: int = 100_000,
) -> pd.DataFrame:
    merged_root = Path(merged_root)
    chunk_root = Path(chunk_root)

    parquet_path = merged_root / "events.parquet"
    if parquet_path.exists():
        try:
            events = pd.read_parquet(parquet_path, columns=["event_name"])
            return count_table(events["event_name"], "event_name")
        except Exception:
            pass

    merged_csv_path = merged_root / "events.csv"
    if merged_csv_path.exists():
        events = pd.read_csv(merged_csv_path, usecols=["event_name"])
        return count_table(events["event_name"], "event_name")

    files = sorted(chunk_root.glob("events_chunk_*.csv"))
    if not files:
        raise FileNotFoundError("Could not find events parquet/csv or chunk CSV fallbacks.")

    counter: Counter[str] = Counter()
    for path in files:
        for chunk in pd.read_csv(path, usecols=["event_name"], chunksize=chunksize):
            normalized = chunk["event_name"].map(_normalize_text_value)
            counter.update(normalized.tolist())

    counts = pd.DataFrame(
        [{"event_name": key, "count": value} for key, value in counter.items()]
    ).sort_values(["count", "event_name"], ascending=[False, True], ignore_index=True)
    total = counts["count"].sum()
    counts["share"] = counts["count"] / total if total else 0.0
    return counts


def count_table(
    series: pd.Series,
    label_column: str,
    *,
    numeric_sort: bool = False,
    unknown_label: str = "Unknown",
) -> pd.DataFrame:
    if numeric_sort:
        numeric = pd.to_numeric(series, errors="coerce")
        labels = series.map(lambda value: _normalize_text_value(value, unknown_label=unknown_label))
        formatted_labels = []
        for label, number in zip(labels, numeric):
            if pd.isna(number):
                formatted_labels.append(label)
            elif float(number).is_integer():
                formatted_labels.append(str(int(number)))
            else:
                formatted_labels.append(str(number))

        counts = (
            pd.DataFrame({label_column: formatted_labels, "_sort_key": numeric})
            .groupby([label_column, "_sort_key"], dropna=False)
            .size()
            .reset_index(name="count")
        )
        counts = counts.sort_values(
            ["_sort_key", label_column],
            ascending=[True, True],
            na_position="last",
            ignore_index=True,
        ).drop(columns="_sort_key")
    else:
        normalized = series.map(lambda value: _normalize_text_value(value, unknown_label=unknown_label))
        counts = (
            normalized.value_counts(dropna=False)
            .rename_axis(label_column)
            .reset_index(name="count")
        )
        counts = counts.sort_values(
            ["count", label_column],
            ascending=[False, True],
            ignore_index=True,
        )

    total = counts["count"].sum()
    counts["share"] = counts["count"] / total if total else 0.0
    return counts


def build_messages_per_game_distribution(
    games: pd.DataFrame,
    public_messages: pd.DataFrame,
) -> pd.DataFrame:
    game_ids = games[["game_id"]].drop_duplicates()
    message_counts = (
        public_messages.groupby("game_id")
        .size()
        .rename("message_count")
        .reset_index()
    )
    distribution = game_ids.merge(message_counts, on="game_id", how="left")
    distribution["message_count"] = distribution["message_count"].fillna(0).astype(int)
    return distribution.sort_values("game_id", ignore_index=True)


def summarize_messages_per_game(distribution: pd.DataFrame) -> pd.DataFrame:
    counts = distribution["message_count"]
    summary = {
        "n_games": int(len(distribution)),
        "mean_messages_per_game": float(counts.mean()),
        "median_messages_per_game": float(counts.median()),
        "std_messages_per_game": float(counts.std(ddof=1)) if len(counts) > 1 else 0.0,
        "min_messages_per_game": int(counts.min()),
        "p25_messages_per_game": float(counts.quantile(0.25)),
        "p75_messages_per_game": float(counts.quantile(0.75)),
        "max_messages_per_game": int(counts.max()),
    }
    return pd.DataFrame([summary])


def build_role_survival_table(players: pd.DataFrame) -> pd.DataFrame:
    alive = players["alive_end"].map(_normalize_bool_value)
    temp = players.assign(
        role=players["role"].map(_normalize_text_value),
        alive_end=alive,
    )
    grouped = (
        temp.groupby("role", dropna=False)["alive_end"]
        .agg(n_players="size", n_survived="sum", survival_rate="mean")
        .reset_index()
        .sort_values(["survival_rate", "n_players", "role"], ascending=[False, False, True], ignore_index=True)
    )
    return grouped


def build_overview_metrics(
    games: pd.DataFrame,
    players: pd.DataFrame,
    public_messages: pd.DataFrame,
    event_type_counts: pd.DataFrame,
) -> pd.DataFrame:
    message_lengths = public_messages["text_len"].dropna()
    metrics = [
        {"metric": "total_games", "value": int(len(games))},
        {"metric": "total_players", "value": int(len(players))},
        {"metric": "total_public_messages", "value": int(len(public_messages))},
        {"metric": "total_events", "value": int(event_type_counts["count"].sum())},
        {"metric": "avg_players_per_game", "value": float(games["n_players"].dropna().mean())},
        {"metric": "avg_message_length_chars", "value": float(message_lengths.mean()) if not message_lengths.empty else 0.0},
        {"metric": "median_message_length_chars", "value": float(message_lengths.median()) if not message_lengths.empty else 0.0},
        {"metric": "games_missing_winner_team", "value": int(games["winner_team"].isna().sum())},
    ]
    return pd.DataFrame(metrics)


def generate_summary_text(
    overview_metrics: pd.DataFrame,
    winner_team_counts: pd.DataFrame,
    role_survival_rates: pd.DataFrame,
    messages_per_game_summary: pd.DataFrame,
    event_type_counts: pd.DataFrame,
) -> str:
    metrics = dict(zip(overview_metrics["metric"], overview_metrics["value"]))
    top_winner = winner_team_counts.iloc[0]
    top_survival = role_survival_rates.iloc[0]
    top_event = event_type_counts.iloc[0]
    msg_summary = messages_per_game_summary.iloc[0]

    lines = [
        "Descriptive Analysis Summary",
        f"- Total games: {int(metrics['total_games'])}",
        f"- Total players: {int(metrics['total_players'])}",
        f"- Total public messages: {int(metrics['total_public_messages'])}",
        f"- Total events: {int(metrics['total_events'])}",
        f"- Average players per game: {metrics['avg_players_per_game']:.2f}",
        f"- Average message length: {metrics['avg_message_length_chars']:.2f} characters",
        f"- Most common winner team: {top_winner['winner_team']} ({int(top_winner['count'])} games, {top_winner['share']:.1%})",
        f"- Highest survival role: {top_survival['role']} ({top_survival['survival_rate']:.1%})",
        (
            "- Messages per game: "
            f"mean={msg_summary['mean_messages_per_game']:.2f}, "
            f"median={msg_summary['median_messages_per_game']:.2f}, "
            f"min={int(msg_summary['min_messages_per_game'])}, "
            f"max={int(msg_summary['max_messages_per_game'])}"
        ),
        f"- Most frequent event type: {top_event['event_name']} ({int(top_event['count'])}, {top_event['share']:.1%})",
    ]
    return "\n".join(lines) + "\n"


def write_outputs(
    *,
    output_dir: Path | str | None,
    tables: Iterable[tuple[str, pd.DataFrame]],
    summary_text: str,
) -> tuple[Path, Path]:
    table_dir, plot_dir = resolve_output_dirs(output_dir)
    for filename, dataframe in tables:
        dataframe.to_csv(table_dir / filename, index=False)

    (table_dir / "summary.txt").write_text(summary_text, encoding="utf-8")
    return table_dir, plot_dir


def run_overview_statistics(
    *,
    merged_root: Path | str = DEFAULT_MERGED_ROOT,
    chunk_root: Path | str = DEFAULT_CHUNK_ROOT,
    output_dir: Path | str | None = None,
) -> dict[str, pd.DataFrame]:
    games = load_table("games", merged_root=merged_root, chunk_root=chunk_root)
    players = load_table("players", merged_root=merged_root, chunk_root=chunk_root)
    public_messages = load_table(
        "public_messages",
        merged_root=merged_root,
        chunk_root=chunk_root,
    )
    event_type_counts = load_event_name_counts(merged_root=merged_root, chunk_root=chunk_root)

    winner_team_counts = count_table(games["winner_team"], "winner_team")
    game_length_counts = count_table(games["last_day"], "last_day", numeric_sort=True)
    role_counts = count_table(players["role"], "role")
    role_survival_rates = build_role_survival_table(players)
    messages_per_game_distribution = build_messages_per_game_distribution(games, public_messages)
    messages_per_game_summary = summarize_messages_per_game(messages_per_game_distribution)
    message_length_summary = pd.DataFrame(
        [
            {
                "n_messages": int(len(public_messages)),
                "mean_text_len": float(public_messages["text_len"].mean()),
                "median_text_len": float(public_messages["text_len"].median()),
                "std_text_len": float(public_messages["text_len"].std(ddof=1)),
                "min_text_len": int(public_messages["text_len"].min()),
                "max_text_len": int(public_messages["text_len"].max()),
            }
        ]
    )
    overview_metrics = build_overview_metrics(games, players, public_messages, event_type_counts)
    summary_text = generate_summary_text(
        overview_metrics,
        winner_team_counts,
        role_survival_rates,
        messages_per_game_summary,
        event_type_counts,
    )

    results = {
        "overview_metrics": overview_metrics,
        "winner_team_counts": winner_team_counts,
        "game_length_counts": game_length_counts,
        "role_counts": role_counts,
        "role_survival_rates": role_survival_rates,
        "messages_per_game_distribution": messages_per_game_distribution,
        "messages_per_game_summary": messages_per_game_summary,
        "message_length_summary": message_length_summary,
        "event_type_counts": event_type_counts,
    }

    write_outputs(
        output_dir=output_dir,
        tables=[
            ("overview_metrics.csv", overview_metrics),
            ("winner_team_counts.csv", winner_team_counts),
            ("game_length_counts.csv", game_length_counts),
            ("role_counts.csv", role_counts),
            ("role_survival_rates.csv", role_survival_rates),
            ("messages_per_game_distribution.csv", messages_per_game_distribution),
            ("messages_per_game_summary.csv", messages_per_game_summary),
            ("message_length_summary.csv", message_length_summary),
            ("event_type_counts.csv", event_type_counts),
        ],
        summary_text=summary_text,
    )
    results["summary_text"] = pd.DataFrame([{"text": summary_text}])
    return results
