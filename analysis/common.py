"""Shared helpers for analysis scripts."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from werewolf.config import ROLE_ORDER, TABLE_NAMES
from werewolf.io_utils import ensure_dir, read_table


def setup_plot_style() -> None:
    sns.set_theme(style="whitegrid", context="talk")
    plt.rcParams["figure.dpi"] = 160
    plt.rcParams["savefig.bbox"] = "tight"


def load_tables(processed_root: str | Path) -> dict[str, pd.DataFrame]:
    processed_root = Path(processed_root)
    tables: dict[str, pd.DataFrame] = {}
    for table_name in TABLE_NAMES:
        try:
            tables[table_name] = read_table(processed_root, table_name)
        except FileNotFoundError:
            tables[table_name] = pd.DataFrame()
    return tables


def prepare_output_dirs(output_root: str | Path, section_name: str) -> tuple[Path, Path]:
    section_root = ensure_dir(Path(output_root) / section_name)
    tables_dir = ensure_dir(section_root / "tables")
    figures_dir = ensure_dir(section_root / "figures")
    return tables_dir, figures_dir


def safe_rate(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.replace(0, pd.NA)
    return (numerator / denominator).fillna(0)


def add_quantile_bin(df: pd.DataFrame, column: str, new_column: str) -> pd.DataFrame:
    if df.empty or df[column].nunique(dropna=True) < 2:
        df[new_column] = "all"
        return df
    quantiles = min(4, int(df[column].nunique(dropna=True)))
    df[new_column] = pd.qcut(df[column], q=quantiles, duplicates="drop")
    df[new_column] = df[new_column].astype(str)
    return df


def build_player_feature_table(
    players: pd.DataFrame,
    votes: pd.DataFrame,
    speeches: pd.DataFrame,
    night_actions: pd.DataFrame,
) -> pd.DataFrame:
    features = players.copy()

    if not votes.empty:
        day_votes = votes[votes["is_day_vote"] == 1].copy()
        day_vote_actor = (
            day_votes.groupby(["episode_id", "actor_id"], as_index=False)
            .agg(
                day_votes_cast=("target_id", "size"),
                unique_day_vote_targets=("target_id", "nunique"),
                day_votes_against_wolf=("vote_target_is_werewolf", "sum"),
                day_votes_against_power_role=("vote_target_is_power_role", "sum"),
                day_votes_for_exiled=("voted_for_exiled_player", "sum"),
                day_vote_no_exile_rounds=("day_no_exile", "sum"),
            )
            .rename(columns={"actor_id": "player_id"})
        )
        first_day_votes = day_votes[day_votes["day"] == 1].sort_values(["episode_id", "actor_id", "event_seq"])
        first_day_votes = (
            first_day_votes.groupby(["episode_id", "actor_id"], as_index=False)
            .first()[["episode_id", "actor_id", "target_role", "vote_target_is_werewolf", "voted_for_exiled_player"]]
            .rename(
                columns={
                    "actor_id": "player_id",
                    "target_role": "day1_target_role",
                    "vote_target_is_werewolf": "day1_voted_wolf",
                    "voted_for_exiled_player": "day1_voted_exiled",
                }
            )
        )
        day_votes_received = (
            day_votes.groupby(["episode_id", "target_id"], as_index=False)
            .size()
            .rename(columns={"target_id": "player_id", "size": "day_votes_received"})
        )
        features = features.merge(day_vote_actor, on=["episode_id", "player_id"], how="left")
        features = features.merge(first_day_votes, on=["episode_id", "player_id"], how="left")
        features = features.merge(day_votes_received, on=["episode_id", "player_id"], how="left")
    else:
        features["day_votes_cast"] = 0
        features["unique_day_vote_targets"] = 0
        features["day_votes_against_wolf"] = 0
        features["day_votes_against_power_role"] = 0
        features["day_votes_for_exiled"] = 0
        features["day_vote_no_exile_rounds"] = 0
        features["day1_target_role"] = None
        features["day1_voted_wolf"] = 0
        features["day1_voted_exiled"] = 0
        features["day_votes_received"] = 0

    if not speeches.empty:
        speech_actor = (
            speeches.groupby(["episode_id", "actor_id"], as_index=False)
            .agg(
                public_message_count=("message", "size"),
                public_char_count=("message_chars", "sum"),
                public_word_count=("message_words", "sum"),
                avg_words_per_message=("message_words", "mean"),
                question_count=("question_count", "sum"),
                mentioned_players_total=("mentioned_players_count", "sum"),
            )
            .rename(columns={"actor_id": "player_id"})
        )
        day1_speech = speeches[speeches["day"] == 1]
        day1_speech = (
            day1_speech.groupby(["episode_id", "actor_id"], as_index=False)
            .agg(
                day1_public_message_count=("message", "size"),
                day1_public_word_count=("message_words", "sum"),
            )
            .rename(columns={"actor_id": "player_id"})
        )
        features = features.merge(speech_actor, on=["episode_id", "player_id"], how="left")
        features = features.merge(day1_speech, on=["episode_id", "player_id"], how="left")
    else:
        features["public_message_count"] = 0
        features["public_char_count"] = 0
        features["public_word_count"] = 0
        features["avg_words_per_message"] = 0
        features["question_count"] = 0
        features["mentioned_players_total"] = 0
        features["day1_public_message_count"] = 0
        features["day1_public_word_count"] = 0

    if not night_actions.empty:
        seer_actions = night_actions[night_actions["action_type"] == "seer_inspect"]
        doctor_actions = night_actions[night_actions["action_type"] == "doctor_heal"]
        wolf_actions = night_actions[night_actions["action_type"] == "werewolf_vote"]

        if not seer_actions.empty:
            seer_features = (
                seer_actions.groupby(["episode_id", "actor_id"], as_index=False)
                .agg(
                    seer_inspects=("target_id", "size"),
                    seer_unique_targets=("target_id", "nunique"),
                    seer_hit_wolf_count=("target_is_werewolf", "sum"),
                )
                .rename(columns={"actor_id": "player_id"})
            )
            features = features.merge(seer_features, on=["episode_id", "player_id"], how="left")

        if not doctor_actions.empty:
            doctor_features = (
                doctor_actions.groupby(["episode_id", "actor_id"], as_index=False)
                .agg(
                    doctor_heals=("target_id", "size"),
                    doctor_unique_heal_targets=("target_id", "nunique"),
                    doctor_healed_pack_target_count=("healed_pack_target", "sum"),
                    doctor_successful_save_count=("successful_save", "sum"),
                    doctor_self_target_count=("self_target", "sum"),
                )
                .rename(columns={"actor_id": "player_id"})
            )
            features = features.merge(doctor_features, on=["episode_id", "player_id"], how="left")

        if not wolf_actions.empty:
            wolf_features = (
                wolf_actions.groupby(["episode_id", "actor_id"], as_index=False)
                .agg(
                    werewolf_night_votes=("target_id", "size"),
                    werewolf_unique_targets=("target_id", "nunique"),
                    werewolf_consensus_votes=("voted_for_pack_target", "sum"),
                    werewolf_targeted_power_role_count=("target_is_power_role", "sum"),
                )
                .rename(columns={"actor_id": "player_id"})
            )
            features = features.merge(wolf_features, on=["episode_id", "player_id"], how="left")

    fill_zero_columns = [
        "day_votes_cast",
        "unique_day_vote_targets",
        "day_votes_against_wolf",
        "day_votes_against_power_role",
        "day_votes_for_exiled",
        "day_vote_no_exile_rounds",
        "day1_voted_wolf",
        "day1_voted_exiled",
        "day_votes_received",
        "public_message_count",
        "public_char_count",
        "public_word_count",
        "avg_words_per_message",
        "question_count",
        "mentioned_players_total",
        "day1_public_message_count",
        "day1_public_word_count",
        "seer_inspects",
        "seer_unique_targets",
        "seer_hit_wolf_count",
        "doctor_heals",
        "doctor_unique_heal_targets",
        "doctor_healed_pack_target_count",
        "doctor_successful_save_count",
        "doctor_self_target_count",
        "werewolf_night_votes",
        "werewolf_unique_targets",
        "werewolf_consensus_votes",
        "werewolf_targeted_power_role_count",
    ]
    for column in fill_zero_columns:
        if column not in features.columns:
            features[column] = 0
        features[column] = features[column].fillna(0)

    if "day1_target_role" not in features.columns:
        features["day1_target_role"] = None

    features["vote_accuracy"] = safe_rate(features["day_votes_against_wolf"], features["day_votes_cast"])
    features["vote_majority_alignment"] = safe_rate(features["day_votes_for_exiled"], features["day_votes_cast"])
    features["question_rate"] = safe_rate(features["question_count"], features["public_message_count"])
    features["seer_hit_rate"] = safe_rate(features["seer_hit_wolf_count"], features["seer_inspects"])
    features["doctor_save_rate"] = safe_rate(features["doctor_successful_save_count"], features["doctor_heals"])
    features["werewolf_consensus_rate"] = safe_rate(features["werewolf_consensus_votes"], features["werewolf_night_votes"])

    if "role" in features.columns:
        features["role"] = pd.Categorical(features["role"], categories=ROLE_ORDER, ordered=True)

    return features
