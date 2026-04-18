"""Parsing raw Werewolf JSON logs into analysis tables."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from werewolf.config import POWER_ROLES, TABLE_NAMES, TEAM_BY_ROLE


def infer_team(role: str | None) -> str | None:
    return TEAM_BY_ROLE.get(role)


def normalize_target(target_id: Any) -> str | None:
    if target_id in {None, "", -1, "-1", "Abstain"}:
        return None
    return str(target_id)


def safe_word_count(text: str | None) -> int:
    if not text:
        return 0
    return len(str(text).split())


def truncate(text: str | None, limit: int = 240) -> str | None:
    if text is None:
        return None
    text = str(text).replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def empty_tables() -> dict[str, list[dict[str, Any]]]:
    return {table_name: [] for table_name in TABLE_NAMES}


def _payload_records(game_obj: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    moderator_blocks = game_obj.get("info", {}).get("MODERATOR_OBSERVATION", [])
    seq = 0
    for block_index, block in enumerate(moderator_blocks):
        if not isinstance(block, list):
            continue
        for event_index, item in enumerate(block):
            if not isinstance(item, dict):
                continue
            json_str = item.get("json_str")
            if not json_str:
                continue
            try:
                payload = json.loads(json_str)
            except json.JSONDecodeError:
                continue
            data = payload.get("data")
            if not isinstance(data, dict):
                data = {}
            records.append(
                {
                    "event_seq": seq,
                    "block_index": block_index,
                    "event_index": event_index,
                    "payload": payload,
                    "data": data,
                }
            )
            seq += 1
    return records


def parse_game_file(path: str | Path) -> dict[str, list[dict[str, Any]]]:
    path = Path(path)
    tables = empty_tables()
    episode_id = path.stem

    try:
        if path.stat().st_size == 0:
            tables["errors"].append(
                {
                    "episode_id": episode_id,
                    "file_name": path.name,
                    "file_path": str(path),
                    "error_type": "empty_file",
                    "error_message": "File size is zero bytes.",
                }
            )
            return tables

        with path.open("r", encoding="utf-8") as handle:
            game_obj = json.load(handle)
    except Exception as exc:  # noqa: BLE001
        tables["errors"].append(
            {
                "episode_id": episode_id,
                "file_name": path.name,
                "file_path": str(path),
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            }
        )
        return tables

    try:
        info = game_obj.get("info", {})
        episode_id = str(info.get("EpisodeId", path.stem))
        config = game_obj.get("configuration", {})
        agents = config.get("agents", [])
        game_end = info.get("GAME_END", {})
        payload_records = _payload_records(game_obj)

        role_map: dict[str, str | None] = {}
        model_map: dict[str, str | None] = {}
        for agent in agents:
            player_id = str(agent.get("id"))
            role_map[player_id] = agent.get("role")
            model_map[player_id] = agent.get("display_name")

        winner_ids = {str(player_id) for player_id in game_end.get("winner_ids", [])}
        loser_ids = {str(player_id) for player_id in game_end.get("loser_ids", [])}
        elimination_info = {
            str(row.get("player_id")): row
            for row in game_end.get("elimination_info", [])
            if isinstance(row, dict)
        }
        all_players_end = {
            str(row.get("id")): row
            for row in game_end.get("all_players", [])
            if isinstance(row, dict)
        }

        day_outcomes: dict[int, dict[str, Any]] = {}
        night_vote_results: dict[int, str | None] = {}
        night_eliminations: dict[int, str | None] = {}
        day_vote_no_exile_count = 0

        for record in payload_records:
            payload = record["payload"]
            data = record["data"]
            name = payload.get("event_name")
            phase = payload.get("phase")
            day = payload.get("day")

            if name == "elimination" and phase == "Day":
                day_outcomes[int(day)] = {
                    "outcome": "exile",
                    "elected_player_id": normalize_target(data.get("elected_player_id")),
                    "reason": None,
                }
            elif name == "vote_result" and phase == "Day":
                outcome = data.get("outcome")
                elected_player_id = normalize_target(data.get("elected_player_id"))
                if outcome == "no_exile":
                    day_vote_no_exile_count += 1
                day_outcomes[int(day)] = {
                    "outcome": outcome or "vote_result",
                    "elected_player_id": elected_player_id,
                    "reason": data.get("reason"),
                }
            elif name == "vote_result" and phase == "Night":
                night_vote_results[int(day)] = normalize_target(data.get("elected_target_player_id"))
            elif name == "elimination" and phase == "Night":
                night_eliminations[int(day)] = normalize_target(data.get("eliminated_player_id"))

        for player_id, role in role_map.items():
            end_row = all_players_end.get(player_id, {})
            elim_row = elimination_info.get(player_id, {})
            eliminated_day = elim_row.get("eliminated_during_day", -1)
            eliminated_phase = elim_row.get("eliminated_during_phase")
            last_day = int(game_end.get("last_day", -1))
            survival_days = last_day if eliminated_day in {-1, None} else int(eliminated_day)
            tables["players"].append(
                {
                    "episode_id": episode_id,
                    "player_id": player_id,
                    "role": role,
                    "team": infer_team(role),
                    "model_name": model_map.get(player_id),
                    "won": int(player_id in winner_ids),
                    "lost": int(player_id in loser_ids),
                    "alive_final": int(bool(end_row.get("alive"))),
                    "survived_to_end": int(eliminated_day in {-1, None}),
                    "eliminated_during_day": eliminated_day,
                    "eliminated_during_phase": eliminated_phase,
                    "survival_days": survival_days,
                    "is_power_role": int(role in POWER_ROLES),
                }
            )

        vote_rows: list[dict[str, Any]] = []
        speech_rows: list[dict[str, Any]] = []
        night_action_rows: list[dict[str, Any]] = []

        for record in payload_records:
            payload = record["payload"]
            data = record["data"]
            event_seq = record["event_seq"]
            name = payload.get("event_name")
            day = int(payload.get("day", -1))
            phase = payload.get("phase")
            public = bool(payload.get("public"))
            source = payload.get("source")
            actor_id = data.get("actor_id")
            if actor_id is None and source in role_map:
                actor_id = source
            actor_id = str(actor_id) if actor_id is not None else None

            target_id = normalize_target(
                data.get("target_id")
                or data.get("elected_target_player_id")
                or data.get("eliminated_player_id")
                or data.get("elected_player_id")
                or data.get("player_id")
            )
            message = data.get("message")
            mentioned_player_ids = data.get("mentioned_player_ids")
            if not isinstance(mentioned_player_ids, list):
                mentioned_player_ids = []

            tables["events"].append(
                {
                    "episode_id": episode_id,
                    "event_seq": event_seq,
                    "block_index": record["block_index"],
                    "event_index": record["event_index"],
                    "event_name": name,
                    "day": day,
                    "phase": phase,
                    "detailed_phase": payload.get("detailed_phase"),
                    "public": int(public),
                    "source": source,
                    "actor_id": actor_id,
                    "target_id": target_id,
                    "actor_role": role_map.get(actor_id),
                    "target_role": role_map.get(target_id),
                    "message": message if public and name == "discussion" else None,
                    "message_words": safe_word_count(message),
                    "mentioned_players_count": len(mentioned_player_ids),
                    "outcome": data.get("outcome"),
                    "reason": data.get("reason"),
                    "visible_to_count": len(payload.get("visible_to") or []),
                    "created_at": payload.get("created_at"),
                    "description_short": truncate(payload.get("description")),
                }
            )

            if name == "discussion" and public and actor_id is not None:
                speech_rows.append(
                    {
                        "episode_id": episode_id,
                        "event_seq": event_seq,
                        "day": day,
                        "phase": phase,
                        "actor_id": actor_id,
                        "actor_role": role_map.get(actor_id),
                        "actor_team": infer_team(role_map.get(actor_id)),
                        "won": int(actor_id in winner_ids),
                        "message": message,
                        "message_chars": len(message or ""),
                        "message_words": safe_word_count(message),
                        "question_count": (message or "").count("?"),
                        "mentioned_players_count": len(mentioned_player_ids),
                    }
                )

            if name == "vote_action" and actor_id is not None:
                day_outcome = day_outcomes.get(day, {})
                night_pack_target_id = night_vote_results.get(day) if phase == "Night" else None
                vote_rows.append(
                    {
                        "episode_id": episode_id,
                        "event_seq": event_seq,
                        "day": day,
                        "phase": phase,
                        "actor_id": actor_id,
                        "actor_role": role_map.get(actor_id),
                        "actor_team": infer_team(role_map.get(actor_id)),
                        "target_id": target_id,
                        "target_role": role_map.get(target_id),
                        "target_team": infer_team(role_map.get(target_id)),
                        "is_day_vote": int(phase == "Day"),
                        "is_night_vote": int(phase == "Night"),
                        "is_abstain": int(target_id is None),
                        "vote_target_is_werewolf": int(role_map.get(target_id) == "Werewolf"),
                        "vote_target_is_power_role": int(role_map.get(target_id) in POWER_ROLES),
                        "day_vote_outcome": day_outcome.get("outcome") if phase == "Day" else None,
                        "day_exiled_player_id": day_outcome.get("elected_player_id") if phase == "Day" else None,
                        "voted_for_exiled_player": int(
                            phase == "Day"
                            and target_id is not None
                            and target_id == day_outcome.get("elected_player_id")
                        ),
                        "day_no_exile": int(phase == "Day" and day_outcome.get("outcome") == "no_exile"),
                        "night_pack_target_id": night_pack_target_id,
                        "voted_for_pack_target": int(
                            phase == "Night"
                            and target_id is not None
                            and target_id == night_pack_target_id
                        ),
                    }
                )

            if name in {"inspect_action", "heal_action"} and actor_id is not None:
                action_type = "seer_inspect" if name == "inspect_action" else "doctor_heal"
                night_action_rows.append(
                    {
                        "episode_id": episode_id,
                        "event_seq": event_seq,
                        "day": day,
                        "phase": phase,
                        "action_type": action_type,
                        "source_event": name,
                        "actor_id": actor_id,
                        "actor_role": role_map.get(actor_id),
                        "actor_team": infer_team(role_map.get(actor_id)),
                        "target_id": target_id,
                        "target_role": role_map.get(target_id),
                        "target_team": infer_team(role_map.get(target_id)),
                        "target_is_werewolf": int(role_map.get(target_id) == "Werewolf"),
                        "target_is_power_role": int(role_map.get(target_id) in POWER_ROLES),
                        "night_pack_target_id": night_vote_results.get(day),
                        "night_eliminated_player_id": night_eliminations.get(day),
                    }
                )

            if name == "vote_action" and phase == "Night" and role_map.get(actor_id) == "Werewolf":
                pack_target = night_vote_results.get(day)
                night_action_rows.append(
                    {
                        "episode_id": episode_id,
                        "event_seq": event_seq,
                        "day": day,
                        "phase": phase,
                        "action_type": "werewolf_vote",
                        "source_event": name,
                        "actor_id": actor_id,
                        "actor_role": role_map.get(actor_id),
                        "actor_team": infer_team(role_map.get(actor_id)),
                        "target_id": target_id,
                        "target_role": role_map.get(target_id),
                        "target_team": infer_team(role_map.get(target_id)),
                        "target_is_werewolf": int(role_map.get(target_id) == "Werewolf"),
                        "target_is_power_role": int(role_map.get(target_id) in POWER_ROLES),
                        "night_pack_target_id": pack_target,
                        "night_eliminated_player_id": night_eliminations.get(day),
                        "voted_for_pack_target": int(target_id is not None and target_id == pack_target),
                    }
                )

        for row in night_action_rows:
            if row["action_type"] == "doctor_heal":
                row["healed_pack_target"] = int(row["target_id"] == row.get("night_pack_target_id"))
                row["successful_save"] = int(
                    row["target_id"] is not None
                    and row["target_id"] == row.get("night_pack_target_id")
                    and row.get("night_eliminated_player_id") != row["target_id"]
                )
                row["self_target"] = int(row["actor_id"] == row["target_id"])
            elif row["action_type"] == "seer_inspect":
                row["healed_pack_target"] = 0
                row["successful_save"] = 0
                row["self_target"] = int(row["actor_id"] == row["target_id"])
            elif row["action_type"] == "werewolf_vote":
                row["healed_pack_target"] = 0
                row["successful_save"] = 0
                row["self_target"] = int(row["actor_id"] == row["target_id"])

        tables["votes"].extend(vote_rows)
        tables["speeches"].extend(speech_rows)
        tables["night_actions"].extend(night_action_rows)

        tables["games"].append(
            {
                "episode_id": episode_id,
                "file_name": path.name,
                "file_path": str(path),
                "file_size_bytes": path.stat().st_size,
                "player_count": len(agents),
                "winner_team": game_end.get("winner_team"),
                "winner_count": len(winner_ids),
                "loser_count": len(loser_ids),
                "last_day": int(game_end.get("last_day", -1)),
                "last_phase": game_end.get("last_phase"),
                "game_end_reason": game_end.get("reason"),
                "terminated_with_agent_error": int(bool(game_end.get("terminated_with_agent_error"))),
                "public_discussion_count": len(speech_rows),
                "day_vote_count": sum(1 for row in vote_rows if row["is_day_vote"] == 1),
                "night_vote_count": sum(1 for row in vote_rows if row["is_night_vote"] == 1),
                "night_action_count": len(night_action_rows),
                "day_vote_no_exile_count": day_vote_no_exile_count,
            }
        )
        return tables
    except Exception as exc:  # noqa: BLE001
        tables = empty_tables()
        tables["errors"].append(
            {
                "episode_id": episode_id,
                "file_name": path.name,
                "file_path": str(path),
                "error_type": f"parse_{type(exc).__name__}",
                "error_message": str(exc),
            }
        )
        return tables
