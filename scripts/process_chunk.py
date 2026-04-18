import csv
import json
import argparse
from pathlib import Path


def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def parse_nested_json_str(raw):
    """
    raw may be:
    - a dict
    - a JSON string
    - something else
    """
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return None
    return None


def extract_game_and_players(obj, json_path):
    episode_id = safe_get(obj, "info", "EpisodeId", default=json_path.stem)
    game_end = safe_get(obj, "info", "GAME_END", default={}) or {}

    winner_team = game_end.get("winner_team")
    last_day = game_end.get("last_day")
    reason = game_end.get("reason")
    players = game_end.get("all_players", []) or []

    game_row = {
        "game_id": episode_id,
        "filename": json_path.name,
        "winner_team": winner_team,
        "last_day": last_day,
        "n_players": len(players),
        "end_reason": reason,
    }

    player_rows = []
    for p in players:
        agent = p.get("agent", {}) or {}
        player_rows.append({
            "game_id": episode_id,
            "player_id": p.get("id"),
            "role": agent.get("role"),
            "model_name": agent.get("display_name"),
            "alive_end": p.get("alive"),
            "eliminated_during_day": p.get("eliminated_during_day"),
            "eliminated_during_phase": p.get("eliminated_during_phase"),
        })

    return game_row, player_rows


def extract_observation_rows(obj, json_path):
    """
    From info.MODERATOR_OBSERVATION, extract:
    - public_messages rows
    - general events rows

    This version is intentionally conservative and robust:
    - event_rows: keep almost everything structured
    - public_message_rows: only keep clearly public, player-sourced descriptions
    """
    episode_id = safe_get(obj, "info", "EpisodeId", default=json_path.stem)
    moderator_obs = safe_get(obj, "info", "MODERATOR_OBSERVATION", default=[]) or []

    public_message_rows = []
    event_rows = []

    for outer_idx, block in enumerate(moderator_obs):
        if not isinstance(block, list):
            continue

        for inner_idx, item in enumerate(block):
            if not isinstance(item, dict):
                continue

            data_type = item.get("data_type")
            json_str = item.get("json_str")
            parsed = parse_nested_json_str(json_str)

            if not isinstance(parsed, dict):
                continue

            event_name = parsed.get("event_name")
            day = parsed.get("day")
            phase = parsed.get("phase")
            detailed_phase = parsed.get("detailed_phase")
            description = parsed.get("description")
            public = parsed.get("public")
            source = parsed.get("source")
            created_at = parsed.get("created_at")
            visible_in_ui = parsed.get("visible_in_ui")

            data = parsed.get("data", {})
            if data is None:
                data = {}

            actor_id = None
            target_id = None
            reasoning = None
            player_id = None

            if isinstance(data, dict):
                actor_id = data.get("actor_id")
                target_id = data.get("target_id")
                reasoning = data.get("reasoning")
                player_id = data.get("player_id")

            # Fall back if actor_id missing
            if actor_id is None:
                actor_id = player_id

            event_rows.append({
                "game_id": episode_id,
                "filename": json_path.name,
                "outer_idx": outer_idx,
                "inner_idx": inner_idx,
                "data_type": data_type,
                "event_name": event_name,
                "day": day,
                "phase": phase,
                "detailed_phase": detailed_phase,
                "source": source,
                "public": public,
                "visible_in_ui": visible_in_ui,
                "created_at": created_at,
                "actor_id": actor_id,
                "target_id": target_id,
                "reasoning": reasoning,
                "description": description,
            })

            # Conservative rule for public messages:
            # Keep only rows that are public, have a text description,
            # and come from a non-moderator source.
            if (
                public is True
                and isinstance(description, str)
                and description.strip() != ""
                and source not in [None, "MODERATOR"]
            ):
                public_message_rows.append({
                    "game_id": episode_id,
                    "filename": json_path.name,
                    "day": day,
                    "phase": phase,
                    "speaker_id": source,
                    "event_name": event_name,
                    "text": description,
                    "text_len": len(description),
                    "created_at": created_at,
                })

    return public_message_rows, event_rows


def process_json_file(json_path: Path):
    with open(json_path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    game_row, player_rows = extract_game_and_players(obj, json_path)
    public_message_rows, event_rows = extract_observation_rows(obj, json_path)

    return game_row, player_rows, public_message_rows, event_rows


def write_csv(rows, out_path, fieldnames):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk_file", required=True, help="Path to chunk manifest txt")
    parser.add_argument("--chunk_id", required=True, help="Chunk id such as 00000")
    parser.add_argument("--output_dir", required=True, help="Directory to save output csv files")
    args = parser.parse_args()

    chunk_file = Path(args.chunk_file)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    games_out = output_dir / f"games_chunk_{args.chunk_id}.csv"
    players_out = output_dir / f"players_chunk_{args.chunk_id}.csv"
    messages_out = output_dir / f"public_messages_chunk_{args.chunk_id}.csv"
    events_out = output_dir / f"events_chunk_{args.chunk_id}.csv"
    errors_out = output_dir / f"errors_chunk_{args.chunk_id}.csv"

    game_rows = []
    player_rows = []
    public_message_rows = []
    event_rows = []
    error_rows = []

    with open(chunk_file, "r", encoding="utf-8") as f:
        json_files = [Path(line.strip()) for line in f if line.strip()]

    for json_path in json_files:
        try:
            g_row, p_rows, m_rows, e_rows = process_json_file(json_path)
            game_rows.append(g_row)
            player_rows.extend(p_rows)
            public_message_rows.extend(m_rows)
            event_rows.extend(e_rows)
        except Exception as e:
            error_rows.append({
                "filepath": str(json_path),
                "error": str(e),
            })

    # Write outputs
    write_csv(
        game_rows,
        games_out,
        ["game_id", "filename", "winner_team", "last_day", "n_players", "end_reason"]
    )

    write_csv(
        player_rows,
        players_out,
        ["game_id", "player_id", "role", "model_name", "alive_end",
         "eliminated_during_day", "eliminated_during_phase"]
    )

    write_csv(
        public_message_rows,
        messages_out,
        ["game_id", "filename", "day", "phase", "speaker_id", "event_name",
         "text", "text_len", "created_at"]
    )

    write_csv(
        event_rows,
        events_out,
        ["game_id", "filename", "outer_idx", "inner_idx", "data_type", "event_name",
         "day", "phase", "detailed_phase", "source", "public", "visible_in_ui",
         "created_at", "actor_id", "target_id", "reasoning", "description"]
    )

    write_csv(
        error_rows,
        errors_out,
        ["filepath", "error"]
    )

    print(f"Processed {len(json_files)} json files")
    print(f"Wrote {games_out}")
    print(f"Wrote {players_out}")
    print(f"Wrote {messages_out}")
    print(f"Wrote {events_out}")
    print(f"Wrote {errors_out}")
    print(f"Game rows: {len(game_rows)}")
    print(f"Player rows: {len(player_rows)}")
    print(f"Public message rows: {len(public_message_rows)}")
    print(f"Event rows: {len(event_rows)}")
    print(f"Errors: {len(error_rows)}")


if __name__ == "__main__":
    main()
