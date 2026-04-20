import os
import pandas as pd

base_dir = "/Users/menghanyu/Desktop/STAT 605/group_project"

data_dir = os.path.join(base_dir, "download/outputs/merged")
output_dir = os.path.join(base_dir, "analysis/vote_analysis/outputs")

os.makedirs(output_dir, exist_ok=True)

# =========================
# 2. Loding data
# =========================
events = pd.read_parquet(os.path.join(data_dir, "events.parquet"))
players = pd.read_parquet(os.path.join(data_dir, "players.parquet"))
games = pd.read_parquet(os.path.join(data_dir, "games.parquet"))

print("events:", events.shape)
print("players:", players.shape)
print("games:", games.shape)

print("\nTop event types:")
print(events["event_name"].value_counts().head(30))

# =========================
# 3. Extract vote_action
# =========================
vote_actions = events[events["event_name"] == "vote_action"].copy()

print("\nRaw vote_action shape:", vote_actions.shape)

# Only retain the real voting behavior
vote_actions_clean = vote_actions[
    vote_actions["description"].str.contains("has voted for", na=False)
    & ~vote_actions["description"].str.contains("submitted", na=False)
].copy()

print("\nClean vote shape:", vote_actions_clean.shape)
print(vote_actions_clean["description"].head(20))

# =========================
# 4. Distinguish between daytime and nighttime voting
# =========================
vote_actions_clean["vote_type"] = vote_actions_clean["phase"].apply(
    lambda x: "day" if "Day" in str(x) else "night"
)

print("\nVote type counts:")
print(vote_actions_clean["vote_type"].value_counts())

# Save the cleaned voting events
vote_actions_clean.to_csv(
    os.path.join(output_dir, "vote_events_clean.csv"),
    index=False
)

# =========================
# 5. player-level features
# =========================

# How many times is each player thrown in total in each round
votes_received = (
    vote_actions_clean.groupby(["game_id", "target_id"])
    .size()
    .reset_index(name="n_votes_received")
    .rename(columns={"target_id": "player_id"})
)

# How many times did each player throw in total in each round
votes_cast = (
    vote_actions_clean.groupby(["game_id", "actor_id"])
    .size()
    .reset_index(name="n_votes_cast")
    .rename(columns={"actor_id": "player_id"})
)

# The number of votes during the day
day_votes_cast = (
    vote_actions_clean[vote_actions_clean["vote_type"] == "day"]
    .groupby(["game_id", "actor_id"])
    .size()
    .reset_index(name="n_day_votes_cast")
    .rename(columns={"actor_id": "player_id"})
)

# The number of votes at night
night_votes_cast = (
    vote_actions_clean[vote_actions_clean["vote_type"] == "night"]
    .groupby(["game_id", "actor_id"])
    .size()
    .reset_index(name="n_night_votes_cast")
    .rename(columns={"actor_id": "player_id"})
)

# The number of votes received during the day
day_votes_received = (
    vote_actions_clean[vote_actions_clean["vote_type"] == "day"]
    .groupby(["game_id", "target_id"])
    .size()
    .reset_index(name="n_day_votes_received")
    .rename(columns={"target_id": "player_id"})
)

# The number of votes received at night
night_votes_received = (
    vote_actions_clean[vote_actions_clean["vote_type"] == "night"]
    .groupby(["game_id", "target_id"])
    .size()
    .reset_index(name="n_night_votes_received")
    .rename(columns={"target_id": "player_id"})
)

vote_features_by_player = (
    players.merge(votes_received, on=["game_id", "player_id"], how="left")
           .merge(votes_cast, on=["game_id", "player_id"], how="left")
           .merge(day_votes_cast, on=["game_id", "player_id"], how="left")
           .merge(night_votes_cast, on=["game_id", "player_id"], how="left")
           .merge(day_votes_received, on=["game_id", "player_id"], how="left")
           .merge(night_votes_received, on=["game_id", "player_id"], how="left")
)

# Fill in 0 for the missing value
vote_cols = [
    "n_votes_received",
    "n_votes_cast",
    "n_day_votes_cast",
    "n_night_votes_cast",
    "n_day_votes_received",
    "n_night_votes_received"
]

for col in vote_cols:
    vote_features_by_player[col] = vote_features_by_player[col].fillna(0)

# Merge the game-level information
vote_features_by_player = vote_features_by_player.merge(
    games[["game_id", "winner_team", "last_day", "n_players", "end_reason"]],
    on="game_id",
    how="left"
)

print("\nPlayer-level vote features:")
print(vote_features_by_player.head())

vote_features_by_player.to_csv(
    os.path.join(output_dir, "vote_features_by_player.csv"),
    index=False
)

# =========================
# 6. game-day-level feature：tie / concentration
# =========================

# How many votes are received for each target in each round, each day
vote_counts = (
    vote_actions_clean.groupby(["game_id", "day", "vote_type", "target_id"])
    .size()
    .reset_index(name="votes")
)

# The maximum number of votes for each round and each day
vote_counts["max_votes"] = vote_counts.groupby(
    ["game_id", "day", "vote_type"]
)["votes"].transform("max")

# Whether it is the highest vote in this round
vote_counts["is_top"] = vote_counts["votes"] == vote_counts["max_votes"]

# tie info
tie_info = (
    vote_counts.groupby(["game_id", "day", "vote_type"])["is_top"]
    .sum()
    .reset_index(name="n_top_players")
)

tie_info["is_tie"] = tie_info["n_top_players"] > 1

# concentration info
vote_summary = (
    vote_counts.groupby(["game_id", "day", "vote_type"])
    .agg(
        max_votes=("votes", "max"),
        total_votes=("votes", "sum"),
        n_targets=("target_id", "nunique")
    )
    .reset_index()
)

vote_summary["concentration"] = (
    vote_summary["max_votes"] / vote_summary["total_votes"]
)

vote_summary = vote_summary.merge(
    tie_info,
    on=["game_id", "day", "vote_type"],
    how="left"
)

vote_summary = vote_summary.merge(
    games[["game_id", "winner_team", "last_day", "n_players", "end_reason"]],
    on="game_id",
    how="left"
)

print("\nGame-day-level vote summary:")
print(vote_summary.head())

vote_summary.to_csv(
    os.path.join(output_dir, "vote_summary_by_round.csv"),
    index=False
)

# =========================
# 7. game-level figures
# =========================

# The total number of votes in each round
game_vote_totals = (
    vote_actions_clean.groupby(["game_id", "vote_type"])
    .size()
    .reset_index(name="total_votes")
)

# Wider table
game_vote_totals_wide = (
    game_vote_totals.pivot(index="game_id", columns="vote_type", values="total_votes")
    .reset_index()
    .rename_axis(None, axis=1)
)

# Ensure the existence of the column
for col in ["day", "night"]:
    if col not in game_vote_totals_wide.columns:
        game_vote_totals_wide[col] = 0

game_vote_totals_wide = game_vote_totals_wide.rename(columns={
    "day": "total_day_votes",
    "night": "total_night_votes"
})

# tie rate
game_tie_summary = (
    vote_summary.groupby("game_id")
    .agg(
        n_rounds=("day", "count"),
        n_tie_rounds=("is_tie", "sum"),
        avg_concentration=("concentration", "mean")
    )
    .reset_index()
)

game_tie_summary["tie_rate"] = (
    game_tie_summary["n_tie_rounds"] / game_tie_summary["n_rounds"]
)

# Nighttime werewolf consistency
night_agreement = (
    vote_summary[vote_summary["vote_type"] == "night"]
    .groupby("game_id")
    .agg(
        avg_night_agreement=("concentration", "mean")
    )
    .reset_index()
)

vote_features_by_game = (
    games.merge(game_vote_totals_wide, on="game_id", how="left")
         .merge(game_tie_summary, on="game_id", how="left")
         .merge(night_agreement, on="game_id", how="left")
)

fill_zero_cols = [
    "total_day_votes",
    "total_night_votes",
    "n_rounds",
    "n_tie_rounds",
    "avg_concentration",
    "tie_rate",
    "avg_night_agreement"
]

for col in fill_zero_cols:
    if col in vote_features_by_game.columns:
        vote_features_by_game[col] = vote_features_by_game[col].fillna(0)

print("\nGame-level vote features:")
print(vote_features_by_game.head())

vote_features_by_game.to_csv(
    os.path.join(output_dir, "vote_features_by_game.csv"),
    index=False
)

print("\nDone. Files saved to:")
print(output_dir)