import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

base_dir = "/Users/menghanyu/Desktop/STAT 605/group_project"
input_dir = os.path.join(base_dir, "analysis/vote_analysis/outputs")
plot_dir = os.path.join(base_dir, "analysis/vote_analysis/plots")

os.makedirs(plot_dir, exist_ok=True)

# =======================
# 1. Loading data
# =========================
vote_events = pd.read_csv(os.path.join(input_dir, "vote_events_clean.csv"))
vote_features_by_player = pd.read_csv(os.path.join(input_dir, "vote_features_by_player.csv"))
vote_summary_by_round = pd.read_csv(os.path.join(input_dir, "vote_summary_by_round.csv"))
vote_features_by_game = pd.read_csv(os.path.join(input_dir, "vote_features_by_game.csv"))

print("vote_events:", vote_events.shape)
print("vote_features_by_player:", vote_features_by_player.shape)
print("vote_summary_by_round:", vote_summary_by_round.shape)
print("vote_features_by_game:", vote_features_by_game.shape)

# =========================
# 2. Summary
# =========================
print("\nAverage votes received by survival status:")
print(
    vote_features_by_player.groupby("alive_end")["n_votes_received"]
    .mean()
    .sort_values(ascending=False)
)

print("\nAverage votes received by role:")
print(
    vote_features_by_player.groupby("role")["n_votes_received"]
    .mean()
    .sort_values(ascending=False)
)

print("\nAverage votes received by winning team:")
print(
    vote_features_by_player.groupby("winner_team")["n_votes_received"]
    .mean()
    .sort_values(ascending=False)
)

print("\nTie rate summary:")
print(vote_features_by_game["tie_rate"].describe())

print("\nVote concentration by winning team:")
print(
    vote_features_by_game.groupby("winner_team")["avg_concentration"]
    .mean()
    .sort_values(ascending=False)
)

print("\nNight agreement summary:")
print(vote_features_by_game["avg_night_agreement"].describe())

# =========================
# 4. Visiualizion
# =========================
sns.set_theme(style="whitegrid")

# =========================
# 5. Figure 1：Votes received vs survival
# =========================
plt.figure(figsize=(8, 5))
sns.boxplot(data=vote_features_by_player, x="alive_end", y="n_votes_received")
plt.title("Votes Received by Survival Status")
plt.xlabel("Alive at End")
plt.ylabel("Number of Votes Received")
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "01_votes_received_vs_survival.png"), dpi=300)
plt.show()

# =========================
# 6. Figure 2：Votes received by role
# =========================
plt.figure(figsize=(10, 5))
sns.boxplot(data=vote_features_by_player, x="role", y="n_votes_received")
plt.title("Votes Received by Role")
plt.xlabel("Role")
plt.ylabel("Number of Votes Received")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "02_votes_received_by_role.png"), dpi=300)
plt.show()

# =========================
# 7. Figure 3：Votes received by winning team
# =========================
plt.figure(figsize=(8, 5))
sns.boxplot(data=vote_features_by_player, x="winner_team", y="n_votes_received")
plt.title("Votes Received by Winning Team")
plt.xlabel("Winning Team")
plt.ylabel("Number of Votes Received")
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "03_votes_received_by_winning_team.png"), dpi=300)
plt.show()

# =========================
# 8. Figure 4：Day vs Night vote counts
# =========================
plt.figure(figsize=(6, 5))
sns.countplot(data=vote_events, x="vote_type")
plt.title("Number of Vote Actions: Day vs Night")
plt.xlabel("Vote Type")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "04_day_vs_night_vote_counts.png"), dpi=300)
plt.show()

# =========================
# 9. Figure 5：Tie frequency
# =========================
plt.figure(figsize=(6, 5))
sns.countplot(data=vote_summary_by_round, x="is_tie")
plt.title("Frequency of Tie Rounds")
plt.xlabel("Tie Occurred")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "05_tie_frequency.png"), dpi=300)
plt.show()

# =========================
# 10. Figure 6：Vote concentration by winning team
# =========================
plt.figure(figsize=(8, 5))
sns.boxplot(data=vote_features_by_game, x="winner_team", y="avg_concentration")
plt.title("Average Vote Concentration by Winning Team")
plt.xlabel("Winning Team")
plt.ylabel("Average Vote Concentration")
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "06_vote_concentration_by_winning_team.png"), dpi=300)
plt.show()

# =========================
# 11. Figure 7：Night vote agreement
# =========================
plt.figure(figsize=(8, 5))
sns.histplot(vote_features_by_game["avg_night_agreement"], bins=20)
plt.title("Distribution of Average Night Vote Agreement")
plt.xlabel("Average Night Vote Agreement")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig(os.path.join(plot_dir, "07_night_vote_agreement_distribution.png"), dpi=300)
plt.show()

print("\nDone. Plots saved to:")
print(plot_dir)