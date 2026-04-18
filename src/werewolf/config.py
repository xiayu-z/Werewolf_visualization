"""Shared configuration for the Werewolf project."""

from __future__ import annotations

TABLE_NAMES = [
    "games",
    "players",
    "votes",
    "speeches",
    "night_actions",
    "events",
    "errors",
]

ROLE_ORDER = ["Villager", "Doctor", "Seer", "Werewolf"]
POWER_ROLES = {"Doctor", "Seer", "Werewolf"}

TEAM_BY_ROLE = {
    "Villager": "Villagers",
    "Doctor": "Villagers",
    "Seer": "Villagers",
    "Werewolf": "Werewolves",
}
