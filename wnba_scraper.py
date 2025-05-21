#!/usr/bin/env python3
"""
wnba_scraper.py

Fetch 2025 WNBA player totals via ESPN’s Stats API and save to CSV.
"""

import requests
import pandas as pd

# -----------------------------
# Configuration
# -----------------------------
STATS_URL = "https://stats.wnba.com/stats/leagueLeaders"
PARAMS = {
    "LeagueID": "10",              # WNBA league ID
    "Season": "2025",              # season year
    "SeasonType": "Regular Season",
    "PerMode": "Totals",           # totals, not per game
    "StatCategory": "PTS"          # default category (you can change this)
}
HEADERS = {
    "Host": "stats.wnba.com",
    "Connection": "keep-alive",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
    "Referer": "https://stats.wnba.com/",
    "Origin": "https://stats.wnba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true"
}
OUTPUT_CSV = "wnba_player_stats.csv"

# -----------------------------
# Fetch & Save
# -----------------------------
def fetch_wnba_player_stats():
    """Hit the JSON API and return a DataFrame of the resultSet."""
    print("🚀 Fetching 2025 WNBA stats via JSON API…")
    resp = requests.get(STATS_URL, params=PARAMS, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    data = resp.json().get("resultSet", {})
    headers = data.get("headers", [])
    rows = data.get("rowSet", [])
    if not headers or not rows:
        raise ValueError("No data returned from WNBA API.")
    return pd.DataFrame(rows, columns=headers)

def save_to_csv(df: pd.DataFrame, path: str = OUTPUT_CSV):
    """Write the DataFrame out to CSV."""
    df.to_csv(path, index=False)
    print(f"💾 WNBA player stats saved to '{path}'")

# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    try:
        df = fetch_wnba_player_stats()
        print(df.head(5).to_string(index=False))  # quick preview
        save_to_csv(df)
    except Exception as e:
        print(f"❌ Error fetching WNBA stats: {e}")