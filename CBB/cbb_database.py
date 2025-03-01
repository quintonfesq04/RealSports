import requests
import pandas as pd
import time

# ESPN API URL
BASE_URL = "https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-college-basketball/statistics/byathlete"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Fetch player data from ESPN API
def fetch_players():
    """Fetches all players from ESPN API and saves to an Excel file."""
    players = []
    page = 1
    retries = 3

    print("🚀 Starting CBB player stats scraper...")

    while True:
        url = f"{BASE_URL}?region=us&lang=en&contentorigin=espn&page={page}&limit=50&sort=offensive.avgPoints:desc"
        attempt = 0

        # Retry mechanism
        while attempt < retries:
            try:
                print(f"📦 Fetching page {page}...")
                response = requests.get(url, headers=HEADERS, timeout=10)
                response.raise_for_status()  # Raise exception for non-200 status codes
                break
            except requests.RequestException as e:
                attempt += 1
                print(f"⚠️ Attempt {attempt} failed: {e}")
                time.sleep(2)  # Wait before retrying

        if attempt == retries:
            print("❌ Max retries reached. Stopping.")
            break

        data = response.json()
        athletes = data.get("athletes", [])
        if not athletes:
            print("✅ No more players found. Scraper completed.")
            break

        for player in athletes:
            athlete_info = player.get("athlete", {})
            name = athlete_info.get("displayName", "Unknown")

            team_info = athlete_info.get("teams", [{}])[0]
            team_abbr = team_info.get("abbreviation", "Unknown").upper()

            # Convert ESPN's "WIS" to RealSports' "WISC"
            if team_abbr == "WIS":
                team_abbr = "WISC"

            stats = {category["name"]: category.get("totals", [0]) for category in player.get("categories", [])}
            games_played = int(stats.get("general", [0])[15]) if "general" in stats and len(stats.get("general", [])) > 15 else 1

            try:
                players.append([
                    name,
                    team_abbr,
                    float(stats.get("offensive", [0])[0]),  # PPG
                    float(stats.get("offensive", [0])[10]), # APG
                    float(stats.get("general", [0])[12]),  # RPG
                    float(stats.get("offensive", [0])[4]),  # 3PM
                    games_played  # Total Games Played
                ])
            except (ValueError, IndexError):
                print(f"⚠️ Skipped player due to missing stats: {name}")

        page += 1  # Move to the next page

    # Convert to DataFrame and save
    if players:
        df = pd.DataFrame(players, columns=["Player", "Team", "PPG", "APG", "RPG", "3PM", "Games"])
        df.to_csv("CBB/cbb_players_stats.csv", index=False)
        print("✅ Player stats saved successfully to 'cbb_player_stats.xlsx'!")
    else:
        print("⚠️ No player data collected.")

# Run scraper
if __name__ == "__main__":
    fetch_players()