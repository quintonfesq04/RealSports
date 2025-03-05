import requests
import pandas as pd
import time
import os
from bs4 import BeautifulSoup  # You'll need to install BeautifulSoup (bs4)

# ESPN API URL and headers (already in your code)
BASE_URL = "https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-college-basketball/statistics/byathlete"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_players_api():
    """Fetches players from the ESPN API."""
    players = []
    page = 1
    retries = 3
    print("🚀 Starting CBB player stats API scraper...")

    while True:
        url = f"{BASE_URL}?region=us&lang=en&contentorigin=espn&page={page}&limit=50&sort=offensive.avgPoints:desc"
        attempt = 0
        while attempt < retries:
            try:
                print(f"📦 Fetching API page {page}...")
                response = requests.get(url, headers=HEADERS, timeout=10)
                response.raise_for_status()
                break
            except requests.RequestException as e:
                attempt += 1
                print(f"⚠️ Attempt {attempt} failed: {e}")
                time.sleep(2)
        if attempt == retries:
            print("❌ Max retries reached on API. Stopping.")
            break

        data = response.json()
        athletes = data.get("athletes", [])
        if not athletes:
            print("✅ No more players found from API. Scraper completed.")
            break

        for player in athletes:
            athlete_info = player.get("athlete", {})
            name = athlete_info.get("displayName", "Unknown")
            team_info = athlete_info.get("teams", [{}])[0]
            team_abbr = team_info.get("abbreviation", "Unknown").upper()
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

        page += 1

    return players

def fetch_players_html():
    """Fetches players from the new HTML link."""
    url = "https://www.espn.com/mens-college-basketball/team/stats/_/id/2511"
    print("🚀 Starting HTML scraper for new link...")
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ Failed to fetch HTML data: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    players = []
    
    # You will need to inspect the HTML to find the correct table and columns.
    # This example assumes there is a table element containing player stats.
    table = soup.find('table')
    if not table:
        print("❌ No table found on the HTML page.")
        return players

    rows = table.find_all('tr')
    # Skip header row; adjust the index if necessary.
    for row in rows[1:]:
        cols = row.find_all('td')
        if len(cols) < 7:
            continue  # Not enough columns; skip this row.
        try:
            name = cols[0].get_text(strip=True)
            # Depending on the page, you might have the team info elsewhere.
            # For demonstration, we set team_abbr as a fixed value or extract accordingly.
            team_abbr = "NEW"  # Replace with actual extraction logic if available.
            ppg = float(cols[1].get_text(strip=True).replace(',', ''))  # Adjust indices based on actual table
            apg = float(cols[2].get_text(strip=True).replace(',', ''))
            rpg = float(cols[3].get_text(strip=True).replace(',', ''))
            threepm = float(cols[4].get_text(strip=True).replace(',', ''))
            games = int(cols[5].get_text(strip=True).replace(',', '')) if cols[5].get_text(strip=True) else 1

            players.append([name, team_abbr, ppg, apg, rpg, threepm, games])
        except (ValueError, IndexError) as e:
            print(f"⚠️ Skipped a row due to parsing error: {e}")

    print("✅ HTML scraping completed.")
    return players

def fetch_all_players():
    """Fetches players from both the API and the HTML link, then combines them."""
    players_api = fetch_players_api()
    players_html = fetch_players_html()
    all_players = players_api + players_html
    return all_players

def save_players_to_csv(players):
    if players:
        df = pd.DataFrame(players, columns=["Player", "Team", "PPG", "APG", "RPG", "3PM", "Games"])
        output_dir = "CBB"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        csv_path = os.path.join(output_dir, "cbb_players_stats.csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ Player stats saved successfully to '{csv_path}'!")
    else:
        print("⚠️ No player data collected.")

if __name__ == "__main__":
    players = fetch_all_players()
    save_players_to_csv(players)