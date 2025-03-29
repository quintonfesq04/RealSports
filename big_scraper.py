#!/usr/bin/env python3
"""
big_scraper.py
--------------
This file combines the scrapers for:
  - College Basketball (CBB)
  - NHL
  - NBA
  - MLB

It runs each sport’s scraping process and saves the corresponding CSV files.
"""

# ==============================
# Common Imports
# ==============================
import requests
import pandas as pd
import time
import csv
import os
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException

# ==============================
# CBB Scraper (College Basketball)
# ==============================

# Helper: Get Column Index from Headers
def get_index(headers, key):
    """
    Returns the index of the header that contains the given key (case-insensitive),
    or -1 if not found.
    """
    key = key.upper()
    for idx, header in enumerate(headers):
        if key in header.upper():
            return idx
    return -1

# Function: API Scraper
def fetch_players_api():
    """Fetches players from the ESPN API."""
    players = []
    page = 1
    retries = 3

    print("🚀 Starting CBB player stats API scraper...")

    BASE_URL_CBB = "https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-college-basketball/statistics/byathlete"
    while True:
        url = f"{BASE_URL_CBB}?region=us&lang=en&contentorigin=espn&page={page}&limit=50&sort=offensive.avgPoints:desc"
        attempt = 0
        while attempt < retries:
            try:
                print(f"📦 Fetching API page {page}...")
                response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
                response.raise_for_status()
                break
            except requests.RequestException as e:
                attempt += 1
                print(f"⚠️ Attempt {attempt} failed: {e}")
                time.sleep(2)
        if attempt == retries:
            print("❌ Max retries reached on API. Stopping API scraper.")
            break

        data = response.json()
        athletes = data.get("athletes", [])
        if not athletes:
            print("✅ No more players found from API. API scraper completed.")
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
                    float(stats.get("offensive", [0])[0]),   # PPG
                    float(stats.get("offensive", [0])[10]),  # APG
                    float(stats.get("general", [0])[12]),      # RPG
                    float(stats.get("offensive", [0])[4]),     # 3PM
                    games_played
                ])
            except (ValueError, IndexError):
                print(f"⚠️ Skipped player due to missing stats: {name}")

        page += 1

    return players

# Function: HTML Scraper Using ResponsiveTable Selectors
def fetch_players_html():
    """
    Fetches players from the new HTML link using Selenium and BeautifulSoup.
    """
    print("🚀 Starting HTML scraper for CBB using ResponsiveTable selectors...")

    HTML_URL = "https://www.espn.com/mens-college-basketball/team/stats/_/id/2511"
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--disable-gpu")
    # Adjust the chromedriver path if necessary.
    service = Service("/opt/homebrew/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.get(HTML_URL)
        time.sleep(10)
        html = driver.page_source
    except Exception as e:
        print(f"❌ Selenium error: {e}")
        driver.quit()
        return []
    driver.quit()

    soup = BeautifulSoup(html, 'html.parser')
    responsive_tables = soup.select(".ResponsiveTable.ResponsiveTable--fixed-left.mt5.remove_capitalize")
    if not responsive_tables:
        print("❌ No ResponsiveTable elements found on the page.")
        return []
    table = responsive_tables[0]
    header_row = table.find('tr')
    if not header_row:
        print("❌ No header row found.")
        return []
    headers = [cell.get_text(strip=True) for cell in header_row.find_all(['th', 'td'])]
    print("Headers found:", headers)
    idx_player = get_index(headers, "player")
    if idx_player == -1:
        idx_player = get_index(headers, "name")
    if idx_player == -1:
        print("❌ Could not determine the player name column.")
        return []
    idx_ppg = get_index(headers, "ppg")
    if idx_ppg == -1:
        idx_ppg = get_index(headers, "pts")
    idx_apg = get_index(headers, "apg")
    if idx_apg == -1:
        idx_apg = get_index(headers, "ast")
    idx_rpg = get_index(headers, "rpg")
    if idx_rpg == -1:
        idx_rpg = get_index(headers, "reb")
    idx_3pm = get_index(headers, "3pm")
    idx_games = get_index(headers, "gp")
    if idx_games == -1:
        idx_games = get_index(headers, "games")

    players = []
    rows = table.find_all('tr')[1:]
    team_elem = soup.select_one(".ResponsiveWrapper")
    team_name = team_elem.get_text(strip=True) if team_elem and team_elem.get_text(strip=True) else ""
    print("Team name:", team_name)

    for row in rows:
        cols = row.find_all('td')
        print("Row:", [col.get_text(strip=True) for col in cols])
        if not cols or len(cols) < (idx_player + 1):
            continue
        try:
            name = cols[idx_player].get_text(strip=True)
            if len(name) > 0:
                name = name[:-1]
            team_abbr = "QUC"  # Hardcoded team abbreviation
            ppg = float(cols[idx_ppg].get_text(strip=True).replace(',', '')) if idx_ppg != -1 and len(cols) > idx_ppg else 0.0
            apg = float(cols[idx_apg].get_text(strip=True).replace(',', '')) if idx_apg != -1 and len(cols) > idx_apg else 0.0
            rpg = float(cols[idx_rpg].get_text(strip=True).replace(',', '')) if idx_rpg != -1 and len(cols) > idx_rpg else 0.0
            threepm = float(cols[idx_3pm].get_text(strip=True).replace(',', '')) if idx_3pm != -1 and len(cols) > idx_3pm else 0.0
            games = int(cols[idx_games].get_text(strip=True).replace(',', '')) if idx_games != -1 and len(cols) > idx_games else 1
            players.append([name, team_abbr, ppg, apg, rpg, threepm, games])
        except (ValueError, IndexError) as e:
            print(f"⚠️ Skipped a row due to parsing error: {e}")

    if players:
        print("✅ HTML scraping completed; found", len(players), "players.")
    else:
        print("⚠️ HTML scraper did not find any players.")
    return players

def fetch_all_cbb_players():
    players_api = fetch_players_api()
    players_html = fetch_players_html()
    all_players = players_api + players_html
    print(f"✅ Total CBB players collected: {len(all_players)}")
    return all_players

def save_cbb_players_to_csv():
    players = fetch_all_cbb_players()
    if players:
        df = pd.DataFrame(players, columns=["Player", "Team", "PPG", "APG", "RPG", "3PM", "Games"])
        csv_path = os.path.join("cbb_players_stats.csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ CBB player stats saved to '{csv_path}'!")
    else:
        print("⚠️ No CBB player data collected.")

# ==============================
# NHL Scraper
# ==============================
base_stats_url = "https://www.nhl.com/stats/skaters?reportName=summary&reportType=season&sort=points,a_gamesPlayed&seasonFrom=20242025&seasonTo=20242025&gameType=2"
injury_url_nhl = "https://www.cbssports.com/nhl/injuries/"

def fetch_nhl_player_stats():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    all_rows = []
    page = 1
    try:
        driver.get(base_stats_url)
        while True:
            table = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "rt-table"))
            )
            if page == 1:
                headers = [th.text.strip() for th in table.find_element(By.TAG_NAME, 'thead').find_elements(By.TAG_NAME, 'th')]
            tbody = table.find_element(By.TAG_NAME, 'tbody')
            for tr in tbody.find_elements(By.TAG_NAME, 'tr'):
                cells = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, 'td')]
                all_rows.append(cells)
            pagination_element = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "d3-l-wrap"))
            )
            try:
                next_button = pagination_element.find_element(By.XPATH, ".//*[contains(text(), 'Next')]")
                if not next_button.is_enabled():
                    break
                driver.execute_script("arguments[0].scrollIntoView();", next_button)
                try:
                    next_button.click()
                except ElementClickInterceptedException:
                    driver.execute_script("arguments[0].click();", next_button)
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "rt-table"))
                )
                page += 1
            except NoSuchElementException:
                break
        df = pd.DataFrame(all_rows, columns=headers)
        output_file = "nhl_player_stats.csv"
        df.to_csv(output_file, index=False)
        print(f"💾 NHL player stats saved to {output_file}")
    except TimeoutException:
        print("Timeout: NHL stats table did not load in time.")
    except Exception as e:
        print(f"An error occurred in NHL scraper: {e}")
    finally:
        driver.quit()

def extract_nhl_injury_data():
    try:
        response = requests.get(injury_url_nhl)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        table_shadows_divs = soup.find_all('div', class_='TableBase-shadows')
        if not table_shadows_divs:
            print("NHL injury sections not found.")
            return pd.DataFrame()
        data = []
        for div in table_shadows_divs:
            team_name_element = div.find_previous_sibling('h4', class_='TableBase-title')
            team_name = team_name_element.get_text(strip=True) if team_name_element else 'Unknown'
            rows = div.find_all('tr')
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) >= 5:
                    player_name = cols[0].find('span', class_='CellPlayerName--long').get_text(strip=True) if cols[0].find('span', class_='CellPlayerName--long') else cols[0].get_text(strip=True)
                    position = cols[1].get_text(strip=True)
                    updated = cols[2].get_text(strip=True)
                    injury = cols[3].get_text(strip=True)
                    injury_status = cols[4].get_text(strip=True)
                    data.append({
                        'teamName': team_name,
                        'playerName': player_name,
                        'position': position,
                        'updated': updated,
                        'injury': injury,
                        'injuryStatus': injury_status
                    })
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error in NHL injury scraper: {e}")
        return pd.DataFrame()

def save_nhl_injuries_csv():
    df_injuries = extract_nhl_injury_data()
    if not df_injuries.empty:
        df_injuries.to_csv("nhl_injuries.csv", index=False)
        print("💾 Saved NHL injury data to 'nhl_injuries.csv'")
    else:
        print("No NHL injury data to save.")

# ==============================
# NBA Scraper
# ==============================
stats_url_nba = "https://stats.nba.com/stats/leagueLeaders"
injury_url_nba = "https://www.cbssports.com/nba/injuries/"
params_nba = {
    "LeagueID": "00",
    "PerMode": "PerGame",
    "Scope": "S",
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "StatCategory": "PTS"
}
headers_nba = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Referer": "https://stats.nba.com/",
    "Origin": "https://stats.nba.com"
}

def fetch_nba_player_stats():
    response = requests.get(stats_url_nba, params=params_nba, headers=headers_nba)
    data = response.json()
    player_stats = data["resultSet"]["rowSet"]
    columns = data["resultSet"]["headers"]
    output_file = "nba_player_stats.csv"
    df = pd.DataFrame(player_stats, columns=columns)
    df.to_csv(output_file, index=False)
    print(f"💾 NBA player stats saved to {output_file}")
    return df

def extract_nba_injury_data():
    try:
        response = requests.get(injury_url_nba)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        table_shadows_divs = soup.find_all('div', class_='TableBase-shadows')
        if not table_shadows_divs:
            print("NBA injury sections not found.")
            return pd.DataFrame()
        data = []
        for div in table_shadows_divs:
            team_name_element = div.find_previous_sibling('h4', class_='TableBase-title')
            team_name = team_name_element.get_text(strip=True) if team_name_element else 'Unknown'
            rows = div.find_all('tr')
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) >= 5:
                    player_name = cols[0].find('span', class_='CellPlayerName--long').get_text(strip=True) if cols[0].find('span', class_='CellPlayerName--long') else cols[0].get_text(strip=True)
                    print(f"Original player_name: {player_name}")
                    position = cols[1].get_text(strip=True)
                    updated = cols[2].get_text(strip=True)
                    injury = cols[3].get_text(strip=True)
                    injury_status = cols[4].get_text(strip=True)
                    data.append({
                        'teamName': team_name,
                        'playerName': player_name,
                        'position': position,
                        'updated': updated,
                        'injury': injury,
                        'injuryStatus': injury_status
                    })
                    print(f"Extracted data: {team_name}, {player_name}, {position}, {updated}, {injury}, {injury_status}")
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error in NBA injury scraper: {e}")
        return pd.DataFrame()

def merge_nba_stats_with_injuries(stats_df, injuries_df):
    stats_df['PLAYER'] = stats_df['PLAYER'].str.strip()
    injuries_df['playerName'] = injuries_df['playerName'].str.strip()
    merged_df = pd.merge(stats_df, injuries_df, left_on='PLAYER', right_on='playerName', how='left')
    healthy_players_df = merged_df[merged_df['injury'].isnull()]
    return healthy_players_df

# ==============================
# MLB Scraper
# ==============================
BASE_URL_MLB = "https://www.mlb.com/stats/rbi/2025?page={}"
MAX_PAGES = 47

def fetch_raw_table_data():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(20)
    all_rows = []
    for page in range(1, MAX_PAGES + 1):
        url = BASE_URL_MLB.format(page)
        print("Fetching MLB stats from:", url)
        try:
            driver.get(url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            time.sleep(3)
        except Exception as e:
            print("Error loading MLB stats page", page, e)
            continue
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        table = soup.find("table")
        if not table:
            print("No table found on MLB stats page", page)
            continue
        if page == 1:
            thead = table.find("thead")
            if thead:
                header_cells = thead.find_all(["th", "td"])
                headers = [cell.get_text(strip=True) for cell in header_cells]
                all_rows.append(headers)
        tbody = table.find("tbody")
        if tbody:
            for tr in tbody.find_all("tr"):
                cells = tr.find_all(["td", "th"])
                row = [cell.get_text(strip=True) for cell in cells]
                all_rows.append(row)
        else:
            print("No table body found on MLB stats page", page)
        time.sleep(1)
    driver.quit()
    return all_rows

def save_mlb_stats_csv():
    data = fetch_raw_table_data()
    print("Total MLB stats rows fetched:", len(data))
    # Save the new 2025 season stats to a separate file.
    new_file = "mlb_2025_stats.csv"
    with open(new_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(data)
    print(f"💾 Saved {new_file} with raw MLB stats data.")

INJURY_URL_MLB = "https://www.cbssports.com/mlb/injuries/"

def fetch_mlb_injury_data():
    try:
        response = requests.get(INJURY_URL_MLB)
        response.raise_for_status()
    except Exception as e:
        print("Error fetching MLB injury page:", e)
        return pd.DataFrame()
    soup = BeautifulSoup(response.content, "html.parser")
    injury_sections = soup.find_all("div", class_="TableBase-shadows")
    if not injury_sections:
        print("No MLB injury sections found.")
        return pd.DataFrame()
    injury_data = []
    for section in injury_sections:
        team_header = section.find_previous_sibling("h4", class_="TableBase-title")
        team_name = team_header.get_text(strip=True) if team_header else "Unknown"
        rows = section.find_all("tr")
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) >= 5:
                playerName = cells[0].get_text(strip=True)
                position = cells[1].get_text(strip=True)
                updated = cells[2].get_text(strip=True)
                injury = cells[3].get_text(strip=True)
                injuryStatus = cells[4].get_text(strip=True)
                injury_data.append({
                    "teamName": team_name,
                    "playerName": playerName,
                    "position": position,
                    "updated": updated,
                    "injury": injury,
                    "injuryStatus": injuryStatus
                })
    df_injuries = pd.DataFrame(injury_data)
    print("💾 Fetched MLB injury data with", len(df_injuries), "rows.")
    return df_injuries

def save_mlb_injuries_csv():
    df_injuries = fetch_mlb_injury_data()
    if not df_injuries.empty:
        df_injuries.to_csv("mlb_injuries.csv", index=False)
        print("💾 Saved mlb_injuries.csv.")
    else:
        print("No MLB injury data to save.")

# ==============================
# MLB 2025 Regular Season Pitchers Scraper
# ==============================
# URL for 2025 Regular Season Pitchers
BASE_URL_MLB_PITCHING_2025 = "https://www.mlb.com/stats/pitching/2025?page={}"
MAX_PAGES_PITCHING_2025 = 50  # Adjust if needed

def fetch_mlb_pitching_2025_stats():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(20)
    all_rows = []
    for page in range(1, MAX_PAGES_PITCHING_2025 + 1):
        url = BASE_URL_MLB_PITCHING_2025.format(page)
        print("Fetching MLB 2025 Pitcher stats from:", url)
        try:
            driver.get(url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            time.sleep(3)
        except Exception as e:
            print("Error loading 2025 pitcher stats page", page, e)
            continue
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        table = soup.find("table")
        if not table:
            print("No table found on 2025 pitcher stats page", page)
            continue
        if page == 1:
            thead = table.find("thead")
            if thead:
                header_cells = thead.find_all(["th", "td"])
                headers = [cell.get_text(strip=True) for cell in header_cells]
                all_rows.append(headers)
        tbody = table.find("tbody")
        if tbody:
            for tr in tbody.find_all("tr"):
                cells = tr.find_all(["td", "th"])
                row = [cell.get_text(strip=True) for cell in cells]
                all_rows.append(row)
        else:
            print("No table body found on 2025 pitcher stats page", page)
        time.sleep(1)
    driver.quit()
    return all_rows

def save_mlb_pitching_2025_stats_csv():
    data = fetch_mlb_pitching_2025_stats()
    print("Total MLB 2025 pitcher stats rows fetched:", len(data))
    csv_path = os.path.join("mlb_pitching_2025_stats.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(data)
    print(f"💾 MLB 2025 pitcher stats saved to '{csv_path}'!")

# ==============================
# Main Function: Run All Scrapers
# ==============================
def main():
    print("Starting Big Scraper...")

    # CBB Scraper
    print("\n=== CBB Scraper ===")
    save_cbb_players_to_csv()

    # NHL Scraper
    print("\n=== NHL Scraper ===")
    fetch_nhl_player_stats()
    save_nhl_injuries_csv()

    # NBA Scraper
    print("\n=== NBA Scraper ===")
    stats_df_nba = fetch_nba_player_stats()
    injuries_df_nba = extract_nba_injury_data()
    if not injuries_df_nba.empty:
        injuries_df_nba.to_csv('nba_injury_report.csv', index=False)
        print("💾 Saved NBA injury report data")
    else:
        print("❌ No NBA injury data found")
    healthy_players_df_nba = merge_nba_stats_with_injuries(stats_df_nba, injuries_df_nba)
    healthy_output_file = "nba_healthy_player_stats.csv"
    healthy_players_df_nba.to_csv(healthy_output_file, index=False)
    print(f"💾 Saved healthy NBA player stats to {healthy_output_file}")

    # MLB Batting Scraper
    print("\n=== MLB Scraper ===")
    save_mlb_stats_csv()
    save_mlb_injuries_csv()

    # MLB Pitching Scraper
    print("\n=== MLB Pitching Scraper ===")
    save_mlb_pitching_2025_stats_csv()

    print("\nBig Scraper completed.")

if __name__ == "__main__":
    main()
