import requests
import pandas as pd
import time
import os
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# ---------------------------
# Configuration & Constants
# ---------------------------
# ESPN API URL and Headers
BASE_URL = "https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-college-basketball/statistics/byathlete"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# New HTML link for additional players
HTML_URL = "https://www.espn.com/mens-college-basketball/team/stats/_/id/2511"

# ---------------------------
# Helper: Get Column Index from Headers
# ---------------------------
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

# ---------------------------
# Function: API Scraper
# ---------------------------
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

            # Convert ESPN's "WIS" to RealSports' "WISC"
            if team_abbr == "WIS":
                team_abbr = "WISC"

            stats = {category["name"]: category.get("totals", [0]) for category in player.get("categories", [])}
            # Safely get games played; default to 1 if not available.
            games_played = int(stats.get("general", [0])[15]) if "general" in stats and len(stats.get("general", [])) > 15 else 1

            try:
                players.append([
                    name,
                    team_abbr,
                    float(stats.get("offensive", [0])[0]),   # PPG
                    float(stats.get("offensive", [0])[10]),  # APG
                    float(stats.get("general", [0])[12]),      # RPG
                    float(stats.get("offensive", [0])[4]),     # 3PM
                    games_played                              # Total Games Played
                ])
            except (ValueError, IndexError):
                print(f"⚠️ Skipped player due to missing stats: {name}")

        page += 1

    return players

# ---------------------------
# Function: HTML Scraper Using ResponsiveTable Selectors
# ---------------------------
def fetch_players_html():
    """
    Fetches players from the new HTML link using Selenium and BeautifulSoup.
    This version uses CSS selectors to locate the ResponsiveTable element(s)
    where player stats are located.
    """
    print("🚀 Starting HTML scraper for new link using ResponsiveTable selectors...")

    # Set up Selenium in headless mode
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    # Adjust executable_path if needed (or ensure chromedriver is in your PATH)
    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get(HTML_URL)
        # Wait for the JavaScript to load content - increased to 10 seconds
        time.sleep(10)
        html = driver.page_source
    except Exception as e:
        print(f"❌ Selenium error: {e}")
        driver.quit()
        return []
    driver.quit()

    soup = BeautifulSoup(html, 'html.parser')

    # Use the CSS selector provided to target the ResponsiveTable elements
    responsive_tables = soup.select(".ResponsiveTable.ResponsiveTable--fixed-left.mt5.remove_capitalize")
    if not responsive_tables:
        print("❌ No ResponsiveTable elements found on the page.")
        return []

    # Assume the first ResponsiveTable is the one with per-game stats
    table = responsive_tables[0]

    # Extract header row from the table (assumes headers are in the first <tr>)
    header_row = table.find('tr')
    if not header_row:
        print("❌ No header row found in the ResponsiveTable.")
        return []
    headers = [cell.get_text(strip=True) for cell in header_row.find_all(['th', 'td'])]
    print("Headers found:", headers)

    # Try to detect the player name column; try "player" then "name"
    idx_player = get_index(headers, "player")
    if idx_player == -1:
        idx_player = get_index(headers, "name")
    if idx_player == -1:
        print("❌ Could not determine the player name column from the ResponsiveTable headers.")
        return []

    # Determine indices for other desired stats
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

    # If some stat columns are not found, you might still continue (defaulting to 0 or 1)
    players = []
    # Process all rows except the header row
    rows = table.find_all('tr')[1:]

    # Extract team name from the ResponsiveWrapper
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
            # Remove the last letter from the name
            if len(name) > 0:
                name = name[:-1]
            # Hardcode the team abbreviation to QUC
            team_abbr = "QUC"
            ppg = float(cols[idx_ppg].get_text(strip=True).replace(',', '')) if idx_ppg != -1 and len(cols) > idx_ppg else 0.0
            apg = float(cols[idx_apg].get_text(strip=True).replace(',', '')) if idx_apg != -1 and len(cols) > idx_apg else 0.0
            rpg = float(cols[idx_rpg].get_text(strip=True).replace(',', '')) if idx_rpg != -1 and len(cols) > idx_rpg else 0.0
            threepm = float(cols[idx_3pm].get_text(strip=True).replace(',', '')) if idx_3pm != -1 and len(cols) > idx_3pm else 0.0
            games = int(cols[idx_games].get_text(strip=True).replace(',', '')) if idx_games != -1 and len(cols) > idx_games else 1

            players.append([name, team_abbr, ppg, apg, rpg, threepm, games])
        except (ValueError, IndexError) as e:
            print(f"⚠️ Skipped a row due to parsing error: {e}")

    if players:
        print("✅ HTML scraping completed using ResponsiveTable selectors; found", len(players), "players.")
    else:
        print("⚠️ HTML scraper did not find any players using ResponsiveTable selectors.")
    return players

# ---------------------------
# Function: Combine Data from Both Sources
# ---------------------------
def fetch_all_players():
    """
    Fetches players from both the API and the HTML source, then combines them.
    """
    players_api = fetch_players_api()
    players_html = fetch_players_html()
    all_players = players_api + players_html
    print(f"✅ Total players collected: {len(all_players)}")
    return all_players

# ---------------------------
# Function: Save DataFrame to CSV
# ---------------------------
def save_players_to_csv(players):
    if players:
        df = pd.DataFrame(players, columns=["Player", "Team", "PPG", "APG", "RPG", "3PM", "Games"])
        csv_path = os.path.join("cbb_players_stats.csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ Player stats saved successfully to '{csv_path}'!")
    else:
        print("⚠️ No player data collected.")

def fetch_cbb_injuries_oddstrader():
    """
    Scrapes CBB injury data from Oddstrader using Selenium.
    
    This updated version:
      - Scrolls to the bottom repeatedly to trigger lazy-loading.
      - Loops through all tables on the page.
      - Attempts to extract headers from <thead> (or falls back to the first row).
      - Skips any row that contains the phrase "no injuries to report".
    """
    url = "https://newsday.sportsdirectinc.com/basketball/ncaab-injuries.aspx?page=/data/ncaab/injury/injuries.html"
    
    # Set up Selenium in headless mode
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 30)
        # Wait until at least one table is present on the page
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
        
        # Scroll to bottom repeatedly to trigger lazy-loading
        SCROLL_PAUSE_TIME = 2
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        
        # Locate all table elements on the page
        tables = driver.find_elements(By.CSS_SELECTOR, "table")
        all_injuries = []
        
        for table in tables:
            headers_list = []
            # Try extracting header from <thead>
            try:
                thead = table.find_element(By.TAG_NAME, "thead")
                header_rows = thead.find_elements(By.TAG_NAME, "tr")
                if header_rows:
                    headers_list = [cell.text.strip() for cell in header_rows[-1].find_elements(By.TAG_NAME, "th")]
            except Exception as e:
                # (Optional) Comment out or remove this print to suppress warnings:
                # print(f"Warning: Unable to extract headers from <thead>: {e}")
                pass
            
            # Get rows: if headers not found from <thead>, use the first row of table
            rows = table.find_elements(By.TAG_NAME, "tr")
            if not headers_list and rows:
                headers_list = [cell.text.strip() for cell in rows[0].find_elements(By.TAG_NAME, "td")]
                data_rows = rows[1:]
            else:
                try:
                    tbody = table.find_element(By.TAG_NAME, "tbody")
                    data_rows = tbody.find_elements(By.TAG_NAME, "tr")
                except Exception:
                    data_rows = rows[1:] if rows else []
            
            for row in data_rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if not cells:
                    continue
                # Skip rows that contain "no injuries to report"
                if any("no injuries to report" in cell.text.strip().lower() for cell in cells):
                    continue
                row_data = {}
                for i, cell in enumerate(cells):
                    col_name = headers_list[i] if i < len(headers_list) and headers_list[i] else f"col_{i}"
                    row_data[col_name] = cell.text.strip()
                if row_data:
                    all_injuries.append(row_data)
        
        if all_injuries:
            print(f"✅ Found {len(all_injuries)} injury record(s).")
        else:
            print("⚠️ Injury table was found, but no injury records were extracted.")
        return all_injuries
    
    except TimeoutException:
        print("❌ Timeout waiting for the injury table to load.")
        return []
    except Exception as e:
        print(f"❌ Selenium error: {e}")
        return []
    finally:
        driver.quit()

def save_injuries_to_csv(injuries, filename="cbb_injuries.csv"):
    if injuries:
        df = pd.DataFrame(injuries)
        df.to_csv(filename, index=False)
        print(f"✅ Injury data saved successfully to '{filename}'!")
    else:
        print("⚠️ No injury data to save.")

# ---------------------------
# Main Process
# ---------------------------
if __name__ == "__main__":
    all_players = fetch_all_players()
    save_players_to_csv(all_players)
    
    # Now scrape and save injury data from Oddstrader
    injuries = fetch_cbb_injuries_oddstrader()
    save_injuries_to_csv(injuries)