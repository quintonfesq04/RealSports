import os
import time
import re
import pandas as pd
import urllib.parse
from notion_client import Client
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# Import banned check from Universal_Sports_Analyzer
from Universal_Sports_Analyzer import is_banned

# --------------------------
# Notion & Scraper Settings
# --------------------------
NOTION_TOKEN = "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"
DATABASE_ID = "1ac71b1c663e808e9110eee23057de0e"
# StatMuse base settings
BASE_URL = "https://www.statmuse.com"
TIME_PERIOD = "past month"

# Initialize Notion client
notion = Client(auth=NOTION_TOKEN)

# --------------------------
# Notion Database Functions
# --------------------------
def fetch_unprocessed_rows():
    """
    Queries the Notion database for rows where Processed equals "no".
    Assumes your database has properties: Teams, Sport, Stat, Processed, and ID.
    """
    try:
        response = notion.databases.query(
            database_id=DATABASE_ID,
            filter={
                "property": "Processed",
                "select": {"equals": "no"}
            }
        )
    except Exception as e:
        print("Error querying Notion database:", e)
        return []
    
    rows = []
    for result in response.get("results", []):
        page_id = result["id"]
        props = result.get("properties", {})
        
        # Extract Teams (assume it's a title or rich_text property)
        team_prop = props.get("Teams", {})
        if team_prop.get("type") == "title":
            team_parts = team_prop.get("title", [])
        else:
            team_parts = team_prop.get("rich_text", [])
        teams = "".join(part.get("plain_text", "") for part in team_parts)
        
        # Extract Sport (select property)
        sport_prop = props.get("Sport", {}).get("select", {})
        sport = sport_prop.get("name", "") if sport_prop else ""
        
        # Extract Stat (select or rich_text)
        stat_prop = props.get("Stat", {})
        if "select" in stat_prop:
            stat = stat_prop.get("select", {}).get("name", "")
        elif "rich_text" in stat_prop:
            stat = "".join(part.get("plain_text", "") for part in stat_prop.get("rich_text", []))
        else:
            stat = ""
        
        rows.append({
            "page_id": page_id,
            "teams": teams,
            "sport": sport,
            "stat": stat
        })
    return rows

def mark_row_as_processed(page_id):
    """Marks the given page (row) as processed by updating the Processed property to "Yes"."""
    try:
        notion.pages.update(
            page_id=page_id,
            properties={
                "Processed": {"select": {"name": "Yes"}}
            }
        )
    except Exception as e:
        print(f"Error marking page {page_id} as processed:", e)

# --------------------------
# Scraping Functions
# --------------------------
def build_query_url(query, teams):
    """
    Constructs the full StatMuse query URL.
    Assumes 'teams' is a string of team abbreviations.
    """
    teams_str = teams.replace(" ", "")  # Remove extra spaces if any.
    full_query = f"{query} {TIME_PERIOD} {teams_str}"
    encoded_query = urllib.parse.quote_plus(full_query)
    url = f"{BASE_URL}/ask?q={encoded_query}"
    return url

def fetch_html(url):
    """Uses Selenium to load the dynamic StatMuse page and returns the HTML."""
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    
    try:
        wait = WebDriverWait(driver, 15)
        # Wait for the container that holds the data (adjust if needed)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.flex-1.overflow-x-auto")))
        print("Data appears to have loaded.")
    except Exception as e:
        print("Explicit wait failed:", e)
    
    html = driver.page_source
    driver.quit()
    return html

def parse_table(html_content):
    """
    Parses the HTML and extracts the table of player data.
    Returns a list of dictionaries (one per row).
    """
    soup = BeautifulSoup(html_content, "html.parser")
    container = soup.select_one("div.flex-1.overflow-x-auto")
    if not container:
        print("Container not found.")
        return None
    
    table = container.find("table")
    if not table:
        print("Table element not found.")
        return None
    
    header_row = table.find("thead")
    if not header_row:
        print("No table header found.")
        return None
    headers = [th.get_text(strip=True) for th in header_row.find_all("th")]
    
    body = table.find("tbody")
    if not body:
        print("No table body found.")
        return None
    rows = []
    for tr in body.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) == len(headers):
            row_dict = dict(zip(headers, cells))
            # Clean the NAME field if present
            if "NAME" in row_dict:
                row_dict["NAME"] = clean_name(row_dict["NAME"])
            rows.append(row_dict)
        else:
            print("Skipping row with unexpected number of cells:", cells)
    return rows

def scrape_statmuse_data(sport, stat, teams):
    """
    Builds a dynamic query for the given sport, stat, and teams,
    fetches and parses the page,
    and returns the data (list of dictionaries).
    """
    query = f"{stat} leaders {sport.lower()}"
    url = build_query_url(query, teams)
    html = fetch_html(url)
    data = parse_table(html)
    return data

def clean_name(name):
    name = name.strip()
    period_index = name.find('.')
    # Only modify if a period exists and it's not the very beginning
    if period_index != -1 and period_index > 0:
        # Return the substring up to one character before the period.
        return name[:period_index-1].strip()
    return name

# --------------------------
# PSP Analyzer Functions
# --------------------------
def analyze_nba_psp(file_path, stat_key):
    """
    Reads the NBA PSP CSV file and merges it with player stats.
    Then filters out injured and banned players.
    Outputs PSP groups based on ranking:
      • Yellow: ranks 1–3
      • Green: ranks 6–8
      • Red: ranks 13–15
    """
    try:
        df_psp = pd.read_csv(file_path)
        df_psp.columns = [col.upper() for col in df_psp.columns]
    except Exception as e:
        return f"Error reading PSP CSV: {e}"
    
    try:
        df_stats = pd.read_csv("NBA/nba_player_stats.csv")
        df_stats.columns = [col.upper() for col in df_stats.columns]
    except Exception as e:
        return f"Error reading NBA player stats CSV: {e}"
    
    try:
        df_inj = pd.read_csv("NBA/nba_injury_report.csv")
        df_inj["playerName"] = df_inj["playerName"].str.strip()
        injured_names = set(df_inj["playerName"].dropna().unique())
    except Exception as e:
        return f"Error loading or processing NBA injuries CSV: {e}"
    
    # Merge PSP data with player stats to get the latest team information
    df_merged = pd.merge(df_psp, df_stats, on="NAME", how="left", suffixes=('_psp', '_stats'))
    
    # Filter out injured players
    df_merged = df_merged[~df_merged["NAME"].isin(injured_names)]
    
    if stat_key not in df_merged.columns:
        return f"Stat column '{stat_key}' not found in CSV."
    
    try:
        df_merged[stat_key] = pd.to_numeric(df_merged[stat_key].replace({',': ''}, regex=True), errors='coerce')
    except Exception as e:
        return f"Error converting stat column: {e}"
    
    sorted_df = df_merged.sort_values(by=stat_key, ascending=False).reset_index(drop=True)
    # Define ranking groups based on your PSP rules:
    yellow = sorted_df.iloc[0:3]       # Ranks 1-3
    green = sorted_df.iloc[5:8]        # Ranks 6-8 (adjusted from previous 7-9)
    red = sorted_df.iloc[12:15]        # Ranks 13-15
    
    player_col = "NAME" if "NAME" in sorted_df.columns else None
    if player_col is None:
        return "Player column not found in CSV."
    
    # Filter out banned players using is_banned (passing stat_key as context)
    green_list = [x for x in green[player_col].tolist() if not is_banned(str(x), stat_key)]
    yellow_list = [x for x in yellow[player_col].tolist() if not is_banned(str(x), stat_key)]
    red_list = [x for x in red[player_col].tolist() if not is_banned(str(x), stat_key)]
    
    output = f"🟢 {', '.join(str(x) for x in green_list)}\n"
    output += f"🟡 {', '.join(str(x) for x in yellow_list)}\n"
    output += f"🔴 {', '.join(str(x) for x in red_list)}"
    return output

# --------------------------
# Main Process
# --------------------------
def main():
    rows = fetch_unprocessed_rows()
    if not rows:
        print("No unprocessed rows found.")
        return

    for row in rows:
        page_id = row["page_id"]
        teams = row["teams"]  # Expecting a comma-separated string of teams.
        sport = row["sport"]
        stat = row["stat"]
        
        data = scrape_statmuse_data(sport, stat, teams)
        if data:
            # Build a file name based on sport and stat (replace spaces with underscores)
            file_name = f"{sport.lower()}_{stat.lower().replace(' ', '_')}_psp_data.csv"
            output_file = os.path.join("PSP", file_name)
            pd.DataFrame(data).to_csv(output_file, index=False)
        else:
            print("No data scraped for this row.")
        
        mark_row_as_processed(page_id)

if __name__ == "__main__":
    main()