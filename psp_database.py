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

from Universal_Sports_Analyzer import is_banned

# Define base directory and PSP folder path.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PSP_DIR = os.path.join(BASE_DIR, "PSP")

# --------------------------
# Notion & Scraper Settings
# --------------------------
NOTION_TOKEN = "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"
DATABASE_ID = "1ac71b1c663e808e9110eee23057de0e"
BASE_URL = "https://www.statmuse.com"
TIME_PERIOD = "past month"

notion = Client(auth=NOTION_TOKEN)

# --------------------------
# Notion Database Functions
# --------------------------
def fetch_unprocessed_rows():
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
        
        team_prop = props.get("Teams", {})
        if team_prop.get("type") == "title":
            team_parts = team_prop.get("title", [])
        else:
            team_parts = team_prop.get("rich_text", [])
        teams = "".join(part.get("plain_text", "") for part in team_parts)
        
        sport_prop = props.get("Sport", {}).get("select", {})
        sport = sport_prop.get("name", "") if sport_prop else ""
        
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
    teams_str = teams.replace(" ", "")
    full_query = f"{query} {TIME_PERIOD} {teams_str}"
    encoded_query = urllib.parse.quote_plus(full_query)
    url = f"{BASE_URL}/ask?q={encoded_query}"
    return url

def fetch_html(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    
    try:
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.flex-1.overflow-x-auto")))
        print("Data appears to have loaded.")
    except Exception as e:
        print("Explicit wait failed:", e)
    
    html = driver.page_source
    driver.quit()
    return html

def parse_table(html_content):
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
            if "NAME" in row_dict:
                row_dict["NAME"] = clean_name(row_dict["NAME"])
            rows.append(row_dict)
        else:
            print("Skipping row with unexpected number of cells:", cells)
    return rows

def scrape_statmuse_data(sport, stat, teams):
    query = f"{stat} leaders {sport.lower()}"
    url = build_query_url(query, teams)
    html = fetch_html(url)
    data = parse_table(html)
    return data

def clean_name(name):
    name = name.strip()
    period_index = name.find('.')
    if period_index != -1 and period_index > 0:
        return name[:period_index-1].strip()
    return name

# --------------------------
# PSP Analyzer Functions
# --------------------------
def analyze_nba_psp(file_path, stat_key):
    try:
        df_psp = pd.read_csv(file_path)
        df_psp.columns = [col.upper() for col in df_psp.columns]
    except Exception as e:
        return f"Error reading PSP CSV: {e}"
    
    try:
        df_stats = pd.read_csv(os.path.join(BASE_DIR, "NBA", "nba_player_stats.csv"))
        df_stats.columns = [col.upper() for col in df_stats.columns]
    except Exception as e:
        return f"Error reading NBA player stats CSV: {e}"
    
    try:
        df_inj = pd.read_csv(os.path.join(BASE_DIR, "NBA", "nba_injury_report.csv"))
        # Assume NBA stats use "PLAYER" as the column name
        df_inj["PLAYER"] = df_inj["PLAYER"].str.strip() if "PLAYER" in df_inj.columns else df_inj["playerName"].str.strip()
        injured_names = set(df_inj["PLAYER"].dropna().unique())
    except Exception as e:
        return f"Error loading or processing NBA injuries CSV: {e}"
    
    # Merge using PSP CSV's "NAME" and NBA stats "PLAYER"
    try:
        df_merged = pd.merge(df_psp, df_stats, left_on="NAME", right_on="PLAYER", how="left", suffixes=('_psp', '_stats'))
    except Exception as e:
        return f"Error merging PSP and NBA stats: {e}"
    
    df_merged = df_merged[~df_merged["NAME"].isin(injured_names)]
    
    if stat_key not in df_merged.columns:
        return f"Stat column '{stat_key}' not found in CSV."
    
    try:
        df_merged[stat_key] = pd.to_numeric(df_merged[stat_key].replace({',': ''}, regex=True), errors='coerce')
    except Exception as e:
        return f"Error converting stat column: {e}"
    
    sorted_df = df_merged.sort_values(by=stat_key, ascending=False).reset_index(drop=True)
    green = sorted_df.iloc[0:3]
    yellow = sorted_df.iloc[3:6]
    red = sorted_df.iloc[6:9]
    
    player_col = "NAME" if "NAME" in sorted_df.columns else None
    if player_col is None:
        return "Player column not found in CSV."
    
    output = f"🟢 {', '.join(str(x) for x in green[player_col].tolist() if not is_banned(str(x), stat_key))}\n"
    output += f"🟡 {', '.join(str(x) for x in yellow[player_col].tolist() if not is_banned(str(x), stat_key))}\n"
    output += f"🔴 {', '.join(str(x) for x in red[player_col].tolist() if not is_banned(str(x), stat_key))}"
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
        teams = row["teams"]
        sport = row["sport"]
        stat = row["stat"]
        
        data = scrape_statmuse_data(sport, stat, teams)
        if data:
            file_name = f"{sport.lower()}_{stat.lower().replace(' ', '_')}_psp_data.csv"
            output_file = os.path.join(PSP_DIR, file_name)
            pd.DataFrame(data).to_csv(output_file, index=False)
        else:
            print("No data scraped for this row.")
        
        mark_row_as_processed(page_id)

if __name__ == "__main__":
    main()