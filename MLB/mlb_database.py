import time
import csv
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# ===============================
# MLB Stats Scraping Section
# ===============================
BASE_URL = "https://www.mlb.com/stats/rbi?timeframe=-14&page={}"
MAX_PAGES = 47  # Scrape all 47 pages

def fetch_raw_table_data():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(20)
    
    all_rows = []
    
    for page in range(1, MAX_PAGES + 1):
        url = BASE_URL.format(page)
        print("Fetching stats from:", url)
        try:
            driver.get(url)
            # Wait for the table element to load.
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            time.sleep(3)  # Extra wait for dynamic content.
        except Exception as e:
            print("Error loading stats page", page, e)
            continue
        
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        table = soup.find("table")
        if not table:
            print("No table found on stats page", page)
            continue
        
        # If a header exists, extract it from <thead> on page 1.
        if page == 1:
            thead = table.find("thead")
            if thead:
                header_cells = thead.find_all(["th", "td"])
                headers = [cell.get_text(strip=True) for cell in header_cells]
                all_rows.append(headers)
        
        # Extract table body rows.
        tbody = table.find("tbody")
        if tbody:
            for tr in tbody.find_all("tr"):
                cells = tr.find_all(["td", "th"])
                row = [cell.get_text(strip=True) for cell in cells]
                all_rows.append(row)
        else:
            print("No table body found on stats page", page)
        time.sleep(1)
    
    driver.quit()
    return all_rows

def save_stats_csv():
    data = fetch_raw_table_data()
    print("Total stats rows fetched:", len(data))
    with open("mlb_stats.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(data)
    print("💾 Saved mlb_stats.csv with raw stats data.")

# ===============================
# MLB Injury Scraping Section
# ===============================
INJURY_URL = "https://www.cbssports.com/mlb/injuries/"

def fetch_mlb_injury_data():
    """
    Uses requests and BeautifulSoup to load the CBS Sports MLB injuries page,
    then finds each injury section by locating divs with class "TableBase-shadows".
    For each section, it looks for a preceding <h4> with class "TableBase-title" 
    (which should contain the team name) and then parses the injury table rows.
    """
    try:
        response = requests.get(INJURY_URL)
        response.raise_for_status()
    except Exception as e:
        print("Error fetching MLB injury page:", e)
        return pd.DataFrame()

    soup = BeautifulSoup(response.content, "html.parser")
    
    # Look for containers that hold injury data.
    injury_sections = soup.find_all("div", class_="TableBase-shadows")
    if not injury_sections:
        print("No injury sections found on the page.")
        return pd.DataFrame()
    
    injury_data = []
    for section in injury_sections:
        # Look for a previous sibling header that might contain the team name.
        team_header = section.find_previous_sibling("h4", class_="TableBase-title")
        team_name = team_header.get_text(strip=True) if team_header else "Unknown"
        
        # Within the section, find the table rows.
        rows = section.find_all("tr")
        # Skip header row (assume first row is header).
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

def save_injuries_csv():
    df_injuries = fetch_mlb_injury_data()
    if not df_injuries.empty:
        df_injuries.to_csv("mlb_injuries.csv", index=False)
        print("💾 Saved mlb_injuries.csv.")
    else:
        print("No injury data to save.")

# ===============================
# Main Function: Run Both Scrapers
# ===============================
def main():
    print("Starting MLB data scraping...")
    save_stats_csv()
    save_injuries_csv()
    print("MLB data scraping complete.")

if __name__ == "__main__":
    main()