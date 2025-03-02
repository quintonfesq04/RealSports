import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException

# --------------------------------------------------
# URLs for NHL Data
# --------------------------------------------------
# 1. Skater stats URL (uses pagination via Next button)
skater_stats_url = "https://www.nhl.com/stats/skaters?reportName=summary&reportType=season&sort=points,a_gamesPlayed&seasonFrom=20242025&seasonTo=20242025&gameType=2"
# 2. Goalie stats URL – we will scrape pages 0 and 1 (adjust if needed)
goalie_stats_base_url = "https://www.nhl.com/stats/goalies?reportType=season&seasonFrom=20242025&seasonTo=20242025&gameType=2&sort=saves&page={}&pageSize=50"
# 3. Fox Sports Hits URL – loop over several pages (adjust pages as needed)
hits_base_url = "https://www.foxsports.com/nhl/stats?category=defense&sort=h&season=2024&seasonType=reg&sortOrder=desc&page={}"
# 4. Injury Report URL for NHL from CBS Sports
injury_url = "https://www.cbssports.com/nhl/injuries/"

# --------------------------------------------------
# Selenium WebDriver Setup
# --------------------------------------------------
def create_webdriver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    return driver

# --------------------------------------------------
# 1. NHL Skater Stats Scraper
# --------------------------------------------------
def fetch_nhl_skater_stats():
    driver = create_webdriver()
    all_rows = []
    page = 1
    try:
        driver.get(skater_stats_url)
        # Wait for the table to load
        table = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "rt-table"))
        )
        # Get headers (from the first page)
        headers = [th.text.strip() for th in table.find_element(By.TAG_NAME, 'thead').find_elements(By.TAG_NAME, 'th')]
        # Process first page rows
        tbody = table.find_element(By.TAG_NAME, 'tbody')
        for tr in tbody.find_elements(By.TAG_NAME, 'tr'):
            cells = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, 'td')]
            all_rows.append(cells)
        # Paginate using the Next button
        while True:
            pagination = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "d3-l-wrap"))
            )
            try:
                next_button = pagination.find_element(By.XPATH, ".//*[contains(text(), 'Next')]")
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
                time.sleep(2)
                table = driver.find_element(By.CLASS_NAME, "rt-table")
                tbody = table.find_element(By.TAG_NAME, 'tbody')
                for tr in tbody.find_elements(By.TAG_NAME, 'tr'):
                    cells = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, 'td')]
                    all_rows.append(cells)
                page += 1
            except NoSuchElementException:
                break
    except TimeoutException:
        print("Timeout: NHL skater stats table did not load.")
    except Exception as e:
        print("Error fetching NHL skater stats:", e)
    finally:
        driver.quit()

    df_skater = pd.DataFrame(all_rows, columns=headers)
    output_file = "nhl_player_stats.csv"
    df_skater.to_csv(output_file, index=False)
    print(f"💾 Saved NHL skater stats to {output_file} with {len(df_skater)} rows.")
    return df_skater

# --------------------------------------------------
# 2. NHL Goalie Stats Scraper (Pages 0 and 1)
# --------------------------------------------------
def fetch_nhl_goalie_stats():
    driver = create_webdriver()
    all_rows = []
    headers = None
    try:
        for page in range(0, 2):  # pages 0 and 1
            url = goalie_stats_base_url.format(page)
            print("Fetching goalie stats from:", url)
            driver.get(url)
            table = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "rt-table"))
            )
            if headers is None:
                headers = [th.text.strip() for th in table.find_element(By.TAG_NAME, 'thead').find_elements(By.TAG_NAME, 'th')]
            tbody = table.find_element(By.TAG_NAME, 'tbody')
            for tr in tbody.find_elements(By.TAG_NAME, 'tr'):
                cells = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, 'td')]
                all_rows.append(cells)
            time.sleep(1)
    except TimeoutException:
        print("Timeout: NHL goalie stats table did not load.")
    except Exception as e:
        print("Error fetching NHL goalie stats:", e)
    finally:
        driver.quit()

    if headers is None:
        headers = [f"Column{i}" for i in range(1, 11)]
    df_goalie = pd.DataFrame(all_rows, columns=headers)
    output_file = "nhl_goalie_stats.csv"
    df_goalie.to_csv(output_file, index=False)
    print(f"💾 Saved NHL goalie stats to {output_file} with {len(df_goalie)} rows.")
    return df_goalie

# --------------------------------------------------
# 3. NHL Hit Leaders Scraper (from Fox Sports)
# --------------------------------------------------
def fetch_nhl_hit_leaders(pages=5):
    driver = create_webdriver()
    all_rows = []
    header_line = None
    try:
        for page in range(1, pages + 1):
            url = hits_base_url.format(page)
            print("Fetching hits from:", url)
            driver.get(url)
            time.sleep(3)  # Wait for dynamic content to render
            soup = BeautifulSoup(driver.page_source, "html.parser")
            # Target the main container that holds the stats
            main_content = soup.find("div", class_="fscom-main-content")
            if not main_content:
                print(f"No main content found on Fox Sports hits page {page}.")
                continue
            stats_div = main_content.find("div", class_="expanded-view-page stats")
            if not stats_div:
                print(f"No expanded stats div found on Fox Sports hits page {page}.")
                continue
            text = stats_div.get_text(separator="\n").strip()
            lines = text.split("\n")
            # Use the first non-empty line as header
            for line in lines:
                if line.strip():
                    temp_header = line.split()
                    if len(temp_header) > 1:
                        header_line = temp_header
                    break
            # Process remaining lines as rows
            for line in lines[1:]:
                if line.strip():
                    row = line.split()
                    if len(row) < 5:
                        continue
                    all_rows.append(row)
            time.sleep(1)
    except Exception as e:
        print("Error fetching NHL hit leaders:", e)
    finally:
        driver.quit()

    if not header_line or (all_rows and len(header_line) != len(all_rows[0])):
        if all_rows:
            header_line = [f"Column{i}" for i in range(1, len(all_rows[0]) + 1)]
        else:
            header_line = []
    try:
        df_hits = pd.DataFrame(all_rows, columns=header_line)
    except Exception as e:
        print("Error creating DataFrame for hits:", e)
        df_hits = pd.DataFrame(all_rows)
    output_file = "nhl_hit_leaders.csv"
    df_hits.to_csv(output_file, index=False)
    print(f"💾 Saved NHL hit leaders to {output_file} with {len(df_hits)} rows.")
    return df_hits

# --------------------------------------------------
# 4. NHL Injury Data Scraper (from CBS Sports)
# --------------------------------------------------
def fetch_nhl_injury_data():
    try:
        response = requests.get(injury_url)
        response.raise_for_status()
    except Exception as e:
        print("Error fetching NHL injury page:", e)
        return pd.DataFrame()
    
    soup = BeautifulSoup(response.content, "html.parser")
    injury_sections = soup.find_all("div", class_="TableBase-shadows")
    if not injury_sections:
        print("No injury sections found on the NHL injury page.")
        return pd.DataFrame()
    
    injury_data = []
    for section in injury_sections:
        team_header = section.find_previous_sibling("h4", class_="TableBase-title")
        team_name = team_header.get_text(strip=True) if team_header else "Unknown"
        rows = section.find_all("tr")
        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) >= 5:
                player_elem = cols[0].find("span", class_="CellPlayerName--long")
                player_name = player_elem.get_text(strip=True) if player_elem else cols[0].get_text(strip=True)
                position = cols[1].get_text(strip=True)
                updated = cols[2].get_text(strip=True)
                injury = cols[3].get_text(strip=True)
                injury_status = cols[4].get_text(strip=True)
                injury_data.append({
                    "Team": team_name,
                    "Player": player_name,
                    "Position": position,
                    "Updated": updated,
                    "Injury": injury,
                    "injuryStatus": injury_status
                })
    df_injuries = pd.DataFrame(injury_data)
    output_file = "nhl_injuries.csv"
    df_injuries.to_csv(output_file, index=False)
    print(f"💾 Saved NHL injury data to {output_file} with {len(df_injuries)} rows.")
    return df_injuries

# --------------------------------------------------
# Main Execution
# --------------------------------------------------
if __name__ == "__main__":
    print("Starting NHL data scraping...")
    try:
        fetch_nhl_skater_stats()
        fetch_nhl_goalie_stats()
        fetch_nhl_hit_leaders(pages=5)
        fetch_nhl_injury_data()
    except KeyboardInterrupt:
        print("Script interrupted by user.")
    finally:
        print("NHL data scraping finished.")