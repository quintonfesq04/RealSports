import time
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException

# URL for the NHL Stats
base_stats_url = "https://www.nhl.com/stats/skaters?reportName=summary&reportType=season&sort=points,a_gamesPlayed&seasonFrom=20242025&seasonTo=20242025&gameType=2"
injury_url = "https://www.cbssports.com/nhl/injuries/"

def fetch_nhl_player_stats():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Enable headless mode
    driver = webdriver.Chrome(options=options)

    all_rows = []
    page = 1
    try:
        driver.get(base_stats_url)
        
        while True:
            # Wait for the table to load
            table = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "rt-table"))
            )

            # Extract table headers (only from the first page)
            if page == 1:
                headers = [th.text.strip() for th in table.find_element(By.TAG_NAME, 'thead').find_elements(By.TAG_NAME, 'th')]

            # Extract table rows
            tbody = table.find_element(By.TAG_NAME, 'tbody')
            for tr in tbody.find_elements(By.TAG_NAME, 'tr'):
                cells = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, 'td')]
                all_rows.append(cells)

            # Locate pagination controls
            pagination_element = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "d3-l-wrap"))
            )
            
            # Check if the "Next" button exists and is enabled
            try:
                next_button = pagination_element.find_element(By.XPATH, ".//*[contains(text(), 'Next')]")
                if not next_button.is_enabled():
                    break
                
                # Scroll the button into view
                driver.execute_script("arguments[0].scrollIntoView();", next_button)
                # Try to click the button
                try:
                    next_button.click()
                except ElementClickInterceptedException:
                    # If the click is intercepted, use JavaScript to click
                    driver.execute_script("arguments[0].click();", next_button)
                # Wait for the next page to load
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "rt-table"))
                )
                page += 1
            except NoSuchElementException:
                break

        # Save data to CSV
        df = pd.DataFrame(all_rows, columns=headers)
        output_file = "nhl_player_stats.csv"
        df.to_csv(output_file, index=False)
        print(f"💾 Saved player stats to {output_file}")

    except TimeoutException:
        print("Timeout: Table did not load within the specified time.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.quit()

def extract_nhl_injury_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        soup = BeautifulSoup(response.content, 'html.parser')
        table_shadows_divs = soup.find_all('div', class_='TableBase-shadows')

        if not table_shadows_divs:
            print("TableBase-shadows divs not found.")
            return pd.DataFrame()

        data = []

        for table_shadows_div in table_shadows_divs:
            # Get the team name from the previous sibling h4 element
            team_name_element = table_shadows_div.find_previous_sibling('h4', class_='TableBase-title')
            team_name = team_name_element.get_text(strip=True) if team_name_element else 'Unknown'

            # Extract injury data from the table
            rows = table_shadows_div.find_all('tr')
            for row in rows[1:]:  # Skip the header row
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

    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    try:
        fetch_nhl_player_stats()
        df_injuries = extract_nhl_injury_data(injury_url)
        if not df_injuries.empty:
            df_injuries.to_csv("nhl_injuries.csv", index=False)
            print("💾 Saved NHL injury report data")
        else:
            print("No injury data to save.")
    except KeyboardInterrupt:
        print("Script interrupted by user.")
    finally:
        print("Script finished.")
