import time
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# Base URL with a page parameter.
BASE_URL = "https://www.mlb.com/stats/rbi?timeframe=-14&page={}"
MAX_PAGES = 47  # Scrape all 47 pages

def fetch_raw_table_data():
    # Set up Selenium WebDriver in headless mode.
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(20)
    
    all_rows = []
    
    for page in range(1, MAX_PAGES + 1):
        url = BASE_URL.format(page)
        print("Fetching:", url)
        try:
            driver.get(url)
            # Wait for the table element to load.
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            time.sleep(3)  # Extra wait for dynamic content.
        except Exception as e:
            print("Error loading page", page, e)
            continue
        
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        table = soup.find("table")
        if not table:
            print("No table found on page", page)
            continue
        
        # If a header exists, extract it from <thead> (only add header once, e.g. from page 1)
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
            print("No table body found on page", page)
        time.sleep(1)
    
    driver.quit()
    return all_rows

def main():
    data = fetch_raw_table_data()
    print("Total rows fetched:", len(data))
    # Write the raw data to CSV.
    with open("mlb_stats.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(data)
    print("💾 Saved mlb_stats.csv with raw data.")

if __name__ == "__main__":
    main()