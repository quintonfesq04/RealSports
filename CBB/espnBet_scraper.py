import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Setup WebDriver
options = webdriver.ChromeOptions()
options.add_argument("--headless")  
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# ESPN Bet Odds Page
url = "https://www.espn.com/mens-college-basketball/odds"
print("🔄 Fetching odds from ESPN...")

try:
    driver.get(url)
    time.sleep(5)  

    # Find all `betSixPack-*` elements
    bet_elements = driver.find_elements(By.XPATH, "//*[contains(@id, 'betSixPack-')]")

    if not bet_elements:
        print("⚠️ No valid odds found.")
    else:
        game_data = []
        for bet in bet_elements:
            bet_id = bet.get_attribute("id")
            bet_text = bet.text.strip()

            # Try extracting nested content (spreads, moneyline, totals)
            spreads = bet.find_elements(By.XPATH, ".//span[contains(text(), 'Spread')]")
            moneylines = bet.find_elements(By.XPATH, ".//span[contains(text(), 'Moneyline')]")
            totals = bet.find_elements(By.XPATH, ".//span[contains(text(), 'Total')]")

            # Convert to text
            spread_text = ', '.join([s.text for s in spreads]) if spreads else "N/A"
            moneyline_text = ', '.join([m.text for m in moneylines]) if moneylines else "N/A"
            total_text = ', '.join([t.text for t in totals]) if totals else "N/A"

            print(f"✅ Found {bet_id}: {bet_text} | Spread: {spread_text} | ML: {moneyline_text} | Total: {total_text}")

            game_data.append([bet_id, bet_text, spread_text, moneyline_text, total_text])

        # Save results
        df = pd.DataFrame(game_data, columns=["Bet Section", "Odds Data", "Spread", "Moneyline", "Total"])
        output_file = os.path.join(os.getcwd(), "espn_odds.xlsx")
        df.to_excel(output_file, index=False)
        print(f"✅ Parsed odds saved to {output_file}")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    driver.quit()