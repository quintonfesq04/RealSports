import requests
import pandas as pd
from bs4 import BeautifulSoup

SUMMER_URL = "https://basketball.realgm.com/nba/summer/1/NBA-2K26-Summer-League/59/stats"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_summer_league_stats():
    print("🚀 Fetching NBA Summer League stats from RealGM…")
    resp = requests.get(SUMMER_URL, headers=HEADERS)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    # find the bootstrap table by its data-toggle attribute
    table = soup.find("table", attrs={"data-toggle": "table"})
    if not table:
        raise RuntimeError("❌ Could not locate the Summer League stats table on RealGM.")

    # read it with pandas
    df = pd.read_html(str(table))[0]

    # optionally, clean it up:
    # — drop the rank column
    if "#" in df.columns:
        df = df.drop(columns=["#"])
    # — ensure column names match what you expect
    df.columns = df.columns.str.strip().str.upper()

    return df

def save_summer_league_stats_csv():
    df = fetch_summer_league_stats()
    out = "summer_league_stats.csv"
    df.to_csv(out, index=False)
    print(f"💾 Saved Summer League stats to {out}")

if __name__ == "__main__":
    save_summer_league_stats_csv()