#!/usr/bin/env python3
"""
main.py
--------
This file is a unified version that merges the functionality of:
  - Universal_Sports_Analyzer.py
  - notion_database.py
  - psp_database.py

It provides:
  1. An interactive sports analyzer (for NBA, CBB, NHL, MLB).
  2. A process that reads unprocessed Notion poll rows (including PSP queries)
     – for PSP rows (except for CBB) it always scrapes StatMuse on each run.
  3. A PSP scraper that scrapes StatMuse data and writes CSV files.

Note: This version fixes issues such as duplicate CBB players,
      filtering out injured MLB players, and ensuring that NBA picks only include players
      that meet the success rate thresholds. It also includes definitions for
      analyze_nhl_noninteractive, analyze_mlb_noninteractive, and analyze_mlb_by_team_interactive.
"""

import os
import sys
import re
import time
import asyncio
import subprocess
import urllib.parse
import pandas as pd
import requests
import numpy as np
import unicodedata

# Selenium & BeautifulSoup for PSP scraping
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

# Notion client
from notion_client import Client

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
# …

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=chrome_options
)

# ----------------------------
# Global Directories
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REALSPORTS_DIR = BASE_DIR  # Assuming main.py is at the root of RealSports
PSP_FOLDER = os.path.join(REALSPORTS_DIR, "PSP")

# ----------------------------
# Stat Category Definitions
# ----------------------------
STAT_CATEGORIES_NBA = {
    "PPG": "PTS",
    "APG": "AST",
    "RPG": "REB",
    "3PM": "FG3M"
}

STAT_CATEGORIES_CBB = {
    "PPG": "PPG",
    "APG": "APG",
    "RPG": "RPG",
    "3PM": "3PM"
}

STAT_CATEGORIES_NHL = {
    "GOALS": "G",
    "ASSISTS": "A",     # to be converted to per-game
    "POINTS": "PTS",     # to be converted to per-game
    "S": "shotsPerGame"  # shots per game
}

DESIRED_MLB_COLS = ["PLAYER", "TEAM", "G", "AB", "R", "H", "RBI", "AVG", "OBP", "OPS"]
STAT_CATEGORIES_MLB = {
    "RBI": "RBI",
    "G": "G",
    "AB": "AB",
    "R": "R",
    "H": "H",
    "AVG": "AVG",
    "OBP": "OBP",
    "OPS": "OPS"
}

STAT_CATEGORIES_WNBA = {
    "PPG": "PTS",
    "APG": "AST",
    "RPG": "TRB",   # BBRef calls it “TRB”
    "3PM": "3P"     # BBRef calls it “3P”
}

# ----------------------------
# Team Name Normalization & Traded Players
# ----------------------------
TEAM_ALIASES = {
    "QUC": "QUOC",
    "AZ": "ARI",
    "NCSU": "NCST",
    "PRES": "PRE",
    "LR": "UALR",
    "BOIS": "BSU",
    "HPU": "HP",
    "CAM": "CAMP",
    "HCU": "HBU",
    "ATH": "OAK"
}

def normalize_team_name(team):
    team = team.strip().upper()
    return TEAM_ALIASES.get(team, team)

TRADED_PLAYERS = {
    "kyle kuzma": "MIL"
}

def update_traded_players(df, player_col="PLAYER", team_col="TEAM"):
    df[team_col] = df.apply(
        lambda row: TRADED_PLAYERS.get(str(row[player_col]).strip().lower(), row[team_col]),
        axis=1
    )
    return df

def is_traded_excluded(player_name, current_teams):
    normalized_name = player_name.strip().lower()
    if normalized_name in TRADED_PLAYERS:
        new_team = TRADED_PLAYERS[normalized_name].strip().upper()
        if new_team not in current_teams:
            return True
    return False

# ----------------------------
# Banned Players Handling
# ----------------------------
GLOBAL_BANNED_PLAYERS = [
    "Bobby Portis",
    "Jonas Valančiūnas",
    "Ethen Frank",
    "Killian Hayes",
    "Khris Middleton",
    "Bradley Beal",
    "Simone Fontecchio"
]
GLOBAL_BANNED_PLAYERS_SET = {p.strip().lower() for p in GLOBAL_BANNED_PLAYERS}

STAT_SPECIFIC_BANNED = {
    "ASSISTS": {"Jordan Poole"},
    "HITS": {"Brenden Dillon"},
    "3PM": {"Klay Thompson"}
}

def is_banned(player_name, stat=None):
    player = player_name.strip().lower()
    if stat:
        banned_for_stat = {p.lower() for p in STAT_SPECIFIC_BANNED.get(stat.upper(), set())}
        if player in banned_for_stat:
            return True
    return player in GLOBAL_BANNED_PLAYERS_SET

# ----------------------------
# Utility Functions: Header Cleaning and MLB Name Fixing
# ----------------------------
import re

def clean_header(header):
    header = header.strip()
    # collapse duplicated headers like "FGFG"
    if header.isupper() and len(header) % 2 == 0:
        mid = len(header) // 2
        if header[:mid] == header[mid:]:
            header = header[:mid]
    # map any partial match to a known stat/column
    keys = sorted(
        ["PLAYER","TEAM","RBI","AVG","OBP","OPS","AB","R","H","G","SO"],
        key=len, reverse=True
    )
    for key in keys:
        if key.lower() in header.lower():
            return key
    return header

preserved_suffixes = {"JR", "SR", "III", "IV", "V"}
known_positions    = {"RF", "CF", "LF", "SS", "C", "1B", "2B", "3B", "OF", "DH"}

def deduplicate_token(tok):
    n = len(tok)
    for i in range(1, n//2 + 1):
        if n % i == 0 and tok.lower() == tok[:i].lower() * (n//i):
            return tok[:i]
    return tok

def fix_mlb_player_name(name: str) -> str:
    # 1) split any camel-runs ("CruzDe"→"Cruz De")
    name = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', name)

    # 2) normalize whitespace, strip digits/punctuation (preserve Jr/Sr/etc.)
    name = re.sub(r'\b(Jr|SR|III|IV|V)[\.]?\b', r' \1 ', name, flags=re.IGNORECASE)
    name = re.sub(r'^\d+|\d+$', '', name)
    name = re.sub(r'[^A-Za-z\'\s]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()

    # 3) split into tokens, drop pure-position tokens and strip any pos suffix
    toks = []
    for tok in name.split():
        up = tok.upper()
        if up in preserved_suffixes:
            toks.append(tok); continue
        if up in known_positions:
            continue
        for pos in known_positions:
            if up.endswith(pos) and len(tok) > len(pos):
                tok = tok[:-len(pos)]
                up  = tok.upper()
        toks.append(tok)

    # 4) collapse any repeated **trailing** block of tokens
    n = len(toks)
    for k in range(n//2, 0, -1):
        if toks[-k:] == toks[-2*k:-k]:
            toks = toks[:-k]
            break

    # 5) drop stray single-letter runs (unless Jr/Sr) and trim stray trailing uppercase
    clean = []
    for t in toks:
        if len(t) == 1 and t.upper() not in preserved_suffixes:
            continue
        if len(t) >= 3 and t[-1].isupper():
            t = t[:-1]
        t = deduplicate_token(t)
        clean.append(t)

    # 6) remove any consecutive dupes
    final = []
    for t in clean:
        if not final or final[-1].lower() != t.lower():
            final.append(t)

    return " ".join(final)

# ----------------------------
# NHL Per-Game Stat Calculation
# ----------------------------
def calculate_per_game_stat(df, raw_stat, new_stat_name, games_column="GP"):
    if games_column in df.columns:
        games = pd.to_numeric(df[games_column], errors='coerce')
    elif "G" in df.columns:
        games = pd.to_numeric(df["G"], errors='coerce')
    else:
        games = pd.Series([1] * len(df))
    stat_values = pd.to_numeric(df[raw_stat], errors='coerce')
    df[new_stat_name] = stat_values / games.replace(0, pd.NA)
    return df

def calculate_nhl_per_game_stats(df):
    df = calculate_per_game_stat(df, "A", "A")
    df = calculate_per_game_stat(df, "P", "PTS")
    df = calculate_per_game_stat(df, "S", "shotsPerGame")
    return df

# ----------------------------
# Categorization Function for All Sports
# ----------------------------
def categorize_players(df, stat_choice, target_value, player_col, team_col, stat_for_ban=None):
    if df.empty:
        print("❌ DataFrame is empty. Check if the CSV data are correct.")
        return "❌ DataFrame is empty. Check if the CSV data are correct."
    df = df[~df[player_col].apply(lambda x: is_banned(x, stat_for_ban))]
    try:
        df[stat_choice] = pd.to_numeric(df[stat_choice], errors='coerce')
    except Exception as e:
        print("Error converting stat column to numeric:", e)
        return f"Error converting stat column: {e}"
    df = df.dropna(subset=[stat_choice])
    df = df.drop_duplicates(subset=[player_col])
    if target_value is None or target_value == 0:
        return "Target value required and must be nonzero."
    df["Success_Rate"] = ((df[stat_choice] / target_value) * 100).round(1)
    df.loc[df["Success_Rate"] >= 120, "Category"] = "🟡 Favorite"
    df.loc[(df["Success_Rate"] >= 100) & (df["Success_Rate"] < 120), "Category"] = "🟢 Best Bet"
    df.loc[df["Success_Rate"] < 100, "Category"] = "🔴 Underdog"
    df = df.drop_duplicates(subset=[player_col, team_col])
    
    MIN_CBB_RED_SUCCESS_RATE = 80
    red_df = df[df["Category"] == "🔴 Underdog"]
    red_df = red_df[red_df["Success_Rate"] >= MIN_CBB_RED_SUCCESS_RATE]
    red_players = red_df.nlargest(3, "Success_Rate")
    if len(red_players) < 3:
        extra = df[df["Success_Rate"] < 100].nlargest(3 - len(red_players), "Success_Rate")
        red_players = pd.concat([red_players, extra]).drop_duplicates().nlargest(3, "Success_Rate")
    
    green_players = df[df["Category"] == "🟢 Best Bet"].nlargest(3, "Success_Rate")
    if len(green_players) < 3:
        extra = df[df["Success_Rate"] >= 100].nlargest(3 - len(green_players), "Success_Rate")
        green_players = pd.concat([green_players, extra]).drop_duplicates().nlargest(3, "Success_Rate")
    yellow_players = df[df["Category"] == "🟡 Favorite"].nlargest(3, "Success_Rate")
    if len(yellow_players) < 3:
        extra = df[df["Success_Rate"] >= 120].nlargest(3 - len(yellow_players), "Success_Rate")
        yellow_players = pd.concat([yellow_players, extra]).drop_duplicates().nlargest(3, "Success_Rate")
    
    final_df = pd.concat([green_players, yellow_players, red_players]).drop_duplicates(subset=[player_col, team_col]).reset_index(drop=True)
    final_df = pd.concat([
        final_df[final_df["Category"] == "🟢 Best Bet"].sort_values(by="Success_Rate", ascending=False),
        final_df[final_df["Category"] == "🟡 Favorite"].sort_values(by="Success_Rate", ascending=False),
        final_df[final_df["Category"] == "🔴 Underdog"].sort_values(by="Success_Rate", ascending=True)
    ]).reset_index(drop=True)
    
    green_list = final_df[final_df["Category"] == "🟢 Best Bet"][player_col].tolist()
    yellow_list = final_df[final_df["Category"] == "🟡 Favorite"][player_col].tolist()
    red_list = final_df[final_df["Category"] == "🔴 Underdog"][player_col].tolist()
    
    unique_green = []
    for name in green_list:
        if name not in unique_green:
            unique_green.append(name)
    unique_yellow = [name for name in yellow_list if name not in unique_green]
    unique_red = [name for name in red_list if name not in unique_green and name not in unique_yellow]
    
    green_output = ", ".join(unique_green) if unique_green else "No Green Plays"
    yellow_output = ", ".join(unique_yellow) if unique_yellow else "No Yellow Plays"
    red_output = ", ".join(unique_red) if unique_red else "No Red Plays"
    output = f"🟢 {green_output}\n"
    output += f"🟡 {yellow_output}\n"
    output += f"🔴 {red_output}"
    return output

# ----------------------------
# Integration Functions for Each Sport
# ----------------------------

# ---------- NHL Integration ----------
def load_nhl_player_stats(file_path):
    return pd.read_csv(file_path)

def load_nhl_injury_data(file_path):
    return pd.read_csv(file_path)

def integrate_nhl_data(player_stats_file, injury_data_file):
    stats_path = os.path.join(BASE_DIR, player_stats_file)
    inj_path = os.path.join(BASE_DIR, injury_data_file)
    try:
        stats_df = load_nhl_player_stats(stats_path)
    except FileNotFoundError:
        print(f"Error: The file {stats_path} was not found.")
        return pd.DataFrame()
    try:
        injuries_df = load_nhl_injury_data(inj_path)
    except FileNotFoundError:
        print(f"Error: The file {inj_path} was not found.")
        return stats_df
    if "playerName" in injuries_df.columns:
        injuries_df.rename(columns={"playerName": "Player"}, inplace=True)
    try:
        integrated_data = pd.merge(stats_df, injuries_df, how='left', on='Player')
    except Exception as e:
        print("Merge error for NHL data:", e)
        return stats_df
    integrated_data = integrated_data[integrated_data['injuryStatus'].isnull()]
    if "Team" not in integrated_data.columns:
        integrated_data["Team"] = stats_df["Team"]
    integrated_data.columns = [col.strip() for col in integrated_data.columns]
    integrated_data = calculate_nhl_per_game_stats(integrated_data)
    if "GP" in integrated_data.columns:
        games = pd.to_numeric(integrated_data["GP"], errors='coerce')
    elif "G" in integrated_data.columns:
        games = pd.to_numeric(integrated_data["G"], errors='coerce')
    else:
        games = pd.Series([1] * len(integrated_data))
    integrated_data = integrated_data[games >= 15]
    integrated_data = update_traded_players(integrated_data, player_col="Player", team_col="Team")
    return integrated_data

# ---------- MLB Integration ----------
def load_and_clean_mlb_stats():
    stats_file_path = os.path.join(BASE_DIR, "mlb_2025_stats.csv")
    try:
        df_new = pd.read_csv(stats_file_path)
    except Exception as e:
        print(f"Error loading MLB stats from {stats_file_path}: {e}")
        return pd.DataFrame()
    if df_new.empty:
        print(f"Error: The file {stats_file_path} is empty.")
        return pd.DataFrame()
    df_new.columns = [clean_header(col) for col in df_new.columns]
    df_new = df_new.loc[:, ~df_new.columns.duplicated()]
    for col in DESIRED_MLB_COLS:
        if col not in df_new.columns:
            df_new[col] = None
    df_new = df_new.reindex(columns=DESIRED_MLB_COLS)
    df_new["PLAYER"] = df_new["PLAYER"].apply(fix_mlb_player_name)
    if "TEAM" not in df_new.columns:
        print("Error: 'TEAM' column not found in the MLB stats CSV.")
        return pd.DataFrame()
    df_new["TEAM"] = df_new["TEAM"].astype(str).apply(normalize_team_name)
    return df_new

def integrate_mlb_data():
    try:
        df_stats = load_and_clean_mlb_stats()
        if "TEAM" not in df_stats.columns:
            print("Error: 'TEAM' column not found in the MLB stats CSV.")
            return pd.DataFrame()
    except Exception as e:
        print("Error loading and cleaning MLB stats:", e)
        return pd.DataFrame()
    inj_path = os.path.join(BASE_DIR, "mlb_injuries.csv")
    try:
        df_inj = pd.read_csv(inj_path)
    except Exception as e:
        print(f"Error loading mlb_injuries.csv from {inj_path}: {e}")
        return df_stats
    df_inj["playerName"] = df_inj["playerName"].str.strip()
    df_inj["playerName_clean"] = df_inj["playerName"].apply(fix_mlb_player_name)
    injured_names = set(df_inj["playerName_clean"].dropna().unique())
    healthy_df = df_stats[~df_stats["PLAYER"].isin(injured_names)].copy()
    healthy_df = update_traded_players(healthy_df, player_col="PLAYER", team_col="TEAM")
    return healthy_df[DESIRED_MLB_COLS]

# ---------- NBA Integration ----------
def load_nba_player_stats(file_path):
    return pd.read_csv(file_path)

def load_nba_injury_report(file_path):
    return pd.read_csv(file_path)

def merge_nba_stats_with_injuries(stats_df, injuries_df):
    stats_df['PLAYER'] = stats_df['PLAYER'].str.strip()
    injuries_df['playerName'] = injuries_df['playerName'].str.strip()
    merged_df = pd.merge(stats_df, injuries_df, left_on='PLAYER', right_on='playerName', how='left')
    healthy_players_df = merged_df[merged_df['injury'].isnull()]
    return healthy_players_df

def integrate_nba_data(player_stats_file, injury_report_file):
    nba_stats_path = os.path.join(BASE_DIR, "NBA", player_stats_file)
    nba_injuries_path = os.path.join(BASE_DIR, "NBA", injury_report_file)
    stats_df = load_nba_player_stats(nba_stats_path)
    injuries_df = load_nba_injury_report(nba_injuries_path)
    merged_df = merge_nba_stats_with_injuries(stats_df, injuries_df)
    merged_df = update_traded_players(merged_df, player_col="PLAYER", team_col="TEAM")
    return merged_df

# ---------- WNBA Integration ----------
def integrate_wnba_data(stats_file="wnba_player_stats.csv"):
    """
    Load the raw ESPN WNBA CSV, normalize column names and teams,
    and return a DataFrame with PLAYER and TEAM columns.
    """
    path = os.path.join(BASE_DIR, stats_file)
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"Error loading WNBA stats from {path}: {e}")
        return pd.DataFrame()

    # ESPN JSON returns a column named 'PLAYER' and 'TEAM'
    df = df.rename(columns={"PLAYER": "PLAYER", "TEAM": "TEAM"})
    df["PLAYER"] = df["PLAYER"].astype(str).str.strip()
    # normalize team abbreviations so they match what's in your Notion 'Teams' field
    df["TEAM"] = df["TEAM"].astype(str).apply(normalize_team_name)
    return df

# ---------- CBB Integration ----------
def integrate_cbb_data(player_stats_file="cbb_players_stats.csv", injury_data_file="cbb_injuries.csv"):
    stats_path = os.path.join(BASE_DIR, player_stats_file)
    inj_path = os.path.join(BASE_DIR, injury_data_file)
    print(f"Loading player stats from: {stats_path}")
    try:
        stats_df = pd.read_csv(stats_path)
    except FileNotFoundError:
        print(f"Error: The file {stats_path} was not found.")
        return pd.DataFrame()
    try:
        injuries_df = pd.read_csv(inj_path)
    except FileNotFoundError:
        print(f"Error: The file {inj_path} was not found.")
        return stats_df
    if "playerName" in injuries_df.columns:
        injuries_df.rename(columns={"playerName": "Player"}, inplace=True)
    elif "col_0" in injuries_df.columns:
        injuries_df.rename(columns={"col_0": "Player"}, inplace=True)
    if "injuryStatus" not in injuries_df.columns and "col_2" in injuries_df.columns:
        injuries_df.rename(columns={"col_2": "injuryStatus"}, inplace=True)
    try:
        integrated_data = pd.merge(stats_df, injuries_df, how='left', on='Player')
    except Exception as e:
        print("Merge error for CBB data:", e)
        return stats_df
    if "injuryStatus" in integrated_data.columns:
        mask = (
            integrated_data["injuryStatus"].fillna("")
            .str.lower()
            .str.contains("out indefinitely") |
            integrated_data["injuryStatus"].fillna("")
            .str.lower()
            .str.contains("out for season")
        )
        integrated_data = integrated_data[~mask]
    if "Team" not in integrated_data.columns:
        integrated_data["Team"] = stats_df["Team"]
    integrated_data.columns = [col.strip() for col in integrated_data.columns]
    integrated_data = update_traded_players(integrated_data, player_col="Player", team_col="Team")
    return integrated_data

# ---------- WNBA Integration ----------
def load_wnba_player_stats(file_path):
    """Load the WNBA stats CSV produced by your scraper."""
    return pd.read_csv(file_path)

def integrate_wnba_data(player_stats_file="wnba_player_stats.csv"):
    """
    Read, clean, and return a DataFrame of WNBA totals stats.
    We assume columns PLAYER and TEAM exist, plus PTS, AST, REB, FG3M, etc.
    """
    stats_path = os.path.join(BASE_DIR, player_stats_file)
    try:
        df = load_wnba_player_stats(stats_path)
    except FileNotFoundError:
        print(f"❌ WNBA stats file not found at {stats_path}")
        return pd.DataFrame()
    # normalize column names & trim whitespace
    df.columns = [c.strip().upper() for c in df.columns]
    df["PLAYER"] = df["PLAYER"].str.strip()
    df["TEAM"]   = df["TEAM"].str.strip().apply(normalize_team_name)
    return df

# ----------------------------
# PSP Scraping and Analyzer Functions (PSP Section)
# ----------------------------
# Define missing constants for PSP scraping:
TIME_PERIOD = "past two weeks"
BASE_URL = "https://www.statmuse.com"

def build_query_url(query, teams):
    if isinstance(teams, list):
        teams_str = ",".join(teams)
    else:
        teams_str = teams
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
        # wait for any <table> to appear on the page
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        print("✅ Table loaded.")
    except Exception:
        print("⚠️ Timeout waiting for table to load—continuing anyway.")
    
    html = driver.page_source
    driver.quit()
    return html

def parse_table(html_content):
    soup = BeautifulSoup(html_content, "html.parser")

    # Find the first table in the page
    table = soup.find("table")
    if not table:
        print("❌ No table element found at all—cannot scrape PSP data.")
        return None

    # Attempt to read headers from a <thead>; otherwise fall back to first row
    thead = table.find("thead")
    if thead:
        headers = [th.get_text(strip=True).upper() for th in thead.find_all("th")]
    else:
        first_row = table.find("tr")
        headers = [cell.get_text(strip=True).upper() for cell in first_row.find_all(["th", "td"])]

    # Find the body rows
    tbody = table.find("tbody")
    rows_iter = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]

    rows = []
    for tr in rows_iter:
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if len(cells) != len(headers):
            # skip rows that don't align
            continue
        row_dict = dict(zip(headers, cells))
        if "NAME" in row_dict:
            row_dict["NAME"] = clean_name(row_dict["NAME"])
        rows.append(row_dict)

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
        df_inj["PLAYER"] = df_inj["PLAYER"].str.strip() if "PLAYER" in df_inj.columns else df_inj["playerName"].str.strip()
        injured_names = set(df_inj["PLAYER"].dropna().unique())
    except Exception as e:
        return f"Error loading or processing NBA injuries CSV: {e}"
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
    yellow = sorted_df.iloc[0:3]
    green = sorted_df.iloc[3:6]
    red = sorted_df.iloc[6:9]
    player_col = "NAME" if "NAME" in sorted_df.columns else None
    if player_col is None:
        return "Player column not found in CSV."
    output = f"🟢 {', '.join(str(x) for x in green[player_col].tolist() if not is_banned(str(x), stat_key))}\n"
    output += f"🟡 {', '.join(str(x) for x in yellow[player_col].tolist() if not is_banned(str(x), stat_key))}\n"
    output += f"🔴 {', '.join(str(x) for x in red[player_col].tolist() if not is_banned(str(x), stat_key))}"
    return output

def analyze_nhl_psp(file_path, stat_key):
    NHL_PSP_COLUMN_MAP = {
        "SHOTS": "S",
        "POINTS": "P",
        "ASSISTS": "A",
        "GOALS": "G",
        "HITS": "HIT",
        "SAVES": "SV"
    }
    mapped_stat = NHL_PSP_COLUMN_MAP.get(stat_key, stat_key)
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return f"Error reading PSP CSV: {e}"
    df.columns = [col.upper() for col in df.columns]
    if mapped_stat not in df.columns:
        return f"Error: Column '{mapped_stat}' not found in PSP CSV. Available columns: {df.columns.tolist()}"
    try:
        df[mapped_stat] = pd.to_numeric(df[mapped_stat].replace({',': ''}, regex=True), errors='coerce')
    except Exception as e:
        return f"Error converting stat column: {e}"
    sorted_df = df.sort_values(by=mapped_stat, ascending=False).reset_index(drop=True)
    if len(sorted_df) >= 15:
        yellow = sorted_df.iloc[0:3]
        green = sorted_df.iloc[5:8]
        red = sorted_df.iloc[12:15]
    else:
        yellow = sorted_df.iloc[0:3]
        green = sorted_df.iloc[3:6]
        red = sorted_df.iloc[6:9]
    player_col = "NAME" if "NAME" in sorted_df.columns else None
    if player_col is None:
        return "Player column not found in CSV."
    green_list = [x for x in green[player_col].tolist() if not is_banned(str(x), stat_key)]
    yellow_list = [x for x in yellow[player_col].tolist() if not is_banned(str(x), stat_key)]
    red_list = [x for x in red[player_col].tolist() if not is_banned(str(x), stat_key)]
    output = f"🟢 {', '.join(str(x) for x in green_list)}\n"
    output += f"🟡 {', '.join(str(x) for x in yellow_list)}\n"
    output += f"🔴 {', '.join(str(x) for x in red_list)}"
    return output

def analyze_mlb_psp(file_path, stat_key, teams):
    """
    Reads the StatMuse CSV at file_path, filters to `teams` (if present),
    sorts by stat_key descending, then returns:
      🟢 names[3:6]
      🟡 names[0:3]
      🔴 names[6:9]
    """
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return f"Error reading PSP CSV: {e}"
    # normalize column names
    df.columns = [c.upper() for c in df.columns]
    if stat_key not in df.columns:
        return f"Error: Column '{stat_key}' not found in PSP CSV."
    # convert to numeric
    df[stat_key] = pd.to_numeric(df[stat_key].replace({',': ''}, regex=True), errors='coerce')
    # filter by team if we have a TEAM column
    if "TEAM" in df.columns and teams:
        team_list = (
            teams if isinstance(teams, list)
            else [t.strip().upper() for t in str(teams).split(",")]
        )
        df = df[df["TEAM"].str.upper().isin(team_list)]
    # sort and pick slices
    sorted_df = df.sort_values(by=stat_key, ascending=False).reset_index(drop=True)
    yellow = sorted_df.iloc[0:3]
    green  = sorted_df.iloc[3:6]
    red    = sorted_df.iloc[6:9]
    # assemble output
    names = lambda df_slice: ", ".join(df_slice["NAME"].astype(str).tolist())
    return (
        f"🟢 {names(green)}\n"
        f"🟡 {names(yellow)}\n"
        f"🔴 {names(red)}"
    )

def analyze_nba_psp_notion(file_path, stat_key):
    return analyze_nba_psp(file_path, stat_key)

# ----------------------------
# Missing Functions for MLB and NHL Interactive Analysis
# ----------------------------

def analyze_mlb_noninteractive(df, teams, stat_choice, banned_stat=None):
    if "TEAM" not in df.columns:
        return "❌ 'TEAM' column not found in the DataFrame."
    if teams:
        team_list = ([normalize_team_name(t) for t in teams.split(",") if t.strip()]
                     if isinstance(teams, str) else [normalize_team_name(t) for t in teams])
        filtered_df = df[df["TEAM"].astype(str).apply(normalize_team_name).isin(team_list)].copy()
    else:
        filtered_df = df.copy()
    if filtered_df.empty:
        return "❌ No matching teams found."
    mapped_stat = STAT_CATEGORIES_MLB.get(stat_choice)
    if mapped_stat is None:
        return "❌ Invalid stat choice."
    try:
        filtered_df[mapped_stat] = pd.to_numeric(filtered_df[mapped_stat], errors='coerce')
    except Exception as e:
        return f"Error converting stat column: {e}"
    sorted_df = filtered_df.sort_values(by=mapped_stat, ascending=False)
    sorted_df = sorted_df.drop_duplicates(subset=["PLAYER"])
    sorted_df = sorted_df[~sorted_df["PLAYER"].apply(lambda x: is_banned(x, stat_choice))]
    non_banned = sorted_df["PLAYER"].tolist()
    players_to_use = non_banned[:9] if len(non_banned) >= 9 else non_banned
    yellow_list = players_to_use[0:3]
    green_list = players_to_use[3:6]
    red_list = players_to_use[6:9]
    output = "🟢 " + ", ".join(green_list) + "\n"
    output += "🟡 " + ", ".join(yellow_list) + "\n"
    output += "🔴 " + ", ".join(red_list)
    return output

def analyze_nhl_noninteractive(df, teams, stat_choice, target_value=None, banned_stat=None):
    # filter to only the requested teams
    filtered_df = df[df["Team"].isin(teams)].copy()
    if filtered_df.empty:
        return "❌ No matching teams found."
    
    # compute per‑game as needed
    if stat_choice in ["ASSISTS", "POINTS", "S"]:
        df_mode = calculate_nhl_per_game_stats(filtered_df.copy())
    else:
        df_mode = filtered_df.copy()
    
    mapped_stat = STAT_CATEGORIES_NHL.get(stat_choice)
    if mapped_stat is None:
        return "❌ Invalid NHL stat choice."
    df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
    df_mode = df_mode.dropna(subset=[mapped_stat])
    
    # Shots case, two teams
    if stat_choice == "S" and len(teams) == 2:
        # If Notion provided a target_value, use it directly:
        if target_value is not None:
            return categorize_players(
                df_mode, 
                mapped_stat, 
                target_value, 
                player_col="Player", 
                team_col="Team", 
                stat_for_ban=stat_choice
            )
        
        # otherwise fall back to prompting
        prompt = f"\nEnter target {stat_choice} value (per game): "
        user_in = input(prompt).strip()
        if not user_in:
            return "❌ Target value is required for Shots."
        try:
            tv = float(user_in)
        except Exception as e:
            return f"❌ Invalid target value: {e}"
        return categorize_players(
            df_mode, mapped_stat, tv, 
            player_col="Player", team_col="Team", 
            stat_for_ban=stat_choice
        )
    
    # everything else (league total or single‑team shots)
    sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
    players = sorted_df["Player"].drop_duplicates().tolist()
    
    # splicing 0–3 yellow, 3–6 green, 6–9 red for PSP style
    yellow = players[0:3]
    green  = players[3:6]
    red    = players[6:9]
    
    return (
        f"🟢 {', '.join(green)}\n"
        f"🟡 {', '.join(yellow)}\n"
        f"🔴 {', '.join(red)}"
    )

# ----------------------------
# Notion Database Functions (for Polls)
# ----------------------------
NOTION_TOKEN = "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"
DATABASE_ID = "1aa71b1c-663e-8035-bc89-fb1e84a2d919"
PSP_DATABASE_ID = "1ac71b1c663e808e9110eee23057de0e"
POLL_PAGE_ID = "18e71b1c663e80cdb8a0fe5e8aeee5a9"

client = Client(auth=NOTION_TOKEN)

def fetch_unprocessed_rows(database_id):
    try:
        response = client.databases.query(
            database_id=database_id,
            filter={
                "property": "Processed",
                "select": {"equals": "no"}
            },
            sort=[{
                "property": "Order",
                "direction": "ascending"
            }]
        )
    except Exception as e:
        print("Error querying database:", e)
        return []
    rows = []
    for result in response.get("results", []):
        page_id = result["id"]
        created_time = result.get("created_time", "")
        props = result.get("properties", {})
        if "Teams" in props:
            team_prop = props["Teams"]
            if team_prop.get("type") == "title":
                team_parts = team_prop.get("title", [])
            else:
                team_parts = team_prop.get("rich_text", [])
            teams_raw = "".join(part.get("plain_text", "") for part in team_parts)
            teams_list = [t.strip().upper() for t in teams_raw.split(",") if t.strip()]
            team1 = teams_list[0] if teams_list else ""
            team2 = teams_list[1] if len(teams_list) > 1 else ""
            row_teams = teams_list
        else:
            team1_data = props.get("Team 1", {})
            if team1_data.get("type") == "title":
                team1_parts = team1_data.get("title", [])
            else:
                team1_parts = team1_data.get("rich_text", [])
            team1 = "".join(part.get("plain_text", "") for part in team1_parts).strip().upper()
            team2_parts = props.get("Team 2", {}).get("rich_text", [])
            team2 = "".join(part.get("plain_text", "") for part in team2_parts).strip().upper()
            row_teams = [team1, team2] if team2 else [team1]
        sport_select = props.get("Sport", {}).get("select", {})
        sport = sport_select.get("name", "") if sport_select else ""
        stat_prop = props.get("Stat", {})
        if stat_prop.get("type") == "select":
            stat = stat_prop.get("select", {}).get("name", "")
        elif stat_prop.get("type") == "rich_text":
            stat = "".join(part.get("plain_text", "") for part in stat_prop.get("rich_text", []))
        else:
            stat = ""
        target_prop = props.get("Target", {})
        if target_prop.get("type") == "number":
            target_value = str(target_prop.get("number", ""))
        elif target_prop.get("type") == "rich_text":
            target_value = "".join(part.get("plain_text", "") for part in target_prop.get("rich_text", []))
        else:
            target_value = ""
        order_val = None
        if "Order" in props:
            order_prop = props["Order"]
            if order_prop.get("type") == "unique_id":
                order_val = order_prop.get("unique_id", {}).get("number")
        is_psp = (database_id == PSP_DATABASE_ID)
        rows.append({
            "page_id": page_id,
            "team1": team1,
            "team2": team2,
            "teams": row_teams,
            "sport": sport,
            "stat": stat,
            "target": target_value,
            "created_time": created_time,
            "Order": order_val,
            "psp": is_psp
        })
    rows.sort(key=lambda x: float(x.get("Order") if x.get("Order") is not None else float('inf')))
    return rows

async def append_poll_entries_to_page(entries):
    blocks = []
    for entry in entries:
        # always coerce title and output to strings (never None)
        title_text = str(entry.get("title", "") or "")
        output_text = str(entry.get("output", "") or "")

        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{
                    "type": "text",
                    "text": {"content": title_text}
                }]
            }
        })
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{
                    "type": "text",
                    "text": {"content": output_text}
                }]
            }
        })
        blocks.append({"object": "block", "type": "divider", "divider": {}})
    # chunking logic unchanged
    max_blocks = 100
    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    for block_chunk in chunk_list(blocks, max_blocks):
        try:
            await asyncio.to_thread(
                client.blocks.children.append,
                block_id=POLL_PAGE_ID,
                children=block_chunk
            )
        except Exception as e:
            print(f"Error updating poll page with a block chunk: {e}")
            return None
    return True

async def mark_row_as_processed(page_id):
    try:
        await asyncio.to_thread(client.pages.update,
                                page_id=page_id,
                                properties={"Processed": {"select": {"name": "Yes"}}})
    except Exception as e:
        if "Conflict occurred while saving" in str(e):
            print(f"Conflict error while marking row {page_id} as processed. Retrying...")
            await asyncio.sleep(1)
            await mark_row_as_processed(page_id)
        else:
            print(f"Error marking row {page_id} as processed: {e}")

def update_psp_files():
    psp_path = os.path.join(REALSPORTS_DIR, "psp_database.py")
    try:
        result = subprocess.run(["python", psp_path], capture_output=True, text=True)
        print(result.stdout)
    except Exception as e:
        print("Error running psp_database.py:", e)

def run_universal_sports_analyzer_programmatic(row):
    sport_upper = row["sport"].upper()
    teams = row.get("teams", [])
    if not teams:
        teams = [team.strip().upper() for team in [row.get("team1", ""), row.get("team2", "")] if team]
    def parse_target(target):
        t = target.strip().lower()
        if t in ["", "none"]:
            return None
        try:
            return float(t)
        except Exception:
            return None
    target_val = parse_target(row["target"])
    
    if row.get("psp", False):
        # CBB PSP stays the same
        if sport_upper == "CBB":
            df = integrate_cbb_data("cbb_players_stats.csv", "cbb_injuries.csv")
            if df.empty:
                return "❌ CBB stats not found or empty."
            teams_list = row.get("teams", [])
            if isinstance(teams_list, str):
                teams_list = [normalize_team_name(t) for t in teams_list.split(",") if t.strip()]
            else:
                teams_list = [normalize_team_name(t) for t in teams_list]
            if teams_list:
                df = df[df["Team"].apply(normalize_team_name).isin(teams_list)]
            return categorize_players(
                df,
                row["stat"].upper(),
                target_val,
                player_col="Player",
                team_col="Team",
                stat_for_ban=row["stat"].upper()
            )

        # NEW: WNBA PSP uses same integrate + categorize logic
        elif sport_upper == "WNBA":
            df = integrate_wnba_data("wnba_player_stats.csv")
            if df.empty:
                return "❌ WNBA stats not found or empty."

            # filter to the two teams, if provided
            teams_list = row.get("teams", [])
            if isinstance(teams_list, str):
                teams_list = [normalize_team_name(t) for t in teams_list.split(",") if t.strip()]
            else:
                teams_list = [normalize_team_name(t) for t in teams_list]
            if teams_list:
                df = df[df["TEAM"].apply(normalize_team_name).isin(teams_list)]

            # pick the right stat column
            used_stat = row["stat"].upper() if row["stat"].strip() else "PPG"
            stat_key  = STAT_CATEGORIES_WNBA.get(used_stat, used_stat)

            return categorize_players(
                df,
                stat_key,
                target_val,
                player_col="PLAYER",
                team_col="TEAM",
                stat_for_ban=used_stat
            )
        elif sport_upper in {"NHL", "NBA", "MLB", "WNBA", "FC"}:
            # Force a fresh StatMuse scrape for NHL, NBA, and MLB PSP rows.
            data = scrape_statmuse_data(sport_upper, row["stat"], row.get("teams", ""))
            if not data:
                return f"❌ No PSP data scraped for {sport_upper}."
            file_name = f"{sport_upper.lower()}_{row['stat'].lower().replace(' ', '_')}_psp_data.csv"
            file_path = os.path.join(PSP_FOLDER, file_name)
            pd.DataFrame(data).to_csv(file_path, index=False)
            if sport_upper == "NHL":
                stat_key = row["stat"].upper()
                return analyze_nhl_psp(file_path, stat_key)
            elif sport_upper == "NBA":
                stat_key = row["stat"].upper()
                if stat_key == "FG3M":
                    stat_key = "3PM"
                if stat_key not in STAT_CATEGORIES_NBA:
                    return f"❌ Invalid NBA stat choice."
                return analyze_nba_psp_notion(file_path, stat_key)
            elif sport_upper == "MLB":
                # fresh StatMuse scrape
                data = scrape_statmuse_data(sport_upper, row["stat"], row.get("teams", ""))
                if not data:
                    return f"❌ No PSP data scraped for {sport_upper}."
                # write CSV
                file_name = f"{sport_upper.lower()}_{row['stat'].lower().replace(' ', '_')}_psp_data.csv"
                file_path = os.path.join(PSP_FOLDER, file_name)
                pd.DataFrame(data).to_csv(file_path, index=False)

                raw_stat = row["stat"].strip().upper() or "RBI"
                # blank output for Strikeouts/K
                if raw_stat in {"K", "SO", "STRIKEOUT", "STRIKEOUTS"}:
                    return "🟢 \n🟡 \n🔴 "

                # map TB to OPS
                if raw_stat in {"TB", "TOTAL BASES"}:
                    stat_key = "OPS"
                else:
                    stat_key = raw_stat

                return analyze_mlb_psp(file_path, stat_key, row.get("teams", []))
        else:
            return "PSP processing not configured for this sport."
        # *** PSP Branch End ***
    else:
        # Non-PSP branch (regular game processing) – leave this section unchanged.
        if sport_upper == "NBA":
            nba_stats_path = os.path.join(REALSPORTS_DIR, "NBA", "nba_player_stats.csv")
            nba_injuries_path = os.path.join(REALSPORTS_DIR, "NBA", "nba_injury_report.csv")
            df = integrate_nba_data('nba_player_stats.csv', 'nba_injury_report.csv')
            # Filter by teams from the Notion row:
            teams_list = row.get("teams", [])
            if isinstance(teams_list, str):
                teams_list = [normalize_team_name(t) for t in teams_list.split(",") if t.strip()]
            else:
                teams_list = [normalize_team_name(t) for t in teams_list]
            if teams_list:
                df = df[df["TEAM"].apply(normalize_team_name).isin(teams_list)]
            used_stat = row["stat"].upper() if row["stat"].strip() else "PPG"
            player_col = "PLAYER" if "PLAYER" in df.columns else "NAME"
            return categorize_players(df, STAT_CATEGORIES_NBA.get(used_stat, used_stat), target_val, player_col, "TEAM", stat_for_ban=used_stat)

    if sport_upper == "CBB":
        player_stats_file = "cbb_players_stats.csv"
        if not os.path.exists(os.path.join(REALSPORTS_DIR, player_stats_file)):
            return f"❌ '{player_stats_file}' file not found."
        try:
            df = integrate_cbb_data(player_stats_file=player_stats_file)
        except FileNotFoundError:
            return f"❌ '{player_stats_file}' file not found."
        # Filter by teams from the Notion row:
        teams_list = row.get("teams", [])
        if isinstance(teams_list, str):
            teams_list = [normalize_team_name(t) for t in teams_list.split(",") if t.strip()]
        else:
            teams_list = [normalize_team_name(t) for t in teams_list]
        if teams_list:
            df = df[df["Team"].apply(normalize_team_name).isin(teams_list)]
        used_stat = row["stat"].upper() if row["stat"].strip() else "PPG"
        return categorize_players(df, STAT_CATEGORIES_CBB.get(used_stat, used_stat), target_val, "Player", "Team", stat_for_ban=used_stat)
    elif sport_upper == "MLB":
        df = integrate_mlb_data()
        if df.empty or "TEAM" not in df.columns:
            return "❌ 'TEAM' column not found in the MLB data."
        used_stat = row["stat"].upper() if row["stat"].strip() else "RBI"
        return analyze_mlb_noninteractive(df, teams, used_stat, banned_stat=used_stat)
    elif sport_upper == "NHL":
        df = integrate_nhl_data("nhl_player_stats.csv", "nhl_injuries.csv")
        nhl_stat = row["stat"].upper() if row["stat"].strip() else "GOALS"
        return analyze_nhl_noninteractive(df, teams, nhl_stat, target_val, nhl_stat)

    elif sport_upper == "WNBA":
        # 1) load
        df = integrate_wnba_data("wnba_player_stats.csv")
        if df.empty or "TEAM" not in df.columns:
            return "❌ WNBA stats not found or empty."

        # 2) normalize & filter by Notion-selected teams
        teams_list = row.get("teams", [])
        if isinstance(teams_list, str):
            teams_list = [normalize_team_name(t) for t in teams_list.split(",") if t.strip()]
        else:
            teams_list = [normalize_team_name(t) for t in teams_list]
        if teams_list:
            df = df[df["TEAM"].isin(teams_list)]

        # 3) stat mapping & categorize
        used_stat = row["stat"].upper() if row["stat"].strip() else "PPG"
        stat_key  = STAT_CATEGORIES_NBA.get(used_stat, used_stat)
        return categorize_players(
            df,
            stat_key,
            target_val,
            player_col="PLAYER",
            team_col="TEAM",
            stat_for_ban=used_stat
        )
    else:
        return "Sport not recognized."

async def process_rows():
    main_rows = fetch_unprocessed_rows(DATABASE_ID)
    psp_rows = fetch_unprocessed_rows(PSP_DATABASE_ID)
    all_rows = main_rows + psp_rows
    poll_entries = []
    for row in all_rows:
        result = run_universal_sports_analyzer_programmatic(row)
        if row.get("psp", False):
            title = f"{row['sport'].upper()} PSP - {row['stat'].upper()}"
        else:
            title = f"Game: {row.get('team1','')} vs {row.get('team2','')} ({row['sport']}, {row['stat']}, Target: {row['target']})"
        poll_entries.append({
            "title": title,
            "output": result
        })
        await mark_row_as_processed(row["page_id"])
    await append_poll_entries_to_page(poll_entries)

def psp_scrape_main():
    rows = fetch_unprocessed_rows(PSP_DATABASE_ID)
    if not rows:
        print("No unprocessed PSP rows found.")
        return
    for row in rows:
        page_id = row["page_id"]
        teams = row["teams"]
        sport = row["sport"]
        stat = row["stat"]
        data = scrape_statmuse_data(sport, stat, teams)
        if data:
            file_name = f"{sport.lower()}_{stat.lower().replace(' ', '_')}_psp_data.csv"
            output_file = os.path.join(PSP_FOLDER, file_name)
            pd.DataFrame(data).to_csv(output_file, index=False)
            print(f"PSP data written to {output_file}")
        else:
            print("No data scraped for this row.")
        mark_row_as_processed(page_id)

def analyze_sport(df, stat_categories, player_col, team_col):
    while True:
        teams_input = input("\nEnter team names separated by commas (or 'exit' to return to main menu): ")
        if teams_input.lower() == 'exit':
            break
        team_list = [normalize_team_name(t) for t in teams_input.split(",") if t.strip()]
        filtered_df = df[df[team_col].astype(str).apply(normalize_team_name).isin(team_list)].copy()
        if filtered_df.empty:
            print("❌ No matching teams found. Please check the team names.")
            continue
        stat_choice = input("\nEnter stat to sort by: ").strip().upper()
        if stat_choice not in stat_categories:
            print("❌ Invalid stat choice. Please try again.")
            continue
        mapped_stat = stat_categories[stat_choice]
        df_mode = filtered_df.copy()
        try:
            df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
        except Exception as e:
            print("Error converting stat column to numeric:", e)
            continue
        target_value = input(f"\nEnter target {stat_choice} value (per game): ").strip()
        if not target_value:
            print("❌ Target value is required.")
            continue
        try:
            target_value = float(target_value)
        except Exception as e:
            print("❌ Invalid target value.", e)
            continue
        result = categorize_players(df_mode, mapped_stat, target_value, player_col, team_col, stat_for_ban=stat_choice)
        print(f"\nPlayer Performance Based on Target {target_value} {stat_choice}:")
        print(result)

def analyze_nhl_flow(df):
    while True:
        teams = input("\nEnter NHL team names separated by commas (or 'exit' to return to main menu): ").replace(" ", "").upper()
        if teams.lower() == "exit":
            break
        team_list = teams.split(",")
        filtered_df = df[df["Team"].isin(team_list)].copy()
        if filtered_df.empty:
            print("❌ No matching teams found. Please check the team names.")
            continue
        stat_choice = input("\nEnter NHL stat to analyze: ").strip().upper()
        if stat_choice not in STAT_CATEGORIES_NHL:
            print("❌ Invalid NHL stat choice.")
            continue
        if stat_choice in ["ASSISTS", "POINTS", "S"]:
            df_mode = calculate_nhl_per_game_stats(filtered_df.copy())
        else:
            df_mode = filtered_df.copy()
        mapped_stat = STAT_CATEGORIES_NHL[stat_choice]
        try:
            df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
        except Exception as e:
            print("Error converting stat column to numeric:", e)
            continue
        if len(team_list) == 2:
            sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
            if stat_choice == "S":
                target_value = input(f"\nEnter target {stat_choice} value (per game): ").strip()
                if not target_value:
                    print("❌ Target value is required for Shots.")
                    continue
                try:
                    target_value = float(target_value)
                except Exception as e:
                    print("❌ Invalid target value.", e)
                    continue
                result = categorize_players(df_mode, mapped_stat, target_value, "Player", "Team", stat_for_ban=stat_choice)
            else:
                if len(df_mode) >= 15:
                    yellow = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[0:3]
                    green = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[5:8]
                    red = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[12:15]
                else:
                    yellow = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[0:3]
                    green = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[3:6]
                    red = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[6:9]
                result = f"🟢 {', '.join(green['Player'].tolist())}\n"
                result += f"🟡 {', '.join(yellow['Player'].tolist())}\n"
                result += f"🔴 {', '.join(red['Player'].tolist())}"
            print("\n" + result)
        else:
            if stat_choice == "S":
                if "GP" in df_mode.columns and "S" in df_mode.columns:
                    df_mode = df_mode.assign(shotsPerGame = pd.to_numeric(df_mode["S"], errors="coerce") / pd.to_numeric(df_mode["GP"], errors="coerce"))
                    sorted_df = df_mode.sort_values(by="shotsPerGame", ascending=False)
                else:
                    print("Required raw data for shots per game is missing.")
                    continue
            elif stat_choice == "POINTS":
                try:
                    df_mode["PTS"] = pd.to_numeric(df_mode["PTS"], errors='coerce')
                except Exception as e:
                    print("Error converting points to numeric:", e)
                    continue
                sorted_df = df_mode.sort_values(by="PTS", ascending=False)
            else:
                sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
            sorted_df = sorted_df.drop_duplicates(subset=["Player"])
            sorted_df = sorted_df[~sorted_df["Player"].apply(lambda x: is_banned(x, stat_choice))]
            non_banned = sorted_df["Player"].tolist()
            if len(non_banned) >= 15:
                yellow = non_banned[0:3]
                green = non_banned[5:8]
                red = non_banned[12:15]
            else:
                yellow = non_banned[0:3]
                green = non_banned[3:6]
                red = non_banned[6:9]
            result = f"🟢 {', '.join(green)}\n"
            result += f"🟡 {', '.join(yellow)}\n"
            result += f"🔴 {', '.join(red)}"
            print("\n" + result)

def analyze_mlb_interactive(df):
    while True:
        teams_input = input("\nEnter MLB team names separated by commas (or 'exit' to return to main menu): ")
        if teams_input.lower() == 'exit':
            break
        team_list = [normalize_team_name(t) for t in teams_input.split(",") if t.strip()]
        filtered_df = df[df["TEAM"].astype(str).apply(normalize_team_name).isin(team_list)].copy()
        if filtered_df.empty:
            print("❌ No matching teams found. Please check the team names.")
            continue
        stat_choice = input("\nEnter MLB stat to sort by (e.g., RBI, G, AB, R, H, AVG, OBP, OPS, TB, SO): ").strip().upper()
        # If user types TB or TOTAL BASES, map to OPS and skip target input
        if stat_choice in {"TB", "TOTAL BASES"}:
            stat_choice = "OPS"
            # Call non-interactive MLB analysis to simply get top 9 players
            result = analyze_mlb_noninteractive(filtered_df, teams_input, stat_choice, banned_stat=stat_choice)
            print(f"\nMLB Top 9 Players for {stat_choice}:")
            print(result)
            continue
        if stat_choice not in STAT_CATEGORIES_MLB:
            print("❌ Invalid MLB stat choice. Available options:", ", ".join(STAT_CATEGORIES_MLB.keys()))
            continue
        mapped_stat = STAT_CATEGORIES_MLB[stat_choice]
        df_mode = filtered_df.copy()
        try:
            df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
        except Exception as e:
            print("Error converting stat column to numeric:", e)
            continue
        target_value = input(f"\nEnter target {stat_choice} value (per game): ").strip()
        if not target_value:
            print("❌ Target value is required.")
            continue
        try:
            target_value = float(target_value)
        except Exception as e:
            print("❌ Invalid target value.", e)
            continue
        result = categorize_players(df_mode, mapped_stat, target_value, "PLAYER", "TEAM", stat_for_ban=stat_choice)
        print(f"\nMLB Player Performance Based on Target {target_value} {stat_choice}:")
        print(result)

def analyze_mlb_by_team_interactive_wrapper():
    df_mlb = integrate_mlb_data()
    if df_mlb.empty:
        print("MLB stats CSV not found or empty.")
        return
    analyze_mlb_by_team_interactive(df_mlb, mapped_stat="RBI")

# ----------------------------
# Missing function for MLB interactive analysis
# ----------------------------
def analyze_mlb_by_team_interactive(df, mapped_stat):
    if df.empty:
        print("MLB stats CSV not found or empty.")
        return
    while True:
        print("\nTop MLB Players (filtered by team if provided):")
        teams_input = input("Enter MLB team names separated by commas (or type 'exit' to return to main menu): ").strip().upper()
        if teams_input.lower() == "exit":
            break
        if teams_input:
            team_list = [x.strip() for x in teams_input.split(",")]
            filtered_df = df[df["TEAM"].astype(str).apply(normalize_team_name).isin(team_list)]
        else:
            filtered_df = df
        if filtered_df.empty:
            print("❌ No matching teams found.")
            continue
        sorted_df = filtered_df.sort_values(by=[mapped_stat], ascending=False)
        sorted_df = sorted_df[~sorted_df["PLAYER"].apply(lambda x: is_banned(x, mapped_stat))]
        non_banned = sorted_df["PLAYER"].tolist()
        if len(non_banned) < 9:
            players_to_use = non_banned
        else:
            players_to_use = non_banned[:9]
        yellow = players_to_use[0:3]
        green = players_to_use[3:6]
        red = players_to_use[6:9]
        print("🟢 " + ", ".join(green))
        print("🟡 " + ", ".join(yellow))
        print("🔴 " + ", ".join(red))

# ----------------------------
# PSP Scraping and Analyzer Functions (PSP Section) End
# ----------------------------

# ----------------------------
# Main Notion Processing and PSP Scraper Entry Point
# ----------------------------
async def process_rows():
    main_rows = fetch_unprocessed_rows(DATABASE_ID)
    psp_rows = fetch_unprocessed_rows(PSP_DATABASE_ID)
    all_rows = main_rows + psp_rows
    poll_entries = []
    for row in all_rows:
        result = run_universal_sports_analyzer_programmatic(row)
        if row.get("psp", False):
            title = f"{row['sport'].upper()} PSP - {row['stat'].upper()}"
        else:
            title = f"Game: {row.get('team1','')} vs {row.get('team2','')} ({row['sport']}, {row['stat']}, Target: {row['target']})"
        poll_entries.append({
            "title": title,
            "output": result
        })
        await mark_row_as_processed(row["page_id"])
    await append_poll_entries_to_page(poll_entries)

def psp_scrape_main():
    rows = fetch_unprocessed_rows(PSP_DATABASE_ID)
    if not rows:
        print("No unprocessed PSP rows found.")
        return
    for row in rows:
        page_id = row["page_id"]
        teams = row["teams"]
        sport = row["sport"]
        stat = row["stat"]
        data = scrape_statmuse_data(sport, stat, teams)
        if data:
            file_name = f"{sport.lower()}_{stat.lower().replace(' ', '_')}_psp_data.csv"
            output_file = os.path.join(PSP_FOLDER, file_name)
            pd.DataFrame(data).to_csv(output_file, index=False)
            print(f"PSP data written to {output_file}")
        else:
            print("No data scraped for this row.")
        mark_row_as_processed(page_id)

# ----------------------------
# Main Menu and Interactive Functions
# ----------------------------
def analyze_sport(df, stat_categories, player_col, team_col):
    while True:
        teams_input = input("\nEnter team names separated by commas (or 'exit' to return to main menu): ")
        if teams_input.lower() == 'exit':
            break
        team_list = [normalize_team_name(t) for t in teams_input.split(",") if t.strip()]
        filtered_df = df[df[team_col].astype(str).apply(normalize_team_name).isin(team_list)].copy()
        if filtered_df.empty:
            print("❌ No matching teams found. Please check the team names.")
            continue
        stat_choice = input("\nEnter stat to sort by: ").strip().upper()
        if stat_choice not in stat_categories:
            print("❌ Invalid stat choice. Please try again.")
            continue
        mapped_stat = stat_categories[stat_choice]
        df_mode = filtered_df.copy()
        try:
            df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
        except Exception as e:
            print("Error converting stat column to numeric:", e)
            continue
        target_value = input(f"\nEnter target {stat_choice} value (per game): ").strip()
        if not target_value:
            print("❌ Target value is required.")
            continue
        try:
            target_value = float(target_value)
        except Exception as e:
            print("❌ Invalid target value.", e)
            continue
        result = categorize_players(df_mode, mapped_stat, target_value, player_col, team_col, stat_for_ban=stat_choice)
        print(f"\nPlayer Performance Based on Target {target_value} {stat_choice}:")
        print(result)

def analyze_nhl_flow(df):
    while True:
        teams = input("\nEnter NHL team names separated by commas (or 'exit' to return to main menu): ").replace(" ", "").upper()
        if teams.lower() == "exit":
            break
        team_list = teams.split(",")
        filtered_df = df[df["Team"].isin(team_list)].copy()
        if filtered_df.empty:
            print("❌ No matching teams found. Please check the team names.")
            continue
        stat_choice = input("\nEnter NHL stat to analyze: ").strip().upper()
        if stat_choice not in STAT_CATEGORIES_NHL:
            print("❌ Invalid NHL stat choice.")
            continue
        if stat_choice in ["ASSISTS", "POINTS", "S"]:
            df_mode = calculate_nhl_per_game_stats(filtered_df.copy())
        else:
            df_mode = filtered_df.copy()
        mapped_stat = STAT_CATEGORIES_NHL[stat_choice]
        try:
            df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
        except Exception as e:
            print("Error converting stat column to numeric:", e)
            continue
        if len(team_list) == 2:
            sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
            if stat_choice == "S":
                target_value = input(f"\nEnter target {stat_choice} value (per game): ").strip()
                if not target_value:
                    print("❌ Target value is required for Shots.")
                    continue
                try:
                    target_value = float(target_value)
                except Exception as e:
                    print("❌ Invalid target value.", e)
                    continue
                result = categorize_players(df_mode, mapped_stat, target_value, "Player", "Team", stat_for_ban=stat_choice)
            else:
                if len(df_mode) >= 15:
                    yellow = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[0:3]
                    green = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[5:8]
                    red = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[12:15]
                else:
                    yellow = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[0:3]
                    green = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[3:6]
                    red = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[6:9]
                result = f"🟢 {', '.join(green['Player'].tolist())}\n"
                result += f"🟡 {', '.join(yellow['Player'].tolist())}\n"
                result += f"🔴 {', '.join(red['Player'].tolist())}"
            print("\n" + result)
        else:
            if stat_choice == "S":
                if "GP" in df_mode.columns and "S" in df_mode.columns:
                    df_mode = df_mode.assign(shotsPerGame = pd.to_numeric(df_mode["S"], errors="coerce") / pd.to_numeric(df_mode["GP"], errors="coerce"))
                    sorted_df = df_mode.sort_values(by="shotsPerGame", ascending=False)
                else:
                    print("Required raw data for shots per game is missing.")
                    continue
            elif stat_choice == "POINTS":
                try:
                    df_mode["PTS"] = pd.to_numeric(df_mode["PTS"], errors='coerce')
                except Exception as e:
                    print("Error converting points to numeric:", e)
                    continue
                sorted_df = df_mode.sort_values(by="PTS", ascending=False)
            else:
                sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
            sorted_df = sorted_df.drop_duplicates(subset=["Player"])
            sorted_df = sorted_df[~sorted_df["Player"].apply(lambda x: is_banned(x, stat_choice))]
            non_banned = sorted_df["Player"].tolist()
            if len(non_banned) >= 15:
                yellow = non_banned[0:3]
                green = non_banned[5:8]
                red = non_banned[12:15]
            else:
                yellow = non_banned[0:3]
                green = non_banned[3:6]
                red = non_banned[6:9]
            result = f"🟢 {', '.join(green)}\n"
            result += f"🟡 {', '.join(yellow)}\n"
            result += f"🔴 {', '.join(red)}"
            print("\n" + result)

def analyze_mlb_by_team_interactive_wrapper():
    df_mlb = integrate_mlb_data()
    if df_mlb.empty:
        print("MLB stats CSV not found or empty.")
        return
    analyze_mlb_by_team_interactive(df_mlb, mapped_stat="RBI")

# ----------------------------
# Main Menu for MLB interactive analysis (Missing function already defined above)
# ----------------------------

# ----------------------------
# PSP Scraping and Analyzer Functions (PSP Section) End
# ----------------------------

# ----------------------------
# Main Notion Processing and PSP Scraper Entry Point
# ----------------------------
async def process_rows():
    main_rows = fetch_unprocessed_rows(DATABASE_ID)
    psp_rows = fetch_unprocessed_rows(PSP_DATABASE_ID)
    all_rows = main_rows + psp_rows
    poll_entries = []
    for row in all_rows:
        result = run_universal_sports_analyzer_programmatic(row)
        if row.get("psp", False):
            title = f"{row['sport'].upper()} PSP - {row['stat'].upper()}"
        else:
            title = f"Game: {row.get('team1','')} vs {row.get('team2','')} ({row['sport']}, {row['stat']}, Target: {row['target']})"
        poll_entries.append({
            "title": title,
            "output": result
        })
        await mark_row_as_processed(row["page_id"])
    await append_poll_entries_to_page(poll_entries)

def psp_scrape_main():
    rows = fetch_unprocessed_rows(PSP_DATABASE_ID)
    if not rows:
        print("No unprocessed PSP rows found.")
        return
    for row in rows:
        page_id = row["page_id"]
        teams = row["teams"]
        sport = row["sport"]
        stat = row["stat"]
        data = scrape_statmuse_data(sport, stat, teams)
        if data:
            file_name = f"{sport.lower()}_{stat.lower().replace(' ', '_')}_psp_data.csv"
            output_file = os.path.join(PSP_FOLDER, file_name)
            pd.DataFrame(data).to_csv(output_file, index=False)
            print(f"PSP data written to {output_file}")
        else:
            print("No data scraped for this row.")
        mark_row_as_processed(page_id)

# ----------------------------
# Main Menu and Interactive Functions
# ----------------------------
def analyze_sport(df, stat_categories, player_col, team_col):
    while True:
        teams_input = input("\nEnter team names separated by commas (or 'exit' to return to main menu): ")
        if teams_input.lower() == 'exit':
            break
        team_list = [normalize_team_name(t) for t in teams_input.split(",") if t.strip()]
        filtered_df = df[df[team_col].astype(str).apply(normalize_team_name).isin(team_list)].copy()
        if filtered_df.empty:
            print("❌ No matching teams found. Please check the team names.")
            continue
        stat_choice = input("\nEnter stat to sort by: ").strip().upper()
        if stat_choice not in stat_categories:
            print("❌ Invalid stat choice. Please try again.")
            continue
        mapped_stat = stat_categories[stat_choice]
        df_mode = filtered_df.copy()
        try:
            df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
        except Exception as e:
            print("Error converting stat column to numeric:", e)
            continue
        target_value = input(f"\nEnter target {stat_choice} value (per game): ").strip()
        if not target_value:
            print("❌ Target value is required.")
            continue
        try:
            target_value = float(target_value)
        except Exception as e:
            print("❌ Invalid target value.", e)
            continue
        result = categorize_players(df_mode, mapped_stat, target_value, player_col, team_col, stat_for_ban=stat_choice)
        print(f"\nPlayer Performance Based on Target {target_value} {stat_choice}:")
        print(result)

def analyze_nhl_flow(df):
    while True:
        teams = input("\nEnter NHL team names separated by commas (or 'exit' to return to main menu): ").replace(" ", "").upper()
        if teams.lower() == "exit":
            break
        team_list = teams.split(",")
        filtered_df = df[df["Team"].isin(team_list)].copy()
        if filtered_df.empty:
            print("❌ No matching teams found. Please check the team names.")
            continue
        stat_choice = input("\nEnter NHL stat to analyze: ").strip().upper()
        if stat_choice not in STAT_CATEGORIES_NHL:
            print("❌ Invalid NHL stat choice.")
            continue
        if stat_choice in ["ASSISTS", "POINTS", "S"]:
            df_mode = calculate_nhl_per_game_stats(filtered_df.copy())
        else:
            df_mode = filtered_df.copy()
        mapped_stat = STAT_CATEGORIES_NHL[stat_choice]
        try:
            df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
        except Exception as e:
            print("Error converting stat column to numeric:", e)
            continue
        if len(team_list) == 2:
            sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
            if stat_choice == "S":
                target_value = input(f"\nEnter target {stat_choice} value (per game): ").strip()
                if not target_value:
                    print("❌ Target value is required for Shots.")
                    continue
                try:
                    target_value = float(target_value)
                except Exception as e:
                    print("❌ Invalid target value.", e)
                    continue
                result = categorize_players(df_mode, mapped_stat, target_value, "Player", "Team", stat_for_ban=stat_choice)
            else:
                if len(df_mode) >= 15:
                    yellow = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[0:3]
                    green = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[5:8]
                    red = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[12:15]
                else:
                    yellow = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[0:3]
                    green = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[3:6]
                    red = df_mode.sort_values(by=mapped_stat, ascending=False).iloc[6:9]
                result = f"🟢 {', '.join(green['Player'].tolist())}\n"
                result += f"🟡 {', '.join(yellow['Player'].tolist())}\n"
                result += f"🔴 {', '.join(red['Player'].tolist())}"
            print("\n" + result)
        else:
            if stat_choice == "S":
                if "GP" in df_mode.columns and "S" in df_mode.columns:
                    df_mode = df_mode.assign(shotsPerGame = pd.to_numeric(df_mode["S"], errors="coerce") / pd.to_numeric(df_mode["GP"], errors="coerce"))
                    sorted_df = df_mode.sort_values(by="shotsPerGame", ascending=False)
                else:
                    print("Required raw data for shots per game is missing.")
                    continue
            elif stat_choice == "POINTS":
                try:
                    df_mode["PTS"] = pd.to_numeric(df_mode["PTS"], errors='coerce')
                except Exception as e:
                    print("Error converting points to numeric:", e)
                    continue
                sorted_df = df_mode.sort_values(by="PTS", ascending=False)
            else:
                sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
            sorted_df = sorted_df.drop_duplicates(subset=["Player"])
            sorted_df = sorted_df[~sorted_df["Player"].apply(lambda x: is_banned(x, stat_choice))]
            non_banned = sorted_df["Player"].tolist()
            if len(non_banned) >= 15:
                yellow = non_banned[0:3]
                green = non_banned[5:8]
                red = non_banned[12:15]
            else:
                yellow = non_banned[0:3]
                green = non_banned[3:6]
                red = non_banned[6:9]
            result = f"🟢 {', '.join(green)}\n"
            result += f"🟡 {', '.join(yellow)}\n"
            result += f"🔴 {', '.join(red)}"
            print("\n" + result)

def analyze_mlb_by_team_interactive_wrapper():
    df_mlb = integrate_mlb_data()
    if df_mlb.empty:
        print("MLB stats CSV not found or empty.")
        return
    analyze_mlb_by_team_interactive(df_mlb, mapped_stat="RBI")

# ----------------------------
# Main Menu for MLB interactive analysis (Missing function already defined above)
# ----------------------------

# ----------------------------
# Main Notion Processing and PSP Scraper Entry Point
# ----------------------------
async def process_rows():
    main_rows = fetch_unprocessed_rows(DATABASE_ID)
    psp_rows = fetch_unprocessed_rows(PSP_DATABASE_ID)
    all_rows = main_rows + psp_rows
    poll_entries = []
    for row in all_rows:
        result = run_universal_sports_analyzer_programmatic(row)
        if row.get("psp", False):
            title = f"{row['sport'].upper()} PSP - {row['stat'].upper()}"
        else:
            title = f"Game: {row.get('team1','')} vs {row.get('team2','')} ({row['sport']}, {row['stat']}, Target: {row['target']})"
        poll_entries.append({
            "title": title,
            "output": result
        })
        await mark_row_as_processed(row["page_id"])
    await append_poll_entries_to_page(poll_entries)

def psp_scrape_main():
    rows = fetch_unprocessed_rows(PSP_DATABASE_ID)
    if not rows:
        print("No unprocessed PSP rows found.")
        return
    for row in rows:
        page_id = row["page_id"]
        teams = row["teams"]
        sport = row["sport"]
        stat = row["stat"]
        data = scrape_statmuse_data(sport, stat, teams)
        if data:
            file_name = f"{sport.lower()}_{stat.lower().replace(' ', '_')}_psp_data.csv"
            output_file = os.path.join(PSP_FOLDER, file_name)
            pd.DataFrame(data).to_csv(output_file, index=False)
            print(f"PSP data written to {output_file}")
        else:
            print("No data scraped for this row.")
        mark_row_as_processed(page_id)

# ----------------------------
# Main Menu and Interactive Functions
# ----------------------------
def main_menu():
    print("✅ Files loaded successfully")
    while True:
        print("\nSelect Option:")
        print("1️⃣ Interactive Sports Analyzer")
        print("2️⃣ Process Notion Poll Rows (Update Poll Page)")
        print("3️⃣ Run PSP Scraper (StatMuse Data)")
        print("4️⃣ Update Stats & Injuries (Run Big Scraper)")
        print("5️⃣ Exit")
        choice = input("Choose an option (1/2/3/4/5): ").strip()
        if choice == '1':
            print("\n--- Interactive Sports Analyzer ---")
            print("Select Sport:")
            print("1: College Basketball (CBB)")
            print("2: NBA")
            print("3: NHL")
            print("4: MLB")
            print("5: WNBA")
            sport_choice = input("Choose an option (1/2/3/4): ").strip()
            if sport_choice == '1':
                df_cbb = integrate_cbb_data(player_stats_file="cbb_players_stats.csv", injury_data_file="cbb_injuries.csv")
                if df_cbb.empty:
                    continue
                analyze_sport(df_cbb, STAT_CATEGORIES_CBB, "Player", "Team")
            elif sport_choice == '2':
                df_nba = integrate_nba_data('nba_player_stats.csv', 'nba_injury_report.csv')
                analyze_sport(df_nba, STAT_CATEGORIES_NBA, "PLAYER", "TEAM")
            elif sport_choice == '3':
                df_nhl = integrate_nhl_data("nhl_player_stats.csv", "nhl_injuries.csv")
                analyze_nhl_flow(df_nhl)
            elif sport_choice == '4':
                df_mlb = integrate_mlb_data()
                if df_mlb.empty:
                    print("MLB stats CSV not found or empty.")
                    continue
                analyze_mlb_interactive(df_mlb)
            elif sport_choice == '5':
                # WNBA just reuses the NBA mapping and the same analyze_sport()
                df_wnba = integrate_wnba_data("wnba_player_stats.csv")
                if df_wnba.empty:
                    print("WNBA stats CSV not found or empty.")
                    continue
                analyze_sport(df_wnba, STAT_CATEGORIES_NBA, "PLAYER", "TEAM")
            else:
                print("❌ Invalid sport choice.")
        elif choice == '2':
            print("\n--- Processing Notion Poll Rows ---")
            asyncio.run(process_rows())
        elif choice == '3':
            print("\n--- Running PSP Scraper ---")
            psp_scrape_main()
        elif choice == '4':
            print("\n--- Updating Stats & Injuries via Big Scraper ---")
            subprocess.run(["python3", "big_scraper.py"])
        elif choice == '5':
            print("👋 Exiting... Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please select 1, 2, 3, 4, or 5.")

if __name__ == "__main__":
    main_menu()