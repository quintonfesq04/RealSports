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
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

import urllib.parse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# configure headless
chrome_options = Options()
chrome_options.add_argument("--headless")

# now you can safely do:
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
    "RPG": "RPG",   # BBRef calls it “TRB”
    "3PM": "3PM"     # BBRef calls it “3P”
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

# ----------------------------
# SNBA‐specific Team Aliases
# ----------------------------
SNBA_TEAM_ALIASES = {
    "BRK": "BKN",
    "GOS": "GSW",
    "PHL": "PHI",
}

def normalize_snba_team_name(team: str) -> str:
    """Normalize only SNBA team codes."""
    t = team.strip().upper()
    return SNBA_TEAM_ALIASES.get(t, t)

def normalize_team_name(team):
    team = team.strip().upper()
    return TEAM_ALIASES.get(team, team)

TRADED_PLAYERS = {
    "kyle kuzma": "MIL",
    "julie vanloo": "LAS",
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

# permanently bump these studs into the 🟡 Favorite bucket
PERMANENT_YELLOW_PLAYERS = {
    "Shohei Ohtani",
    "Aaron Judge",
    "Rafael Devers",
    "Vladimir Guerrero Jr",
    "Pete Alonso",
    "José Ramírez",
    "Cal Raleigh",
    "Pete Crow-Armstrong",
    "Elly De La Cruz",
    "Fernando Tatis Jr",
    "Ronald Acuña Jr",
    "James Wood",
    "Oneil Cruz",
    "Bobby Witt Jr",
    "Bryce Harper",
    "Jacob Wilson",
}

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
    "Simone Fontecchio",
    "Aari McDonald",
    "Kayla McBride",
    "Iván Herrera",
    "Rowdy Tellez",
    "Kennedy Chandler",
    "David Jones",
    "Jordan Miller",
    "Mason Jones",
    "Kira Lewis, Jr.",
    "Kenny Lofton, Jr.",
    "Charles Bassey",
    "M.J. Walker",
    "Cooper Flagg",
    "Adam Flagler",
    "Javon Freeman-liberty",
    "Phillip Wheeler",
    "Gabe Mcglothan",
    "Jordan Hall",
    "Javonte Cooke",
    "Boogie Ellis",
    "Darius Bazley",
    "Cole Swider",
    "Sir'jabari Rice",
    "D.j. Steward",
    "Zavier Simpson",
    "Reece Beekman",
    "Judah Mintz",
    "Armando Bacot",
    "Reed Sheppard",
    "Jack Mcveigh",
    "Dexter Dennis",
    "Isaiah Mobley",
    "D.j. Carton",
    "Quincy Olivari",
    "Mark Armstrong"
    "Josh Jung",
    "Wendell Moore, Jr.",
    "Markquis Nowell",
    "Antonio Reeves",
    "Jeremy Peña",
    "Kayla Thornton"

]
GLOBAL_BANNED_PLAYERS_SET = {p.strip().lower() for p in GLOBAL_BANNED_PLAYERS}

STAT_SPECIFIC_BANNED = {
    "ASSISTS": {"Jordan Poole"},
    "HITS": {"Brenden Dillon"},
    "3PM": {"Klay Thompson"}
}

def is_banned(player_name, stat=None):
    # Only strings get checked
    if not isinstance(player_name, str):
        return False

    player = player_name.strip().lower()
    if stat:
        banned_for_stat = {p.lower() for p in STAT_SPECIFIC_BANNED.get(stat.upper(), set())}
        if player in banned_for_stat:
            return True
    return player in GLOBAL_BANNED_PLAYERS_SET

import re

# ----------------------------
# Utility Functions: Header Cleaning and MLB Name Fixing
# ----------------------------
import re
import unicodedata

def clean_header(header: str) -> str:
    header = header.strip()
    if header.isupper() and len(header) % 2 == 0:
        half = len(header) // 2
        if header[:half] == header[half:]:
            header = header[:half]
    keys = ["PLAYER","TEAM","RBI","AVG","OBP","OPS","AB","R","H","G","SO"]
    for key in sorted(keys, key=len, reverse=True):
        if key.lower() in header.lower():
            return key
    return header

# MLB name fixer
_suffixes = {"Jr","Sr","II","III","IV","V"}
# match either a capitalized word (allowing accents & apostrophes) OR an exact suffix
_token_re = re.compile(
    r"(?:[A-ZÀ-ÖØ-öø-ÿ][a-zà-öø-ÿ']+)|(?:" + "|".join(_suffixes) + r")"
)

# add a dict of any “weird” raw→desired names
_MLB_NAME_OVERRIDES = {
    "Vladimir V Guerrero Jr": "Vladimir Guerrero Jr",
    "Vinnie V Pasquantino": "Vinnie Pasquantino",
    "Victor V Scott II": "Victor Scott II",
    "Friedl": "TJ Friedl",
    "Ke' Bryan Hayes": "Ke'Bryan Hayes",
    "Logan O' Hoppe": "Logan O'Hoppe",
    "Matt Mc Lain": "Matt McLain",
    "Crawford": "J.P. Crawford",
    "Abrams": "CJ Abrams",
    "Realmuto": "J.T. Realmuto",
    "Ryan O' Hearn": "Ryan O'Hearn",
    "Bleday": "JJ Bleday",
    "Ryan Mc Mahon": "Ryan McMahon",
    "Andrew Mc Cutchen": "Andrew McCutchen",
    "Jeff Mc Neil": "Jeff McNeil",
    "Reese Mc Guire": "Reese McGuire",
    "Jake Mc Carthy": "Jake McCarthy",
    "Zach Mc Kinstry": "Zack McKinstry",
    
    # you can add more exceptions here if they pop up
}

# ----------------------------
# SNBA Name Overrides
# ----------------------------
_raw_snba_overrides = {
    # Raw scraped → Desired clean name
    "Eli N'diaye": "Eli John N'diaye",
    "K.j. Simpson": "KJ Simpson",
    "Liam Mcneeley": "Liam McNeeley",
    "Walter Clayton, Jr.": "Walter Clayton Jr",
    "Cameron Christie": "Cam Christie",
    "Ty Johnson": "TY Johnson",
    "Rayj Dennis": "RayJ Dennis",
    "A.j. Lawson": "A.J. Lawson",
    "G.g. Jackson": "GG Jackson II",
    "Ron Holland": "Ron Holland II",
    "Bronny James": "Bronny James Jr",
    "R.j. Davis": "R.J. Davis",
    "Kevin Mccullar, Jr.": "Kevin McCullar Jr",
    "Patrick Baldwin, Jr.": "Patrick Baldwin Jr",
    "Ja'kobe Walter": "Ja'Kobe Walter",
    "Daron Holmes Ii": "DaRon Holmes II",

    # ... add as you discover more discrepancies
}

def fix_mlb_player_name(raw: str) -> str:
    # 1) normalize accents
    s = unicodedata.normalize("NFC", raw or "")
    # 2) drop digits & unwanted punctuation
    s = re.sub(r"[\d\.]", "", s)
    s = re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ'\s]", " ", s)
    # 3) collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    # 4) extract only proper name tokens or exact suffixes
    tokens = _token_re.findall(s)
    # 5) drop any repeat of a token (except suffixes, which may follow once)
    cleaned = []
    for t in tokens:
        if t in _suffixes:
            if cleaned and cleaned[-1] in _suffixes:
                continue
            cleaned.append(t)
        else:
            if t not in cleaned:
                cleaned.append(t)
    cleaned_name = " ".join(cleaned)

    # 6) apply any manual overrides
    return _MLB_NAME_OVERRIDES.get(cleaned_name, cleaned_name)

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
            df.loc[:, stat_choice] = pd.to_numeric(df[stat_choice], errors='coerce')
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

    stud_lower = {p.lower() for p in PERMANENT_YELLOW_PLAYERS}
    df.loc[
        df[player_col].str.strip().str.lower().isin(stud_lower),
        "Category"
    ] = "🟡 Favorite"


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
    inj_path   = os.path.join(BASE_DIR, injury_data_file)
    
    try:
        stats_df = load_nhl_player_stats(stats_path)
    except FileNotFoundError:
        print(f"Error: cannot find {stats_path}")
        return pd.DataFrame()
    
    try:
        injuries_df = load_nhl_injury_data(inj_path)
    except FileNotFoundError:
        print(f"Error: cannot find {inj_path}")
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
    integrated_data = update_traded_players(integrated_data, player_col="Player", team_col="Team")

    # normalize playoffs or regular-season names to our abbreviations
    integrated_data["Team"] = (
        integrated_data["Team"]
        .astype(str)
        .str.strip()
        .apply(normalize_team_name)
    )
    return integrated_data

# ----------------------------
# MLB Integration (stats + injuries)
# ----------------------------

DESIRED_MLB_COLS = ["PLAYER", "TEAM", "G", "AB", "R", "H", "RBI", "AVG", "OBP", "OPS"]
STAT_CATEGORIES_MLB = {
    "RBI": "RBI", "G": "G", "AB": "AB", "R": "R",
    "H": "H", "AVG": "AVG", "OBP": "OBP", "OPS": "OPS"
}

def load_and_clean_mlb_stats():
    """Read raw MLB stats CSV, normalize headers & player names."""
    stats_file = os.path.join(BASE_DIR, "mlb_2025_stats.csv")
    df = pd.read_csv(stats_file)

    # collapse duplicate headers & map to our desired keys
    df.columns = [clean_header(c) for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]

    # ensure all desired columns exist
    for col in DESIRED_MLB_COLS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df.reindex(columns=DESIRED_MLB_COLS)

    # fix names & normalize teams
    df["PLAYER"] = df["PLAYER"].apply(fix_mlb_player_name)
    df["TEAM"]   = df["TEAM"].astype(str).apply(normalize_team_name)
    return df

def load_mlb_injuries():
    """Read the scraped mlb_injuries.csv and extract clean player names."""
    inj_file = os.path.join(BASE_DIR, "mlb_injuries.csv")
    df = pd.read_csv(inj_file)
    if "playerName" not in df.columns:
        raise RuntimeError("Injury CSV missing 'playerName' column")
    # clean up names just like in the stats
    df["playerName_clean"] = df["playerName"].apply(fix_mlb_player_name)
    return df

def integrate_mlb_data():
    """
    Combine stats + injuries; drop injured players;
    return cleaned DataFrame with DESIRED_MLB_COLS.
    """
    # 1) load & clean stats
    stats_df = load_and_clean_mlb_stats()

    # 2) load & clean injuries
    try:
        inj_df = load_mlb_injuries()
    except Exception:
        return stats_df

    # 3) build set of injured names
    inj_clean = set(inj_df["playerName_clean"].dropna().unique())

    # only swap first/last for exactly two‐token names:
    inj_alt = set()
    for name in inj_clean:
        parts = name.split()
        if len(parts) == 2:
            first, last = parts
            inj_alt.add(f"{last} {first}")

    injured = inj_clean.union(inj_alt)

    # 4) filter them out
    before = len(stats_df)
    stats_df = stats_df[~stats_df["PLAYER"].isin(injured)].copy()
    # dropped = before - len(stats_df)  # no longer printed

    # 5) numeric‐ify the rest (drop any values that can’t convert)
    for col in DESIRED_MLB_COLS:
        if col not in ("PLAYER", "TEAM"):
            try:
                stats_df[col] = pd.to_numeric(stats_df[col])
            except Exception:
                # if a column can’t be converted, just leave it as-is
                pass

    return stats_df

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
def load_wnba_player_stats(file_path):
    """Load the WNBA stats CSV produced by your scraper."""
    return pd.read_csv(file_path)

def integrate_wnba_data(player_stats_file="wnba_player_stats.csv"):
    stats_path = os.path.join(BASE_DIR, player_stats_file)
    df = pd.read_csv(stats_path)

    # normalize headers
    df.columns = df.columns.str.strip().str.upper()
    # **BBRef uses "3P" → we convert it to "3PM"**
    df.rename(columns={"3P": "3PM"}, inplace=True, errors="ignore")
    df.rename(columns={"TRB": "RPG"}, inplace=True, errors="ignore")

    # now your STAT_CATEGORIES_WNBA = {"3PM":"3PM", …} will always find a 3PM column
    df["PLAYER"] = df["PLAYER"].str.strip()
    df["TEAM"]   = df["TEAM"].str.strip().apply(normalize_team_name)

    # --- Injury filtering ---
    inj_path = os.path.join(BASE_DIR, "wnba_injuries.csv")
    if os.path.exists(inj_path):
        df_inj = pd.read_csv(inj_path)
        if "playerName" in df_inj.columns:
            injured = set(df_inj["playerName"].astype(str).str.strip().unique())
            before = len(df)
            df = df[~df["PLAYER"].isin(injured)].copy()
            dropped = before - len(df)
            #print(f"🔍 Dropped {dropped} injured WNBA players")
        else:
            print("⚠️ 'playerName' column missing in wnba_injuries.csv; skipping injury filter")
    else:
        print("⚠️ wnba_injuries.csv not found; skipping injury filter")

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

# ---------- Summer League Integration ----------
# normalize the keys once
_SNBA_NAME_OVERRIDES = {
    key.lower().replace(".", "").replace("'", "").strip(): val
    for key, val in _raw_snba_overrides.items()
}

def fix_snba_player_name(raw: str) -> str:
    """
    Normalize and correct raw Summer League player names
    using _SNBA_NAME_OVERRIDES.
    """
    name = raw or ""
    # build our lookup key: lowercase, drop dots & apostrophes, strip whitespace
    key = name.lower().replace(".", "").replace("'", "").strip()

    # 1) if we have a manual override, use it
    if key in _SNBA_NAME_OVERRIDES:
        return _SNBA_NAME_OVERRIDES[key]

    # 2) otherwise title-case each part
    return " ".join(part.capitalize() for part in name.split())

def load_summer_league_stats():
    path = os.path.join(BASE_DIR, "summer_league_stats.csv")
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        print(f"❌ Summer League stats not found at {path}")
        return pd.DataFrame()

    df.columns = df.columns.str.strip().str.upper()
    for human, api in STAT_CATEGORIES_NBA.items():
        if human in df.columns:
            df = df.rename(columns={human: api})

    if "PLAYER" not in df.columns:
        for col in df.columns:
            if "PLAYER" in col:
                df = df.rename(columns={col: "PLAYER"})
                break
    if "TEAM" not in df.columns:
        df["TEAM"] = ""

    # **NEW: fix SNBA names**
    df["PLAYER"] = df["PLAYER"].astype(str).apply(fix_snba_player_name)

    df["PLAYER"] = df["PLAYER"].str.strip()
    df["TEAM"]   = df["TEAM"].str.strip()
    return df



def analyze_summer_league_noninteractive(df, stat_choice, target_value):
    """
    Exactly like NBA non-interactive, but on the Summer League DataFrame.
    """
    if df.empty:
        return "❌ No Summer League data available."

    # map poll-stat to DataFrame column
    mapped = STAT_CATEGORIES_NBA.get(stat_choice.upper())
    if not mapped or mapped not in df.columns:
        return f"❌ Invalid stat '{stat_choice}'. Choose from {list(STAT_CATEGORIES_NBA)}."

    # numeric + drop NaNs
    df[mapped] = pd.to_numeric(df[mapped], errors="coerce")
    df = df.dropna(subset=[mapped])

    # drop duplicates & banned
    df = df[~df["PLAYER"].apply(lambda x: is_banned(x, stat_choice))]
    df = df.drop_duplicates(subset=["PLAYER"])

    # pick a valid team_col (we need _something_ to satisfy categorize_players)
    team_col = "TEAM" if "TEAM" in df.columns else "PLAYER"

    # hand off to your generic categorizer
    return categorize_players(
        df,
        mapped,
        target_value,
        player_col="PLAYER",
        team_col=team_col,
        stat_for_ban=stat_choice
    )

# ----------------------------
# PSP Scraping and Analyzer Functions (PSP Section)
# ----------------------------
# Define missing constants for PSP scraping:
TIME_PERIOD = "last 2 weeks"  # or "last 7 days", "last 60 days", etc.
BASE_URL    = "https://www.statmuse.com"

def build_query_url(query: str, teams=None) -> str:
    teams_str = ",".join(teams) if isinstance(teams, list) else (teams or "")
    full_query = f"{query} {TIME_PERIOD} {teams_str}"
    return f"{BASE_URL}/ask?q={urllib.parse.quote_plus(full_query)}"

def fetch_html(url: str) -> str:
    """Fetch fully-rendered HTML for the StatMuse query, but never block indefinitely."""
    chrome_opts = Options()
    chrome_opts.add_argument("--headless")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")

    # instantiate a fresh headless driver each call  [oai_citation:0‡reddit.com](https://www.reddit.com/r/learnpython/comments/vvpsdl/timeout_exception_while_using_selenium/?utm_source=chatgpt.com)
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_opts
    )

    driver.get(url)
    try:
        # wait up to 20s for at least one row in the table  [oai_citation:1‡selenium-python.readthedocs.io](https://selenium-python.readthedocs.io/_sources/waits.rst.txt?utm_source=chatgpt.com) [oai_citation:2‡stackoverflow.com](https://stackoverflow.com/questions/56797263/my-selenium-program-fails-to-find-element)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
        )
    except TimeoutException:
        # if no rows appear in time, log and continue with whatever we have  [oai_citation:3‡browserstack.com](https://www.browserstack.com/guide/understanding-selenium-timeouts?utm_source=chatgpt.com) [oai_citation:4‡browserstack.com](https://www.browserstack.com/guide/selenium-wait-commands-using-python?utm_source=chatgpt.com)
        print(f"⚠️ Timeout waiting for table rows on {url}. Proceeding anyway.")
    finally:
        html = driver.page_source
        driver.quit()  # always quit to avoid orphaned processes  [oai_citation:5‡frugaltesting.com](https://www.frugaltesting.com/blog/exception-handling-in-selenium-a-comprehensive-guide?utm_source=chatgpt.com)

    return html

def parse_table(html_content: str) -> list[dict]:
    """
    Extracts the first <table> from the HTML and returns a list of row-dicts.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    table = soup.find("table")
    if not table:
        print("❌ No <table> found—cannot scrape PSP data.")
        return []

    # headers from <thead> or first <tr>
    thead = table.find("thead")
    if thead:
        headers = [th.get_text(strip=True).upper() for th in thead.find_all("th")]
    else:
        first_row = table.find("tr")
        headers = [cell.get_text(strip=True).upper() 
                   for cell in first_row.find_all(["th","td"])]

    # body rows
    tbody = table.find("tbody")
    rows_iter = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]

    parsed = []
    for tr in rows_iter:
        cells = [td.get_text(strip=True) for td in tr.find_all(["td","th"])]
        if len(cells) != len(headers):
            continue  # skip malformed rows
        parsed.append(dict(zip(headers, cells)))

    return parsed

def scrape_statmuse_data(sport: str, stat: str, teams=None) -> list[dict]:
    """
    Scrape StatMuse for "<stat> leaders <sport>" (filtered by `teams` if given).
    Returns a list of dicts mapping column → value.
    """
    # 1) build the URL
    url = build_query_url(f"{stat} leaders {sport.lower()}", teams)

    # 2) fetch the rendered HTML
    html = fetch_html(url)

    # 3) parse it into structured rows
    return parse_table(html)

# near the top of your PSP section, replace any existing clean_name with this:

_suffixes = {"Jr", "Sr", "II", "III", "IV", "V"}

def clean_name(name: str) -> str:
    """
    1) Split any concatenated uppercase initial off a preceding name part
       (e.g. "LoydJ" → "Loyd J")
    2) Drop lone-letter tokens (with or without a dot)
    3) Strip trailing dots
    4) Remove any duplicate tokens (case-insensitive), preserving first occurrence
    """
    # 1) break "SmithJ" → "Smith J"
    name = re.sub(r'([a-zà-öø-ÿ])([A-Z])', r'\1 \2', name)

    parts = name.strip().split()
    seen = set()
    cleaned = []
    for p in parts:
        token = p.rstrip(".")           # 3) strip trailing dot
        if re.fullmatch(r"[A-Za-z]\.?", p):
            continue                    # 2) drop lone initials
        low = token.lower()
        if low in seen:
            continue                    # 4) skip duplicates
        seen.add(low)
        cleaned.append(token)
    return " ".join(cleaned)
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
    # 1) Load PSP data
    df = pd.read_csv(file_path)
    df.columns = [c.upper() for c in df.columns]
    df[stat_key] = pd.to_numeric(df[stat_key].replace({',': ''}, regex=True), errors='coerce')

    # 2) Filter out injured players
    try:
        inj_df       = load_mlb_injuries()
        # make a lowercase set for matching:
        injured_set  = {n.lower() for n in inj_df["playerName_clean"].dropna()}

        # compute cleaned names (preserves Title Case)
        df["NAME_CLEAN"] = df["NAME"].apply(fix_mlb_player_name)

        # filter by lowercase comparison
        df = df[~df["NAME_CLEAN"].str.lower().isin(injured_set)]
    except Exception:
        pass

    # 2a) overwrite NAME with the cleaned, Title-Case version
    if "NAME_CLEAN" in df.columns:
        df["NAME"] = df["NAME_CLEAN"]
        df.drop(columns=["NAME_CLEAN"], inplace=True)

    # 3) (optional) team‐filtering, etc.
    if "TEAM" in df.columns and teams:
        team_list = teams if isinstance(teams, list) else [t.strip().upper() for t in str(teams).split(",")]
        df = df[df["TEAM"].str.upper().isin(team_list)]

    # 4) Sort & slice
    sorted_df = df.sort_values(by=stat_key, ascending=False).reset_index(drop=True)
    yellow = sorted_df.iloc[0:3]
    green  = sorted_df.iloc[3:6]
    red    = sorted_df.iloc[6:9]

    # 5) Format output using the now‐clean NAME column
    def names(slice_df):
        return ", ".join(slice_df["NAME"].tolist())

    return (
        f"🟢 {names(green)}\n"
        f"🟡 {names(yellow)}\n"
        f"🔴 {names(red)}"
    )

def analyze_wnba_psp(file_path, stat_key):
    """
    Reads the StatMuse–dumped CSV at file_path, cleans names,
    drops banned players, sorts by stat_key desc, and slices into 🟢/🟡/🔴 buckets.
    """
    # 1) Load
    df = pd.read_csv(file_path)
    df.columns = [c.upper() for c in df.columns]

    # 2) Clean up the NAME column (restore your original logic)
    #    so "Sonia CitronS. Citron" → "Sonia Citron"
    df["NAME"] = df["NAME"].astype(str).apply(clean_name)

    # 3) Ensure stat column is numeric and drop rows where stat or NAME is missing
    df[stat_key] = pd.to_numeric(df[stat_key].replace({',': ''}, regex=True), errors='coerce')
    df = df.dropna(subset=[stat_key, "NAME"])

    # 4) Drop banned players
    df = df[~df["NAME"].apply(lambda nm: is_banned(nm, stat_key))]

    # 5) Sort & slice into buckets
    sorted_df = df.sort_values(by=stat_key, ascending=False).reset_index(drop=True)
    top3 = sorted_df.iloc[0:3]["NAME"].tolist()
    mid3 = sorted_df.iloc[3:6]["NAME"].tolist()
    bot3 = sorted_df.iloc[6:9]["NAME"].tolist()

    # 6) Format output
    return (
        f"🟢 {', '.join(mid3)}\n"
        f"🟡 {', '.join(top3)}\n"
        f"🔴 {', '.join(bot3)}"
    )

def analyze_nba_psp_notion(file_path, stat_key):
    return analyze_nba_psp(file_path, stat_key)

# ----------------------------
# Missing Functions for MLB and NHL Interactive Analysis
# ----------------------------

def analyze_mlb_noninteractive(df, teams, stat_choice, banned_stat=None):
    if "TEAM" not in df.columns:
        return "❌ 'TEAM' column not found in the DataFrame."

    # 1) Filter by team(s)
    if teams:
        team_list = (
            [normalize_team_name(t) for t in teams.split(",") if t.strip()]
            if isinstance(teams, str)
            else [normalize_team_name(t) for t in teams]
        )
        filtered_df = df[df["TEAM"].astype(str)
                          .apply(normalize_team_name)
                          .isin(team_list)].copy()
    else:
        filtered_df = df.copy()

    if filtered_df.empty:
        return "❌ No matching teams found."

    # 2) Map stat choice to column and convert to numeric
    mapped_stat = STAT_CATEGORIES_MLB.get(stat_choice)
    if mapped_stat is None:
        return "❌ Invalid stat choice."
    try:
        filtered_df[mapped_stat] = pd.to_numeric(filtered_df[mapped_stat], errors='coerce')
    except Exception as e:
        return f"Error converting stat column: {e}"

    # 3) Sort, drop duplicates and banned players
    sorted_df = (filtered_df
                 .sort_values(by=mapped_stat, ascending=False)
                 .drop_duplicates(subset=["PLAYER"]))
    sorted_df = sorted_df[~sorted_df["PLAYER"].apply(lambda x: is_banned(x, stat_choice))]
    non_banned = sorted_df["PLAYER"].tolist()

    # 4) Take the top 9 (or fewer) and slice into buckets
    players_to_use = non_banned[:9]
    yellow_list = players_to_use[0:3]
    green_list  = players_to_use[3:6]
    red_list    = players_to_use[6:9]

    # ──────────────────────────────────────────────────────────────────────────
    # 5) Bump any “stud” into the 🟡 Favorite bucket
    studs_lower = {p.lower() for p in PERMANENT_YELLOW_PLAYERS}

    # move them out of green and into front of yellow
    for stud in list(green_list):
        if stud.lower() in studs_lower:
            green_list.remove(stud)
            if stud not in yellow_list:
                yellow_list.insert(0, stud)

    # 6) Pull from the back of yellow up into green until green has 3
    while len(green_list) < 3 and yellow_list:
        mover = yellow_list.pop()    # take the last (lowest) yellow
        green_list.append(mover)

    # 7) Trim both lists back to max 3 entries
    green_list  = green_list[:3]
    yellow_list = yellow_list[:3]
    # ──────────────────────────────────────────────────────────────────────────

    # 8) Build the output
    output  = "🟢 " + ", ".join(green_list)  + "\n"
    output += "🟡 " + ", ".join(yellow_list) + "\n"
    output += "🔴 " + ", ".join(red_list)
    return output

def analyze_nhl_noninteractive(df, teams, stat_choice, target_value=None, banned_stat=None):
    # normalize the teams list too
    team_list = [normalize_team_name(t) for t in teams]  # teams is already a list
    filtered_df = df[
        df["Team"]
        .astype(str)
        .str.upper()
        .apply(normalize_team_name)
        .isin(team_list)
    ].copy()

    if filtered_df.empty:
        return "❌ No matching teams found."

    # per-game adjustment
    if stat_choice in ["ASSISTS", "POINTS", "S"]:
        df_mode = calculate_nhl_per_game_stats(filtered_df.copy())
    else:
        df_mode = filtered_df.copy()

    mapped_stat = STAT_CATEGORIES_NHL.get(stat_choice)
    if not mapped_stat:
        return "❌ Invalid NHL stat choice."

    df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
    df_mode = df_mode.dropna(subset=[mapped_stat])

    # two-team shots with a target
    if stat_choice == "S" and len(team_list) == 2:
        if target_value is None:
            return "❌ Target value is required for Shots."
        return categorize_players(
            df_mode, mapped_stat, target_value,
            player_col="Player", team_col="Team",
            stat_for_ban=stat_choice
        )

    # default slice
    sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
    players = sorted_df["Player"].drop_duplicates().tolist()
    yellow, green, red = players[:3], players[3:6], players[6:9]
    return f"🟢 {', '.join(green)}\n🟡 {', '.join(yellow)}\n🔴 {', '.join(red)}"

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
        # ─── normalize Notion “teams” into teams_list ────────────────────
        raw_teams = row.get("teams", [])
        if isinstance(raw_teams, str):
            teams_list = [
                normalize_team_name(t)
                for t in raw_teams.split(",")
                if t.strip()
            ]
        else:
            teams_list = [
                normalize_team_name(t)
                for t in raw_teams
            ]

        sport_upper = row["sport"].upper()
        target_val  = parse_target(row["target"])

        # ─── CBB & SNBA PSP ──────────────────────────
        if sport_upper in {"CBB", "SNBA"}:
            if sport_upper == "CBB":
                df = integrate_cbb_data("cbb_players_stats.csv", "cbb_injuries.csv")
                if df.empty:
                    return "❌ CBB stats not found or empty."
            else:  # SNBA
                df = load_summer_league_stats()
                if df.empty:
                    return "❌ Summer League stats not found."

            # 1) map human‐readable stat to actual column
            human = row["stat"].strip().upper()
            mapped = STAT_CATEGORIES_NBA.get(human)
            if not mapped:
                return f"❌ Invalid SNBA stat '{human}'. Choose from {list(STAT_CATEGORIES_NBA)}."

            # 2) filter by Notion teams (TEAM column is uppercase)
            teams_raw = row.get("teams", [])
            teams_list = (
                [normalize_team_name(t) for t in teams_raw.split(",")] 
                if isinstance(teams_raw, str) 
                else [normalize_team_name(t) for t in teams_raw]
            )
            if teams_list:
                df = df[df["TEAM"].apply(normalize_team_name).isin(teams_list)]
                if df.empty:
                    return "❌ No SNBA players found for those teams."

            # 3) numeric conversion & generic categorization
            try:
                df[mapped] = pd.to_numeric(df[mapped], errors="coerce")
            except Exception as e:
                return f"Error converting stat column: {e}"

            return categorize_players(
                df,
                mapped,               # e.g. "PTS", "AST", etc.
                target_val,
                player_col="PLAYER",
                team_col="TEAM",
                stat_for_ban=human    # still use human for banning logic
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
            elif sport_upper == "WNBA":
                # 1) scrape fresh from StatMuse
                data = scrape_statmuse_data(sport_upper, row["stat"], row.get("teams", []))
                if not data:
                    return f"❌ No PSP data scraped for WNBA."
                
                # 2) save it
                file_name = f"wnba_{row['stat'].lower().replace(' ', '_')}_psp_data.csv"
                file_path = os.path.join(PSP_FOLDER, file_name)
                pd.DataFrame(data).to_csv(file_path, index=False)

                # 3) map your poll-stat to the CSV column
                stat_key = STAT_CATEGORIES_WNBA.get(row["stat"].upper(), row["stat"].upper())

                # 4) hand off to the PSP analyzer
                return analyze_wnba_psp(file_path, stat_key)
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

            # use the TB column for Total Bases
            if raw_stat in {"TB", "TOTAL BASES"}:
                stat_key = "TB"
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
        if df.empty:
            return "❌ No MLB data."
        used_stat = row["stat"].upper() or "RBI"
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
    
    # ——— Summer League / SNBA ———
    elif sport_upper in {"SUMMER LEAGUE", "SNBA", "NBA SUMMER LEAGUE"}:
        # 1) load
        df_sl = load_summer_league_stats()
        if df_sl.empty:
            return "❌ Summer League stats not found."

        # 2) filter to the two teams (using your SNBA normalizer)
        raw_teams = row.get("teams", [])
        if isinstance(raw_teams, str):
            teams_list = [normalize_snba_team_name(t) for t in raw_teams.split(",") if t.strip()]
        else:
            teams_list = [normalize_snba_team_name(t) for t in raw_teams]
        if teams_list:
            df_sl = df_sl[df_sl["TEAM"].apply(normalize_snba_team_name).isin(teams_list)]
            if df_sl.empty:
                return "❌ No SNBA players found for those teams."

        # 2.5) **NEW**: drop anyone with under 4 games played
        if "G" in df_sl.columns:
            df_sl["G"] = pd.to_numeric(df_sl["G"], errors="coerce")
            df_sl = df_sl[df_sl["G"] >= 4]
            if df_sl.empty:
                return "❌ No SNBA players with at least 3 games."

        # 3) dispatch to analyzer
        try:
            target = float(row["target"])
        except:
            return "❌ Invalid target for Summer League."
        stat = (row["stat"] or "PPG").strip().upper()
        return analyze_summer_league_noninteractive(df_sl, stat, target)
    
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
    # debug: print out exactly what abbreviations you have
    print("Available NHL team codes:", sorted(df["Team"].unique()))
    while True:
        teams_input = input(
            "\nEnter NHL team codes or full names separated by commas (or 'exit' to return): "
        ).strip()
        if teams_input.lower() == "exit":
            break

        # normalize the user’s input to your 3-letter codes
        team_list = [
            normalize_team_name(t)
            for t in teams_input.split(",")
            if t.strip()
        ]

        # normalize and filter your DataFrame’s Team column
        filtered_df = df[
            df["Team"]
              .astype(str)
              .apply(normalize_team_name)
              .isin(team_list)
        ].copy()

        if filtered_df.empty:
            print(f"❌ No matching teams found for {team_list}. Check the codes above.")
            continue

        stat_choice = input("\nEnter NHL stat to analyze (GOALS, ASSISTS, POINTS, S): ").strip().upper()
        if stat_choice not in STAT_CATEGORIES_NHL:
            print("❌ Invalid NHL stat; try GOALS, ASSISTS, POINTS, or S.")
            continue

        # convert to per-game if needed
        if stat_choice in ["ASSISTS", "POINTS", "S"]:
            df_mode = calculate_nhl_per_game_stats(filtered_df.copy())
        else:
            df_mode = filtered_df.copy()

        mapped_stat = STAT_CATEGORIES_NHL[stat_choice]
        df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')

        # simple top/mid/bottom slices
        players = df_mode.sort_values(by=mapped_stat, ascending=False)["Player"].drop_duplicates().tolist()
        yellow, green, red = players[:3], players[3:6], players[6:9]
        print(f"\n🟢 {', '.join(green)}")
        print(f"🟡 {', '.join(yellow)}")
        print(f"🔴 {', '.join(red)}")

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
            print("6: SNBA")
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
                analyze_sport(df_wnba, STAT_CATEGORIES_WNBA, "PLAYER", "TEAM")
            elif sport_choice == '6':
                df_sl = load_summer_league_stats()
                if df_sl.empty:
                    print("❌ Summer League stats not found.")
                else:
                    # Re-use analyze_sport to get buckets by stat and target
                    analyze_sport(df_sl, STAT_CATEGORIES_NBA, "PLAYER", team_col=None)
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