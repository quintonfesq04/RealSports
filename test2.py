#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StatMuse → Notion updater
- CFB: season-mode for ALL stats (per-team queries); TD keeps the special 'td, rec, yard leaders' phrasing.
        Always output; attach 'View query' links; DO NOT post-filter by team.
- NFL: separate timeframe controls (SEASON/DATES/LAST_N_DAYS). League-wide queries; optional post-filter by team.
- Other sports: past-week; teams included in query; MLB trades/injuries; WNBA injuries optional CSV.
- MLB Strikeouts (K/SO/STRIKEOUTS): colors-only (blank names).
"""

import os
import re
import time
import urllib.parse
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, date

import requests
import pandas as pd
from bs4 import BeautifulSoup
from notion_client import Client

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

_PAGE_CACHE: Dict[str, str] = {}

# ============================== ENV / Notion ===============================

NOTION_TOKEN_FALLBACK = "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"

def _load_env_token() -> str:
    tok = os.getenv("NOTION_TOKEN", "").strip()
    if tok:
        return tok
    try:
        from dotenv import load_dotenv
        load_dotenv()
        tok = os.getenv("NOTION_TOKEN", "").strip()
        if tok:
            return tok
    except Exception:
        pass
    print("[warn] using fallback NOTION token; set NOTION_TOKEN in your environment.")
    return NOTION_TOKEN_FALLBACK

NOTION_TOKEN = _load_env_token()
client = Client(auth=NOTION_TOKEN)

DATABASE_ID     = os.getenv("DATABASE_ID",  "1aa71b1c-663e-8035-bc89-fb1e84a2d919")
PSP_DATABASE_ID = os.getenv("PSP_DATABASE_ID","1ac71b1c663e808e9110eee23057de0e")
POLL_PAGE_ID    = os.getenv("POLL_PAGE_ID", "18e71b1c663e80cdb8a0fe5e8aeee5a9")

BASE_URL                   = "https://www.statmuse.com"
CBSSPORTS_MLB_INJURIES_URL = "https://www.cbssports.com/mlb/injuries/"
MLB_TRANSACTIONS_URL       = "https://www.mlb.com/transactions"

DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ============================== NFL controls ===============================

NFL_QUERY_MODE   = os.getenv("NFL_QUERY_MODE", "SEASON").upper()   # "SEASON" | "DATES" | "LAST_N_DAYS"
NFL_SEASON_YEAR  = int(os.getenv("NFL_SEASON_YEAR", "2024"))
NFL_START_DATE   = os.getenv("NFL_START_DATE", "")  # e.g., "September 5, 2024"
NFL_END_DATE     = os.getenv("NFL_END_DATE",   "")  # e.g., "September 12, 2024"
NFL_LAST_N_DAYS  = int(os.getenv("NFL_LAST_N_DAYS", "7"))

# ============================== HTTP session ===============================

try:
    from urllib3.util.retry import Retry
    from requests.adapters import HTTPAdapter
except Exception:
    Retry = None
    HTTPAdapter = None

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": DEFAULT_UA})
    if Retry and HTTPAdapter:
        retry = Retry(
            total=5, backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

SESSION = make_session()

# ============================== Time helpers ===============================

def week_window(today: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    today = today or datetime.today()
    start = today - timedelta(days=6)
    start = datetime(start.year, start.month, start.day)
    end   = datetime(today.year, today.month, today.day)
    return start, end

def _nice_date(d: datetime) -> str:
    return d.strftime("%B %-d, %Y") if os.name != "nt" else d.strftime("%B %#d, %Y")

def football_season_year(today: Optional[date] = None) -> int:
    d = today or date.today()
    return d.year if d.month >= 8 else d.year - 1

CFB_SEASON_YEAR = int(os.getenv("CFB_SEASON_YEAR", football_season_year()))

# ============================== Notion input ===============================

def fetch_unprocessed_rows(db_id: str) -> List[Dict]:
    resp = client.databases.query(
        database_id=db_id,
        filter={"property": "Processed", "select": {"equals": "no"}},
        sort=[{"property": "Order", "direction": "ascending"}]
    )
    out = []
    for r in resp.get("results", []):
        pid   = r["id"]
        props = r["properties"]
        sport = props["Sport"]["select"]["name"]
        st    = props["Stat"]
        if st.get("type") == "select":
            stat = st["select"]["name"].strip()
        else:
            stat = "".join(t["plain_text"] for t in st.get("rich_text", [])).strip()
        if props.get("Teams", {}).get("type") == "multi_select":
            teams = [t["name"] for t in props["Teams"]["multi_select"]]
        else:
            t1 = props.get("Team 1", {}).get("title", [])
            t2 = props.get("Team 2", {}).get("rich_text", [])
            t1txt = t1[0]["plain_text"] if t1 else ""
            t2txt = t2[0]["plain_text"] if t2 else ""
            teams = [x.strip() for x in (t1txt + "," + t2txt).split(",") if x.strip()]
        out.append({"page_id": pid, "sport": sport, "stat": stat, "teams": teams})
    return out

# ============================== Selenium (pooled) ===========================

class _DriverPool:
    _drv: Optional[webdriver.Chrome] = None

    @staticmethod
    def _new() -> webdriver.Chrome:
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
        drv.set_page_load_timeout(35)
        return drv

    @classmethod
    def get(cls) -> webdriver.Chrome:
        if cls._drv is None:
            cls._drv = cls._new()
        return cls._drv

    @classmethod
    def close(cls):
        try:
            if cls._drv:
                cls._drv.quit()
        finally:
            cls._drv = None

# --- Selenium driver factory (place ABOVE fetch_html) ---
def _new_driver(headless: bool = True) -> webdriver.Chrome:
    """
    Create a Chrome WebDriver with sensible defaults.
    Uses webdriver-manager to install the right driver.
    """
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")

    drv = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )
    drv.set_page_load_timeout(35)
    return drv

def fetch_html(url: str, wait_css: str = "table", wait_seconds: int = 25) -> str:
    """
    Fetch a page with Selenium, wait for a CSS selector, and cache the HTML for the duration
    of the run so repeated queries to the same URL are instant.

    - Uses your existing _new_driver()
    - Retries with small backoffs on transient failures
    - Returns cached HTML if the same URL was already fetched
    """
    # Serve from cache if we've already fetched this URL during the run
    cached = _PAGE_CACHE.get(url)
    if cached is not None:
        return cached

    last_err = None
    backoffs = [0.0, 1.0, 2.0]  # seconds
    for delay in backoffs:
        if delay:
            time.sleep(delay)

        drv = None
        try:
            drv = _new_driver()
            drv.get(url)

            # Wait for something table-ish to exist; still accept the page if it times out
            try:
                WebDriverWait(drv, wait_seconds).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                )
            except TimeoutException:
                pass

            html = drv.page_source or ""
            if html:
                _PAGE_CACHE[url] = html
            return html

        except WebDriverException as e:
            last_err = e

        finally:
            try:
                if drv:
                    drv.quit()
            except Exception:
                pass

    if last_err:
        print(f"[webdriver] error for {url}: {last_err}")
    return ""

# ============================== HTML parse =================================

def parse_table(html: str) -> List[Dict[str, str]]:
    soup  = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []
    headers = [th.get_text(strip=True).upper() for th in table.select("thead th")]
    if not headers:
        first_row = table.find("tr")
        if first_row:
            cells = first_row.find_all(["th", "td"])
            headers = [c.get_text(strip=True).upper() or f"COL{i+1}" for i, c in enumerate(cells)]
    body_rows = table.select("tbody tr") or table.find_all("tr")[1:]
    rows: List[Dict[str, str]] = []
    for tr in body_rows:
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not cols:
            cols = [th.get_text(strip=True) for th in tr.find_all("th")]
        if not cols:
            continue
        if len(cols) < len(headers):
            cols += [""] * (len(headers) - len(cols))
        if len(cols) > len(headers):
            cols = cols[:len(headers)]
        rows.append(dict(zip(headers, cols)))
    return rows

# ============================== Team maps & CFB aliases ====================

TEAM_NAME_MAPS = {
    "MLB": {
        "TOR":"Blue Jays","BOS":"Red Sox","NYY":"Yankees","TB":"Rays","BAL":"Orioles",
        "DET":"Tigers","CLE":"Guardians","KC":"Royals","MIN":"Twins","CWS":"White Sox",
        "HOU":"Astros","SEA":"Mariners","TEX":"Rangers","LAA":"Angels","OAK":"Athletics",
        "NYM":"Mets","PHI":"Phillies","MIA":"Marlins","ATL":"Braves","WSH":"Nationals",
        "MIL":"Brewers","CHC":"Cubs","CIN":"Reds","STL":"Cardinals","PIT":"Pirates",
        "LAD":"Dodgers","SD":"Padres","SF":"Giants","ARI":"Diamondbacks","COL":"Rockies",
    },
    "NBA": {
        "BKN":"Nets","OKC":"Thunder","LAL":"Lakers","LAC":"Clippers","CHI":"Bulls",
        "POR":"Trail Blazers","WAS":"Wizards","SAC":"Kings","PHI":"76ers","MIL":"Bucks",
        "DEN":"Nuggets","ORL":"Magic","SAS":"Spurs","MIA":"Heat","UTA":"Jazz",
        "NOP":"Pelicans","BOS":"Celtics","GSW":"Warriors","PHX":"Suns","ATL":"Hawks",
        "NYK":"Knicks","MIN":"Timberwolves","MEM":"Grizzlies","DET":"Pistons",
        "HOU":"Rockets","DAL":"Mavericks","CHA":"Hornets","TOR":"Raptors",
        "CLE":"Cavaliers","IND":"Pacers",
    },
    "WNBA": {
        "NYL":"Liberty","ATL":"Dream","IND":"Fever","WAS":"Mystics","CHI":"Sky",
        "CON":"Sun","MIN":"Lynx","PHO":"Mercury","SEA":"Storm","LVA":"Aces",
        "LAS":"Sparks","DAL":"Wings","GSV":"Valkyries",
    },
    "NHL": {
        "TOR":"Maple Leafs","TBL":"Lightning","FLA":"Panthers","OTT":"Senators",
        "MTL":"Canadiens","DET":"Red Wings","BUF":"Sabres","BOS":"Bruins",
        "WSH":"Capitals","CAR":"Hurricanes","NJD":"Devils","CBJ":"Blue Jackets",
        "NYR":"Rangers","NYI":"Islanders","PIT":"Penguins","PHI":"Flyers",
        "WPG":"Jets","DAL":"Stars","COL":"Avalanche","MIN":"Wild","STL":"Blues",
        "UTA":"Hockey Club","NSH":"Predators","CHI":"Blackhawks","VGK":"Golden Knights",
        "LAK":"Kings","EDM":"Oilers","CGY":"Flames","VAN":"Canucks","ANA":"Ducks",
        "SEA":"Kraken","SJS":"Sharks",
    },
    "CFB": {
        "NCSU": "NC State", "WAKE": "Wake Forest", "INST": "Institute", "UCF": "Central Florida",
        "IND": "Indiana", "COLG": "Colgate", "SYR": "Syracuse", "COLO": "Colorado",
        "HOU": "Houston", "KSU": "Kansas State", "ARIZ": "Arizona", "UNM": "New Mexico",
        "UCLA": "UCLA", "CLEM": "Clemson", "GT": "Georgia Tech", "DUKE": "Duke",
        "TULN": "Tulane", "TOW": "Towson", "MD": "Maryland", "OU": "Oklahoma",
        "TEM": "Temple", "CMU": "Central Michigan", "MICH": "Michigan", "BUFF": "Buffalo",
        "KENT": "Kent State", "ORE": "Oregon", "WISC": "Wisconsin", "ALA": "Alabama",
        "HCU": "Houston Christian", "NEB": "Nebraska", "W&M": "William & Mary",
        "SAM": "Samford", "BAY": "Baylor", "MEM": "Memphis", "TROY": "Troy",
        "USA": "South Alabama", "AUB": "Auburn", "UNH": "New Hampshire",
        "BALL": "Ball State", "CONN": "UConn", "DEL": "Delaware", "YSU": "Youngstown State",
        "MSU": "Michigan State", "MORG": "Morgan State", "TOL": "Toledo",
        "NWST": "Northwestern", "CIN": "Cincinnati", "USC": "USC", "PUR": "Purdue",
        "SMU": "SMU", "MOST": "Missouri State", "WSU": "Washington State",
        "UNT": "North Texas", "RICH": "Richmond", "UNC": "North Carolina",
        "ORST": "Oregon State", "TTU": "Texas Tech", "UGA": "Georgia",
        "TENN": "Tennessee", "PITT": "Pittsburgh", "WVU": "West Virginia",
        "NORF": "Norfolk State", "RUTG": "Rutgers", "VILL": "Villanova", "PSU": "Penn State",
        "UIW": "Incarnate Word", "UTSA": "UTSA", "UL": "Louisiana", "MIZ": "Missouri",
        "ISU": "Iowa State", "ARST": "Arkansas State", "UTEP": "UTEP", "TEX": "Texas",
        "USF": "South Florida", "MIAM": "Miami", "LIB": "Liberty", "BGSU": "Bowling Green",
        "MTSU": "Middle Tennessee", "NEV": "Nevada", "ALCN": "Alcorn State",
        "MSST": "Mississippi State", "EKU": "Eastern Kentucky", "MRSH": "Marshall",
        "FAU": "Florida Atlantic", "FIU": "FIU", "MRMK": "Merrimack", "KENN": "Kennesaw State",
        "MONM": "Monmouth", "CLT": "Charlotte", "ODU": "Old Dominion", "VT": "Virginia Tech",
        "JVST": "Jacksonville State", "GASO": "Georgia Southern", "WMU": "Western Michigan",
        "ILL": "Illinois", "PV": "Prairie View", "RICE": "Rice", "APP": "Appalachian State",
        "USM": "Southern Miss", "ARK": "Arkansas", "MISS": "Ole Miss", "MURR": "Murray State",
        "GAST": "Georgia State", "OHIO": "Ohio", "OSU": "Ohio State", "FLA": "Florida",
        "LSU": "LSU", "EMU": "Eastern Michigan", "UKY": "Kentucky", "TA&M": "Texas A&M",
        "ND": "Notre Dame", "MASS": "UMass", "IOWA": "Iowa", "NMSU": "New Mexico State",
        "LT": "Louisiana Tech", "ECU": "East Carolina", "CCU": "Coastal Carolina",
        "VAN": "Vanderbilt", "SC": "South Carolina", "NAVY": "Navy", "TLSA": "Tulsa",
        "ACU": "Abilene Christian", "TCU": "TCU", "UTAH": "Utah", "WYO": "Wyoming",
        "AKR": "Akron", "UAB": "UAB", "AFA": "Air Force", "USU": "Utah State",
        "SOU": "Southern", "FRES": "Fresno State", "MINN": "Minnesota",
        "CAL": "California", "TXST": "Texas State", "ASU": "Arizona State",
        "BC": "Boston College", "STAN": "Stanford", "PRST": "Portland State",
        "HAW": "Hawaii", "OKST": "Oklahoma State", "WOFF": "Wofford", "KU": "Kansas",
        "UCF": "UCF", "M-OH": "Miami (OH)", "FSU": "Florida State", "LOU": "Louisville",
        "UL": "UL Monroe", "UVA": "Virginia", "BYU": "BYU", "WAG": "Wagner",
        "NIU": "Northern Illinois", "IDHO": "Idaho", "SJSU": "San Jose State",
        "DUQ": "Duquesne", "WKU": "Western Kentucky", "MEM": "Maine",
        "UTM": "UT Martin", "BSU": "Boise State", "WASH": "Washington", "SELA": "Southeastern Louisiana",
        "NICH": "Nicholls", "SHSU": "Sam Houston State", "SDSU": "San Diego State",
    },
    "NFL": {
        "WAS": "Commanders", "GB": "Packers", "NYG": "Giants", "DAL": "Cowboys",
        "CLE": "Browns", "BAL": "Ravens", "JAX": "Jaguars", "CIN": "Bengals",
        "CHI": "Bears", "DET": "Lions", "NE": "Patriots", "MIA": "Dolphins",
        "SF": "49ers", "NO": "Saints", "BUF": "Bills", "NJJ": "Jets",
        "SEA": "Seahawks", "PIT": "Steelers", "LAR": "Rams", "TEN": "Titans",
        "CAR": "Panthers", "ARI": "Cardinals", "DEN": "Broncos", "IND": "Colts",
        "PHI": "Eagles", "KC": "Chiefs", "ATL": "Falcons", "MIN": "Vikings",
        "TB": "Buccaneers", "HOU": "Texans", "LAC": "Chargers", "LV": "Raiders",
    }
}

def _k(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", s).upper()

CFB_ALIASES: Dict[str, str] = {}
def _add_cfb(canonical: str, *aliases: str):
    CFB_ALIASES[_k(canonical)] = canonical
    for a in aliases:
        CFB_ALIASES[_k(a)] = canonical

# (abbrev) Examples + recent adds you flagged
_add_cfb("Louisville Cardinals", "Louisville", "UL", "LOU")
_add_cfb("James Madison Dukes", "James Madison", "JMU")
_add_cfb("Northern Illinois Huskies", "Northern Illinois", "NIU")
_add_cfb("Maryland Terrapins", "Maryland", "MD")
_add_cfb("Northwestern Wildcats", "Northwestern", "NU")
_add_cfb("Wisconsin Badgers", "Wisconsin", "WISC", "UWisc", "UW")
_add_cfb("Western Illinois Leathernecks", "Western Illinois", "WIU")
# ... (keep building this as you go — prior big lists are fine to paste here)

def map_cfb_teams_for_statmuse(teams: List[str]) -> Tuple[List[str], List[str]]:
    mapped, unmapped = [], []
    for t in (teams or []):
        key = _k(t)
        name = CFB_ALIASES.get(key)
        if not name:
            t2 = re.sub(r"\b(UNIVERSITY|UNIV|THE)\b", "", t, flags=re.I).strip()
            name = CFB_ALIASES.get(_k(t2))
        if name:
            mapped.append(name)
        else:
            unmapped.append(t)
            mapped.append(t)
    return mapped, unmapped

# ============================== StatMuse helpers ===========================

def _is_nfl(sport_up: str) -> bool:
    return sport_up == "NFL"

def _quote(s: str) -> str:
    return urllib.parse.quote_plus(s)

def build_query_url(
    query: str,
    teams: Optional[List[str]] = None,
    sport: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    include_dates: bool = True
) -> str:
    base = query
    if include_dates:
        if not (start and end):
            start, end = week_window()
        base += f" from {_nice_date(start)} to {_nice_date(end)}"

    if teams:
        if sport and sport.strip().upper() in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
            mapped, unmapped = map_cfb_teams_for_statmuse(teams)
        else:
            team_map = TEAM_NAME_MAPS.get((sport or "").strip().upper(), {})
            mapped = [team_map.get(t.strip().upper(), t) for t in teams]
            unmapped = [t for t in teams if team_map.get(t.strip().upper()) is None]
        if unmapped:
            print(f"[warn] Team(s) not mapped for {sport}: {', '.join(unmapped)}")
        base += f" {','.join(mapped)}"

    return f"{BASE_URL}/ask?q={_quote(base)}"

def scrape_statmuse_data(stat: str, sport: str, teams=None) -> Tuple[List[Dict[str, str]], bool, int, List[str]]:
    """
    Return (table_rows, used_season_mode, season_year, debug_links)
    - CFB: season-mode (per-team) for ALL stats (TD uses special phrasing)
    - NFL: SEASON/DATES/LAST_N_DAYS (league-wide; filter after)
    - Others: weekly-mode (teams included)
    """
    sport_up = (sport or "").strip().upper()
    stat_up  = (stat  or "").strip().upper()
    debug_links: List[str] = []

    # ----- CFB: season-mode for ALL stats (per-team) -----
    if sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
        season = CFB_SEASON_YEAR
        all_rows: List[Dict[str, str]] = []
        teams = teams or []
        mapped, unmapped = map_cfb_teams_for_statmuse(teams)
        if unmapped:
            print(f"[warn] Team(s) not mapped for CFB: {', '.join(unmapped)}")

        def _cfb_query(team: Optional[str]) -> str:
            if stat_up in {"TD","TDS","TOUCHDOWNS"}:
                # special phrasing improves StatMuse results for TD
                return f"td, rec, yard leaders cfb {season}" + (f" {team}" if team else "")
            else:
                return f"{stat} leaders cfb {season}" + (f" {team}" if team else "")

        if not mapped:
            q = _cfb_query(None)
            url = build_query_url(q, None, "CFB", include_dates=False)
            debug_links.append(url)
            html = fetch_html(url)
            all_rows.extend(parse_table(html))
        else:
            for t in mapped:
                q   = _cfb_query(t)
                url = build_query_url(q, None, "CFB", include_dates=False)
                debug_links.append(url)
                html = fetch_html(url)
                all_rows.extend(parse_table(html))

        if not all_rows:
            print("[parse] no CFB rows for:", debug_links)
        return all_rows, True, season, debug_links

    # ----- NFL: separate timeframe controls; league-wide -----
    if _is_nfl(sport_up):
        if NFL_QUERY_MODE == "SEASON":
            q   = f"{stat} leaders nfl {NFL_SEASON_YEAR}"
            url = build_query_url(q, None, "NFL", include_dates=False)
        elif NFL_QUERY_MODE == "DATES" and NFL_START_DATE and NFL_END_DATE:
            q   = f"{stat} leaders nfl from {NFL_START_DATE} to {NFL_END_DATE}"
            url = f"{BASE_URL}/ask?q={_quote(q)}"
        else:
            end = datetime.today()
            start = end - timedelta(days=max(1, NFL_LAST_N_DAYS - 1))
            q   = f"{stat} leaders nfl"
            url = build_query_url(q, None, "NFL", start, end, include_dates=True)

        debug_links.append(url)
        html = fetch_html(url)
        rows = parse_table(html)
        if not rows:
            print(f"[parse] no NFL rows for: {url}")
        used_season = (NFL_QUERY_MODE == "SEASON")
        return rows, used_season, (NFL_SEASON_YEAR if used_season else 0), debug_links
    
    # If SEASON mode returned nothing, try a stricter wording once
    if NFL_QUERY_MODE == "SEASON" and not rows:
        q2  = f"{stat} leaders in the {NFL_SEASON_YEAR} nfl season"
        url2 = f"{BASE_URL}/ask?q={_quote(q2)}"
        debug_links.append(url2)
        html2 = fetch_html(url2)
        rows2 = parse_table(html2)
        if rows2:
            rows = rows2   

    # ----- Others: weekly-mode with teams -----
    start, end = week_window()
    q   = f"{stat} leaders {sport.lower()}"
    url = build_query_url(q, teams, sport, start, end, include_dates=True)
    debug_links.append(url)
    html = fetch_html(url)
    rows = parse_table(html)
    if not rows:
        print(f"[parse] no data for statmuse url: {url}")
    return rows, False, 0, debug_links

# ============================== Name cleaning / stat keys ==================

NAME_OVERRIDES = {
    "Aari Mc Donald": "Aari McDonald",
    "De Wanna Bonner": "DeWanna Bonner",
    "Ryan Mc Mahon": "Ryan McMahon",
    "Zach Mc Kinstry": "Zack McKinstry",
    "Jeff Mc Neil": "Jeff McNeil",
    "Reese Mc Guire": "Reese McGuire",
    "Andrew Mc Cutchen": "Andrew McCutchen",
    "Paul De Jong": "Paul DeJong",
    "Jake Mc Carthy": "Jake McCarthy",
    "JJBleday": "JJ Bleday",
    "Michael Harris IIM II": "Michael Harris II",
    "TJFriedl": "TJ Friedl",
    "Matt Mc Lain": "Matt McLain",
}

BANNED_PLAYERS = set()

def clean_name(raw: str) -> str:
    s = (raw or "").replace(".", " ")
    s = re.sub(r'([a-zà-öø-ÿ])([A-Z])', r'\1 \2', s)
    s = re.sub(r'([A-Z]) ([A-Z][a-z])', r'\1\2', s)
    s = re.sub(r'\s+', ' ', s).strip()
    parts, seen, out = s.split(), set(), []
    for p in parts:
        if len(p) == 1: continue
        low = p.lower()
        if low in seen: continue
        seen.add(low); out.append(p)
    cleaned = " ".join(out)
    return NAME_OVERRIDES.get(cleaned, cleaned)

STAT_ALIASES = {
    "K":  {"K", "SO", "STRIKEOUTS"},
    "HR": {"HR", "HOMERS", "HOME RUNS"},
    "RBI":{"RBI", "RBIS"},
    "3P": {"3P", "3PM", "3-PT", "3PT", "THREE POINTERS"},
    "PTS":{"PTS", "POINTS"},
    "REB":{"REB", "REBOUNDS"},
    "AST":{"AST", "ASSISTS"},
    # Football-ish
    "TD": {"TD", "TDS", "TOUCHDOWNS"},
    "YDS":{"YDS","YARDS"},
}

def pick_stat_key(data: List[Dict[str, str]], desired: str) -> str:
    target = (desired or "").strip().upper()
    if not data:
        return target

    headers = list(data[0].keys())
    upper   = {h.upper(): h for h in headers}

    # 1) exact / alias matches
    if target in upper:
        return upper[target]
    for alias in STAT_ALIASES.get(target, {target}):
        if alias in upper:
            return upper[alias]

    # 2) prefer "total-like" numeric columns over obvious rates
    rate_like = re.compile(r"(?:%|/|PER|AVG|RATE|Y/A|Y\/A|ATT/G|G/|PG)$", re.I)
    numeric_candidates = []
    for h in headers:
        v = str(data[0].get(h, "")).replace(",", "")
        try:
            float(v)
            numeric_candidates.append(h)
        except Exception:
            continue

    # pick first numeric that doesn't look like a rate
    for h in numeric_candidates:
        if not rate_like.search(h):
            return h
    # fallback: first numeric
    return numeric_candidates[0] if numeric_candidates else target

# ============================== Bucketing & formatting =====================

def bucket_top12(data: List[Dict[str, str]], stat_key: str) -> Tuple[List[str], List[str], List[str], List[str]]:
    if not data:
        return [], [], [], []
    name_col = "NAME" if "NAME" in data[0] else "PLAYER" if "PLAYER" in data[0] else list(data[0].keys())[0]
    cleaned: List[Tuple[str, float]] = []
    for rec in data:
        raw_val = str(rec.get(stat_key, "")).replace(",", "")
        try:
            num = float(raw_val)
        except Exception:
            continue
        nm = clean_name(rec.get(name_col, ""))
        if not nm:
            continue
        cleaned.append((nm, num))
    cleaned.sort(key=lambda x: x[1], reverse=True)
    top12 = cleaned[:12]
    green  = [nm for nm, _ in top12[0:3]]
    yellow = [nm for nm, _ in top12[3:6]]
    red    = [nm for nm, _ in top12[6:9]]
    purple = [nm for nm, _ in top12[9:12]]
    return green, yellow, red, purple

def _format_buckets_default(g, y, rd, p) -> str:
    def line(dot, names): return f"{dot} {', '.join(names) if names else 'None'}"
    return "\n".join([line("🟢", g), line("🟡", y), line("🔴", rd), line("🟣", p)])

def _format_buckets_cfb(g, y, rd, p) -> str:
    # Drop empty colors for CFB
    lines = []
    if g:  lines.append(f"🟢 {', '.join(g)}")
    if y:  lines.append(f"🟡 {', '.join(y)}")
    if rd: lines.append(f"🔴 {', '.join(rd)}")
    if p:  lines.append(f"🟣 {', '.join(p)}")
    return "\n".join(lines)

def _colors_only_summary() -> str:
    return "🟢 \n🟡 \n🔴 \n🟣 "

# ============================== MLB: injuries & trades =====================

def get_mlb_injured_players() -> set:
    try:
        r = SESSION.get(CBSSPORTS_MLB_INJURIES_URL, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"[injuries-mlb] fetch error: {e}")
        return set()
    soup = BeautifulSoup(r.text, "html.parser")
    injured = set()
    for section in soup.find_all("div", class_="TableBase-shadows"):
        table = section.find("table")
        if not table: continue
        for tr in table.find_all("tr")[1:]:
            cols = tr.find_all("td")
            if not cols: continue
            a = cols[0].find_all("a")
            if len(a) >= 2: name = a[1].get_text(strip=True)
            elif a:        name = a[0].get_text(strip=True)
            else:          name = cols[0].get_text(strip=True)
            injured.add(clean_name(name))
    injured.discard("")
    return injured

def _parse_mlb_tx_date(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    for fmt in ("%B %d, %Y",):
        try:
            dt = datetime.strptime(s, fmt)
            return datetime(dt.year, dt.month, dt.day)
        except Exception:
            pass
    return None

def fetch_trades_past_week() -> Dict[str, str]:
    start_dt, end_dt = week_window()
    trades: Dict[str, str] = {}
    try:
        r = SESSION.get(MLB_TRANSACTIONS_URL, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"[trades-web] fetch error: {e}")
        return trades
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table: return trades
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3: continue
        date_txt = tds[0].get_text(strip=True)
        desc     = tds[2].get_text(strip=True)
        tx_date = _parse_mlb_tx_date(date_txt)
        if not tx_date or tx_date < start_dt or tx_date > end_dt:
            continue
        if "traded" not in desc.lower():
            continue
        m = re.search(r"traded\s+(?:[A-Z/]{1,4}\s+)?([A-Za-z' .-]+?)\s+to\s+([A-Za-z' .-]+)", desc, re.I)
        if not m: continue
        player = clean_name(m.group(1))
        dest_name = m.group(2)
        team_abbr = None
        for abbr, name in TEAM_NAME_MAPS["MLB"].items():
            if dest_name.lower() in (name.lower(), abbr.lower()) or name.lower() in dest_name.lower():
                team_abbr = abbr; break
        if player and team_abbr:
            trades[player] = team_abbr.upper()
    return trades

def _get_team_from_row(rec: Dict[str, str]) -> str:
    if "TEAM" in rec and rec["TEAM"]: return rec["TEAM"].strip().upper()
    if "TM"   in rec and rec["TM"]:   return rec["TM"].strip().upper()
    for k in rec.keys():
        ku = k.strip().upper()
        if ku in {"TEAM ABBR","TEAM(S)"} and rec[k]:
            return rec[k].strip().upper()
    return ""

def filter_traded_banned_and_teams(
    data: List[Dict[str, str]],
    teams: Optional[List[str]],
    traded_players: Optional[Dict[str, str]] = None,
    banned_players: Optional[set] = None
) -> List[Dict[str, str]]:
    teams_set = set(t.upper() for t in (teams or []))
    traded_clean = {clean_name(k): v.upper() for k, v in (traded_players or {}).items()}
    banned_clean = {clean_name(n) for n in (banned_players or set())}
    out = []
    for rec in data:
        name_col  = "NAME" if "NAME" in rec else "PLAYER" if "PLAYER" in rec else list(rec.keys())[0]
        nm        = clean_name(rec.get(name_col, ""))
        team_code = _get_team_from_row(rec)
        if nm in banned_clean: continue
        mapped = traded_clean.get(nm)
        if mapped and team_code and team_code != mapped: continue
        if teams_set and team_code and team_code not in teams_set: continue
        out.append(rec)
    return out

# ============================== Notion helpers =============================

def notion_append_blocks(blocks: List[Dict]):
    for attempt in range(3):
        try:
            client.blocks.children.append(block_id=POLL_PAGE_ID, children=blocks); return
        except Exception as e:
            msg = str(e)
            if "Rate limited" in msg or "429" in msg:
                time.sleep(2 ** attempt); continue
            print(f"[notion] append error: {e}"); return

def notion_update_page(page_id: str, props: Dict):
    for attempt in range(3):
        try:
            client.pages.update(page_id=page_id, properties=props); return
        except Exception as e:
            msg = str(e)
            if "Rate limited" in msg or "429" in msg:
                time.sleep(2 ** attempt); continue
            print(f"[notion] update error: {e}"); return

def _link_para(text: str, url: str) -> Dict:
    return {
        "object":"block","type":"paragraph",
        "paragraph":{"rich_text":[{"type":"text","text":{"content":text,"link":{"url":url}}}]}
    }

def _post_colors_only_block(page_id: str, heading_text: str):
    summary = _colors_only_summary()
    notion_append_blocks([
        {"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":heading_text}}]}},
        {"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":summary}}]}},
        {"object":"block","type":"divider","divider":{}}
    ])
    notion_update_page(page_id, {"Processed":{"select":{"name":"Yes"}}})

# ============================== Core flows =================================

def process_games():
    rows = fetch_unprocessed_rows(DATABASE_ID)
    rows.reverse()
    for r in rows:
        sport, stat, teams = r["sport"], r["stat"], r["teams"]
        sport_up = (sport or "").strip().upper()
        stat_up  = (stat  or "").strip().upper()

        # Colors-only for strikeouts (any sport)
        if stat_up in {"K","SO","STRIKEOUTS"}:
            _post_colors_only_block(r["page_id"], f"Game: {', '.join(teams)} — {stat} leaders")
            print(f"✅ Updated {teams} — {stat} (colors only for K)")
            continue

        traded_players = {}
        if sport_up == "MLB":
            traded_players = fetch_trades_past_week()

        data, used_season_mode, season_year, debug_links = scrape_statmuse_data(stat, sport, teams)

        # Post-filters
        if sport_up == "MLB":
            data = filter_traded_banned_and_teams(data, teams, traded_players=traded_players, banned_players=BANNED_PLAYERS)
            injured = get_mlb_injured_players()
            if data:
                name_col = "NAME" if "NAME" in data[0] else "PLAYER"
                data = [rec for rec in data if clean_name(rec.get(name_col, "")) not in injured]
        elif sport_up == "NFL" and teams:
            # NFL is league-wide scrape; restrict to requested teams if provided
            data = filter_traded_banned_and_teams(data, teams)
        elif sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
            # CFB season-mode: DO NOT filter by team after
            pass
        else:
            if data and teams:
                data = filter_traded_banned_and_teams(data, teams)

        # Summarize
        if not data:
            if sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
                summary = "(no qualifying leaders found)"
            else:
                summary = _format_buckets_default([], [], [], [])
        else:
            desired_key = "TD" if (sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"} and stat_up in {"TD","TDS","TOUCHDOWNS"}) else stat
            stat_key = pick_stat_key(data, desired_key)
            g, y, rd, p = bucket_top12(data, stat_key)
            if sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
                summary = _format_buckets_cfb(g, y, rd, p) or "(no qualifying leaders found)"
            else:
                summary = _format_buckets_default(g, y, rd, p)

        # Heading suffix
        if sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"} and used_season_mode:
            suffix = f"{season_year}"
        elif sport_up == "NFL":
            if NFL_QUERY_MODE == "SEASON":
                suffix = f"{NFL_SEASON_YEAR}"
            elif NFL_QUERY_MODE == "DATES" and NFL_START_DATE and NFL_END_DATE:
                suffix = f"{NFL_START_DATE} → {NFL_END_DATE}"
            else:
                suffix = f"last {NFL_LAST_N_DAYS} days"
        else:
            suffix = "past week"

        heading = f"Game: {', '.join(teams)} — {stat} leaders ({suffix})"
        blocks = [
            {"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":heading}}]}},
            {"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":summary}}]}},
        ]

        # For CFB, always add per-team query links (helps debug one-sided returns)
        if sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"} and debug_links:
            for i, u in enumerate(debug_links, 1):
                blocks.append(_link_para(f"View query {i}", u))

        blocks.append({"object":"block","type":"divider","divider":{}})
        notion_append_blocks(blocks)
        notion_update_page(r["page_id"], {"Processed":{"select":{"name":"Yes"}}})
        print(f"✅ Updated {teams} — {sport}/{stat} ({suffix})")

def process_psp_rows():
    resp = client.databases.query(
        database_id=PSP_DATABASE_ID,
        filter={"property":"Processed","select":{"equals":"no"}},
        sort=[{"property":"Order","direction":"ascending"}]
    )
    rows = resp.get("results", [])
    rows.reverse()
    for r in rows:
        pid   = r["id"]
        props = r["properties"]
        sport = props["Sport"]["select"]["name"].strip()
        st = props["Stat"]
        if st["type"] == "select":
            stat = st["select"]["name"].strip()
        else:
            stat = "".join(t["plain_text"] for t in st.get("rich_text", [])).strip()
        tp = props.get("Teams", {})
        if tp.get("type") == "multi_select":
            teams = [o["name"] for o in tp["multi_select"]] or None
        elif tp.get("type") in ("rich_text","title"):
            raw = "".join(t["plain_text"] for t in tp.get(tp["type"], []))
            teams = [x.strip() for x in raw.split(",") if x.strip()]
        else:
            teams = None

        sport_up = sport.upper()
        stat_up  = stat.upper()

        # Strikeouts -> colors-only
        if stat_up in {"K","SO","STRIKEOUTS"}:
            _post_colors_only_block(pid, f"{sport_up} PSP - {stat_up}")
            print(f"✅ PSP updated for {sport}/{stat} (colors only for K)")
            continue

        data, used_season_mode, season_year, debug_links = scrape_statmuse_data(stat, sport, teams)

        if sport_up == "MLB":
            injured = get_mlb_injured_players()
            if data:
                name_col = "NAME" if "NAME" in data[0] else "PLAYER"
                data = [rec for rec in data if clean_name(rec.get(name_col, "")) not in injured]
        elif sport_up == "NFL" and teams:
            data = filter_traded_banned_and_teams(data, teams)
        # CFB: no post-filter

        if not data:
            if sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
                summary = "(no qualifying leaders found)"
            else:
                summary = _format_buckets_default([], [], [], [])
        else:
            desired_key = "TD" if (sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"} and stat_up in {"TD","TDS","TOUCHDOWNS"}) else stat_up
            stat_key = pick_stat_key(data, desired_key)
            g, y, rd, p = bucket_top12(data, stat_key)
            if sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
                summary = _format_buckets_cfb(g, y, rd, p) or "(no qualifying leaders found)"
            else:
                summary = _format_buckets_default(g, y, rd, p)

        if sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"} and used_season_mode:
            suffix = f"{season_year}"
        elif sport_up == "NFL":
            if NFL_QUERY_MODE == "SEASON":
                suffix = f"{NFL_SEASON_YEAR}"
            elif NFL_QUERY_MODE == "DATES" and NFL_START_DATE and NFL_END_DATE:
                suffix = f"{NFL_START_DATE} → {NFL_END_DATE}"
            else:
                suffix = f"last {NFL_LAST_N_DAYS} days"
        else:
            suffix = "past week"

        heading = f"{sport_up} PSP - {stat_up} leaders ({suffix})"
        blocks = [
            {"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":heading}}]}},
            {"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":summary}}]}},
        ]
        if sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"} and debug_links:
            for i, u in enumerate(debug_links, 1):
                blocks.append(_link_para(f"View query {i}", u))
        blocks.append({"object":"block","type":"divider","divider":{}})

        notion_append_blocks(blocks)
        notion_update_page(pid, {"Processed":{"select":{"name":"Yes"}}})
        print(f"✅ PSP updated for {sport}/{stat} ({suffix})")

# ============================== Main =======================================

def process_all():
    process_games()
    process_psp_rows()

if __name__ == "__main__":
    process_all()