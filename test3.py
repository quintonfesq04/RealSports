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
from typing import Iterable 

# ---- DK fast HTTP (no retries) ----
import time as _time

# ---- User-Agent & DK fast HTTP (no retries) ----
DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

import time as _time
_RAW = requests.Session()               # no retry adapter, fails fast
_RAW.headers.update({
    "User-Agent": DEFAULT_UA,
    "Accept": "application/json",
    "Referer": "https://sportsbook.draftkings.com/"
})

def _fast_get(url: str, t_conn: float, t_read: float):
    return _RAW.get(url, timeout=(t_conn, t_read))

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": DEFAULT_UA,
        "Accept": "application/json",
        "Referer": "https://sportsbook.draftkings.com/"
    })

_PAGE_CACHE: Dict[str, str] = {}
TRY_PSP_STATMUSE = True  # PSP rows are just rollups; skip StatMuse scraping noise unless you set True
BLANK_NHL_PICKS = int(os.getenv("BLANK_NHL_PICKS", "1"))  # 1 = print colors only for ALL NHL stats
# How many CFB teams to combine per StatMuse query before chunking
MAX_CFB_COMBINED_TEAMS = 8

# ============================== ENV / Notion ===============================

NOTION_TOKEN_FALLBACK = "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"

DK_SITE = os.getenv("DK_SITE", "US-OH-SB")

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

if os.getenv("VERBOSE", "0") == "1":
    print("[env] DK_SITE =", os.getenv("DK_SITE"))
    print("[env] USE_DK_FOR_NBA =", os.getenv("USE_DK_FOR_NBA"))
    print("[env] DK_EVENT_GROUP_IDS_NBA =", os.getenv("DK_EVENT_GROUP_IDS_NBA"))

NOTION_TOKEN = _load_env_token()
client = Client(auth=NOTION_TOKEN)

# Verbose logging: set VERBOSE=1 in env to see mapping warnings, retries, etc.
VERBOSE = int(os.getenv("VERBOSE", "0"))

DATABASE_ID     = os.getenv("DATABASE_ID",  "1aa71b1c-663e-8035-bc89-fb1e84a2d919")
PSP_DATABASE_ID = os.getenv("PSP_DATABASE_ID","1ac71b1c663e808e9110eee23057de0e")
POLL_PAGE_ID    = os.getenv("POLL_PAGE_ID", "18e71b1c663e80cdb8a0fe5e8aeee5a9")

BASE_URL                   = "https://www.statmuse.com"
CBSSPORTS_MLB_INJURIES_URL = "https://www.cbssports.com/mlb/injuries/"
MLB_TRANSACTIONS_URL       = "https://www.mlb.com/transactions"


# --- DraftKings HTTP knobs ---
# --- DraftKings runtime controls / fallbacks ---
DK_TIMEOUT = float(os.getenv("DK_TIMEOUT", "5.5"))  # per-request read timeout
DK_SITES_FALLBACK = [
    os.getenv("DK_SITE", "US-SB").strip() or "US-SB",
    "US-SB",       # regionless
    "US-NJ-SB",    # another common region
]
DK_FAIL_LIMIT = int(os.getenv("DK_FAIL_LIMIT", "3"))  # after N consecutive failures, disable DK
_DK_CONSEC_FAILS = 0

# --- DraftKings runtime controls / fallbacks ---
DK_TIMEOUT = float(os.getenv("DK_TIMEOUT", "1.8"))          # per-request READ timeout
DK_SITES_FALLBACK = [
    (os.getenv("DK_SITE", "US-SB").strip() or "US-SB"),
    "US-SB",         # regionless
    "US-NJ-SB",      # another common region
]
DK_FAIL_LIMIT = int(os.getenv("DK_FAIL_LIMIT", "1"))         # after N consecutive failures, disable DK
DK_TOTAL_BUDGET_SEC = float(os.getenv("DK_TOTAL_BUDGET_SEC", "4.0"))  # total budget for ALL DK work
_DK_CONSEC_FAILS = 0
_DK_BUDGET_START = None

def football_season_year(today: Optional[date] = None) -> int:
    d = today or date.today()
    return d.year if d.month >= 8 else d.year - 1

SHOW_CFB_DEBUG_LINKS = True

# --- DraftKings site + fallbacks (works even if your state site is missing an EG) ---
DK_SITE = os.getenv("DK_SITE", "US-SB").strip()
DK_SITE_FALLBACKS = [s.strip() for s in os.getenv("DK_SITE_FALLBACKS", "US-SB,US-OH-SB,US-NJ-SB").split(",") if s.strip()]
DK_LANG = os.getenv("DK_LANG", "en-us")
DK_TIMEOUT = float(os.getenv("DK_TIMEOUT", "8"))
DK_RETRIES = int(os.getenv("DK_RETRIES", "0"))

# ---- DK league switches (env) ----
USE_DK_FOR_NHL  = int(os.getenv("USE_DK_FOR_NHL",  "1"))
USE_DK_FOR_NFL  = int(os.getenv("USE_DK_FOR_NFL",  "1"))
USE_DK_FOR_NBA  = int(os.getenv("USE_DK_FOR_NBA",  "1"))
USE_DK_FOR_MLB  = int(os.getenv("USE_DK_FOR_MLB",  "1"))
USE_DK_FOR_WNBA = int(os.getenv("USE_DK_FOR_WNBA", "1"))
# Optional single-ID env hints (default to 0 == disabled)
DK_EVENT_GROUP_ID_NFL = int(os.getenv("DK_EVENT_GROUP_ID_NFL", "0") or "0")
DK_EVENT_GROUP_ID_NHL = int(os.getenv("DK_EVENT_GROUP_ID_NHL", "0") or "0")
DK_EVENT_GROUP_ID_MLB = int(os.getenv("DK_EVENT_GROUP_ID_MLB", "0") or "0")

# IMPORTANT: don't pre-empt DK by printing blank NHL picks first.
# Default this to 0; we'll only print blanks if both DK and StatMuse fail.
BLANK_NHL_PICKS = int(os.getenv("BLANK_NHL_PICKS", "0"))

# StatMuse is only used for the sports listed here.
# Default now includes MLB so TB/RBI/etc. don’t get blocked.
STATMUSE_FALLBACK_SPORTS = {
    s.strip().upper()
    for s in os.getenv("STATMUSE_FALLBACK_SPORTS", "CFB,MLB").split(",")
    if s.strip()
}

def _parse_ids_csv(s: str) -> List[int]:
    return [int(x) for x in re.findall(r"\d+", s or "")]

# Prefer a CSV list; fall back to single ID if you already set DK_EVENT_GROUP_ID_NBA
DK_EVENT_GROUP_IDS_NBA = _parse_ids_csv(os.getenv("DK_EVENT_GROUP_IDS_NBA", "")) or \
                         ([int(os.getenv("DK_EVENT_GROUP_ID_NBA", "0"))] if os.getenv("DK_EVENT_GROUP_ID_NBA") else [])

DK_SITE = os.getenv("DK_SITE", "US-SB")
DK_LANG = os.getenv("DK_LANG", "en-us")


# ============================== NFL controls ===============================

NFL_QUERY_MODE   = os.getenv("NFL_QUERY_MODE", "SEASON").upper()   # "SEASON" | "DATES" | "LAST_N_DAYS"
NFL_SEASON_YEAR = int(os.getenv("NFL_SEASON_YEAR") or football_season_year())
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
            total=DK_RETRIES,
            connect=DK_RETRIES,
            read=DK_RETRIES,
            backoff_factor=0.7,  # 0.7, 1.4, 2.1...
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

SESSION = make_session()

# ============================== Time helpers ===============================

def week_window(today: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    today = today or datetime.today()
    start = today - timedelta(days=14)
    start = datetime(start.year, start.month, start.day)
    end   = datetime(today.year, today.month, today.day)
    return start, end

def _nice_date(d: datetime) -> str:
    return d.strftime("%B %-d, %Y") if os.name != "nt" else d.strftime("%B %#d, %Y")

CFB_SEASON_YEAR = int(os.getenv("CFB_SEASON_YEAR") or football_season_year())

# ============================== Notion input ===============================

def fetch_unprocessed_rows(db_id: str) -> List[Dict]:
    results: List[Dict] = []
    cursor = None
    while True:
        kwargs = {
            "database_id": db_id,
            "filter": {"property": "Processed", "select": {"equals": "no"}},
            "sorts": [{"property": "Order", "direction": "ascending"}],
            "page_size": 100,
        }
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = client.databases.query(**kwargs)

        for r in resp.get("results", []):
            pid   = r["id"]
            props = r["properties"]
            sport = props["Sport"]["select"]["name"]
            st    = props["Stat"]
            if st.get("type") == "select":
                stat = st["select"]["name"].strip()
            else:
                stat = "".join(t["plain_text"] for t in st.get("rich_text", [])).strip()

            # capture Order (number or text)
            order_prop = props.get("Order", {})
            if order_prop.get("type") == "number":
                order_val = order_prop["number"]
            elif order_prop.get("type") == "rich_text":
                order_val = "".join(t["plain_text"] for t in order_prop["rich_text"]).strip()
            else:
                order_val = None

            if props.get("Teams", {}).get("type") == "multi_select":
                teams = [t["name"] for t in props["Teams"]["multi_select"]]
            else:
                t1 = props.get("Team 1", {}).get("title", [])
                t2 = props.get("Team 2", {}).get("rich_text", [])
                t1txt = t1[0]["plain_text"] if t1 else ""
                t2txt = t2[0]["plain_text"] if t2 else ""
                teams = [x.strip() for x in (t1txt + "," + t2txt).split(",") if x.strip()]

            results.append({"page_id": pid, "sport": sport, "stat": stat, "teams": teams, "order": order_val})

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return results

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
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-background-networking")
        opts.add_argument("--dns-prefetch-disable")
        opts.add_argument("--blink-settings=imagesEnabled=false")
        # Block images/fonts/css for speed
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.stylesheets": 2,
            "profile.managed_default_content_settings.fonts": 2,
        }
        opts.add_experimental_option("prefs", prefs)
        drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
        drv.set_page_load_timeout(15)
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

_PAGES_FETCHED = 0
_RECYCLE_EVERY = 60

def fetch_html(url: str, wait_css: str = "table", wait_seconds: int = 25) -> str:
    """
    Fast path: try requests (server-rendered table).
    Fallback: Selenium only if no <table> found or non-200.
    Caches by URL to avoid duplicate hits in the same run.
    """
    if url in _PAGE_CACHE:
        return _PAGE_CACHE[url]

    # --- Fast path: requests ---
    try:
        r = SESSION.get(url, timeout=10)
        r.encoding = "utf-8"
        if r.ok and ("<table" in r.text or "<TABLE" in r.text):
            _PAGE_CACHE[url] = r.text
            return r.text
    except Exception:
        pass  # fall through to Selenium

    # --- Slow path: Selenium (as last resort) ---
    backoffs = [0.0, 1.0, 2.0]
    last_err = None
    for delay in backoffs:
        if delay:
            time.sleep(delay)
        drv = _DriverPool.get()
        try:
            drv.get(url)
            try:
                WebDriverWait(drv, min(wait_seconds, 6)).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                )
            except TimeoutException:
                pass
            html = drv.page_source or ""
            _PAGE_CACHE[url] = html
            return html
        except WebDriverException as e:
            last_err = e
            _DriverPool.close()
    if VERBOSE and last_err:
        print(f"[webdriver] error for {url}: {last_err}")
    return ""

from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_many(urls: List[str]) -> Dict[str, str]:
    out = {}
    urls = [u for u in urls if u not in _PAGE_CACHE]
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(SESSION.get, u, timeout=10): u for u in urls}
        for fut in as_completed(futs):
            u = futs[fut]
            try:
                r = fut.result()
                if r.ok:
                    r.encoding = "utf-8"
                    _PAGE_CACHE[u] = r.text
                    out[u] = r.text
            except Exception:
                _PAGE_CACHE[u] = ""  # cache failures to avoid retry storms
    return out

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
        "NCSU": "NC State", "WAKE": "Wake Forest", "INST": "Indiana State", "UCF": "Central Florida",
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
        "SF": "49ers", "NO": "Saints", "BUF": "Bills", "NYJ": "Jets",
        "SEA": "Seahawks", "PIT": "Steelers", "LAR": "Rams", "TEN": "Titans",
        "CAR": "Panthers", "ARI": "Cardinals", "DEN": "Broncos", "IND": "Colts",
        "PHI": "Eagles", "KC": "Chiefs", "ATL": "Falcons", "MIN": "Vikings",
        "TB": "Buccaneers", "HOU": "Texans", "LAC": "Chargers", "LV": "Raiders",
    }
}

# Build bidirectional maps so we accept either "NYY" or "New York Yankees"
TEAM_NAME_MAPS_BIDI: Dict[str, Dict[str, str]] = {}
for sport_key, abbr2name in TEAM_NAME_MAPS.items():
    abbr2name_u = {k.upper(): v for k, v in abbr2name.items()}
    name2name_u = {v.upper(): v for v in abbr2name.values()}
    TEAM_NAME_MAPS_BIDI[sport_key.upper()] = {**abbr2name_u, **name2name_u}

# --- CFB helpers: preferred abbreviations & input normalization ---

def _cfb_name_to_abbr() -> Dict[str, str]:
    """
    From TEAM_NAME_MAPS["CFB"], build a mapping:
       canonical team name (upper) -> preferred abbr (e.g., "INDIANA" -> "IND")
    """
    by_name = {}
    for abbr, name in TEAM_NAME_MAPS.get("CFB", {}).items():
        by_name[name.upper()] = abbr.upper()
    return by_name

CFB_NAME_TO_ABBR = _cfb_name_to_abbr()

def cfb_preferred_abbr_from_input(t: str) -> str:
    """
    Given something like 'IU' or 'Indiana', return our preferred code (e.g., 'IND').
    Falls back to the original string if no mapping.
    """
    key = _k(t)
    # Try alias -> canonical full name
    canonical = CFB_ALIASES.get(key)
    if canonical:
        abbr = CFB_NAME_TO_ABBR.get(canonical.upper())
        if abbr:
            return abbr
    # Try if user already gave the canonical full name
    abbr = CFB_NAME_TO_ABBR.get(t.strip().upper())
    return abbr or t.strip()

def _k(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", s).upper()

CFB_ALIASES: Dict[str, str] = {}
def _add_cfb(canonical: str, *aliases: str):
    CFB_ALIASES[_k(canonical)] = canonical
    for a in aliases:
        CFB_ALIASES[_k(a)] = canonical

# (examples; keep expanding)
_add_cfb("Louisville Cardinals", "Louisville", "UL", "LOU")
_add_cfb("James Madison Dukes", "James Madison", "JMU")
_add_cfb("Northern Illinois Huskies", "Northern Illinois", "NIU")
_add_cfb("Maryland Terrapins", "Maryland", "MD")
_add_cfb("Northwestern Wildcats", "Northwestern", "NU")
_add_cfb("Wisconsin Badgers", "Wisconsin", "WISC", "UWisc", "UW")
_add_cfb("Western Illinois Leathernecks", "Western Illinois", "WIU")

def map_cfb_teams_for_statmuse(teams: List[str]) -> Tuple[List[str], List[str]]:
    """
    Map various team inputs (abbr like 'USF', short names like 'UL', or full names)
    to StatMuse-friendly canonical full names (e.g., 'South Florida', 'Louisville Cardinals').
    Returns (mapped_full_names, unmapped_originals).
    """
    mapped: List[str] = []
    unmapped: List[str] = []

    abbr2full = {abbr.upper(): name for abbr, name in TEAM_NAME_MAPS.get("CFB", {}).items()}

    for t in (teams or []):
        raw = (t or "").strip()
        if not raw:
            continue

        key_abbr = raw.upper()
        key_norm = _k(raw)  # letters+digits only, upper

        out_name: Optional[str] = None

        # 1) Exact abbreviation in our official map (primary path for Notion inputs)
        if key_abbr in abbr2full:
            out_name = abbr2full[key_abbr]

        # 2) Alias → canonical full name (covers things like "UL", "Louisville")
        if not out_name:
            alias_name = CFB_ALIASES.get(key_norm)
            if alias_name:
                out_name = alias_name

        # 3) Try a light normalization (strip “University/Univ/The”) then alias again
        if not out_name:
            stripped = re.sub(r"\b(UNIVERSITY|UNIV|THE)\b", "", raw, flags=re.I).strip()
            alias_name = CFB_ALIASES.get(_k(stripped))
            if alias_name:
                out_name = alias_name

        # 4) As a last resort, if they already typed the canonical full name, keep it
        if not out_name and raw.upper() in CFB_NAME_TO_ABBR:
            out_name = raw  # already a full name we recognize

        # 5) If still unknown, pass through but flag it
        if not out_name:
            unmapped.append(raw)
            out_name = raw

        mapped.append(out_name)

    # Keep order but remove dupes
    seen = set()
    deduped = []
    for n in mapped:
        if n and n.upper() not in seen:
            seen.add(n.upper())
            deduped.append(n)

    return deduped, unmapped

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
        sport_up = (sport or "").strip().upper()
        if sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
            mapped, unmapped = map_cfb_teams_for_statmuse(teams)
        else:
            # bi-directional map: accepts “NYY” or “New York Yankees”
            m = TEAM_NAME_MAPS_BIDI.get(sport_up, {})
            mapped, unmapped = [], []
            for t in teams:
                key = t.strip().upper()
                name = m.get(key)
                if name:
                    mapped.append(name)
                else:
                    mapped.append(t.strip())
                    if VERBOSE and key.isalpha() and len(key) <= 4:
                        unmapped.append(t)
        if VERBOSE and unmapped:
            print(f"[warn] Team(s) not mapped for {sport_up}: {', '.join(unmapped)}")
        base += f" {','.join(mapped)}"

    return f"{BASE_URL}/ask?q={_quote(base)}"

def scrape_statmuse_data(stat: str, sport: str, teams=None) -> Tuple[List[Dict[str, str]], bool, int, List[str]]:
    """
    Return (table_rows, used_season_mode, season_year, debug_links)
    - CFB: season-mode (per-team) for ALL stats (TD uses special phrasing)
    - NFL: SEASON/DATES/LAST_N_DAYS (league-wide; filter after)
    - Others: weekly-mode with teams
    """
    sport_up = (sport or "").strip().upper()
    stat_up  = (stat  or "").strip().upper()
    debug_links: List[str] = []

    # ----- CFB: season-mode; prefer COMBINED team queries (TeamA, TeamB) -----
    if sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
        season = CFB_SEASON_YEAR
        all_rows: List[Dict[str, str]] = []
        debug_links: List[str] = []

        teams = teams or []
        # Map inputs to StatMuse-friendly full names (don’t filter by team later!)
        mapped_full, unmapped = map_cfb_teams_for_statmuse(teams)
        if unmapped and VERBOSE:
            print(f"[warn] Team(s) not mapped for CFB: {', '.join(unmapped)}")

        # Label wording: TDs need the special phrase
        def _stat_label() -> str:
            return "total touchdowns" if stat_up in {"TD","TDS","TOUCHDOWNS","TOTAL TOUCHDOWNS"} else stat

        label = _stat_label()

        # Deduplicate team names while preserving order
        names_full: List[str] = []
        seen = set()
        for n in mapped_full:
            if n and n not in seen:
                seen.add(n)
                names_full.append(n)

        # Wherever you build/loop queries:
        seen_urls = set()
        def _ask_and_merge(q: str):
            url = build_query_url(q, None, "CFB", include_dates=False)
            if url in seen_urls:  # <— NEW
                return
            seen_urls.add(url)   # <— NEW
            debug_links.append(url)
            html = fetch_html(url)
            rows = parse_table(html)
            if rows:
                all_rows.extend(rows)

        if len(names_full) >= 2:
            # PSP / multi-team: combine ALL teams, chunked, and MERGE their rows
            def _chunks(lst, size):
                for i in range(0, len(lst), size):
                    yield lst[i:i+size]

            for chunk in _chunks(names_full, MAX_CFB_COMBINED_TEAMS):
                combined = ", ".join(chunk)
                q = f"{label} leaders cfb {season} {combined}"
                _ask_and_merge(q)

            # Fallbacks only if absolutely nothing came back
            if not all_rows:
                for t in names_full:
                    _ask_and_merge(f"{label} leaders cfb {season} for {t}")

            # Last resort league-wide
            if not all_rows:
                _ask_and_merge(f"{label} leaders cfb {season}")

        else:
            # 0–1 team: league-wide → per-team fallback
            _ask_and_merge(f"{label} leaders cfb {season}")
            if not all_rows and names_full:
                _ask_and_merge(f"{label} leaders cfb {season} for {names_full[0]}")

        if not all_rows and VERBOSE:
            print("[parse] no CFB rows for:", debug_links)

        # IMPORTANT: Do NOT post-filter CFB by team later; we want the union of results.
        return all_rows, True, season, debug_links

    # ----- NFL: separate timeframe controls; league-wide -----
    if _is_nfl(sport_up):
        if NFL_QUERY_MODE == "SEASON":
            q   = f"{stat} leaders nfl this season"
            url = build_query_url(q, teams, "NFL", include_dates=False)
        elif NFL_QUERY_MODE == "DATES" and NFL_START_DATE and NFL_END_DATE:
            q   = f"{stat} leaders nfl from {NFL_START_DATE} to {NFL_END_DATE}"
            url = build_query_url(q, teams, "NFL", include_dates=False)
        else:
            end = datetime.today()
            start = end - timedelta(days=max(1, NFL_LAST_N_DAYS - 1))
            q   = f"{stat} leaders nfl"
            url = build_query_url(q, teams, "NFL", start, end, include_dates=True)

        debug_links.append(url)
        html = fetch_html(url)
        rows = parse_table(html)
        
        # Optional: if "this season" ever fails, try explicit year as a fallback
        if not rows and NFL_QUERY_MODE == "SEASON":
            q2  = f"{stat} leaders nfl {NFL_SEASON_YEAR}"
            url2 = build_query_url(q2, teams, "NFL", include_dates=False)
            debug_links.append(url2)
            rows = parse_table(fetch_html(url2))

        used_season = (NFL_QUERY_MODE == "SEASON")
        return rows, used_season, (NFL_SEASON_YEAR if used_season else 0), debug_links

    # ----- Others: weekly-mode with teams -----
    start, end = week_window()
    q   = f"{stat} leaders {sport.lower()}"
    url = build_query_url(q, teams, sport, start, end, include_dates=True)
    debug_links.append(url)
    html = fetch_html(url)
    rows = parse_table(html)
    if not rows and VERBOSE:
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
    "CJDaniels": "CJ Daniels",
    "Char Mar Brown": "CharMar Brown",
    "KCConcepcion": "KC Concepcion",
    "Robert Henry Jr": "Robert Henry",
    "Junior Vandeross IIIJ III": "Junior Vandeross III",
    "Dea Monte Trayanum": "Chip Trayanum",
    "Emmanuel Mc Neil-Warren": "Emmanuel McNeil-Warren",
    "James Mc Cann": "James McCann",
    "Robert Hassell IIIR III": "Robert Hassell III",
    "CJKayfus": "CJ Kayfus",
    "Kayla Mc Bride": "Kayla McBride",
    "James Cook IIIJ III": "James Cook",
    "CJAbrams": "CJ Abrams",
    "Calvin Johnson IIC II": "Calvin Johnson II",
    "KJDuff": "KJ Duff",
    "DTSheffield": "DT Sheffield",
    "CJCampbell Jr": "CJ Campbell Jr",
    "Nathan Mc Neil": "Nathan McNeil",
    "Johncarlos Miller IIJ III": "Johncarlos Miller II",
    "JJBuchanan": "JJ Buchanan",
    "Na Quari Rogers": "NaQuari Rogers",
    "CJBrown": "CJ Brown",
    "KJJackson": "KJ Jackson",
    "Vinny Anthony IIV II": "Vinny Anthony II",
    "De Juan Williams": "DeJuan Williams",
    "RJMaryland": "RJ Maryland",
    "DJRogers": "DJ Rogers",
    "Joseph Manjack IVJ IV": "Joseph Manjack IV",
    "Eric Mc Alister": "Eric McAlister",
    "TJHarden": "TJ Harden",
    "RJGarcia II": "RJ Garcia II",
    "De Angelo Irvin Jr": "DeAngelo Irvin Jr",
    "Tre Williams IIIT III": "Tre Williams III",
    "Makenzie Mc Gill IIM II": "Makenzie McGill II",
    "Chris Brazzell IIC II": "Chris Brazzell II",
    "Evan Mc Cray": "Evan McCray",
    "Tommy Winton IIIT III": "Tommy Winton III",
    "De Sean Bishop": "DeSean Bishop",
    "De Corion Temple": "DeCorion Temple",
    "Tommy Mc Intosh": "Tommy McIntosh",
    "Nathan Van Timmeren": "Nathan VanTimmeren",
    "Jeremiah Mc Clellan": "Jeremiah McClellan",
    "Dru De Shields": "Dru DeShields",
    "Isaiah Sategna IIII III": "Isaiah Sategna III",
    "Harrison Wallace IIIH III": "Harrison Wallace III",
    "Donaven Mc Culley": "Donaven McCulley",
    "TJLateef": "TJ Lateef",
    "Michael Jackson IIIM III": "Michael Jackson III",
    "EJHorton Jr": "EJ Horton Jr",
    "CJCarr": "CJ Carr",
    "DJBlack": "DJ Black",
    "MJFlowers": "MJ Flowers",
    "Alonza Barnett IIIA III": "Alonza Barnett III",
    "JCEvans": "JC Evans",
    "Dontae Mc Millan": "Dontae McMillan",
    "De Kalon Taylor": "DeKalon Taylor",
    "Eric Holley IIIE III": "Eric Holley III",
    "CCEzirim": "CC Ezirim",
    "Rodney Harris IIR II": "Rodney Harris II",
    "CJBailey": "CJ Bailey",
    "Anthony Evans IIIA III": "Anthony Evans III",
    "De Aree Rogers": "DeAree Rogers",
    "Floyd Chalk IVF IV": "Floyd Chalk IV",
    "De Shawn Hanika": "DeShawn Hanika",
    "Rodney Gallagher IIIR III": "Rodney Gallagher III",
    "Kyle Mc Neal": "Kyle McNeal",
    "Dexter Williams IID II": "Dexter Williams II",
    "Oscar Adaway IIIO III": "Oscar Adaway III",
    "Reggie Branch IIR II": "Reggie Branch II",
    "Jaden Mc Gill" : "Jaden McGill",
    "George Hart IIIG III": "George Hart III",
    "Maverick Mc Ivore": "Maverick McIvor",
    "Gage La Due": "Gage LaDue",
    "OJArnold": "OJ Arnold",
    "JCFrench IVJ IV": "JC French IV",
    "Miller Mc Crumby": "Miller McCrumby",
    "PJMartin": "PJ Martin",
    "Vernell Brown IIIV III": "Vernell Brown III",
    "Eugene Wilson IIIE III": "Eugene Wilson III",
    "Mario Sanders IIM II": "Mario Sanders II",
    "Decker De Graaf": "Decker DeGraaf",
    "CJWilliams": "CJ Williams",
    "Louis Brown IVL IV": "Louis Brown IV",
    "LJMartin": "LJ Martin",
    "TJJohnson": "TJ Johnson",
    "CJBaxter": "CJ Baxter",
    "De Andre Moore Jr": "DeAndre Moore Jr",
    "Daylan Mc Cutcheon": "Daylan McCutcheon",
    "Eric Willis IIIE III": "Eric Willis III",
    "Devin Mc Cuinn": "Devin McCuinn",
    "Will Henderson IIIW III": "Will Henderson III",
    "AJWilson": "AJ Wilson",
    "David Amador IID II": "David Amador II",
    "DJAllen Jr": "DJ Allen Jr",
    "Owen Mc Cown": "Owen McCown",
    "De Kalon Taylor": "DeKalon Taylor",
    "Jacob De Jesus": "Jacob DeJesus",
    "Qua Ron Adams": "QuaRon Adams",
    "LJJohnson Jr": "LJ Johnson Jr",
    "Lake Mc Reen": "Lake McRee",
    "Chrishon Mc Cray": "Chrishon McCray",
    "DJJordan": "DJ Jordan",
    "De Andre Hopkins": "DeAndre Hopkins",
    "Isaac Te Slaa": "Isaac TeSlaa",
    "Sam La Porta": "Sam LaPorta",
    "Crawford": "JP Crawford",
    "Kenneth Walker IIIK III": "Kenneth Walker III",
    "Trey Mc Bride": "Trey McBride",
    "AJBarner": "AJ Barner",
    "Na Lyssa Smith": "NaLyssa Smith",
    "Malik Mc Clain": "Malik McClain",
    "Luke Mc Gary": "Luke McGary",
    "Realmuto": "J.T. Realmuto",
    "PJJohnson III": "PJ Johnson III",
    "TJPride": "TJ Pride",
    "Christian Mc Caffrey": "Christian McCaffrey",
    "Dwayne Mc Dougle": "Dwayne McDougle",
    "Jai Mason": "E.Jai Mason",
    "Ju Smith-Schuster": "JuJu Smith-Schuster",
    "De Vonta Smith": "DeVonta Smith",
    "Richie Anderson IIIR III": "Richie Anderson III",
    "Arnold Barnes IIIA III": "Arnold Barnes III",

}

BANNED_PLAYERS = {
    "James Conner",
}

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
    # Baseball
    "K":  {"K", "SO", "STRIKEOUTS"},
    "HR": {"HR", "HOMERS", "HOME RUNS"},
    "RBI":{"RBI", "RBIS"},
    # Hoops
    "3P": {"3P", "3PM", "3-PT", "3PT", "THREE POINTERS"},
    "PTS":{"PTS", "POINTS"},
    "REB":{"REB", "REBOUNDS"},
    "AST":{"AST", "ASSISTS"},
    # Football generic
    "TD": {"TD", "TDS", "TOUCHDOWNS"},
    "YDS":{"YDS","YARDS"},
    # NFL/CFB specific labels that often appear
    "RECEPTIONS": {"RECEPTIONS", "REC"},
    "TOTAL TOUCHDOWNS": {"TOTAL TOUCHDOWNS", "TD", "TDS", "TOUCHDOWNS"},
    "TOTAL SCRIMMAGE YARDS": {"TOTAL SCRIMMAGE YARDS", "SCRIMMAGE YDS", "SCRIMMAGE YARDS", "YDS"},
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

    for h in numeric_candidates:
        if not rate_like.search(h):
            return h
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
    lines = []
    if g:  lines.append(f"🟢 {', '.join(g)}")
    if y:  lines.append(f"🟡 {', '.join(y)}")
    if rd: lines.append(f"🔴 {', '.join(rd)}")
    if p:  lines.append(f"🟣 {', '.join(p)}")
    return "\n".join(lines)

def _format_buckets_cfb(g, y, rd, p) -> str:
    lines = []
    if g:  lines.append(f"🟢 {', '.join(g)}")
    if y:  lines.append(f"🟡 {', '.join(y)}")
    if rd: lines.append(f"🔴 {', '.join(rd)}")
    if p:  lines.append(f"🟣 {', '.join(p)}")
    return "\n".join(lines)

def _colors_only_summary() -> str:
    return "🟢 \n🟡 \n🔴 \n🟣 "

def dedupe_by_player(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    if not rows:
        return rows
    name_col = "NAME" if "NAME" in rows[0] else "PLAYER" if "PLAYER" in rows[0] else list(rows[0].keys())[0]
    seen = set()
    out = []
    for rec in rows:
        nm = clean_name(rec.get(name_col, ""))
        if not nm:
            continue
        if nm in seen:
            continue
        seen.add(nm)
        out.append(rec)
    return out

# ============================== MLB: injuries & trades =====================

def get_mlb_injured_players() -> set:
    try:
        r = SESSION.get(CBSSPORTS_MLB_INJURIES_URL, timeout=15)
        r.encoding = "utf-8"  # <-- add this
        soup = BeautifulSoup(r.text, "html.parser") 
        r.raise_for_status()
    except Exception as e:
        if VERBOSE:
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
        r.encoding = "utf-8"  # <-- add this
        soup = BeautifulSoup(r.text, "html.parser")
        r.raise_for_status()
    except Exception as e:
        if VERBOSE:
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

def _normalize_abbr(code: str) -> str:
    """Lightweight alias normalizer for team codes used in StatMuse tables."""
    c = (code or "").upper()
    # Common alias/typo fixes:
    if c == "WSH":  # ESPN/Notion sometimes WSH; StatMuse tends to WAS
        return "WAS"
    if c == "NJJ":  # typo seen in mapping
        return "NYJ"
    return c

def filter_traded_banned_and_teams(
    data: List[Dict[str, str]],
    teams: Optional[List[str]],
    traded_players: Optional[Dict[str, str]] = None,
    banned_players: Optional[set] = None
) -> List[Dict[str, str]]:
    teams_set = set(_normalize_abbr(t) for t in (teams or []))  # <— normalize inputs
    traded_clean = {clean_name(k): _normalize_abbr(v) for k, v in (traded_players or {}).items()}
    banned_clean = {clean_name(n) for n in (banned_players or set())}
    out = []
    for rec in data:
        name_col  = "NAME" if "NAME" in rec else "PLAYER" if "PLAYER" in rec else list(rec.keys())[0]
        nm        = clean_name(rec.get(name_col, ""))
        team_code = _normalize_abbr(_get_team_from_row(rec))      # <— normalize table code
        if nm in banned_clean: 
            continue
        mapped = traded_clean.get(nm)
        if mapped and team_code and team_code != mapped: 
            continue
        if teams_set and team_code and team_code not in teams_set: 
            continue
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

# --- New helpers for grouping + Heading 3 ---

def _heading3(text: str) -> Dict:
    return {
        "object": "block",
        "type": "heading_3",
        "heading_3": {"rich_text": [{"type": "text", "text": {"content": text}}]}
    }

def _canon_game_key(sport: str, teams: List[str]) -> Tuple[str, Tuple[str, str]]:
    t = [t.strip() for t in (teams or []) if t and t.strip()]
    if len(t) == 1:
        t = [t[0], t[0]]
    if len(t) >= 2:
        pair = tuple(sorted((t[0], t[1]), key=lambda s: s.lower()))
    else:
        pair = ("", "")
    return (sport.strip().upper(), pair)

# --- DK: lean v5 category fetch (lighter + faster) ---

_DK_JSON_CACHE = {}  # (url)->json cache for this run

def _dk_get_json(url: str, t_conn: float = 1.2, t_read: float = DK_TIMEOUT) -> dict:
    try:
        r = _fast_get(url, t_conn=t_conn, t_read=t_read)
        if r.status_code == 404:
            return {}
        r.raise_for_status()
        r.encoding = "utf-8"
        return r.json() or {}
    except Exception:
        return {}

def _dk_fetch_eventgroup_v5(eg_id: int) -> dict:
    if not eg_id:
        return {}
    # Try a couple of regional sites quickly
    for site in _dk_sites_to_try():
        url = f"https://sportsbook.draftkings.com//sites/{site}/api/v5/eventgroups/{eg_id}/?format=json&language={DK_LANG}"
        if url in _DK_JSON_CACHE:
            js = _DK_JSON_CACHE[url]
        else:
            js = _dk_get_json(url, t_conn=1.0, t_read=DK_TIMEOUT)
            _DK_JSON_CACHE[url] = js
        if js:
            return js
    return {}

def _dk_fetch_category_v5(eg_id: int, cat_id: int) -> dict:
    for site in _dk_sites_to_try():
        url = f"https://sportsbook.draftkings.com//sites/{site}/api/v5/eventgroups/{eg_id}/categories/{cat_id}?format=json&language={DK_LANG}"
        if url in _DK_JSON_CACHE:
            js = _DK_JSON_CACHE[url]
        else:
            js = _dk_get_json(url, t_conn=1.0, t_read=DK_TIMEOUT)
            _DK_JSON_CACHE[url] = js
        if js:
            return js
    return {}

def _dk_fetch_subcategory_v5(eg_id: int, cat_id: int, subcat_id: int) -> dict:
    for site in _dk_sites_to_try():
        url = f"https://sportsbook.draftkings.com//sites/{site}/api/v5/eventgroups/{eg_id}/categories/{cat_id}/subcategories/{subcat_id}?format=json&language={DK_LANG}"
        if url in _DK_JSON_CACHE:
            js = _DK_JSON_CACHE[url]
        else:
            js = _dk_get_json(url, t_conn=1.0, t_read=DK_TIMEOUT)
            _DK_JSON_CACHE[url] = js
        if js:
            return js
    return {}

def _dk_wanted_market_names(sport: str, stat: str) -> List[str]:
    s  = _league_canonical(sport)
    st = (stat or "").strip().upper()

    # NBA & WNBA
    if s in {"NBA","WNBA"}:
        if st in {"3P","3PM","3-PT","3PT"} or "THREE" in st:
            return ["3-Point Field Goals", "3-Point Field Goals Made", "3PT Made", "3-Pointers Made"]
        if st in {"AST","APG"} or "ASSIST" in st:
            return ["Player Assists", "Assists"]
        if st in {"REB","RPG"} or "REBOUND" in st:
            return ["Player Rebounds", "Rebounds"]
        if st in {"PTS","PPG"} or "POINT" in st:
            return ["Player Points", "Points"]

    # NHL
    if s == "NHL":
        if "GOAL" in st:
            return ["Anytime Goal Scorer", "To Score a Goal"]
        if "SHOT" in st:
            return ["Player Shots On Goal"]
        if "POINT" in st:
            return ["Player Points"]

    # NFL
    if s == "NFL":
        if "RECEPTION" in st or st == "REC":
            return ["Receptions"]
        if "SCRIMMAGE" in st:
            return ["Rushing + Receiving Yards", "Total Rushing + Receiving Yards"]
        if "YARD" in st or st == "YDS":
            return ["Total Receiving Yards", "Total Rushing Yards", "Rushing + Receiving Yards"]
        if "TD" in st or "TOUCHDOWN" in st:
            return ["Anytime Touchdown Scorer", "Player Total Touchdowns"]

    # MLB
    if s == "MLB":
        if st in {"TB","TOTAL BASES"} or "BASES" in st:
            return ["Player Total Bases", "Total Bases"]
        if "RBI" in st:
            return ["Player RBI", "RBI"]
        if st in {"HR","HOMERS","HOME RUNS"} or "HOME RUN" in st:
            return ["To Hit a Home Run", "Player to Hit a Home Run"]
        if st in {"H","HITS"} or "HIT" in st:
            return ["Player Hits", "Hits"]

    return []

def _dk_market_name_matches(name: str, wanted: Iterable[str]) -> bool:
    n = (name or "").lower()
    return any(w.lower() in n for w in wanted)

def _dk_extract_event_props_v5(eg_json: dict, ev_id: int, sport: str, stat: str) -> List[dict]:
    """
    Pull only the subcategories we care about via /categories and /subcategories.
    """
    wanted = _dk_wanted_market_names(sport, stat)
    out: List[dict] = []

    eg = (eg_json.get("eventGroup") or {})
    cats = eg.get("offerCategories") or []
    # Find the parent "Player Props" (or similar) categories first
    for cat in cats:
        cat_id = cat.get("offerCategoryId") or cat.get("id")
        cat_name = (cat.get("name") or "").strip()
        if not cat_id:
            continue

        cat_json = _dk_fetch_category_v5(eg.get("eventGroupId") or eg.get("id") or 0, cat_id)
        descs = (cat_json.get("eventGroup") or {}).get("offerCategories") or []
        # Flatten subcategory descriptors
        subdescs = []
        for d in descs:
            subdescs.extend(d.get("offerSubcategoryDescriptors") or [])

        for sd in subdescs:
            sub_name = sd.get("name") or sd.get("subcategoryName") or ""
            sub_id   = sd.get("subcategoryId") or sd.get("offerSubcategoryId")
            if not sub_id or not _dk_market_name_matches(sub_name, wanted):
                continue

            # Some categories already include "offers"; otherwise fetch subcategory
            offers_blob = sd.get("offers")
            if not offers_blob:
                sub_json = _dk_fetch_subcategory_v5(eg.get("eventGroupId") or 0, cat_id, sub_id)
                sub_cats = (sub_json.get("eventGroup") or {}).get("offerCategories") or []
                # try to locate the subcategory again and grab its offers
                for sc in sub_cats:
                    for sdesc in sc.get("offerSubcategoryDescriptors") or []:
                        if (sdesc.get("subcategoryId") or sdesc.get("offerSubcategoryId")) == sub_id:
                            offers_blob = sdesc.get("offers")
                            if offers_blob:
                                break

            if not offers_blob:
                continue

            # offers is a list of lists; outcomes inside each offer
            for offer_list in offers_blob or []:
                for offer in offer_list or []:
                    if offer.get("eventId") != ev_id:
                        continue
                    for outcome in offer.get("outcomes") or []:
                        player = outcome.get("participant") or outcome.get("label") or ""
                        side   = (outcome.get("label") or outcome.get("description") or "").title()
                        odds   = (outcome.get("oddsAmerican") or
                                  (outcome.get("odds", {}) or {}).get("american") or
                                  outcome.get("americanOdds") or "")
                        line   = outcome.get("line") if outcome.get("line") is not None else offer.get("line")
                        out.append({
                            "market": sub_name,
                            "player": player,
                            "side": side,
                            "line": line,
                            "odds": str(odds),
                            "prob": _implied_prob_from_american(str(odds)),
                        })
    return out

def _dk_fetch_eventgroup_v4(eg_id: int) -> dict:
    if not eg_id:
        return {}
    for site in _dk_sites_to_try():
        url = f"https://sportsbook.draftkings.com//sites/{site}/api/v4/eventgroup/{eg_id}?format=json&language={DK_LANG}"
        try:
            r = _fast_get(url, t_conn=1.0, t_read=DK_TIMEOUT)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.json() or {}
        except Exception:
            continue
    return {}

def _dk_extract_event_props_v4(eg_json: dict, ev_id: int, sport: str, stat: str) -> List[dict]:
    wanted = _dk_wanted_market_names(sport, stat)
    out = []
    eg = (eg_json.get("eventGroup") or {})
    for cat in eg.get("offerCategories", []) or []:
        for sub in cat.get("offerSubcategoryDescriptors", []) or []:
            sub_name = sub.get("name") or sub.get("subcategoryName") or ""
            if not _dk_market_name_matches(sub_name, wanted):
                continue
            for offer_list in sub.get("offers") or []:
                for offer in offer_list or []:
                    if offer.get("eventId") != ev_id:
                        continue
                    for outcome in offer.get("outcomes") or []:
                        player = outcome.get("participant") or outcome.get("label") or ""
                        side   = (outcome.get("label") or outcome.get("description") or "").title()
                        odds   = (outcome.get("oddsAmerican")
                                  or (outcome.get("odds", {}) or {}).get("american")
                                  or outcome.get("americanOdds") or "")
                        line   = outcome.get("line") if outcome.get("line") is not None else offer.get("line")
                        out.append({
                            "market": sub_name,
                            "player": player,
                            "side": side,
                            "line": line,
                            "odds": str(odds),
                            "prob": _implied_prob_from_american(str(odds)),
                        })
    return out

def dk_top12_summary_for_game(sport: str, stat: str, teams: List[str]) -> Tuple[str, List[str]]:
    canon = _league_canonical(sport)
    # respect per-league switches
    if (canon == "NBA"  and not USE_DK_FOR_NBA) or \
       (canon == "WNBA" and not USE_DK_FOR_WNBA) or \
       (canon == "NHL"  and not USE_DK_FOR_NHL) or \
       (canon == "NFL"  and not USE_DK_FOR_NFL) or \
       (canon == "MLB"  and not USE_DK_FOR_MLB):
        return "", []

    if len(teams) < 2:
        return "", []

    eg_ids = _dk_eventgroup_ids_for_sport(canon)
    if not eg_ids:
        if VERBOSE:
            print(f"[dk] no EG ids discovered for {canon}")
        return "", []

    if VERBOSE:
        print(f"[dk] trying DK for {canon}/{stat} with teams={teams} (EGs {eg_ids[:3]}...)")

    for eg_id in eg_ids[:4]:  # limit time spent
        # v5 route
        eg_v5 = _dk_fetch_eventgroup_v5(eg_id)
        ev_id = None
        if eg_v5:
            ev_id = _dk_find_event_id_by_teams_v5(eg_v5, canon, teams[0], teams[1])
            if ev_id and VERBOSE:
                gid = (eg_v5.get("eventGroup") or {}).get("eventGroupId") or eg_id
                print(f"[dk] v5 matched event {ev_id} in EG {gid} for {teams}")
        rows = []
        if ev_id and eg_v5:
            rows = _dk_extract_event_props_v5(eg_v5, ev_id, canon, stat)

        # v4 fallback (if no rows yet)
        if not rows:
            eg_v4 = _dk_fetch_eventgroup_v4(eg_id)
            if eg_v4 and not ev_id:
                ev_id = _dk_find_event_id_by_teams_v4(eg_v4, canon, teams[0], teams[1])
                if ev_id and VERBOSE:
                    gid = (eg_v4.get("eventGroup") or {}).get("eventGroupId") or eg_id
                    print(f"[dk] v4 matched event {ev_id} in EG {gid} for {teams}")
            if eg_v4 and ev_id:
                rows = _dk_extract_event_props_v4(eg_v4, ev_id, canon, stat)

        if not rows:
            continue

        # Decide preferred side
        mkts = [(r.get("market") or "").lower() for r in rows]
        is_yes_no = any(("anytime" in m) or ("to score" in m) or ("to hit a home run" in m) for m in mkts)
        prefer_side = "Yes" if is_yes_no else "Over"

        # merge to one outcome per player, pick by preferred side / highest prob
        rows = _choose_one_outcome_per_player(rows, prefer_side)
        rows.sort(key=lambda r: r.get("prob", 0.0), reverse=True)

        # Top 12 names only
        names = [clean_name(r.get("player","")) for r in rows[:12] if r.get("player")]
        if not names:
            continue

        if VERBOSE:
            print(f"[dk] using {len(names)} DK props for {teams}: first={names[:3]}")

        return _summary_from_names(names), [f"https://sportsbook.draftkings.com/event/{ev_id}"]

    if VERBOSE:
        print(f"[dk] props unavailable for {canon}/{stat} {teams} (no offers or endpoints empty)")
    return "", []

# --- FIX: imports needed by hints below ---
from typing import Iterable

# --- FIX: keep only ONE _fast_get; delete the duplicate earlier in your file. ---
# def _fast_get(url: str, t_conn: float, t_read: float):
#     return _RAW.get(url, timeout=(t_conn, t_read))

# --- FIX: make DK timeouts & budget less brutal (overrides earlier values) ---
DK_TIMEOUT = float(os.getenv("DK_TIMEOUT", "6.0"))             # per-request READ timeout
DK_FAIL_LIMIT = int(os.getenv("DK_FAIL_LIMIT", "3"))           # allow a couple of misses
DK_TOTAL_BUDGET_SEC = float(os.getenv("DK_TOTAL_BUDGET_SEC", "12.0"))

def _league_canonical(s: str) -> str:
    up = (s or "").strip().upper()
    if up.startswith("NBA"):  return "NBA"
    if up.startswith("NHL"):  return "NHL"
    if up.startswith("NFL"):  return "NFL"
    if up.startswith("MLB"):  return "MLB"
    if up.startswith("WNBA"): return "WNBA"
    if up in {"CFB","NCAAF","COLLEGE FOOTBALL"}: return "CFB"
    return up

def _dk_sites_to_try() -> List[str]:
    """Dedup preferred DK sites: primary + fallbacks."""
    seen, out = set(), []
    for s in [os.getenv("DK_SITE", "US-SB")] + [
        s.strip() for s in os.getenv("DK_SITE_FALLBACKS", "US-SB,US-NJ-SB,US-OH-SB").split(",")
        if s.strip()
    ]:
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

def _dk_team_search_strings(sport: str, team_code_or_name: str) -> List[str]:
    """Tokens we expect to appear in DK event names."""
    out = set()
    raw = (team_code_or_name or "").strip()
    if raw:
        out.add(raw.lower())
        # expand to nickname words when we know the league map
        m = TEAM_NAME_MAPS.get(_league_canonical(sport), {})
        nick = m.get(raw.upper())
        if nick:
            out.add(nick.lower())
            for w in nick.split():
                out.add(w.lower())
        # split a code like 'LAK' or 'NYG' into letters too, just in case
        for w in re.split(r"[\s.-]+", raw.replace("/", " ")):
            if w:
                out.add(w.lower())
    return [x for x in out if x]

def _dk_find_event_id_by_teams_v4(eg_json: dict, sport: str, teamA: str, teamB: str) -> Optional[int]:
    if not eg_json:
        return None
    A_tokens = _dk_team_search_strings(sport, teamA)
    B_tokens = _dk_team_search_strings(sport, teamB)
    for ev in (eg_json.get("eventGroup") or {}).get("events", []) or []:
        name = (ev.get("name") or "").lower()
        if any(t in name for t in A_tokens) and any(t in name for t in B_tokens):
            return ev.get("eventId")
    return None

def _dk_find_event_id_by_teams_v5(eg_json: dict, sport: str, teamA: str, teamB: str) -> Optional[int]:
    """
    Find the DraftKings eventId in a v5 eventgroup payload by matching team tokens in the event name.
    Looks in eventGroup.events first, then (rare) offer descriptors.
    """
    if not eg_json:
        return None

    A_tokens = _dk_team_search_strings(sport, teamA)
    B_tokens = _dk_team_search_strings(sport, teamB)

    eg = eg_json.get("eventGroup") or {}
    events = eg.get("events") or eg_json.get("events") or []

    # Primary: events list
    for ev in events:
        name = (ev.get("name") or "").lower()
        if not name:
            continue
        if any(t in name for t in A_tokens) and any(t in name for t in B_tokens):
            return ev.get("eventId") or ev.get("id")

    # Secondary: sometimes only present in category offers (rare)
    for cat in (eg.get("offerCategories") or []):
        for sub in (cat.get("offerSubcategoryDescriptors") or []):
            for offer_list in (sub.get("offers") or []):
                for offer in (offer_list or []):
                    evid = offer.get("eventId") or offer.get("event_id") or offer.get("eventid")
                    evname = (offer.get("eventName") or "").lower()
                    if not evid or not evname:
                        continue
                    if any(t in evname for t in A_tokens) and any(t in evname for t in B_tokens):
                        return evid

    return None

_DK_DISCOVER_CACHE: Dict[Tuple[str,str], List[int]] = {}

def _dk_discover_eventgroups_for_sport(sport_up: str) -> List[int]:
    """Hit /api/v4/sports and find eventGroupId(s) that match the league name."""
    sport_up = (sport_up or "").upper()
    key = (sport_up, ",".join(_dk_sites_to_try()))
    if key in _DK_DISCOVER_CACHE:
        return _DK_DISCOVER_CACHE[key]

    found: List[int] = []
    for site in _dk_sites_to_try():
        url = f"https://sportsbook.draftkings.com//sites/{site}/api/v4/sports?format=json&language={DK_LANG}"
        try:
            r = SESSION.get(url, timeout=float(os.getenv("DK_TIMEOUT", "6.0")))
            if not r.ok:
                continue
            js = r.json() or {}
        except Exception:
            continue

        for sp in (js.get("sports") or []):
            for lg in (sp.get("leagues") or []):
                name = (lg.get("name") or "").strip().upper()
                if name == sport_up:
                    eg = lg.get("eventGroupId") or lg.get("eventGroupIds") or []
                    eg_list = eg if isinstance(eg, list) else [eg]
                    for e in eg_list:
                        try:
                            found.append(int(e))
                        except Exception:
                            pass
        if found:
            break

    # dedupe, preserve order
    seen, out = set(), []
    for eid in found:
        if eid and eid not in seen:
            seen.add(eid); out.append(eid)

    _DK_DISCOVER_CACHE[key] = out
    return out

def _dk_eventgroup_ids_for_sport(sport_up: str) -> List[int]:
    """
    Resolve DK eventGroup ids for a league.
    - Prefer env hints when set (kept first).
    - Always include discovered ids so we never depend on manual values.
    """
    s = _league_canonical(sport_up)
    preferred: List[int] = []

    # 1) Env "hints" first (optional)
    if s == "NBA":
        hints = [int(x) for x in re.findall(r"\d+", os.getenv("DK_EVENT_GROUP_IDS_NBA", ""))]
        preferred.extend([h for h in hints if h])
        try:
            single = int(os.getenv("DK_EVENT_GROUP_ID_NBA", "0"))
            if single:
                preferred.append(single)
        except Exception:
            pass
    else:
        try:
            single = int(os.getenv(f"DK_EVENT_GROUP_ID_{s}", "0"))
            if single:
                preferred.append(single)
        except Exception:
            pass

    # 2) Always merge in discovered ids
    discovered = _dk_discover_eventgroups_for_sport(s)
    if VERBOSE:
        print(f"[dk] discovered EGs for {s}: {discovered}")

    # 3) Merge + dedupe (env first)
    out, seen = [], set()
    for v in preferred + discovered:
        if v and v not in seen:
            seen.add(v); out.append(v)
    return out

def _implied_prob_from_american(odds_str: str) -> float:
    """American odds -> implied probability (0..1)."""
    try:
        s = str(odds_str).strip()
        neg = s.startswith("-")
        v = int(s.replace("+", "").replace("-", ""))
    except Exception:
        return 0.0
    if neg:
        return v / (v + 100.0)
    return 100.0 / (v + 100.0)

def _choose_one_outcome_per_player(rows: List[dict], prefer_side: str) -> List[dict]:
    """
    Keep a single outcome per player:
      - prefer prefer_side ("Over" or "Yes")
      - otherwise keep the highest implied probability.
    """
    best: Dict[str, dict] = {}
    pref = (prefer_side or "").lower()
    for r in rows:
        key = (r.get("player") or "").strip()
        if not key:
            continue
        score = (1 if (r.get("side","").lower() == pref) else 0, r.get("prob", 0.0))
        cur = best.get(key)
        cur_score = (1 if (cur and cur.get("side","").lower() == pref) else 0, (cur or {}).get("prob", 0.0))
        if (cur is None) or (score > cur_score):
            best[key] = r
    return list(best.values())

def _summary_from_names(names: List[str]) -> str:
    g  = names[0:3]
    y  = names[3:6]
    rd = names[6:9]
    p  = names[9:12]
    parts = []
    if g:  parts.append(f"🟢 {', '.join(g)}")
    if y:  parts.append(f"🟡 {', '.join(y)}")
    if rd: parts.append(f"🔴 {', '.join(rd)}")
    if p:  parts.append(f"🟣 {', '.join(p)}")
    return "\n".join(parts)


# --- Replacement for process_games() ---

def process_games():
    """
    Groups rows by (sport, team1+team2) so each game prints once as:
      H3: "<Team 1> vs <Team 2> — <SPORT>"
      <STAT A> — <suffix>
      <picks>
      <divider>
      <STAT B> — <suffix>
      ...
    Marks every included Notion row as Processed=Yes.
    """
    rows = fetch_unprocessed_rows(DATABASE_ID)
    if not rows:
        return

    # when grouping
    grouped: Dict[Tuple[str, Tuple[str,str]], List[Dict]] = {}
    for r in rows:
        k = _canon_game_key(r["sport"], r["teams"])
        grouped.setdefault(k, []).append(r)

    # compute a group-order using the min Order inside each group
    def _order_key(items: List[Dict]):
        # normalize numeric vs text order; text will sort lexicographically
        values = [it.get("order") for it in items if it.get("order") is not None]
        if not values:
            return (float("inf"), "")  # groups with no order go last
        # split by type so numbers beat strings
        nums  = [v for v in values if isinstance(v, (int, float))]
        texts = [v for v in values if isinstance(v, str)]
        if nums:
            return (min(nums), "")
        return (float("inf"), min(texts))

    # Process each game group together (one Notion append per game)
    for (sport_up, (t1, t2)), game_rows in grouped.items():
        STAT_ORDER = ["RECEPTIONS","REC","YARDS","YDS","TOTAL SCRIMMAGE YARDS","TD","TDS","TOUCHDOWNS"]
        STAT_RANK = {s:i for i,s in enumerate(STAT_ORDER)}
        def stat_sort_key(stat: str) -> int:
            return STAT_RANK.get((stat or "").strip().upper(), 999)

        game_rows.sort(key=lambda rr: (stat_sort_key(rr["stat"]), ",".join(rr["teams"]).upper()))

        # Nice game title
        teams_display = ", ".join(game_rows[0]["teams"]) if game_rows and game_rows[0]["teams"] else f"{t1} vs {t2}"
        if len(game_rows[0]["teams"]) >= 2:
            tA, tB = game_rows[0]["teams"][0], game_rows[0]["teams"][1]
            if sport_up in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
                # Show preferred CFB abbreviations in the heading (e.g., IND vs INST)
                abbrA = cfb_preferred_abbr_from_input(tA)
                abbrB = cfb_preferred_abbr_from_input(tB)
                teams_display = f"{abbrA} vs {abbrB}"
            else:
                teams_display = f"{tA} vs {tB}"
        heading_text = f"{teams_display} — {sport_up}"

        blocks_for_game: List[Dict] = []
        blocks_for_game.append(_heading3(heading_text))

        # We’ll mark Processed for each row we actually handle
        pages_to_mark: List[str] = []
        for r in game_rows:
            page_id = r["page_id"]
            sport   = r["sport"]
            stat    = r["stat"]
            teams   = r["teams"]
            stat_up = (stat or "").strip().upper()
            sport_up_local = (sport or "").strip().upper()

                        # 1) Colors-only for MLB strikeouts (leave as-is if you want this rule)
            if stat_up in {"K", "SO", "STRIKEOUTS"} and sport_up_local == "MLB":
                blocks_for_game.append({
                    "object":"block","type":"paragraph",
                    "paragraph":{"rich_text":[{"type":"text","text":{"content":f"{stat} — past week"}}]}
                })
                blocks_for_game.append({
                    "object":"block","type":"paragraph",
                    "paragraph":{"rich_text":[{"type":"text","text":{"content":_colors_only_summary()}}]}
                })
                blocks_for_game.append({"object":"block","type":"divider","divider":{}})
                pages_to_mark.append(page_id)
                continue

            # 2) DraftKings branch (ALWAYS TRY FIRST; short-circuit on success)
            canon = _league_canonical(sport_up_local)
            dk_enabled = (
                (canon == "NHL"  and USE_DK_FOR_NHL)  or
                (canon == "NFL"  and USE_DK_FOR_NFL)  or
                (canon == "NBA"  and USE_DK_FOR_NBA)  or
                (canon == "WNBA" and USE_DK_FOR_WNBA) or
                (canon == "MLB"  and USE_DK_FOR_MLB)
            )

            dk_summary, dk_links = ("", [])
            if dk_enabled:
                if VERBOSE:
                    print(f"[dk] trying DK for {canon}/{stat} with teams={teams}")
                dk_summary, dk_links = dk_top12_summary_for_game(sport_up_local, stat, teams)
                if dk_summary:
                    blocks_for_game.append({
                        "object":"block","type":"paragraph",
                        "paragraph":{"rich_text":[{"type":"text","text":{"content":f"{stat} — DraftKings"}}]}
                    })
                    blocks_for_game.append({
                        "object":"block","type":"paragraph",
                        "paragraph":{"rich_text":[{"type":"text","text":{"content":dk_summary}}]}
                    })
                    if dk_links:
                        blocks_for_game.append(_link_para("View market (DK)", dk_links[0]))
                    blocks_for_game.append({"object":"block","type":"divider","divider":{}})
                    pages_to_mark.append(page_id)
                    continue  # DK success → next stat

            # 3) If DK failed: optional NHL "blank" section (only if you want it)
            if sport_up_local == "NHL" and BLANK_NHL_PICKS:
                blocks_for_game.append({
                    "object":"block","type":"paragraph",
                    "paragraph":{"rich_text":[{"type":"text","text":{"content":f"{stat} — (no props posted yet)"}}]}
                })
                blocks_for_game.append({
                    "object":"block","type":"paragraph",
                    "paragraph":{"rich_text":[{"type":"text","text":{"content":_colors_only_summary()}}]}
                })
                blocks_for_game.append({"object":"block","type":"divider","divider":{}})
                pages_to_mark.append(page_id)
                continue

            # 4) StatMuse fallback (all sports)
            traded_players = {}
            if sport_up_local == "MLB":
                traded_players = fetch_trades_past_week()

            data, used_season_mode, season_year, debug_links = scrape_statmuse_data(stat, sport, teams)

            if sport_up_local == "MLB":
                data = filter_traded_banned_and_teams(data, teams, traded_players=traded_players, banned_players=BANNED_PLAYERS)
                injured = get_mlb_injured_players()
                if data:
                    data = dedupe_by_player(data)
                    name_col = "NAME" if "NAME" in data[0] else "PLAYER"
                    data = [rec for rec in data if clean_name(rec.get(name_col, "")) not in injured]
            elif sport_up_local == "NFL" and teams:
                data = filter_traded_banned_and_teams(data, teams)
            elif sport_up_local in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
                pass
            else:
                if data and teams:
                    data = filter_traded_banned_and_teams(data, teams)

            # Build StatMuse summary
            if not data:
                summary = _colors_only_summary()
            else:
                data = dedupe_by_player(data)
                desired_key = "TD" if (sport_up_local in {"CFB","NCAAF","COLLEGE FOOTBALL"} and stat_up in {"TD","TDS","TOUCHDOWNS"}) else stat
                stat_key    = pick_stat_key(data, desired_key)
                g, y, rd, p = bucket_top12(data, stat_key)
                summary = (_format_buckets_cfb(g, y, rd, p) if sport_up_local in {"CFB","NCAAF","COLLEGE FOOTBALL"}
                        else _format_buckets_default(g, y, rd, p)) or "(no qualifying leaders found)"

            # Suffix for StatMuse
            if sport_up_local in {"CFB","NCAAF","COLLEGE FOOTBALL"} and used_season_mode:
                suffix = f"{season_year}"
            elif sport_up_local == "NFL":
                if NFL_QUERY_MODE == "SEASON":
                    suffix = f"{NFL_SEASON_YEAR}"
                elif NFL_QUERY_MODE == "DATES" and NFL_START_DATE and NFL_END_DATE:
                    suffix = f"{NFL_START_DATE} → {NFL_END_DATE}"
                else:
                    suffix = f"last {NFL_LAST_N_DAYS} days"
            else:
                suffix = "past week"

            blocks_for_game.append({
                "object":"block","type":"paragraph",
                "paragraph":{"rich_text":[{"type":"text","text":{"content":f"{stat} — {suffix}"}}]}
            })
            blocks_for_game.append({
                "object":"block","type":"paragraph",
                "paragraph":{"rich_text":[{"type":"text","text":{"content":summary}}]}
            })
            if SHOW_CFB_DEBUG_LINKS and debug_links:
                seen=set(); shown=0
                for u in debug_links:
                    if not u or u in seen: continue
                    seen.add(u); shown += 1
                    blocks_for_game.append(_link_para(f"View query {shown}", u))
                    if shown >= 6: break

            blocks_for_game.append({"object":"block","type":"divider","divider":{}})
            pages_to_mark.append(page_id)

        # Push one append per game
        notion_append_blocks(blocks_for_game)

        # Mark all rows from this game as processed
        for pid in pages_to_mark:
            notion_update_page(pid, {"Processed": {"select": {"name": "Yes"}}})

        # Console
        try:
            print(f"✅ Posted grouped game: {heading_text} ({len(game_rows)} stats)")
        except Exception:
            print(f"✅ Posted grouped game: {sport_up} ({len(game_rows)} stats)")

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

        # Colors-only for ALL NHL PSP rows when enabled
        if sport_up == "NHL" and BLANK_NHL_PICKS:
            _post_colors_only_block(pid, f"{sport_up} PSP - {stat_up}")
            print(f"✅ PSP updated for {sport}/{stat} (blank NHL picks)")
            continue

        # Strikeouts -> colors-only
        if stat_up in {"K","SO","STRIKEOUTS"}:
            _post_colors_only_block(pid, f"{sport_up} PSP - {stat_up}")
            print(f"✅ PSP updated for {sport}/{stat} (colors only for K)")
            continue

        if TRY_PSP_STATMUSE:
            data, used_season_mode, season_year, debug_links = scrape_statmuse_data(stat, sport, teams)
        else:
            # skip scraping noise; just make a minimal section
            data, used_season_mode, season_year, debug_links = [], False, 0, []

        if sport_up == "MLB":
            injured = get_mlb_injured_players()
            if data:
                data = dedupe_by_player(data)
                name_col = "NAME" if "NAME" in data[0] else "PLAYER"
                data = [rec for rec in data if clean_name(rec.get(name_col, "")) not in injured]
        elif sport_up == "NFL" and teams:
            data = filter_traded_banned_and_teams(data, teams)
        # CFB: no post-filter

        if not data:
            summary = _colors_only_summary()
        else:
            data = dedupe_by_player(data)
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

        if SHOW_CFB_DEBUG_LINKS and debug_links:
            seen = set()
            shown = 0
            for u in debug_links:
                if not u or u in seen:
                    continue
                seen.add(u)
                shown += 1
                blocks.append(_link_para(f"View query {shown}", u))
                if shown >= 6:
                    break

        notion_append_blocks(blocks)
        notion_update_page(pid, {"Processed":{"select":{"name":"Yes"}}})
        print(f"✅ PSP updated for {sport}/{stat} ({suffix})")

# ============================== Main =======================================

def process_all():
    process_games()
    process_psp_rows()

if __name__ == "__main__":
    process_all()