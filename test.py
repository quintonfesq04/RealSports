#!/usr/bin/env python3
import os
import urllib.parse
import pandas as pd
import re
import requests
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from notion_client import Client
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# ---- Env loading that "just works" ----------------------------------------

NOTION_TOKEN_FALLBACK = "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"  # fallback so it runs

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
    return NOTION_TOKEN_FALLBACK

NOTION_TOKEN = _load_env_token()

# ---- Config ---------------------------------------------------------------

DATABASE_ID        = os.getenv("DATABASE_ID", "1aa71b1c-663e-8035-bc89-fb1e84a2d919")
PSP_DATABASE_ID    = os.getenv("PSP_DATABASE_ID", "1ac71b1c663e808e9110eee23057de0e")
POLL_PAGE_ID       = os.getenv("POLL_PAGE_ID", "18e71b1c663e80cdb8a0fe5e8aeee5a9")

BASE_URL                   = "https://www.statmuse.com"
CBSSPORTS_MLB_INJURIES_URL = "https://www.cbssports.com/mlb/injuries/"
MLB_TRANSACTIONS_URL       = "https://www.mlb.com/transactions"

DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

client = Client(auth=NOTION_TOKEN)

# ---- Robust HTTP session with retries ------------------------------------

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
            allowed_methods=frozenset(["GET", "POST"])
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

SESSION = make_session()

# ---- Time window helpers --------------------------------------------------

def week_window(today: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """Return (start,end) covering the last 7 calendar days ending today."""
    today = today or datetime.today()
    start = today - timedelta(days=6)
    start = datetime(start.year, start.month, start.day)
    end   = datetime(today.year, today.month, today.day)
    return start, end

def _nice_date(d: datetime) -> str:
    # Windows uses %#d, Unix uses %-d
    return d.strftime("%B %-d, %Y") if os.name != "nt" else d.strftime("%B %#d, %Y")

# ---- Helpers --------------------------------------------------------------

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
            teams = [x.strip().upper() for x in (t1txt + "," + t2txt).split(",") if x.strip()]
        out.append({"page_id": pid, "sport": sport, "stat": stat, "teams": teams})
    return out

def build_query_url(query: str, teams=None, sport=None, start: Optional[datetime]=None, end: Optional[datetime]=None) -> str:
    """Build a StatMuse URL like '... ask?q=<query> from Aug 4, 2025 to Aug 11, 2025 <teams>'"""
    if not (start and end):
        start, end = week_window()
    base = f"{query} from {_nice_date(start)} to {_nice_date(end)}"

    unmapped = []
    if teams and sport and isinstance(teams, list) and len(teams) <= 2:
        team_map = TEAM_NAME_MAPS.get(sport.strip().upper(), {})
        mapped = []
        for t in teams:
            mapped_name = team_map.get(t)
            if mapped_name is None:
                unmapped.append(t)
                mapped.append(t)
            else:
                mapped.append(mapped_name)
        if unmapped:
            print(f"[warn] Team(s) not mapped for {sport}: {', '.join(unmapped)}")
        teams_str = ",".join(mapped)
        base += f" {teams_str}"
    elif teams:
        teams_str = ",".join(teams) if isinstance(teams, list) else teams
        base += f" {teams_str}"
    return f"{BASE_URL}/ask?q={urllib.parse.quote_plus(base)}"

# ---- Selenium (retry, longer waits) --------------------------------------

def _new_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.set_page_load_timeout(35)
    return drv

def fetch_html(url: str, wait_css: str = "table", wait_seconds: int = 25) -> str:
    last_err = None
    for attempt in range(3):
        drv = None
        try:
            drv = _new_driver()
            drv.get(url)
            try:
                WebDriverWait(drv, wait_seconds).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                )
            except TimeoutException:
                pass
            html = drv.page_source or ""
            return html
        except WebDriverException as e:
            last_err = e
            time.sleep(1.2 * (attempt + 1))
        finally:
            try:
                if drv:
                    drv.quit()
            except Exception:
                pass
    if last_err:
        print(f"[webdriver] error for {url}: {last_err}")
    return ""

# ---- Parse ---------------------------------------------------------------

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

def scrape_statmuse_data(stat: str, sport: str, teams=None) -> List[Dict[str, str]]:
    query = f"{stat} leaders {sport.lower()}"
    start, end = week_window()
    url   = build_query_url(query, teams, sport, start, end)
    html  = fetch_html(url)
    data  = parse_table(html)
    if not data:
        print(f"[parse] no data for statmuse url: {url}")
    return data

# ---- Name helpers ---------------------------------------------------------

NAME_OVERRIDES = {
    "Aari Mc Donald": "Aari McDonald",
    "De Wanna Bonner": "DeWanna Bonner",
    "Ryan Mc Mahon": "Ryan McMahon",
    "Zach Mc Kinstry": "Zack McKinstry",
    "Jeff Mc Neil": "Jeff McNeil",
    "Reese Mc Guire": "Reese McGuire",
    "Escarra": "JC Escarra",
    "Andrew Mc Cutchen": "Andrew McCutchen",
    "CJAbrams": "CJ Abrams",
    "Paul De Jong": "Paul DeJong",
    "TJFriedl": "TJ Friedl",
    "Michael Harris IIM II": "Michael Harris II",
    "CJKayfus": "CJ Kayfus",
    "Na Lyssa Smith": "NaLyssa Smith",
    "James Mc Cann": "James McCann",
    "Di Jonai Carrington": "DiJonai Carrington",
    "Kayla Mc Bride": "Kayla McBride",
    "Victor Scott IIV II": "Victor Scott II",
    "JJQuinerly": "JJ Quinerly",
    "Jake Mc Carthy": "Jake McCarthy",
}

BANNED_PLAYERS = {
    # Add cleaned names if you always want to exclude them
}

def clean_name(raw: str) -> str:
    s = raw.replace(".", " ")
    s = re.sub(r'([a-zà-öø-ÿ])([A-Z])', r'\1 \2', s)
    s = re.sub(r'([A-Z]) ([A-Z][a-z])', r'\1\2', s)
    s = re.sub(r'\s+', ' ', s).strip()
    parts, seen, out = s.split(), set(), []
    for p in parts:
        if len(p) == 1:
            continue
        low = p.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(p)
    cleaned = " ".join(out)
    return NAME_OVERRIDES.get(cleaned, cleaned)

# ---- Injuries -------------------------------------------------------------

def get_mlb_injured_players() -> set:
    r = SESSION.get(CBSSPORTS_MLB_INJURIES_URL, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    injured = set()
    for section in soup.find_all("div", class_="TableBase-shadows"):
        table = section.find("table")
        if not table:
            continue
        for tr in table.find_all("tr")[1:]:
            cols = tr.find_all("td")
            if not cols:
                continue
            a = cols[0].find_all("a")
            if len(a) >= 2:
                name = a[1].get_text(strip=True)
            elif a:
                name = a[0].get_text(strip=True)
            else:
                name = cols[0].get_text(strip=True)
            full_cell = cols[0].get_text(strip=True)
            injured.add(clean_name(name))
            injured.add(clean_name(full_cell))
    injured.discard("")
    return injured

def get_wnba_injured_players() -> set:
    csv_path = "wnba_injuries.csv"
    injured = set()
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        if "playerName" in df.columns:
            for name in df["playerName"].dropna():
                injured.add(clean_name(str(name)))
        injured.discard("")
        return injured
    url = "https://www.cbssports.com/wnba/injuries/"
    r = SESSION.get(url, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    for section in soup.find_all("div", class_="TableBase-shadows"):
        table = section.find("table")
        if not table:
            continue
        for tr in table.find_all("tr")[1:]:
            cols = tr.find_all("td")
            if not cols:
                continue
            a = cols[0].find("a")
            if a:
                name = a.get_text(strip=True)
            else:
                name = cols[0].get_text(strip=True)
            full_cell = cols[0].get_text(strip=True)
            injured.add(clean_name(name))
            injured.add(clean_name(full_cell))
    injured.discard("")
    return injured

# ---- Ranking buckets ------------------------------------------------------

def bucket_top12(data: List[Dict[str, str]], stat_key: str) -> Tuple[List[str], List[str], List[str], List[str]]:
    name_col = "NAME" if data and "NAME" in data[0] else "PLAYER"
    cleaned: List[Tuple[str, float]] = []
    for rec in data:
        raw_val = str(rec.get(stat_key, "")).replace(",", "")
        try:
            num = float(raw_val)
        except Exception:
            continue
        nm = clean_name(rec.get(name_col, ""))
        cleaned.append((nm, num))
    cleaned.sort(key=lambda x: x[1], reverse=True)
    top12 = cleaned[:12]
    green  = [nm for nm, _ in top12[0:3]]
    yellow = [nm for nm, _ in top12[3:6]]
    red    = [nm for nm, _ in top12[6:9]]
    purple = [nm for nm, _ in top12[9:12]]
    return green, yellow, red, purple

def _format_buckets(g, y, rd, p) -> str:
    def line(dot, names): return f"{dot} {', '.join(names) if names else 'None'}"
    return "\n".join([
        line("🟢", g),
        line("🟡", y),
        line("🔴", rd),
        line("🟣", p),
    ])

def _colors_only_summary() -> str:
    # exactly the “just colors” format you asked for (space after each emoji)
    return "🟢 \n🟡 \n🔴 \n🟣 "

# ---- Team maps ------------------------------------------------------------

TEAM_NAME_MAPS = {
    "MLB": {
        "TOR": "Blue Jays", "BOS": "Red Sox", "NYY": "Yankees", "TB": "Rays", "BAL": "Orioles",
        "DET": "Tigers", "CLE": "Guardians", "KC": "Royals", "MIN": "Twins", "CWS": "White Sox",
        "HOU": "Astros", "SEA": "Mariners", "TEX": "Rangers", "LAA": "Angels", "OAK": "Athletics",
        "NYM": "Mets", "PHI": "Phillies", "MIA": "Marlins", "ATL": "Braves", "WSH": "Nationals",
        "MIL": "Brewers", "CHC": "Cubs", "CIN": "Reds", "STL": "Cardinals", "PIT": "Pirates",
        "LAD": "Dodgers", "SD": "Padres", "SF": "Giants", "ARI": "Diamondbacks", "COL": "Rockies",
    },
    "NBA": {
        "BKN": "Nets", "OKC": "Thunder", "LAL": "Lakers", "LAC": "Clippers", "CHI": "Bulls",
        "POR": "Trail Blazers", "WAS": "Wizards", "SAC": "Kings", "PHI": "76ers", "MIL": "Bucks",
        "DEN": "Nuggets", "ORL": "Magic", "SAS": "Spurs", "MIA": "Heat", "UTA": "Jazz",
        "NOP": "Pelicans", "BOS": "Celtics", "GSW": "Warriors", "PHX": "Suns", "ATL": "Hawks",
        "NYK": "Knicks", "MIN": "Timberwolves", "MEM": "Grizzlies", "DET": "Pistons",
        "HOU": "Rockets", "DAL": "Mavericks", "CHA": "Hornets", "TOR": "Raptors",
        "CLE": "Cavaliers", "IND": "Pacers",
    },
    "WNBA": {
        "NYL": "Liberty", "ATL": "Dream", "IND": "Fever", "WAS": "Mystics", "CHI": "Sky",
        "CON": "Sun", "MIN": "Lynx", "PHO": "Mercury", "SEA": "Storm", "LVA": "Aces",
        "LAS": "Sparks", "DAL": "Wings", "GSV": "Valkyries"
    },
    "NHL": {
        "TOR": "Maple Leafs", "TBL": "Lightning", "FLA": "Panthers", "OTT": "Senators",
        "MTL": "Canadiens", "DET": "Red Wings", "BUF": "Sabres", "BOS": "Bruins",
        "WSH": "Capitals", "CAR": "Hurricanes", "NJD": "Devils", "CBJ": "Blue Jackets",
        "NYR": "Rangers", "NYI": "Islanders", "PIT": "Penguins", "PHI": "Flyers",
        "WPG": "Jets", "DAL": "Stars", "COL": "Avalanche", "MIN": "Wild", "STL": "Blues",
        "UTA": "Hockey Club", "NSH": "Predators", "CHI": "Blackhawks", "VGK": "Golden Knights",
        "LAK": "Kings", "EDM": "Oilers", "CGY": "Flames", "VAN": "Canucks", "ANA": "Ducks",
        "SEA": "Kraken", "SJS": "Sharks",
    }
}

def get_team_name_only(abbr: str, sport: str) -> str:
    sport_key = sport.strip().upper()
    team_map = TEAM_NAME_MAPS.get(sport_key, {})
    return team_map.get(abbr.strip().upper(), abbr)

def get_team_abbr(team_name: str) -> str:
    team_map = TEAM_NAME_MAPS["MLB"]
    tn = team_name.strip().lower()
    for abbr, name in team_map.items():
        if tn == name.lower() or tn == abbr.lower():
            return abbr
    for abbr, name in team_map.items():
        if tn in name.lower():
            return abbr
    return team_name.upper()

# ---- Trades (Web only, date-filtered) ------------------------------------

def _parse_mlb_tx_date(s: str) -> Optional[datetime]:
    """
    MLB transactions page shows dates like 'August 10, 2025'.
    Return a date (midnight) or None if parse fails.
    """
    s = (s or "").strip()
    for fmt in ("%B %d, %Y",):
        try:
            dt = datetime.strptime(s, fmt)
            return datetime(dt.year, dt.month, dt.day)
        except Exception:
            pass
    return None

def fetch_trades_past_week() -> Dict[str, str]:
    """
    Scrape https://www.mlb.com/transactions and return {player_name: DEST_ABBR}
    for trades **only within the last 7 days**.
    """
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
    if not table:
        print("[trades-web] no table found")
        return trades

    rows = table.find_all("tr")
    if not rows:
        return trades

    # Skip header if present
    start_idx = 1 if rows and rows[0].find_all("th") else 0

    for tr in rows[start_idx:]:
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        date_txt = tds[0].get_text(strip=True)
        desc     = tds[2].get_text(strip=True)

        tx_date = _parse_mlb_tx_date(date_txt)
        if not tx_date:
            continue

        if tx_date < start_dt or tx_date > end_dt:
            continue

        if "traded" not in desc.lower():
            continue

        m = re.search(
            r"traded\s+(?:[A-Z/]{1,4}\s+)?([A-Za-z' .-]+?)\s+to\s+([A-Za-z' .-]+)",
            desc, re.I
        )
        if not m:
            continue

        player = clean_name(m.group(1))
        dest   = get_team_abbr(m.group(2))
        if player and dest:
            trades[player] = dest

    return trades

# ---- Filtering ------------------------------------------------------------

def filter_traded_banned_and_teams(
    data: List[Dict[str, str]],
    teams: List[str],
    traded_players: Optional[Dict[str, str]] = None,
    banned_players: Optional[set] = None
) -> List[Dict[str, str]]:
    teams_set = set(t.upper() for t in (teams or []))
    traded_clean = {clean_name(k): v.upper() for k, v in (traded_players or {}).items()}
    banned_clean = {clean_name(n) for n in (banned_players or set())}

    out = []
    for rec in data:
        name_col = "NAME" if "NAME" in rec else "PLAYER"
        nm = clean_name(rec.get(name_col, ""))
        team = (rec.get("TEAM") or "").upper()

        if nm in banned_clean:
            continue
        mapped = traded_clean.get(nm)
        if mapped and team != mapped:
            continue
        if team in teams_set:
            out.append(rec)
    return out

# ---- Stat key matching ----------------------------------------------------

STAT_ALIASES = {
    "K": {"K", "SO", "STRIKEOUTS"},
    "HR": {"HR", "HOMERS", "HOME RUNS"},
    "RBI": {"RBI", "RBIS"},
    "3P": {"3P", "3PM", "3-PT", "3PT", "THREE POINTERS"},
    "PTS": {"PTS", "POINTS"},
    "REB": {"REB", "REBOUNDS"},
    "AST": {"AST", "ASSISTS"},
    # Add more as needed
}

def pick_stat_key(data: List[Dict[str, str]], desired: str) -> str:
    """Pick a header from the scraped table that best matches the desired stat name."""
    target = desired.strip().upper()
    if not data:
        return target

    headers = [k.upper() for k in data[0].keys()]
    if target in headers:
        return [k for k in data[0].keys() if k.upper() == target][0]

    alias_set = STAT_ALIASES.get(target, {target})
    for h in headers:
        if h in alias_set:
            return [k for k in data[0].keys() if k.upper() == h][0]

    norm = re.sub(r"[^A-Z0-9]", "", target)
    for h in headers:
        if re.sub(r"[^A-Z0-9]", "", h) == norm:
            return [k for k in data[0].keys() if k.upper() == h][0]

    # Fallback to first numeric-looking column
    for k in data[0].keys():
        val = str(data[0].get(k, "")).replace(",", "")
        try:
            float(val)
            return k
        except Exception:
            continue

    return target

# ---- Notion helpers -------------------------------------------------------

def notion_append_blocks(blocks: List[Dict]):
    for attempt in range(3):
        try:
            client.blocks.children.append(block_id=POLL_PAGE_ID, children=blocks)
            return
        except Exception as e:
            msg = str(e)
            if "Rate limited" in msg or "429" in msg:
                time.sleep(2 ** attempt)
                continue
            print(f"[notion] append error: {e}")
            return

def notion_update_page(page_id: str, props: Dict):
    for attempt in range(3):
        try:
            client.pages.update(page_id=page_id, properties=props)
            return
        except Exception as e:
            msg = str(e)
            if "Rate limited" in msg or "429" in msg:
                time.sleep(2 ** attempt)
                continue
            print(f"[notion] update error: {e}")
            return

# ---- Core flows -----------------------------------------------------------

# Treat these sports as "colors-only" (blank) like MLB strikeouts:
BLANK_SPORTS = {"CFB", "NCAAF", "COLLEGE FOOTBALL"}

def _post_colors_only_block(page_id: str, heading_text: str):
    summary = _colors_only_summary()
    notion_append_blocks([
        {"object": "block", "type": "paragraph", "paragraph": {
            "rich_text": [{"type": "text", "text": {"content": heading_text}}]
        }},
        {"object": "block", "type": "paragraph", "paragraph": {
            "rich_text": [{"type": "text", "text": {"content": summary}}]
        }},
        {"object": "block", "type": "divider", "divider": {}}
    ])
    notion_update_page(page_id, {"Processed": {"select": {"name": "Yes"}}})

def process_games():
    rows = fetch_unprocessed_rows(DATABASE_ID)
    rows.reverse()  # process bottom-to-top
    for r in rows:
        sport, stat, teams = r["sport"], r["stat"], r["teams"]
        statmuse_teams = teams
        sport_upper = (sport or "").strip().upper()
        stat_upper  = (stat or "").strip().upper()

        # If sport is CFB/NCAAF/etc -> colors-only (blank), skip scraping entirely
        if sport_upper in BLANK_SPORTS:
            _post_colors_only_block(r["page_id"], f"Game: {', '.join(statmuse_teams)} — {stat}")
            print(f"✅ Updated {teams} — {sport} (colors-only by sport)")
            continue

        # If Strikeouts, post colors-only and skip scraping/bucketing.
        if stat_upper in STAT_ALIASES["K"]:
            _post_colors_only_block(r["page_id"], f"Game: {', '.join(statmuse_teams)} — {stat} leaders")
            print(f"✅ Updated {teams} — {stat} (colors only for K)")
            continue

        traded_players: Dict[str, str] = {}
        if sport_upper == "MLB":
            traded_players = fetch_trades_past_week()

        data = scrape_statmuse_data(stat, sport, statmuse_teams)

        if data and "TEAM" in data[0]:
            data = filter_traded_banned_and_teams(
                data, statmuse_teams, traded_players=traded_players, banned_players=BANNED_PLAYERS
            )

        name_col = "NAME" if (data and "NAME" in (data[0] if data else {})) else "PLAYER"
        if sport_upper == "MLB":
            injured = get_mlb_injured_players()
            data = [rec for rec in data if clean_name(rec.get(name_col, "")) not in injured]
        elif sport_upper == "WNBA":
            injured = get_wnba_injured_players()
            data = [rec for rec in data if clean_name(rec.get(name_col, "")) not in injured]

        if not data:
            summary = _format_buckets([], [], [], [])
        else:
            stat_key = pick_stat_key(data, stat)
            g, y, rd, p = bucket_top12(data, stat_key)
            summary = _format_buckets(g, y, rd, p)

        notion_append_blocks([
            {"object": "block", "type": "paragraph", "paragraph": {
                "rich_text": [{"type": "text", "text": {
                    "content": f"Game: {', '.join(statmuse_teams)} — {stat}"
                }}]
            }},
            {"object": "block", "type": "paragraph", "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": summary}}]
            }},
            {"object": "block", "type": "divider", "divider": {}}
        ])
        notion_update_page(r["page_id"], {"Processed": {"select": {"name": "Yes"}}})
        print(f"✅ Updated {teams} — {stat}")

def process_psp_rows():
    resp = client.databases.query(
        database_id=PSP_DATABASE_ID,
        filter={"property": "Processed", "select": {"equals": "no"}},
        sort=[{"property": "Order", "direction": "ascending"}]
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
        elif tp.get("type") in ("rich_text", "title"):
            raw = "".join(t["plain_text"] for t in tp.get(tp["type"], []))
            teams = [x.strip() for x in raw.split(",") if x.strip()]
        else:
            teams = None

        sport_upper = sport.upper()
        stat_upper  = stat.upper()

        # Colors-only if the sport is CFB/NCAAF/etc
        if sport_upper in BLANK_SPORTS:
            _post_colors_only_block(pid, f"{sport_upper} PSP - {stat_upper}")
            print(f"✅ PSP updated for {sport}/{stat} (colors-only by sport)")
            continue

        # Colors-only for Strikeouts on PSP too
        if stat_upper in STAT_ALIASES["K"]:
            _post_colors_only_block(pid, f"{sport_upper} PSP - {stat_upper}")
            print(f"✅ PSP updated for {sport}/{stat} (colors only for K)")
            continue

        data = scrape_statmuse_data(stat, sport, teams)

        if sport_upper == "MLB":
            injured = get_mlb_injured_players()
        elif sport_upper == "WNBA":
            injured = get_wnba_injured_players()
        else:
            injured = set()

        name_col = "NAME" if (data and "NAME" in (data[0] if data else {})) else "PLAYER"
        if injured:
            data = [rec for rec in data if clean_name(rec.get(name_col, "")) not in injured]

        if not data:
            summary = _format_buckets([], [], [], [])
        else:
            stat_key = pick_stat_key(data, stat_upper)
            g, y, rd, p = bucket_top12(data, stat_key)
            summary = _format_buckets(g, y, rd, p)

        notion_append_blocks([
            {"object": "block", "type": "paragraph", "paragraph": {
                "rich_text": [{"type": "text", "text": {
                    "content": f"{sport_upper} PSP - {stat_upper}"
                }}]
            }},
            {"object": "block", "type": "paragraph", "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": summary}}]
            }},
            {"object": "block", "type": "divider", "divider": {}}
        ])
        notion_update_page(pid, {"Processed": {"select": {"name": "Yes"}}})
        print(f"✅ PSP updated for {sport}/{stat}")

def process_all():
    process_games()
    process_psp_rows()

if __name__ == "__main__":
    process_all()