#!/usr/bin/env python3
"""
cbb_scraper.py
---------------
College Basketball (CBB) player stat scraper.

Outputs: cbb_players_stats.csv with columns:
["Player","Team","PPG","APG","RPG","3PM","Games"]

Extras:
- Provide additional teams via environment variables:
  * EXTRA_CBB_TEAM_IDS: e.g. "277, 333, 356"
  * EXTRA_CBB_TEAM_URLS: paste ESPN team page links, e.g.
    https://www.espn.com/mens-college-basketball/team/_/id/277/wisconsin-badgers
    https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-college-basketball/statistics/byathlete?team=333
- Teams can also be hardcoded via ESPN_TEAM_STATS_PAGE, e.g.:
    ESPN_TEAM_STATS_PAGE = {
        "WVU": "https://www.espn.com/mens-college-basketball/team/stats/_/id/277"
    }
- Script will also parse ESPN team stats HTML pages listed in ESPN_TEAM_STATS_PAGE and merge/override stats to ensure missing players (e.g., WVU leaders) are included.
"""

import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Dict, Optional
import re
from io import StringIO
from datetime import datetime

_BOOTSTRAP_FLAG = "REALSPORTS_VENV_BOOTSTRAP"
HERE = Path(__file__).resolve().parent

def _maybe_reexec_with_local_venv():
    """Re-executes this script inside ./venv if dependencies missing."""
    if os.getenv("VIRTUAL_ENV"):
        return
    if os.getenv(_BOOTSTRAP_FLAG):
        return
    if os.name == "nt":
        candidates = [HERE / "venv" / "Scripts" / "python.exe"]
    else:
        candidates = [HERE / "venv" / "bin" / "python"]
    for cand in candidates:
        if cand.exists():
            os.environ[_BOOTSTRAP_FLAG] = "1"
            os.execv(str(cand), [str(cand), __file__, *sys.argv[1:]])

try:
    import requests
    import pandas as pd
except ImportError:
    _maybe_reexec_with_local_venv()
    import requests  # retry; if still missing, let it raise
    import pandas as pd

# Optional Selenium import
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    _SELENIUM_AVAILABLE = True
except Exception:
    _SELENIUM_AVAILABLE = False

# ==============================
# Config
# ==============================
ESPN_ENDPOINT: Dict[str, str] = {
    "MLB": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    "WNBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard",
    "CFB": "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard",
    "NFL": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    "NBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "NHL": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
    "CBB": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard",
    "CBB_STATS": "https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-college-basketball/statistics/byathlete",
}

ESPN_CBB_TEAM_INFO_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"

# Individual team stats pages (HTML) — add more as needed
ESPN_TEAM_STATS_PAGE: Dict[str, str] = {
    "WVU": "https://www.espn.com/mens-college-basketball/team/stats/_/id/277",  # West Virginia
    # Example placeholders you can fill out later:
    # "UK":  "https://www.espn.com/mens-college-basketball/team/stats/_/id/333",  # Kentucky
    # "Duke": "https://www.espn.com/mens-college-basketball/team/stats/_/id/150",  # Duke
}

USER_AGENT = {"User-Agent": "Mozilla/5.0"}

TEAM_ABBR_CACHE: Dict[int, str] = {}
TEAM_ABBR_LOCK = threading.Lock()

# Core API helpers
CORE_BASE = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball"
CORE_QUERY = {"lang": "en", "region": "us"}
ESPN_CBB_GROUP = int(os.getenv("ESPN_CBB_GROUP", "50"))  # NCAA Division I
DIVISION_ONE_TEAMS_URL = (
    f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"
    f"?group={ESPN_CBB_GROUP}&limit=500"
)

def _bool_env(key: str, default: str = "0") -> bool:
    return os.getenv(key, default) in ("1", "true", "TRUE", "yes", "on")

# ESPN season year (ending year of the season, default to 2026 for 2025-26)
try:
    CBB_SEASON = int(os.getenv("CBB_SEASON", "2026"))
except Exception:
    CBB_SEASON = 2026

try:
    SEASON_TYPE = int(os.getenv("SEASON_TYPE", "2"))  # 2 = regular season
except Exception:
    SEASON_TYPE = 2

# Strict mode defaults ON
STRICT_CURRENT_SEASON = _bool_env("STRICT_CURRENT_SEASON", "1")
STRICT_TEAM_WORKERS = max(1, int(os.getenv("STRICT_TEAM_WORKERS", "6")))

print(f"[cfg] Using season={CBB_SEASON}, seasonType={SEASON_TYPE}, strict={int(STRICT_CURRENT_SEASON)}")
if STRICT_CURRENT_SEASON:
    print("[cfg] STRICT_CURRENT_SEASON=ON — core roster traversal only.")
else:
    print("[cfg] STRICT_CURRENT_SEASON=OFF — legacy API/HTML fallbacks enabled.")
print("[cfg] Tip: For 2025-26 try CBB_SEASON=2026 first; if empty, rerun with 2025.")

# Selenium config
USE_SELENIUM = os.getenv("CBB_USE_SELENIUM", "0") in ("1", "true", "TRUE", "yes", "on")
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER", "/opt/homebrew/bin/chromedriver")

def _team_abbr_from_api(team_id: int) -> str:
    """Fetch and cache team abbreviation for a given ESPN team id.
    Returns uppercase abbr or "UNK" if not found."""
    with TEAM_ABBR_LOCK:
        if team_id in TEAM_ABBR_CACHE:
            return TEAM_ABBR_CACHE[team_id]
    try:
        url = f"{ESPN_CBB_TEAM_INFO_BASE}/{team_id}"
        r = requests.get(url, headers=USER_AGENT, timeout=15)
        r.raise_for_status()
        data = r.json() or {}
        # two possible shapes seen in ESPN APIs
        team = data.get("team") or data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", [{}])[0].get("team", {})
        abbr = (team.get("abbreviation") or team.get("shortDisplayName") or team.get("name") or "UNK").upper()
        abbr = TEAM_ABBR_FIX.get(abbr, abbr)
    except Exception:
        abbr = "UNK"
    with TEAM_ABBR_LOCK:
        TEAM_ABBR_CACHE[team_id] = abbr
    return abbr

# Normalize a few odd abbreviations if they pop up
TEAM_ABBR_FIX = {
    "WIS": "WISC",   # Wisconsin
}

def _get_json(
    url: str,
    params: Optional[Dict] = None,
    retries: int = 3,
    tag: str = "core",
    allow_404: bool = False,
) -> Optional[Dict]:
    """Generic JSON fetch with retries."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=USER_AGENT, timeout=25)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if allow_404 and status == 404:
                return None
            wait = 1.5 * (attempt + 1)
            print(f"[{tag}] warn: {exc} — retrying in {wait:.1f}s")
            time.sleep(wait)
    print(f"[{tag}] error: failed to fetch {url}")
    return None

def _extract_id_from_href(href: str, token: str) -> Optional[int]:
    """Pull numeric id out of a core-API $ref."""
    if not href:
        return None
    match = re.search(rf"/{token}/(\d+)", href)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None

def _stat_value(stats_map: Dict[str, float], keys: List[str], default: Optional[float] = 0.0) -> Optional[float]:
    for key in keys:
        if key in stats_map and stats_map[key] is not None:
            try:
                return float(stats_map[key])
            except Exception:
                continue
    return default

def fetch_roster_athletes(team_id: int, season: int, limit: int = 400) -> List[str]:
    """Return list of athlete $ref URLs for the roster pinned to this season."""
    refs: List[str] = []
    page = 1
    while True:
        params = {"limit": limit, "page": page, **CORE_QUERY}
        url = f"{CORE_BASE}/seasons/{season}/teams/{team_id}/athletes"
        data = _get_json(url, params=params, tag="roster")
        if not data:
            break
        for item in data.get("items", []):
            href = item.get("$ref") or item.get("href")
            if href:
                refs.append(href)
        page_count = data.get("pageCount") or 1
        if page >= page_count:
            break
        page += 1
        time.sleep(0.05)
    return refs

def fetch_athlete_stats_for_season(athlete_id: int, season: int, season_type: int) -> Optional[Dict[str, float]]:
    """Fetch season/seasonType pinned stats for one athlete."""
    if not athlete_id:
        return None
    params = {**CORE_QUERY}
    url = (
        f"{CORE_BASE}/seasons/{season}/types/{season_type}/athletes/{athlete_id}/statistics"
    )
    data = _get_json(url, params=params, tag="stats", allow_404=True)
    if not data:
        return None
    splits = data.get("splits") or {}
    categories = splits.get("categories") or []
    stats_map: Dict[str, float] = {}
    for cat in categories:
        for stat in cat.get("stats", []):
            name = stat.get("name")
            if not name:
                continue
            stats_map[name] = stat.get("value")

    if not stats_map:
        return None

    games_val = _stat_value(stats_map, ["gamesPlayed", "games"], default=0.0) or 0.0
    games = int(round(games_val)) if games_val else int(games_val)
    ppg = _stat_value(stats_map, ["avgPoints", "pointsPerGame"], default=0.0) or 0.0
    apg = _stat_value(stats_map, ["avgAssists", "assistsPerGame"], default=0.0) or 0.0
    rpg = _stat_value(stats_map, ["avgRebounds", "reboundsPerGame"], default=0.0) or 0.0
    three_avg = _stat_value(stats_map, ["avgThreePointFieldGoalsMade", "threePointFieldGoalsPerGame"], default=None)
    if three_avg is None:
        made_total = _stat_value(stats_map, ["threePointFieldGoalsMade"], default=None)
        if made_total is not None and games:
            three_avg = made_total / max(games, 1)
    three_pm = three_avg if three_avg is not None else 0.0

    return {
        "ppg": float(ppg),
        "apg": float(apg),
        "rpg": float(rpg),
        "three_pm": float(three_pm),
        "games": games,
    }

def _division_one_team_ids() -> List[int]:
    """Fetch all Division I team ids (group=50) to mirror the byathlete coverage."""
    data = _get_json(DIVISION_ONE_TEAMS_URL, tag="teams")
    ids: List[int] = []
    if not data:
        return ids
    sports = data.get("sports") or []
    for sport in sports:
        leagues = sport.get("leagues") or []
        for league in leagues:
            teams = league.get("teams") or []
            for entry in teams:
                team = entry.get("team") or {}
                tid = team.get("id")
                if tid:
                    try:
                        ids.append(int(tid))
                    except Exception:
                        continue
    return ids

def _strict_team_ids() -> List[int]:
    """Build the strict-mode team list (Division I + user extras, always include WVU)."""
    override = os.getenv("STRICT_TEAM_IDS", "").strip()
    team_ids: List[int] = []
    if override:
        for chunk in re.split(r"[,\s]+", override):
            if not chunk:
                continue
            try:
                team_ids.append(int(chunk))
            except Exception:
                continue
    else:
        team_ids = _division_one_team_ids()
    if not team_ids:
        team_ids = []
    team_ids.extend(_default_team_ids_from_constants())
    team_ids.extend(_parse_extra_team_ids())
    team_ids.extend(_parse_extra_team_urls())
    if 277 not in team_ids:
        team_ids.append(277)
    seen = set()
    deduped: List[int] = []
    for tid in team_ids:
        if tid in seen:
            continue
        seen.add(tid)
        deduped.append(tid)
    return deduped

def _row_from_athlete_ref(href: str, abbr: str, season: int, season_type: int) -> Optional[List]:
    athlete_data = _get_json(href, tag="athlete")
    if not athlete_data:
        return None
    name = (athlete_data.get("fullName") or athlete_data.get("displayName") or "Unknown").strip()
    athlete_id = (
        _extract_id_from_href(href, "athletes")
        or _extract_id_from_href(athlete_data.get("$ref", ""), "athletes")
    )
    if not athlete_id:
        try:
            athlete_id = int(athlete_data.get("id"))
        except Exception:
            athlete_id = None
    if not athlete_id:
        return None
    stats = fetch_athlete_stats_for_season(athlete_id, season, season_type)
    if not stats:
        return None
    if stats["games"] <= 0:
        return None
    time.sleep(0.02)
    return [name, abbr, stats["ppg"], stats["apg"], stats["rpg"], stats["three_pm"], stats["games"]]

def fetch_players_via_rosters(team_ids: List[int], season: int, season_type: int) -> List[List]:
    """Strict-mode path: traverse rosters → per-athlete stats for a fixed season."""
    rows: List[List] = []
    total = len(team_ids)

    def _process_team(team_id: int) -> List[List]:
        abbr = _team_abbr_from_api(team_id)
        roster_refs = fetch_roster_athletes(team_id, season)
        team_rows: List[List] = []
        if not roster_refs:
            print(f"[strict] team {team_id} ({abbr}) season {season}: no roster items")
            return team_rows
        for href in roster_refs:
            row = _row_from_athlete_ref(href, abbr, season, season_type)
            if row:
                team_rows.append(row)
        print(f"   ↳ team {team_id} {abbr} season {season}: {len(team_rows)} rows")
        return team_rows

    with ThreadPoolExecutor(max_workers=STRICT_TEAM_WORKERS) as executor:
        future_map = {executor.submit(_process_team, tid): tid for tid in team_ids}
        completed = 0
        for future in as_completed(future_map):
            team_id = future_map[future]
            try:
                team_rows = future.result()
            except Exception as exc:
                print(f"[strict] team {team_id} error: {exc}")
                team_rows = []
            rows.extend(team_rows)
            completed += 1
            print(f"[strict] progress {completed}/{total} — team {team_id}: +{len(team_rows)} rows")
    return rows

# ==============================
# Core: ESPN API (paged)
# ==============================
def _extract_stat(categories: List[Dict], group: str, index: int, default=0.0):
    """
    ESPN returns categories like:
      [{"name":"offensive","totals":[...]} , {"name":"general","totals":[...]}]
    The exact slot order can shift. We stay defensive:
    """
    for cat in categories or []:
        if (cat.get("name") or "").lower() == group.lower():
            totals = cat.get("totals") or []
            if 0 <= index < len(totals):
                try:
                    return float(totals[index])
                except Exception:
                    return default
    return default

def _extract_game_number(text: str):
    """
    Extract trailing number from a GAME-##### string pattern.
    Returns integer if found, else None.
    """
    if not text:
        return None
    match = re.search(r"GAME-(\d+)$", text)
    if match:
        return int(match.group(1))
    return None

def fetch_players_api(limit_per_page: int = 50, max_pages: int = 200) -> List[List]:
    """
    Returns rows: [Player, Team, PPG, APG, RPG, 3PM, Games]
    Sorted by PPG desc via the API query.
    """
    if STRICT_CURRENT_SEASON:
        print("[strict] Skipping statistics/byathlete API (STRICT_CURRENT_SEASON=1).")
        return []
    print("🚀 CBB: fetching from ESPN API…")
    page = 1
    players: List[List] = []

    while page <= max_pages:
        url = (
            f"{ESPN_ENDPOINT['CBB_STATS']}?region=us&lang=en&contentorigin=espn"
            f"&season={CBB_SEASON}&seasontype=2&category=offensive"
            f"&page={page}&limit={limit_per_page}&sort=offensive.avgPoints:desc"
        )
        try:
            r = requests.get(url, headers=USER_AGENT, timeout=20)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"⚠️ API page {page} error: {e}")
            break

        data = r.json() or {}
        athletes = data.get("athletes") or []
        if not athletes:
            print("✅ No more rows from API.")
            break

        for p in athletes:
            ath = p.get("athlete") or {}
            name = ath.get("displayName") or "Unknown"
            teams = (ath.get("teams") or [{}])
            team_id = None
            try:
                raw_id = (teams[0] or {}).get("id")
                if raw_id is not None:
                    team_id = int(raw_id)
            except Exception:
                team_id = None
            abbr = (teams[0] or {}).get("abbreviation") or ""
            if not abbr and team_id:
                abbr = _team_abbr_from_api(team_id)
            if not abbr:
                abbr = "UNK"
            abbr = TEAM_ABBR_FIX.get(abbr, abbr).upper()

            cats = p.get("categories") or []
            # Notes:
            # -"offensive" index 0 is usually PPG
            # -"offensive" index 10 often APG
            # -"general"   index 12 often RPG
            # -"offensive" index 4  often 3PM
            # These match what your old code used; we keep them (defensive fallback).
            ppg   = _extract_stat(cats, "offensive", 0, 0.0)
            apg   = _extract_stat(cats, "offensive", 10, 0.0)
            rpg   = _extract_stat(cats, "general",   12, 0.0)
            three = _extract_stat(cats, "offensive", 4, 0.0)

            # "general" index 15 was your old GP slot; default to 1 if missing
            try:
                gp = int(_extract_stat(cats, "general", 15, 1.0))
            except Exception:
                gp = 1

            players.append([name, abbr, ppg, apg, rpg, three, gp])

        print(f"  • Page {page} (season {CBB_SEASON}) → total rows: {len(players)}")
        page += 1
        time.sleep(0.2)

    return players

def fetch_players_for_team(team_id: int, limit_per_page: int = 200) -> List[List]:
    """
    Return rows [Player, Team, PPG, APG, RPG, 3PM, Games] for ONE team (ESPN team id).
    Uses the same endpoint, filtered by team, so the JSON shape matches your main fetch.
    Only pulls the configured CBB_SEASON.
    """
    if STRICT_CURRENT_SEASON:
        print(f"[strict] Skipping legacy team pull for {team_id} (STRICT_CURRENT_SEASON=1).")
        return []
    print(f"➕ CBB: fetching extra team {team_id}…")
    fixed_abbr = _team_abbr_from_api(team_id)
    page = 1
    rows: List[List] = []

    while True:
        url = (
            f"{ESPN_ENDPOINT['CBB_STATS']}?region=us&lang=en&contentorigin=espn"
            f"&season={CBB_SEASON}&seasontype=2&category=offensive&team={team_id}"
            f"&page={page}&limit={limit_per_page}&sort=offensive.avgPoints:desc"
        )
        try:
            r = requests.get(url, headers=USER_AGENT, timeout=20)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"⚠️ team {team_id} error: {e}")
            break

        data = r.json() or {}
        athletes = data.get("athletes") or []
        if not athletes:
            break

        for p in athletes:
            ath   = p.get("athlete") or {}
            name  = ath.get("displayName") or "Unknown"
            teams_list = ath.get("teams") or []
            team_ids = []
            for t in teams_list:
                try:
                    if t and t.get("id") is not None:
                        team_ids.append(int(t.get("id")))
                except Exception:
                    pass
            if team_id not in team_ids:
                # Skip cross-listed or stale records not actually on this team
                continue
            abbr = fixed_abbr

            cats  = p.get("categories") or []
            ppg   = _extract_stat(cats, "offensive", 0, 0.0)
            apg   = _extract_stat(cats, "offensive", 10, 0.0)
            rpg   = _extract_stat(cats, "general",   12, 0.0)
            three = _extract_stat(cats, "offensive", 4,  0.0)
            try:
                gp = int(_extract_stat(cats, "general", 15, 1.0))
            except Exception:
                gp = 1

            rows.append([name, abbr, ppg, apg, rpg, three, gp])

        if len(athletes) < limit_per_page:
            break
        page += 1
        time.sleep(0.2)

    print(f"   ↳ team {team_id} season {CBB_SEASON}: {len(rows)} rows")
    return rows

def _html_metric(df: pd.DataFrame, candidates: List[str]) -> str:
    """
    Return the first column name in df.columns that matches any of the candidates (case-insensitive).
    Returns empty string if none found.
    """
    cols_upper = [str(c).strip().upper() for c in df.columns]
    for candidate in candidates:
        candidate_upper = candidate.upper()
        if candidate_upper in cols_upper:
            # Return original column name as in df.columns to preserve case
            idx = cols_upper.index(candidate_upper)
            return df.columns[idx]
    return ""

def _resolve_team_id_from_team_page(url: str) -> int:
    try:
        resp = requests.get(url, headers=USER_AGENT, timeout=20)
        resp.raise_for_status()
        text = resp.text or ""
    except Exception:
        return -1
    patterns = [
        r"teamId\"\s*:\s*(\d+)",
        r"\"id\"\s*:\s*(\d+)\s*,\s*\"uid\"\s*:\s*\"s:40~l:41~t:\d+\"",
        r"/id/(\d+)(/|$)",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            try:
                tid = int(m.group(1))
                print(f"[resolve] {url} → teamId={tid}")
                return tid
            except Exception:
                pass
    return -1

def fetch_players_from_team_stats_html(team_abbr: str, url: str) -> List[List]:
    """
    Fetch and parse player stats from ESPN team stats HTML page.
    Returns rows [Player, Team, PPG, APG, RPG, 3PM, Games].
    """
    try:
        response = requests.get(url, headers=USER_AGENT, timeout=30)
        response.raise_for_status()
        html = response.text
    except Exception as e:
        print(f"[html] {team_abbr} error fetching HTML: {e}")
        return []

    try:
        dfs = pd.read_html(StringIO(html))
    except Exception as e:
        print(f"[html] {team_abbr} error parsing HTML tables: {e}")
        return []

    rows: List[List] = []
    for df in dfs:
        # Normalize columns
        df_columns_upper = [str(c).strip().upper() for c in df.columns]
        if not any(col == "PLAYER" for col in df_columns_upper):
            continue  # skip tables without PLAYER column

        # Map metric columns
        col_player = ""
        for c in df.columns:
            if str(c).strip().upper() == "PLAYER":
                col_player = c
                break
        if not col_player:
            continue

        col_ppg = _html_metric(df, ["PTS", "PPG"])
        col_apg = _html_metric(df, ["AST", "APG"])
        col_rpg = _html_metric(df, ["REB", "RPG", "TRB"])
        col_3pm = _html_metric(df, ["3PM", "3PTM", "3-PT MADE"])
        col_gp  = _html_metric(df, ["GP", "G"])

        for _, row in df.iterrows():
            try:
                player_name = str(row[col_player]).strip()
            except Exception:
                player_name = "Unknown"

            def parse_float(val):
                try:
                    if pd.isna(val):
                        return 0.0
                    return float(str(val).replace(',', '').strip())
                except Exception:
                    return 0.0

            def parse_int(val):
                try:
                    if pd.isna(val):
                        return 0
                    return int(float(str(val).replace(',', '').strip()))
                except Exception:
                    return 0

            ppg = parse_float(row[col_ppg]) if col_ppg else 0.0
            apg = parse_float(row[col_apg]) if col_apg else 0.0
            rpg = parse_float(row[col_rpg]) if col_rpg else 0.0
            three = parse_float(row[col_3pm]) if col_3pm else 0.0
            gp = parse_int(row[col_gp]) if col_gp else 0

            rows.append([player_name, team_abbr, ppg, apg, rpg, three, gp])

    # Selenium fallback if enabled and no rows (only when strict mode is off)
    if (not rows) and (not STRICT_CURRENT_SEASON) and USE_SELENIUM and _SELENIUM_AVAILABLE:
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            service = Service(CHROMEDRIVER_PATH)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.get(url)
            import time as _t
            _t.sleep(8)
            html2 = driver.page_source
            driver.quit()
            dfs2 = pd.read_html(StringIO(html2))
            for df in dfs2:
                df_columns_upper = [str(c).strip().upper() for c in df.columns]
                if not any(col == "PLAYER" for col in df_columns_upper):
                    continue
                col_player = next((c for c in df.columns if str(c).strip().upper() == "PLAYER"), None)
                if not col_player:
                    continue
                col_ppg = _html_metric(df, ["PTS", "PPG"]) ; col_apg = _html_metric(df, ["AST", "APG"]) ; col_rpg = _html_metric(df, ["REB", "RPG", "TRB"]) ; col_3pm = _html_metric(df, ["3PM", "3PTM", "3-PT MADE"]) ; col_gp = _html_metric(df, ["GP", "G"]) 
                for _, row in df.iterrows():
                    player_name = str(row[col_player]).strip()
                    def parse_float(val):
                        try:
                            if pd.isna(val):
                                return 0.0
                            return float(str(val).replace(',', '').strip())
                        except Exception:
                            return 0.0
                    def parse_int(val):
                        try:
                            if pd.isna(val):
                                return 0
                            return int(float(str(val).replace(',', '').strip()))
                        except Exception:
                            return 0
                    ppg = parse_float(row[col_ppg]) if col_ppg else 0.0
                    apg = parse_float(row[col_apg]) if col_apg else 0.0
                    rpg = parse_float(row[col_rpg]) if col_rpg else 0.0
                    three = parse_float(row[col_3pm]) if col_3pm else 0.0
                    gp = parse_int(row[col_gp]) if col_gp else 0
                    rows.append([player_name, team_abbr, ppg, apg, rpg, three, gp])
            if rows:
                print(f"[html][selenium] {team_abbr} parsed {len(rows)} players from team stats page")
        except Exception as se:
            print(f"[html][selenium] error: {se}")

    print(f"[html] {team_abbr} parsed {len(rows)} players from team stats page")
    return rows

def _parse_extra_team_ids() -> List[int]:
    """
    Read EXTRA_CBB_TEAM_IDS from env (comma/space/semicolon separated),
    e.g., '277' or '277, 123, 456'
    """
    raw = os.getenv("EXTRA_CBB_TEAM_IDS", "") or ""
    parts = [p.strip() for p in raw.replace(";", ",").replace("|", ",").split(",") if p.strip()]
    ids: List[int] = []
    for p in parts:
        try:
            ids.append(int(p))
        except ValueError:
            pass
    return ids

def _parse_extra_team_urls() -> List[int]:
    """
    Read EXTRA_CBB_TEAM_URLS from env (comma/semicolon/pipe/newline/space separated)
    and extract ESPN team IDs from a variety of URL shapes. Examples supported:
      • https://www.espn.com/mens-college-basketball/team/_/id/277/wisconsin-badgers
      • https://site.web.api.espn.com/.../byathlete?team=333
      • any string containing a standalone number will use the first number as a fallback.
    """
    raw = os.getenv("EXTRA_CBB_TEAM_URLS", "") or ""
    if not raw.strip():
        return []
    # split on common delimiters and newlines
    parts = []
    for chunk in re.split(r"[\n,;|]", raw):
        s = chunk.strip()
        if s:
            parts.append(s)
    ids: List[int] = []
    for p in parts:
        # pattern 1: /id/277/
        m = re.search(r"/id/(\d+)(/|$)", p)
        if m:
            try:
                ids.append(int(m.group(1)))
                continue
            except ValueError:
                pass
        # pattern 2: team=277
        m = re.search(r"[?&]team=(\d+)(?:&|$)", p)
        if m:
            try:
                ids.append(int(m.group(1)))
                continue
            except ValueError:
                pass
        # fallback: first number in the string
        m = re.search(r"(\d+)", p)
        if m:
            try:
                ids.append(int(m.group(1)))
                continue
            except ValueError:
                pass
    return ids

def _default_team_ids_from_constants() -> List[int]:
    ids: List[int] = []
    for name, url in (ESPN_TEAM_STATS_PAGE or {}).items():
        # pattern 1: /id/277/
        m = re.search(r"/id/(\d+)(/|$)", url)
        if m:
            try:
                ids.append(int(m.group(1)))
                continue
            except ValueError:
                pass
        # pattern 2: team=277
        m = re.search(r"[?&]team=(\d+)(?:&|$)", url)
        if m:
            try:
                ids.append(int(m.group(1)))
                continue
            except ValueError:
                pass
        # fallback: first number in the string
        m = re.search(r"(\d+)", url)
        if m:
            try:
                ids.append(int(m.group(1)))
                continue
            except ValueError:
                pass
    return ids

def save_cbb_players_to_csv(out_path: str = "cbb_players_stats.csv"):
    if STRICT_CURRENT_SEASON:
        team_ids = _strict_team_ids()
        print(f"[strict] Targeting {len(team_ids)} Division I team(s) for season {CBB_SEASON}.")
        rows = fetch_players_via_rosters(team_ids, CBB_SEASON, SEASON_TYPE)
        if ESPN_TEAM_STATS_PAGE:
            print("[strict] HTML/team-page fallbacks disabled (STRICT_CURRENT_SEASON=ON)")
    else:
        # 1) Legacy global pull
        rows = fetch_players_api()

        # Defaults from constants + overrides from env vars
        extra_ids = _default_team_ids_from_constants()
        env_ids = _parse_extra_team_ids()
        extra_ids.extend(env_ids)
        extra_ids_from_urls = _parse_extra_team_urls()
        if extra_ids_from_urls:
            print(f"🌐 Parsed {len(extra_ids_from_urls)} team id(s) from EXTRA_CBB_TEAM_URLS: {extra_ids_from_urls}")
            extra_ids.extend(extra_ids_from_urls)
        # de-dupe extra ids while preserving order
        seen_ids = set()
        dedup_extra_ids: List[int] = []
        for tid in extra_ids:
            if tid not in seen_ids:
                dedup_extra_ids.append(tid)
                seen_ids.add(tid)
        extra_ids = dedup_extra_ids

        for tid in extra_ids:
            print(f"➕ including extra team id {tid}")
            rows.extend(fetch_players_for_team(tid))
            if tid == 277:
                print("[debug] WVU rows added:", sum(1 for _ in rows if _[1] == 'WVU'))

        # HTML fallback/augment from team stats pages
        for tag, url in ESPN_TEAM_STATS_PAGE.items():
            # infer team id and abbr
            m = re.search(r"/id/(\d+)(/|$)", url)
            team_id = int(m.group(1)) if m else None
            # Try API pull first using resolved id
            tried_ids = []
            def try_team(team_id_candidate: int) -> List[List]:
                if not team_id_candidate or team_id_candidate in tried_ids:
                    return []
                tried_ids.append(team_id_candidate)
                api_rows = fetch_players_for_team(team_id_candidate)
                if api_rows:
                    ab = _team_abbr_from_api(team_id_candidate)
                    print(f"[team] API rows for {ab} (id={team_id_candidate}): {len(api_rows)}")
                    return api_rows
                return []

            api_rows = try_team(team_id if team_id is not None else 0)
            if not api_rows:
                # Re-resolve id from the HTML source of the team page (handles cases where path id is stale)
                resolved = _resolve_team_id_from_team_page(url)
                api_rows = try_team(resolved)

            # If API returned rows, merge them; otherwise attempt HTML parsing as last resort
            abbr = _team_abbr_from_api(tried_ids[-1]) if tried_ids else tag.upper()
            if api_rows:
                merge_rows = api_rows
            else:
                html_rows = fetch_players_from_team_stats_html(abbr, url)
                merge_rows = html_rows

            if merge_rows:
                before = len(rows)
                by_name_team = {(r[0].strip().lower(), r[1].strip().upper()): i for i, r in enumerate(rows)}
                by_name_only = {}
                for i, r in enumerate(rows):
                    key_name = r[0].strip().lower()
                    if key_name not in by_name_only:
                        by_name_only[key_name] = i
                updated = 0
                added = 0
                for h in merge_rows:
                    name_key = h[0].strip().lower()
                    team_key = h[1].strip().upper()
                    k_same_team = (name_key, team_key)
                    if k_same_team in by_name_team:
                        rows[by_name_team[k_same_team]] = h
                        updated += 1
                    else:
                        if name_key in by_name_only and team_key != rows[by_name_only[name_key]][1].strip().upper():
                            # Do not reassign existing player to new team
                            continue
                        rows.append(h)
                        added += 1
                print(f"[merge] {abbr}: merged ({len(merge_rows)} src rows → updated={updated}, added={added})")
            else:
                print(f"[merge] {tag}: no rows found via API or HTML; skipped")

    if not rows:
        print("❌ CBB: no rows scraped.")
        return

    # 3) De-duplicate by (player name, team abbr)
    seen = set()
    dedup: List[List] = []
    for name, abbr, ppg, apg, rpg, three, gp in rows:
        key = (name.strip().lower(), abbr.strip().upper())
        if key in seen:
            continue
        seen.add(key)
        dedup.append([name, abbr, ppg, apg, rpg, three, gp])

    dedup.sort(key=lambda r: (r[1], r[0]))
    print(f"[sort] Sorted {len(dedup)} rows by Team, then Player.")

    df = pd.DataFrame(dedup, columns=["Player","Team","PPG","APG","RPG","3PM","Games"])
    df.to_csv(out_path, index=False)
    print(f"💾 Saved {len(df):,} CBB rows → {out_path}")
    try:
        wvu_df = df[df["Team"] == "WVU"]
        wvu_sample = wvu_df[["Player", "PPG", "APG", "RPG"]].head(5).to_dict("records")
        print(f"[sanity] WVU rows in CSV: {len(wvu_df)}; sample: {wvu_sample}")
        top5_ppg = df.sort_values("PPG", ascending=False).head(5)[["Player", "Team", "PPG", "APG", "RPG"]]
        print(f"[sanity] Top 5 PPG: {top5_ppg.to_dict('records')}")
    except Exception as exc:
        print(f"[sanity] warning: unable to compute WVU/top5 sample ({exc})")

def main():
    save_cbb_players_to_csv()

if __name__ == "__main__":
    main()
