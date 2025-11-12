from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
SCHEDULE_JSON = DATA_DIR / "schedule.json"
SCHEDULE_CACHE_JSON = DATA_DIR / "schedule_cache.json"
HORIZON_DAYS = int(os.getenv("SCHEDULE_HORIZON_DAYS", "4"))

DEFAULT_SPORTS = ["NHL", "NFL", "NBA", "CFB", "MLB", "WNBA"]

SPORT_STATS: Dict[str, List[str]] = {
    "NHL": ["Total Goals", "Shots on Goal", "Points"],
    "NFL": ["Total Scrimmage Yards", "Receptions", "Rushing TD, Receiving TD"],
    "NBA": ["PPG", "APG", "RPG", "3PM"],
    "MLB": ["TB", "RBI"],
    "WNBA": ["PPG", "APG", "RPG", "3PM"],
    "CFB": ["Total Scrimmage Yards", "Receptions", "Rushing TD, Receiving TD"],
}

PSP_STATS: Dict[str, List[str]] = {
    "NHL": ["Shots on Goal", "Points", "Hits", "Saves"],
    "NFL": ["Total Scrimmage Yards", "Receptions", "Rushing TD, Receiving TD"],
    "NBA": ["PPG", "APG", "RPG", "3PM"],
    "MLB": ["TB", "RBI", "K"],
    "WNBA": ["PPG", "APG", "RPG", "3PM"],
    "CFB": ["Total Scrimmage Yards", "Receptions", "Rushing TD, Receiving TD"],
}

ESPN_ENDPOINT = {
    "MLB": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    "WNBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard",
    "CFB": "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard",
    "NFL": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    "NBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "NHL": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

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
    "NFL": {
        "WAS":"Commanders","GB":"Packers","NYG":"Giants","DAL":"Cowboys",
        "CLE":"Browns","BAL":"Ravens","JAX":"Jaguars","CIN":"Bengals",
        "CHI":"Bears","DET":"Lions","NE":"Patriots","MIA":"Dolphins",
        "SF":"49ers","NO":"Saints","BUF":"Bills","NYJ":"Jets",
        "SEA":"Seahawks","PIT":"Steelers","LAR":"Rams","TEN":"Titans",
        "CAR":"Panthers","ARI":"Cardinals","DEN":"Broncos","IND":"Colts",
        "PHI":"Eagles","KC":"Chiefs","ATL":"Falcons","MIN":"Vikings",
        "TB":"Buccaneers","HOU":"Texans","LAC":"Chargers","LV":"Raiders",
    },
}

NAME_TO_ABBR: Dict[str, Dict[str, str]] = {}
for sport, abbr_map in TEAM_NAME_MAPS.items():
    rev: Dict[str, str] = {}
    for abbr, display in abbr_map.items():
        rev[display.upper()] = abbr
        rev[display.replace(" ", "").upper()] = abbr
        rev[display.split()[-1].upper()] = abbr
    NAME_TO_ABBR[sport] = rev

ESPN_TO_CANON_ABBR: Dict[str, Dict[str, str]] = {
    "NBA": {"NY": "NYK", "GS": "GSW", "NO": "NOP", "SA": "SAS", "WSH": "WAS"},
    "WNBA": {"NY": "NYL", "LV": "LVA", "LA": "LAS", "PHX": "PHO"},
    "NHL": {"NJ": "NJD", "LA": "LAK", "TB": "TBL", "SJ": "SJS"},
    "NFL": {"WSH": "WAS", "ARZ": "ARI"},
}

def _map_name_to_abbr(sport: str, name: str) -> str:
    rev = NAME_TO_ABBR.get(sport.upper(), {})
    key = (name or "").strip().upper()
    if not key:
        return ""
    if key in rev:
        return rev[key]
    parts = key.replace("-", " ").split()
    if parts:
        tail = parts[-1]
        if tail in rev:
            return rev[tail]
    return ""


def _coerce_abbr(sport: str, abbr: str, display: str) -> str:
    sport_up = sport.upper()
    canon = abbr.upper()
    if canon in TEAM_NAME_MAPS.get(sport_up, {}):
        return canon
    alias = ESPN_TO_CANON_ABBR.get(sport_up, {}).get(canon)
    if alias:
        return alias
    mapped = _map_name_to_abbr(sport_up, display)
    if mapped:
        return mapped
    return canon


def _today_yyyymmdd() -> str:
    return datetime.utcnow().strftime("%Y%m%d")


def _normalize_team(entry: dict, sport: str) -> str:
    team = entry.get("team", {}) if isinstance(entry, dict) else {}
    abbr = (team.get("abbreviation") or "").strip().upper()
    display = (team.get("shortDisplayName") or team.get("displayName") or team.get("name") or "").strip()
    if abbr:
        return _coerce_abbr(sport, abbr, display)
    mapped = _map_name_to_abbr(sport, display)
    if mapped:
        return mapped
    return display.upper()


def _fetch_scoreboard(sport: str, date_str: str) -> dict:
    url = ESPN_ENDPOINT.get(sport.upper())
    if not url:
        return {}
    params = {"dates": date_str}
    headers = {"User-Agent": USER_AGENT, "Referer": "https://www.espn.com/"}
    for attempt in range(4):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=20)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.json()
        except Exception:
            time.sleep(1.2 * (attempt + 1))
    return {}


def _extract_games(payload: dict, sport: str) -> Iterable[dict]:
    for event in payload.get("events", []):
        competitions = event.get("competitions") or []
        comp = competitions[0] if competitions else {}
        competitors = comp.get("competitors") or []
        if len(competitors) < 2:
            continue
        team1 = _normalize_team(competitors[0], sport)
        team2 = _normalize_team(competitors[1], sport)
        if not team1 or not team2:
            continue
        yield {
            "event_id": event.get("id") or comp.get("id"),
            "teams": [team1, team2],
            "start_time": event.get("date"),
        }


def build_schedule_for_date(
    date_str: str,
    *,
    sports: Optional[Iterable[str]] = None,
) -> List[dict]:
    date_str = date_str.strip()
    sports_list = [s.upper() for s in sports or os.getenv("SCHEDULE_SPORTS", "").split(",") if s.strip()] or DEFAULT_SPORTS
    schedule: List[dict] = []
    for sport in sports_list:
        stats = SPORT_STATS.get(sport.upper(), [])
        if not stats:
            continue
        payload = _fetch_scoreboard(sport, date_str)
        for game in _extract_games(payload, sport):
            for stat in stats:
                schedule.append(
                    {
                        "page_id": f"{sport}-{game['event_id']}-{stat}".replace(" ", "-"),
                        "sport": sport.upper(),
                        "stat": stat,
                        "teams": game["teams"],
                        "event_id": game["event_id"],
                        "start_time": game["start_time"],
                        "date": date_str,
                        "psp": False,
                    }
                )
        psp_stats = PSP_STATS.get(sport.upper(), [])
        for stat in psp_stats:
            schedule.append(
                {
                    "page_id": f"{sport}-PSP-{stat}-{date_str}".replace(" ", "-"),
                    "sport": sport.upper(),
                    "stat": stat,
                    "teams": [],
                    "event_id": None,
                    "start_time": None,
                        "date": date_str,
                        "psp": True,
                    }
                )
    return schedule


def write_schedule(rows: List[dict], path: Path = SCHEDULE_JSON) -> None:
    path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def refresh_schedule() -> List[dict]:
    horizon = max(1, HORIZON_DAYS)
    today = datetime.utcnow()
    aggregated: Dict[str, List[dict]] = {}
    all_rows: List[dict] = []
    for offset in range(horizon):
        date_str = (today + timedelta(days=offset)).strftime("%Y%m%d")
        rows = build_schedule_for_date(date_str)
        aggregated[date_str] = rows
        write_schedule(rows, DATA_DIR / f"schedule_{date_str}.json")
        if offset == 0:
            write_schedule(rows, SCHEDULE_JSON)
        all_rows.extend(rows)
    SCHEDULE_CACHE_JSON.write_text(json.dumps({"dates": aggregated}, indent=2), encoding="utf-8")
    return all_rows


if __name__ == "__main__":
    result = refresh_schedule()
    print(f"[schedule] captured {len(result)} rows -> {SCHEDULE_JSON}")
