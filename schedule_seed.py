#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Schedule → Notion seeder (ESPN, one day)
- REGULAR: one Notion row per game per configured stat (see SPORT_STATS)
- PSP: one Notion row per sport/stat with a comma-separated list of teams
- De-dupe: only skips rows already created TODAY (UTC)

Env / flags:
  NOTION_TOKEN        (optional if your fallback works)
  DATABASE_ID         (required)  -> regular games DB
  PSP_DATABASE_ID     (required)  -> PSP DB
  DATE=YYYYMMDD       (default: today)
  SPORTS              (comma list; default: NHL,NFL,NBA,CFB,MLB,WNBA)
  VERBOSE=0|1         (default: 0)
"""

import os, sys, time
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Set, Optional

import requests
from notion_client import Client
from datetime import timedelta

# ---------- Config ----------

MLB_TEAM_COUNT = 30

# PSP display order (top-to-bottom)
PSP_ORDER = ["NHL", "NFL", "NBA", "CFB", "MLB", "WNBA"]

PSP_SPORT_RANK = {
    "MLB": 6, "WNBA": 7, "CFB": 4, "NFL": 2, "NHL": 1, "NBA": 3
}

PSP_STAT_RANK: Dict[str, Dict[str, int]] = {
    "NHL": {"Shots on Goal": 1, "Points": 2, "Hits": 3, "Saves": 4},
    "NFL": {"Total Scrimmage Yards": 1, "Receptions": 2, "Rushing TD, Receiving TD": 3},
    "NBA": {"PPG": 1, "APG": 2, "RPG": 3, "3PM": 4},
    #"MLB": {"TB": 1, "RBI": 2, "K": 3},
    #"WNBA": {"PPG": 1, "APG": 2, "RPG": 3, "3PM": 4},
    "CFB": {"Total Scrimmage Yards": 1, "Receptions": 2, "Rushing TD, Receiving TD": 3},
}

# Per-sport REGULAR stat menus
SPORT_STATS: Dict[str, List[str]] = {
    "NHL": ["Total Goals", "Shots", "Points"],
    "NFL": ["Total Scrimmage Yards", "Receptions", "Rushing TD, Receiving TD"],
    "NBA": ["PPG", "APG", "RPG", "3PM"],
    #"MLB": ["TB", "RBI"],
    #"WNBA": ["PPG", "APG", "RPG", "3PM"],
    "CFB": ["Total Scrimmage Yards", "Receptions", "Rushing TD, Receiving TD"],
}

# Per-sport PSP stat menus
PSP_SPORT_STATS: Dict[str, List[str]] = {
    "NHL": ["Shots on Goal", "Points", "Hits", "Saves"],
    "NFL": ["Total Scrimmage Yards", "Receptions", "Rushing TD, Receiving TD"],
    "NBA": ["PPG", "APG", "RPG", "3PM"],
    #"MLB": ["TB", "RBI", "K"],
    #"WNBA": ["PPG", "APG", "RPG", "3PM"],
    "CFB": ["Total Scrimmage Yards", "Receptions", "Rushing TD, Receiving TD"],
}

# ESPN scoreboard endpoints
ESPN_ENDPOINT: Dict[str, str] = {
    "MLB": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    "WNBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard",
    "CFB": "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard",
    "NFL": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    "NBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "NHL": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
}

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
        "UCF": "UCF", "M-OH": "RedHawks", "FSU": "Florida State", "LOU": "Louisville",
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

# Build NAME -> ABBR reverse maps (e.g., "Yankees" -> "NYY")
NAME_TO_ABBR: Dict[str, Dict[str, str]] = {}
for sport_key, abbr2name in TEAM_NAME_MAPS.items():
    rev = {v.upper(): k for k, v in abbr2name.items()}
    NAME_TO_ABBR[sport_key] = rev

# Canonical abbr keys per sport (the ones StatMuse accepts for your queries)
CANON_ABBRS: Dict[str, Set[str]] = {s: set(m.keys()) for s, m in TEAM_NAME_MAPS.items()}

# ESPN → canonical (StatMuse-ready) abbreviation aliases
ESPN_TO_CANON_ABBR: Dict[str, Dict[str, str]] = {
    # NHL: ESPN uses NJ/LA/TB/SJ sometimes; StatMuse prefers NJD/LAK/TBL/SJS
    "NHL": {"NJ":"NJD", "LA":"LAK", "TB":"TBL", "SJ":"SJS"},
    # NBA: ESPN short-codes → your canonical
    "NBA": {"SA":"SAS", "GS":"GSW", "NO":"NOP", "NY":"NYK", "WSH":"WAS"},
    # NFL: common variants
    "NFL": {"WSH":"WAS", "NJJ":"NYJ"},
    # WNBA: ESPN short-codes → your canonical
    "WNBA":{"LV":"LVA", "NY":"NYL", "LA":"LAS", "PHX":"PHO"},
    # (MLB looks aligned; add if you bump into any)
}

def normalize_abbr(sport: str, abbr: str, display_name: str = "") -> str:
    """Map ESPN's abbr to our canonical abbr; if still unknown, try by team name."""
    s = (sport or "").upper()
    a = (abbr or "").upper()

    # 1) sport-specific alias table
    a = ESPN_TO_CANON_ABBR.get(s, {}).get(a, a)

    # 2) If still not recognized as one of our canonical keys, try mapping by name
    if a not in CANON_ABBRS.get(s, set()) and display_name:
        mapped = _name_to_abbr(s, display_name)
        if isinstance(mapped, str):
            m = mapped.upper()
            if m in CANON_ABBRS.get(s, set()):
                return m
    return a

def _name_to_abbr(sport: str, display_name: str) -> str:
    """Fallback: map display name like 'New York Yankees'/'Yankees' to 'NYY' if we know it."""
    rev = NAME_TO_ABBR.get(sport, {})
    dn = (display_name or "").upper()
    # try exact, then last word (e.g., 'Yankees'), then strip city prefixes
    parts = dn.split()
    tail = parts[-1] if parts else ""
    return rev.get(dn) or rev.get(tail) or display_name

# ---------- Env ----------

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
DATABASE_ID     = os.getenv("DATABASE_ID",  "").strip()
PSP_DATABASE_ID = os.getenv("PSP_DATABASE_ID","").strip()
DATE_STR = os.getenv("DATE", "").strip()  # YYYYMMDD
SPORTS = [s.strip().upper() for s in (os.getenv("SPORTS", "NHL,NFL,NBA,CFB,MLB,WNBA").split(",")) if s.strip()]
VERBOSE = int(os.getenv("VERBOSE", "0"))
TEAMS_AS_ABBR = int(os.getenv("TEAMS_AS_ABBR", "1"))  # 1 = use abbreviations, 0 = full names

if not (NOTION_TOKEN and DATABASE_ID and PSP_DATABASE_ID):
    raise SystemExit("Missing Notion configuration. Set NOTION_TOKEN, DATABASE_ID, and PSP_DATABASE_ID.")

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# ---------- Helpers ----------

def _yyyymmdd_tomorrow() -> str:
    return (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")

def _iso_utc_start_of_day(yyyymmdd: str) -> str:
    dt = datetime.strptime(yyyymmdd, "%Y%m%d").replace(tzinfo=timezone.utc)
    sod = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    return sod.isoformat()

def _fetch_scoreboard(sport: str, yyyymmdd: str) -> Dict:
    url = ESPN_ENDPOINT[sport]
    params = {"dates": yyyymmdd}

    hdrs = {"User-Agent": UA, "Referer": "https://www.espn.com/"}
    for attempt in range(4):
        try:
            r = requests.get(url, params=params, headers=hdrs, timeout=20)
            r.encoding = "utf-8"
            r.raise_for_status()
            return r.json()
        except Exception as e:
            wait = 1.5 * (attempt + 1)
            if VERBOSE:
                print(f"[espn:{sport}] warn: {e} — retrying in {wait:.1f}s…")
            time.sleep(wait)
    if VERBOSE:
        print(f"[espn:{sport}] ❌ failed after retries")
    return {}

def _extract_games(data: Dict, sport: str, prefer_abbr: bool = True) -> List[Tuple[str, str]]:
    """
    Return list of (Team1, Team2).
    For WNBA (and any sport you add to CUSTOM_ABBR_FIRST), prefer your NAME->ABBR map
    over ESPN's two-letter abbreviation. That enforces LVA, NYL, GSV, etc.
    """
    games: List[Tuple[str, str]] = []

    CUSTOM_ABBR_FIRST = {"WNBA"}  # add "MLB","NBA",... here if you want to enforce your map too

    def _nm(t: dict) -> str:
        team = t.get("team", {}) if isinstance(t, dict) else {}
        disp = (team.get("displayName") or team.get("name") or team.get("shortDisplayName") or "").strip()

        # Keep this if you like — forces your map first for WNBA
        CUSTOM_ABBR_FIRST = {"WNBA"}
        if sport.upper() in CUSTOM_ABBR_FIRST:
            mapped = _name_to_abbr(sport, disp)
            if mapped and isinstance(mapped, str):
                return mapped

        # Prefer ESPN abbreviation, but normalize it to our canonical set
        if prefer_abbr:
            abbr = (team.get("abbreviation") or "").strip()
            if abbr:
                return normalize_abbr(sport, abbr, disp)
            # fallback if no abbr present
            mapped = _name_to_abbr(sport, disp)
            if mapped:
                return mapped

        # Last resort: readable name
        return disp

    for ev in data.get("events", []):
        comp = (ev.get("competitions") or [None])[0] or {}
        teams = comp.get("competitors") or []
        if len(teams) < 2:
            continue
        t1 = _nm(teams[0]); t2 = _nm(teams[1])
        if t1 and t2:
            games.append((t1, t2))
    return games

def _db_props(client: Client, db_id: str) -> dict:
    return client.databases.retrieve(database_id=db_id).get("properties", {})

def _next_order_seed(client: Client, db_id: str) -> int:
    try:
        resp = client.databases.query(database_id=db_id, page_size=100)
        max_order = 0
        for r in resp.get("results", []):
            props = r.get("properties", {})
            if "Order" in props and props["Order"]["type"] == "number":
                val = props["Order"].get("number")
                if isinstance(val, (int, float)) and val is not None:
                    try:
                        max_order = max(max_order, int(val))
                    except Exception:
                        pass
        return max_order + 1
    except Exception:
        return 1

def _teams_key(t1: str, t2: str) -> Tuple[str, str]:
    a, b = (t1 or "").strip(), (t2 or "").strip()
    return tuple(sorted((a, b), key=lambda s: s.lower()))

def _title_key(db_props: dict) -> Optional[str]:
    for key, meta in db_props.items():
        if isinstance(meta, dict) and meta.get("type") == "title":
            return key
    return None

def _get_prop_text(props: dict, name: str) -> str:
    p = props.get(name)
    if not p: return ""
    t = p.get("type")
    if t == "select" and p.get("select"):         return p["select"]["name"]
    if t == "multi_select" and p.get("multi_select"):
        if name in ("Sport", "Stat") and p["multi_select"]:
            return p["multi_select"][0]["name"]
        return ", ".join(x["name"] for x in p["multi_select"])
    if t == "rich_text" and p.get("rich_text"):   return "".join(x["plain_text"] for x in p["rich_text"])
    if t == "title" and p.get("title"):           return "".join(x["plain_text"] for x in p["title"])
    return ""

def _query_all_today(client: Client, db_id: str, yyyymmdd: str) -> List[dict]:
    iso_start = _iso_utc_start_of_day(yyyymmdd)
    results: List[dict] = []
    cursor = None
    while True:
        payload = {
            "database_id": db_id,
            "page_size": 100,
            "filter": {
                "timestamp": "created_time",
                "created_time": {"on_or_after": iso_start}
            }
        }
        if cursor:
            payload["start_cursor"] = cursor
        resp = client.databases.query(**payload)
        results.extend(resp.get("results", []))
        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break
    return results

def _existing_keys_today(client: Client, db_id: str, yyyymmdd: str) -> Set[Tuple[str, Tuple[str, str], str]]:
    out: Set[Tuple[str, Tuple[str, str], str]] = set()
    for r in _query_all_today(client, db_id, yyyymmdd):
        props = r.get("properties", {})
        sport = _get_prop_text(props, "Sport").strip()
        stat  = _get_prop_text(props, "Stat").strip()
        t1, t2 = "", ""
        if "Teams" in props and props["Teams"].get("type") == "multi_select":
            names = [t["name"] for t in (props["Teams"]["multi_select"] or [])]
            if len(names) >= 2:
                t1, t2 = names[0], names[1]
        else:
            t1 = _get_prop_text(props, "Team 1").strip()
            t2 = _get_prop_text(props, "Team 2").strip()
        if sport and stat and t1 and t2:
            out.add((sport, _teams_key(t1, t2), stat))
    return out

def _existing_psp_keys_today(client: Client, db_id: str, yyyymmdd: str) -> Set[Tuple[str, str]]:
    out: Set[Tuple[str, str]] = set()
    for r in _query_all_today(client, db_id, yyyymmdd):
        props = r.get("properties", {})
        sport = _get_prop_text(props, "Sport").strip()
        stat  = _get_prop_text(props, "Stat").strip()
        if sport and stat:
            out.add((sport, stat))
    return out

def _props(db_props: Dict, sport: str, stat: str, teams: Tuple[str, str], order_val: int) -> Dict:
    t1, t2 = teams
    props: Dict = {}

    # Sport
    if "Sport" in db_props:
        typ = db_props["Sport"]["type"]
        if typ == "select":
            props["Sport"] = {"select": {"name": sport}}
        elif typ == "multi_select":
            props["Sport"] = {"multi_select": [{"name": sport}]}
        else:
            props["Sport"] = {"rich_text": [{"type": "text", "text": {"content": sport}}]}

    # Stat
    if "Stat" in db_props:
        typ = db_props["Stat"]["type"]
        if typ == "select":
            props["Stat"] = {"select": {"name": stat}}
        elif typ == "multi_select":
            props["Stat"] = {"multi_select": [{"name": stat}]}
        else:
            props["Stat"] = {"rich_text": [{"type": "text", "text": {"content": stat}}]}

    # Processed
    if "Processed" in db_props and db_props["Processed"]["type"] == "select":
        props["Processed"] = {"select": {"name": "no"}}

    # Order
    if "Order" in db_props and db_props["Order"]["type"] == "number":
        props["Order"] = {"number": order_val}

    # Teams
    if "Teams" in db_props and db_props["Teams"]["type"] == "multi_select":
        props["Teams"] = {"multi_select": [{"name": t1}, {"name": t2}]}
    else:
        if "Team 1" in db_props and db_props["Team 1"]["type"] == "title":
            props["Team 1"] = {"title": [{"type": "text", "text": {"content": t1}}]}
        if "Team 2" in db_props:
            typ2 = db_props["Team 2"]["type"]
            if typ2 == "rich_text":
                props["Team 2"] = {"rich_text": [{"type": "text", "text": {"content": t2}}]}
            elif typ2 == "title":
                props["Team 2"] = {"title": [{"type": "text", "text": {"content": t2}}]}

    # Title (whatever the DB's title field is)
    title_prop = _title_key(db_props)
    if title_prop and not props.get(title_prop):
        props[title_prop] = {"title": [{"type": "text", "text": {"content": f"{sport}: {t1} vs {t2} — {stat}"}}]}

    return props

def _seed_to_db(
    client: Client,
    db_id: str,
    games: List[Tuple[str, str]],
    sport: str,
    stats: List[str],
    start_order: int,
    existing_today: Set[Tuple[str, Tuple[str, str], str]],
) -> Tuple[int, int, int]:
    db_props = _db_props(client, db_id)
    created = 0
    skipped = 0
    order_val = start_order

    for (t1, t2) in games:
        canon = _teams_key(t1, t2)
        for stat in stats:
            key = (sport, canon, stat)
            if key in existing_today:
                skipped += 1
                continue
            props = _props(db_props, sport, stat, (t1, t2), order_val)
            order_val += 1
            try:
                client.pages.create(parent={"database_id": db_id}, properties=props)
                created += 1
            except Exception as e:
                if VERBOSE:
                    print(f"❌ Notion create failed (REG {sport} {t1} vs {t2} / {stat}): {e}")
    return created, skipped, order_val

def _aggregate_teams(games: List[Tuple[str, str]]) -> List[str]:
    seen = set(); out = []
    for t1, t2 in games:
        for t in (t1, t2):
            if t and t not in seen:
                seen.add(t); out.append(t)
    out.sort()
    return out

def _teams_for_psp(sport: str, games: List[Tuple[str, str]]) -> List[str]:
    teams = _aggregate_teams(games)
    if sport == "MLB" and len(teams) >= MLB_TEAM_COUNT:
        # full slate -> blank PSP “Teams”
        return []
    return teams

def _prop_type(db_props: Dict, name: str) -> str:
    p = db_props.get(name)
    return p.get("type") if isinstance(p, dict) else ""

def _first_title_key(db_props: Dict) -> Optional[str]:
    for k, v in db_props.items():
        if v.get("type") == "title":
            return k
    return None

def _props_psp(db_props: Dict, sport: str, stat: str, teams_list: List[str], order_num: int) -> Dict:
    """
    PSP row props with deterministic Order:
      - If DB title == 'Teams': put comma list as the title (use " " if list is blank).
      - Else: title '{Sport} PSP — {Stat}'. If teams_list not blank, set 'Teams' normally.
    """
    props: Dict = {}
    teams_text = ", ".join(teams_list)
    title_key = _first_title_key(db_props)

    if title_key == "Teams":
        content = teams_text if teams_text else " "
        props["Teams"] = {"title": [{"type": "text", "text": {"content": content}}]}
        title_is_teams = True
    elif title_key:
        props[title_key] = {"title": [{"type": "text", "text": {"content": f"{sport} PSP — {stat}"}}]}
        title_is_teams = False
    else:
        props["Name"] = {"title": [{"type": "text", "text": {"content": f"{sport} PSP — {stat}"}}]}
        title_is_teams = False

    if _prop_type(db_props, "Sport") == "select":
        props["Sport"] = {"select": {"name": sport}}
    elif "Sport" in db_props:
        props["Sport"] = {"rich_text": [{"type": "text", "text": {"content": sport}}]}

    if _prop_type(db_props, "Stat") == "select":
        props["Stat"] = {"select": {"name": stat}}
    elif "Stat" in db_props:
        props["Stat"] = {"rich_text": [{"type": "text", "text": {"content": stat}}]}

    if _prop_type(db_props, "Processed") == "select":
        props["Processed"] = {"select": {"name": "no"}}

    if _prop_type(db_props, "Order") == "number":
        props["Order"] = {"number": order_num}

    if (not title_is_teams) and teams_list and ("Teams" in db_props):
        ttype = _prop_type(db_props, "Teams")
        if ttype == "multi_select":
            props["Teams"] = {"multi_select": [{"name": t} for t in teams_list]}
        elif ttype:
            props["Teams"] = {"rich_text": [{"type": "text", "text": {"content": teams_text}}]}

    return props

def _seed_psp_rollups(
    client: Client,
    db_id: str,
    games: List[Tuple[str, str]],
    sport: str,
    stats: List[str],
    start_order: int,               # kept for signature compatibility (unused)
    existing_today: Set[Tuple[str, str]],
) -> Tuple[int, int, int]:
    """
    Create one PSP row per stat with a deterministic Order:
      Order = sport_rank*100 + stat_rank  (so MLB TB -> 101, RBI -> 102, K -> 103)
    De-dupes vs rows created today using (Sport, Stat).
    Returns (created_count, skipped_today, next_order_unchanged).
    """
    db_props = _db_props(client, db_id)
    created = 0
    skipped = 0

    teams_list = _teams_for_psp(sport, games)
    sport_rank = PSP_SPORT_RANK.get(sport, 99)
    stat_rank_map = PSP_STAT_RANK.get(sport, {})

    for stat in stats:
        if (sport, stat) in existing_today:
            skipped += 1
            continue

        order_num = sport_rank * 100 + stat_rank_map.get(stat, 99)

        try:
            props = _props_psp(db_props, sport, stat, teams_list, order_num)
            client.pages.create(parent={"database_id": db_id}, properties=props)
            created += 1
        except Exception as e:
            if VERBOSE:
                print(f"[warn] PSP create failed ({sport}/{stat}): {e}")

    return created, skipped, start_order

# ---------- Main ----------

def main() -> int:
    if not DATABASE_ID or not PSP_DATABASE_ID:
        print("❌ Set DATABASE_ID and PSP_DATABASE_ID in your environment.")
        return 1

    yyyymmdd = DATE_STR or _yyyymmdd_tomorrow()
    client = Client(auth=NOTION_TOKEN)

    print("=== Schedule Seeder ===")
    print(f"Date:  {yyyymmdd}")
    print(f"Sports: {', '.join(SPORTS)}")

    total_created = 0

    # Cache ESPN scoreboards so REG and PSP share the same data
    scoreboards: Dict[str, Dict] = {}
    games_by_sport: Dict[str, List[Tuple[str, str]]] = {}
    for sport in SPORTS:
        if sport not in ESPN_ENDPOINT:
            if VERBOSE: print(f"[skip] unknown sport key: {sport}")
            continue
        sb = _fetch_scoreboard(sport, yyyymmdd)
        scoreboards[sport] = sb
        games_by_sport[sport] = _extract_games(sb, sport, prefer_abbr=bool(TEAMS_AS_ABBR))

    # ---------- REGULAR ----------
    reg_existing_today = _existing_keys_today(client, DATABASE_ID, yyyymmdd)
    reg_order = _next_order_seed(client, DATABASE_ID)

    for sport in SPORTS:
        if sport not in SPORT_STATS:
            if VERBOSE: print(f"[skip] no stat list for {sport}")
            continue
        games = games_by_sport.get(sport) or []
        if not games:
            print(f"[{sport}] no games")
            continue
        created, skipped, reg_order = _seed_to_db(
            client, DATABASE_ID, games, sport, SPORT_STATS[sport], reg_order, reg_existing_today
        )
        total_created += created
        print(f"[{sport}] regular: +{created} rows (skipped today: {skipped})")

    # ---------- PSP (rollups) ----------
    psp_existing_today = _existing_psp_keys_today(client, PSP_DATABASE_ID, yyyymmdd)
    psp_order = _next_order_seed(client, PSP_DATABASE_ID)  # unused but kept

    for sport in PSP_ORDER:
        if sport not in SPORTS:
            continue
        if sport not in PSP_SPORT_STATS:
            continue
        games = games_by_sport.get(sport) or []
        if not games:
            print(f"[{sport}] PSP: no games")
            continue
        if len(games) <= 1:  # only 1 matchup → 2 teams
            print(f"[{sport}] PSP: skipped (only {len(games)} game)")
            continue

        created, skipped, psp_order = _seed_psp_rollups(
            client, PSP_DATABASE_ID, games, sport, PSP_SPORT_STATS[sport],
            psp_order, psp_existing_today
        )
        total_created += created
        print(f"[{sport}] PSP: +{created} rows (skipped today: {skipped})")

    print(f"✅ Done. Inserted {total_created} total rows.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
