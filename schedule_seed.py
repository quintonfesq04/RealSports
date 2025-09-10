#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Schedule → Notion seeder (ESPN, one day)
- Seeds your "regular games" DB first, then the PSP DB.
- REGULAR: one Notion row per game per configured stat (see SPORT_STATS)
- PSP: one Notion row per sport/stat with a comma-separated list of teams

De-dupe policy: only skip rows that were already created TODAY.
(So prior days never block today’s seed.)

Env / flags:
  NOTION_TOKEN        (optional if your fallback works)
  DATABASE_ID         (required)  -> regular games DB
  PSP_DATABASE_ID     (required)  -> PSP DB
  DATE=YYYYMMDD       (default: today)
  SPORTS              (comma list; default: MLB,WNBA,CFB,NFL,NBA,NHL,CBB)
  VERBOSE=0|1         (default: 0) if 1, prints per-row Notion errors
"""

import os, sys, time
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Set, Optional

import requests
from notion_client import Client

MLB_TEAM_COUNT = 30
PSP_ORDER = ["MLB", "WNBA", "CFB", "NFL"]

# ---------------- Config: per-sport stat menus -----------------

SPORT_STATS: Dict[str, List[str]] = {
    "MLB": ["RBI"],
    "WNBA": ["PPG", "APG", "RPG", "3PM"],
    "CFB": ["Total Scrimmage Yards", "Receptions", "Total Touchdowns"],
    "NFL": ["Total Scrimmage Yards", "Receptions", "Total Touchdowns"],
    "NBA": ["PPG", "APG", "RPG", "3PM"],
    "NHL": ["Shots on Goal", "Points", "Total Goals"],
    "CBB": ["PPG", "APG", "RPG", "3PM"],
}

# PSP stat menu (different from regular games)
PSP_SPORT_STATS: Dict[str, List[str]] = {
    "MLB": ["TB", "RBI", "K"],
    "WNBA": ["PPG", "APG", "RPG", "3PM"],
    "CFB": ["Total Scrimmage Yards", "Receptions", "Total Touchdowns"],
    "NFL": ["Total Scrimmage Yards", "Receptions", "Total Touchdowns"],
    "NBA": ["PPG", "APG", "RPG", "3PM"],
    "CBB": ["PPG", "APG", "RPG", "3PM"],
}

# ESPN scoreboard endpoints
ESPN_ENDPOINT: Dict[str, str] = {
    "MLB": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    "WNBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard",
    "CFB": "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard",
    "NFL": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    "NBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "NHL": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
    "CBB": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard",
}

# --------------- Env ---------------------

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip() or \
               "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"
DATABASE_ID     = os.getenv("DATABASE_ID",  "1aa71b1c-663e-8035-bc89-fb1e84a2d919")
PSP_DATABASE_ID = os.getenv("PSP_DATABASE_ID","1ac71b1c663e808e9110eee23057de0e")
DATE_STR = os.getenv("DATE", "").strip()  # YYYYMMDD
SPORTS = [s.strip().upper() for s in (os.getenv("SPORTS", "MLB,WNBA,CFB,NFL,NBA,NHL,CBB").split(",")) if s.strip()]
VERBOSE = int(os.getenv("VERBOSE", "0"))

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# --------------- Helpers -----------------

def _yyyymmdd_today() -> str:
    return datetime.now().strftime("%Y%m%d")

def _iso_utc_start_of_day(yyyymmdd: str) -> str:
    dt = datetime.strptime(yyyymmdd, "%Y%m%d").replace(tzinfo=timezone.utc)
    # 00:00:00 UTC of that date
    sod = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    return sod.isoformat()

def _fetch_scoreboard(sport: str, yyyymmdd: str) -> Dict:
    url = ESPN_ENDPOINT[sport]
    params = {"dates": yyyymmdd}
    hdrs = {"User-Agent": UA, "Referer": "https://www.espn.com/"}
    for attempt in range(4):
        try:
            r = requests.get(url, params=params, headers=hdrs, timeout=20)
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

def _extract_games(data: Dict) -> List[Tuple[str, str]]:
    games: List[Tuple[str, str]] = []
    for ev in data.get("events", []):
        comp = (ev.get("competitions") or [None])[0] or {}
        teams = comp.get("competitors") or []
        if len(teams) < 2:
            continue
        def _nm(t):
            team = t.get("team", {}) if isinstance(t, dict) else {}
            return (team.get("displayName")
                    or team.get("shortDisplayName")
                    or team.get("name")
                    or team.get("abbreviation")
                    or "").strip()
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
        # we only need *one* value for Sport/Stat duplicate keys
        if name in ("Sport", "Stat") and p["multi_select"]:
            return p["multi_select"][0]["name"]
        return ", ".join(x["name"] for x in p["multi_select"])
    if t == "rich_text" and p.get("rich_text"):   return "".join(x["plain_text"] for x in p["rich_text"])
    if t == "title" and p.get("title"):           return "".join(x["plain_text"] for x in p["title"])
    return ""

def _query_all_today(client: Client, db_id: str, yyyymmdd: str) -> List[dict]:
    """Return all pages created today (UTC) in this DB (paginated)."""
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
    """
    REGULAR DB: set of (Sport, (TeamA,TeamB canonical), Stat) created today.
    """
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
    """
    PSP DB: set of (Sport, Stat) created today.
    """
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

    # Teams (prefer multi_select if available)
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

    # Ensure some title is set (whatever the DB's title field is)
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
    """
    Returns (created_count, skipped_as_dupe_today, next_order).
    """
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
    seen = set()
    out = []
    for t1, t2 in games:
        for t in (t1, t2):
            if t and t not in seen:
                seen.add(t)
                out.append(t)
    out.sort()
    return out

def _teams_for_psp(sport: str, games: List[Tuple[str, str]]) -> List[str]:
    """Alphabetize unique teams. For MLB, if all 30 clubs play, return [] (blank PSP teams)."""
    teams = _aggregate_teams(games)  # already unique + sorted A→Z
    if sport == "MLB" and len(teams) >= MLB_TEAM_COUNT:
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

def _props_psp(db_props: Dict, sport: str, stat: str, teams_list: List[str], order_val: int) -> Dict:
    """
    PSP row props:
      - If DB title == 'Teams': put comma list as the title; if list is blank use " " (Notion requires non-empty title).
      - Else: title is '{Sport} PSP — {Stat}'. If teams_list not blank, set 'Teams' property
              (multi_select if available, else rich_text).
    """
    props: Dict = {}
    teams_text = ", ".join(teams_list)

    # Title
    title_key = _first_title_key(db_props)
    if title_key == "Teams":
        content = teams_text if teams_text else " "
        props["Teams"] = {"title": [{"type": "text", "text": {"content": content}}]}
        title_is_teams = True
    elif title_key:
        props[title_key] = {"title": [{"type": "text", "text": {"content": f"{sport} PSP — {stat}"}}]}
        title_is_teams = False
    else:
        # Fallback title (required by Notion)
        props["Name"] = {"title": [{"type": "text", "text": {"content": f"{sport} PSP — {stat}"}}]}
        title_is_teams = False

    # Sport
    if _prop_type(db_props, "Sport") == "select":
        props["Sport"] = {"select": {"name": sport}}
    elif "Sport" in db_props:
        props["Sport"] = {"rich_text": [{"type": "text", "text": {"content": sport}}]}

    # Stat
    if _prop_type(db_props, "Stat") == "select":
        props["Stat"] = {"select": {"name": stat}}
    elif "Stat" in db_props:
        props["Stat"] = {"rich_text": [{"type": "text", "text": {"content": stat}}]}

    # Processed
    if _prop_type(db_props, "Processed") == "select":
        props["Processed"] = {"select": {"name": "no"}}

    # Order
    if _prop_type(db_props, "Order") == "number":
        props["Order"] = {"number": order_val}

    # Teams (only if Teams is NOT the title and we have a list)
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
    start_order: int,
) -> Tuple[int, int]:
    """
    Create ONE PSP row per stat with a comma-separated team list (blank for MLB full slate).
    Returns (created_count, next_order).
    """
    db_props = _db_props(client, db_id)
    created = 0
    order_val = start_order

    teams_list = _teams_for_psp(sport, games)
    # Note: If teams_list == [] and DB title != 'Teams', we will omit Teams property and just set the title.

    # Avoid dupes by Sport+Stat (first page)
    existing = set()
    try:
        resp = client.databases.query(database_id=db_id, page_size=100)
        for r in resp.get("results", []):
            props = r.get("properties", {})
            s = ""
            st = ""
            if "Sport" in props:
                p = props["Sport"]
                if p["type"] == "select" and p["select"]:
                    s = p["select"]["name"]
                elif p["type"] == "rich_text" and p["rich_text"]:
                    s = "".join(x["plain_text"] for x in p["rich_text"])
            if "Stat" in props:
                p = props["Stat"]
                if p["type"] == "select" and p["select"]:
                    st = p["select"]["name"]
                elif p["type"] == "rich_text" and p["rich_text"]:
                    st = "".join(x["plain_text"] for x in p["rich_text"])
            if s and st:
                existing.add((s.strip(), st.strip()))
    except Exception:
        pass

    for stat in stats:
        if (sport, stat) in existing:
            continue
        try:
            props = _props_psp(db_props, sport, stat, teams_list, order_val)
            client.pages.create(parent={"database_id": db_id}, properties=props)
            order_val += 1
            created += 1
        except Exception as e:
            print(f"[warn] PSP create failed ({sport}/{stat}): {e}")

    return created, order_val

# --------------- Main -----------------

def main() -> int:
    if not DATABASE_ID or not PSP_DATABASE_ID:
        print("❌ Set DATABASE_ID and PSP_DATABASE_ID in your environment.")
        return 1

    yyyymmdd = DATE_STR or _yyyymmdd_today()
    client = Client(auth=NOTION_TOKEN)

    print("=== Schedule Seeder ===")
    print(f"Date:  {yyyymmdd}")
    print(f"Sports: {', '.join(SPORTS)}")

    total_created = 0

    # Cache ESPN scoreboards for this date so REG and PSP share the same data
    scoreboards: Dict[str, Dict] = {}
    games_by_sport: Dict[str, List[Tuple[str, str]]] = {}

    for sport in SPORTS:
        if sport not in ESPN_ENDPOINT:
            if VERBOSE: print(f"[skip] unknown sport key: {sport}")
            continue
        sb = _fetch_scoreboard(sport, yyyymmdd)
        scoreboards[sport] = sb
        games_by_sport[sport] = _extract_games(sb)

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
    psp_order = _next_order_seed(client, PSP_DATABASE_ID)

    # Only these sports, in this exact order
    for sport in PSP_ORDER:
        # still respect the SPORTS filter you pass via env
        if sport not in SPORTS:
            continue
        if sport not in PSP_SPORT_STATS:
            continue
        games = games_by_sport.get(sport) or []
        if not games:
            print(f"[{sport}] PSP: no games")
            continue
        created, skipped, psp_order = _seed_psp_rollups(
            client, PSP_DATABASE_ID, games, sport, PSP_SPORT_STATS[sport], psp_order, psp_existing_today
        )
        total_created += created
        print(f"[{sport}] PSP: +{created} rows (skipped today: {skipped})")

    print(f"✅ Done. Inserted {total_created} total rows.")
    return 0

if __name__ == "__main__":
    sys.exit(main())