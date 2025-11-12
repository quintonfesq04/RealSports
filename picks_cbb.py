#!/usr/bin/env python3
"""
picks_cbb.py
------------
CBB-only "picks" pipeline:
- Loads cbb_players_stats.csv (season per-game averages)
- Buckets top-12 by requested stat among requested teams
- Posts the colored summary to a Poll page in Notion and marks items Processed
"""

# --- core + typing
import json
import os
import sys
import re
import time
import unicodedata
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

_BOOTSTRAP_FLAG = "REALSPORTS_VENV_BOOTSTRAP"
HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
DATA_DIR.mkdir(exist_ok=True)
PICKS_CACHE = DATA_DIR / "cbb.json"
PickRecord = Dict[str, Any]

def _record_pick(collector: Optional[List[PickRecord]], entry: PickRecord) -> None:
    if collector is not None:
        collector.append(entry)

def _write_cache(payload: List[PickRecord]) -> None:
    try:
        PICKS_CACHE.write_text(json.dumps(payload), encoding="utf-8")
    except Exception as exc:
        print(f"[cache] failed to write cbb picks cache: {exc}", file=sys.stderr)

def _maybe_reexec_with_local_venv():
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
    import pandas as pd
    import requests
    from notion_client import Client
except ImportError:
    _maybe_reexec_with_local_venv()
    import pandas as pd
    import requests
    from notion_client import Client

# ---- Env loading that "just works" ----------------------------------------
def _env(name: str, default: str = "") -> str:
    """Return env var, loading .env from common locations if needed."""
    val = os.getenv(name, "").strip()
    if val:
        return val
    try:
        from dotenv import load_dotenv, find_dotenv
        # 1) Try auto-discovery (walks up directories)
        load_dotenv(find_dotenv(), override=False)

        # 2) Try a few explicit, common spots (cwd, script dir, repo root)
        here = Path(__file__).resolve().parent
        for p in (Path.cwd() / ".env",
                  here / ".env",
                  here.parent / ".env"):
            if p.exists():
                load_dotenv(p, override=False)
    except Exception:
        pass
    return os.getenv(name, default).strip()

NOTION_TOKEN = _env("NOTION_TOKEN")
DATABASE_ID  = _env("DATABASE_ID")
POLL_PAGE_ID = _env("POLL_PAGE_ID")
PSP_DATABASE_ID = _env("PSP_DATABASE_ID")

PSP_STAT_ORDER = ["PPG", "APG", "RPG", "3PM"]
PSP_STAT_RANK  = {k: i for i, k in enumerate(PSP_STAT_ORDER)}

if not NOTION_TOKEN:
    raise SystemExit("Missing NOTION_TOKEN (not found in environment or .env).")
if not DATABASE_ID:
    raise SystemExit("Missing DATABASE_ID (not found in environment or .env).")
if not POLL_PAGE_ID:
    raise SystemExit("Missing POLL_PAGE_ID (not found in environment or .env).")

client = Client(auth=NOTION_TOKEN)

# Where your CBB stats CSV lives (from your scraper)
CSV_PATH = os.getenv(
    "CBB_STATS_CSV",
    str((Path(__file__).resolve().parent / "cbb_players_stats.csv"))
)

# If your Notion uses “CBB”, “NCAAB”, “COLLEGE BASKETBALL”—treat them all as CBB
CBB_ALIASES = {"CBB","NCAAB","COLLEGE BASKETBALL"}

STAT_MAP = {
    # Notion “Stat” → CSV column
    "PTS": "PPG", "PPG": "PPG", "POINTS": "PPG",
    "AST": "APG", "APG": "APG", "ASSISTS": "APG",
    "REB": "RPG", "RPG": "RPG", "REBOUNDS": "RPG",
    "3P": "3PM", "3PM": "3PM", "3PT": "3PM", "THREE POINTERS": "3PM",
}

# Display order inside each matchup header
STAT_ORDER = ["PPG", "APG", "RPG", "3PM"]
STAT_RANK  = {k: i for i, k in enumerate(STAT_ORDER)}

NAME_OVERRIDES = {
    # put any bad splits you’ve seen here (optional)
}

def _load_env_overrides(raw: str) -> Dict[str, str]:
    """
    Parse env string like 'Bad Name -> Good Name;AnotherBad->Another Good' into a dict.
    Accepts separators ';' or '|'. Whitespace trimmed.
    """
    mapping: Dict[str, str] = {}
    if not raw:
        return mapping
    parts = re.split(r"[;|]", raw)
    for part in parts:
        if "->" not in part:
            continue
        bad, good = part.split("->", 1)
        bad = bad.strip()
        good = good.strip()
        if bad and good:
            mapping[bad] = good
    return mapping

NAME_OVERRIDES.update(_load_env_overrides(_env("CBB_NAME_OVERRIDES", "")))

def clean_name(s: str) -> str:
    x = (s or "").replace(".", " ")
    x = re.sub(r"\s+", " ", x).strip()
    return NAME_OVERRIDES.get(x, x)

def _name_key(s: str) -> str:
    """Return a canonical key for name comparisons (case/punct/diacritic agnostic)."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = (s.replace("&apos;", "")
           .replace("’", "")
           .replace("'", "")
           .replace(".", "")
           .replace("-", " "))
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

_BASE_BANNED_PLAYERS = {
    "Kadary Richmond",
    "Mark Sears",
    "Deivon Smith",
    "RJ Luis Jr",
    "Grant Nelson",
    "Dawson Garcia",
    "Lu'Cye Patterson",
    "Mike Mitchell Jr",
    "Femi Odukale",
    "Michael Pajeaud",
    "Adou Thiero",
    "Jaden Akins",
    "Jase Richardson",
    "Johnell Davis",
    "David N'Guessan",
    "Coleman Hawkins",
    "Max Jones",
    "Robert Smith",
    "Eddie Lampkin Jr",
    "Jyare Davis",
    "Jaquan Carlos",
    "Lucas Taylor",
    "Andrej Jakimovski",
    "Trevor Baskin",
    "Julian Hammond III",
    "Sam Stockton",
    "Brice Williams",
    "Juwan Gary",
    "Jonathan Aybar",
    "Fousseyni Traore",
    "Caleb Kenney",
    "Egor Demin",
    "Trevin Knell",
    "Mawot Mag",
    "Jahmyl Telfort",
    "Pierre Brooks II",
    "Andre Screen",
    "Tre Johnson",
    "Arthur Kaluma",
    "Kadin Shedrick",
    "Jayson Kent",
    "Maxime Raynaud",
    "Jaylen Blakes",
    "Joe Pridgen",
    "Kai Johnson",
    "Brandon Whitney",
    "Austin Patterson",
    "Chaz Lanier",
    "Josh Dilling",
    "Zakai Zeigler",
    "Jordan Gainey",
    "Igor Milicic Jr",
    "Trey Robinson",
    "Hubertas Pivorius",
    "Sam Vinson",
    "Nolan Hickman",
    "Brycen Goodine",
    "Khalif Battle",
    "Kobe Elvis",
    "Jalon Moore",
    "Ryan Nembhard",
    "Glenn Taylor Jr",
    "Ben Gregg",
    "Bensley Joseph",
    "Mylyjael Poteat",
    "Wesley Cardet Jr",
    "Saša Ciani",
    "Yanic Konan Niederhauser",
    "Nick Kern Jr",
    "Zach Hicks",
    "Ace Baldwin Jr",
    "D'Marco Dunn",
    "Jhamir Brickus",
    "Jordan Longino",
    "Eric Dixon",
    "Wooga Poplar",
    "Enoch Boakye",
    "LJ Cryer",
    "Nendah Tarke",
    "J'Wan Roberts",
    "Tomiwa Sulaiman",
    "Mylik Wilson",
    "Dallan Coleman",
    "Chris Manon",
    "Darius Johnson",
    "Jordan Ivy-Curry",
    "Tyrese Proctor",
    "Kon Knueppel",
    "Bernard Pelote",
    "Cooper Flagg",
    "Mason Gillis",
    "Sion James",
    "Blaise Threatt",
    "Gabe Madsen",
    "Lawson Lovering",
    "Mason Madsen",
    "Vasilije Vucinic",
    "Chris Youngblood",
    "Aaron Scott",
    "Jeremiah Fears",
    "Basheer Jihad",
    "BJ Freeman",
    "Alston Mason",
    "David Joplin",
    "Luke Goode",
    "Kam Jones",
    "Trey Galloway",
    "Stevie Mitchell",
    "Javon Small",
    "Tyler Whitney-Sidney",
    "Toby Okani",
    "Chibuzo Agbo",
    "Wesley Robinson",
    "Saint Thomas",
    "Tyrin Lawrence",
    "Kenny White Jr",
    "Tyler Brelsford",
    "Dakota Leffew",
    "Trent Scott",
    "Andersson Garcia",
    "Henry Coleman III",
    "Abou Ousmane",
    "Zhuric Phelps",
    "Marchelus Avery",
    "Collin Murray-Boyles",
    "Denijay Harris",
    "Jamarii Thomas",
    "Neftali Alvarez",
    "DeAntoni Gordon",
    "Cobie Montgomery",
    "Caleb Grill",
    "Tamar Bates",
    "Marques Warrick",
    "Norchad Omier",
    "Great Osobor",
    "VJ Edgecombe",
    "Oumar Ballo",
    "Samson Johnson",
    "Matthew Cleveland",
    "Lynn Kidd",
    "Cameron Matthews",
    "Johnny O'Neil",
    "Dante Maddox Jr",
    "Marcus Foster",
    "Lance Terry",
    "Kobe Johnson",
    "Lazar Stefanovic",
    "Dayvion McKnight",
    "Gabriel Pozzato", # Injured
    ""
}

_env_banned_raw = _env("CBB_BANNED_PLAYERS", "")
if _env_banned_raw:
    _BASE_BANNED_PLAYERS.update({p.strip() for p in _env_banned_raw.split(",") if p.strip()})

BANNED_PLAYERS = {clean_name(n) for n in _BASE_BANNED_PLAYERS if n}
_BANNED_KEYS = {_name_key(n) for n in BANNED_PLAYERS}

def _is_banned_player(name: str) -> bool:
    if not _BANNED_KEYS:
        return False
    raw = _name_key(name)
    cleaned = _name_key(clean_name(name))
    return raw in _BANNED_KEYS or cleaned in _BANNED_KEYS

def _remove_banned_from_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or not _BANNED_KEYS:
        return df
    mask = ~df["Player"].astype(str).apply(_is_banned_player)
    return df[mask].copy()

def _colors_block(g: List[str], y: List[str], r: List[str], p: List[str]) -> str:
    def line(dot, names): return f"{dot} {', '.join(names) if names else 'None'}"
    return "\n".join([
        line("🟢", g),
        line("🟡", y),
        line("🔴", r),
        line("🟣", p),
    ])

def _h3(text: str) -> Dict:
    return {
        "object": "block",
        "type": "heading_3",
        "heading_3": {"rich_text": [{"type": "text", "text": {"content": text}}]},
    }

def _para(text: str) -> Dict:
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "text": {"content": text}}]},
    }

def bucket_top12(df: pd.DataFrame, stat_col: str) -> Tuple[List[str], List[str], List[str], List[str]]:
    tmp = df[["Player", stat_col]].copy()
    tmp = tmp[pd.to_numeric(tmp[stat_col], errors="coerce").notna()]
    tmp[stat_col] = tmp[stat_col].astype(float)
    tmp = tmp.sort_values(stat_col, ascending=False).head(12)
    names = [clean_name(n) for n in tmp["Player"].tolist()]
    return names[0:3], names[3:6], names[6:9], names[9:12]

def _append(blocks):
    # Light retry for Notion rate limits
    for i in range(3):
        try:
            client.blocks.children.append(block_id=POLL_PAGE_ID, children=blocks)
            return
        except Exception as e:
            if "Rate limited" in str(e) or "429" in str(e):
                time.sleep(2**i)
                continue
            print("[notion] append error:", e, file=sys.stderr)
            return

def _update(page_id: str, props: Dict):
    for i in range(3):
        try:
            client.pages.update(page_id=page_id, properties=props)
            return
        except Exception as e:
            if "Rate limited" in str(e) or "429" in str(e):
                time.sleep(2**i)
                continue
            print("[notion] update error:", e, file=sys.stderr)
            return

def _load_db_rows() -> List[Dict]:
    """
    Pull all unprocessed rows, then sort client-side by:
    No Order (preferred) → Order (fallback) → sport/teams (stable).
    Returns rows shaped like: {page_id, sport, stat, teams}.
    """
    results: List[Dict] = []
    cursor = None
    while True:
        payload = {
            "database_id": DATABASE_ID,
            "filter": {"property": "Processed", "select": {"equals": "no"}},
            "page_size": 100,
        }
        if cursor:
            payload["start_cursor"] = cursor

        resp = client.databases.query(**payload)

        for r in resp.get("results", []):
            pid   = r.get("id")
            props = r.get("properties", {})

            # sport
            sport = (((props.get("Sport") or {}).get("select") or {}).get("name") or "").strip()

            # stat (select OR rich_text)
            st = props.get("Stat", {})
            if st.get("type") == "select" and st.get("select"):
                stat = (st["select"].get("name") or "").strip()
            else:
                typ = st.get("type")
                stat = "".join(t.get("plain_text","") for t in st.get(typ, [])).strip() if typ else ""

            # teams: prefer multi_select; fall back to legacy Team 1 / Team 2
            teams_prop = props.get("Teams", {})
            if teams_prop.get("type") == "multi_select":
                teams = [t["name"].strip().upper() for t in teams_prop["multi_select"]]
            else:
                t1 = (props.get("Team 1", {}) or {}).get("title", [])
                t2 = (props.get("Team 2", {}) or {}).get("rich_text", [])
                t1txt = t1[0]["plain_text"] if t1 else ""
                t2txt = t2[0]["plain_text"] if t2 else ""
                teams = [x.strip().upper() for x in (t1txt + "," + t2txt).split(",") if x.strip()]

            # (optional reads)
            no_order = _prop_text_or_number(props, "No Order")
            order    = _prop_text_or_number(props, "Order")

            results.append({
                "page_id": pid,
                "sport": sport,
                "stat": stat,
                "teams": teams,
                "id_number": (
                    _prop_text_or_number(props, "ID-Number")
                    or _prop_text_or_number(props, "ID Number")
                    or _prop_text_or_number(props, "ID")
                ),
                "no_id": (
                    _prop_text_or_number(props, "No-ID")
                    or _prop_text_or_number(props, "No ID")
                    or _prop_text_or_number(props, "No")
                    or _prop_text_or_number(props, "No Order")
                ),
                "order": _prop_text_or_number(props, "Order"),
                "created_time": r.get("created_time"),
            })

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    # Sort by No Order → Order → sport/teams, using same priority as fetch_unprocessed_rows
    results.sort(key=lambda r: (
        _order_tuple(r.get("id_number")),
        _order_tuple(r.get("no_id")),
        _order_tuple(r.get("order")),
        _created_ts(r),
        (r.get("sport") or "").upper(),
        ",".join(r.get("teams") or []).upper(),
    ))

    # Keep the richer dict (id/order fields are needed for consistent sorting later)
    return results

def _prop_text_or_number(props: Dict, name: str):
    p = props.get(name, {})
    if not isinstance(p, dict):
        return None
    typ = p.get("type")
    if typ == "number":
        return p.get("number")
    if typ == "select" and p.get("select"):
        return (p["select"].get("name") or "").strip()
    if typ in ("title", "rich_text") and p.get(typ):
        return "".join(t.get("plain_text", "") for t in p.get(typ, [])).strip()
    if typ == "formula" and p.get("formula"):
        f = p["formula"]
        f_type = f.get("type")
        if f_type == "number":
            return f.get("number")
        if f_type == "string":
            return (f.get("string") or "").strip()
    if typ == "rollup" and p.get("rollup"):
        r = p["rollup"]
        r_type = r.get("type")
        if r_type == "number":
            return r.get("number")
        if r_type == "array":
            texts = []
            for item in r.get("array", []):
                if not isinstance(item, dict):
                    continue
                it_type = item.get("type")
                if it_type in ("rich_text", "title") and item.get(it_type):
                    texts.append("".join(t.get("plain_text", "") for t in item.get(it_type, [])))
            if texts:
                return ", ".join(t.strip() for t in texts if t.strip())
    return None

def _order_tuple(v):
    if v is None or (isinstance(v, str) and not v.strip()):
        return (2, float("inf"), "")
    if isinstance(v, (int, float)):
        return (0, float(v), "")
    s = str(v).strip()
    try:
        return (0, float(s), "")
    except Exception:
        return (1, float("inf"), s.upper())

def _created_ts(row: Dict) -> float:
    ts = row.get("created_time")
    if not ts:
        return float("inf")
    if isinstance(ts, (int, float)):
        return float(ts)
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()
    except Exception:
        return float("inf")

def fetch_unprocessed_rows(db_id: str) -> List[Dict]:
    results: List[Dict] = []
    cursor = None
    while True:
        kwargs = {
            "database_id": db_id,
            "filter": {"property": "Processed", "select": {"equals": "no"}},
            "page_size": 100,
        }
        if cursor:
            kwargs["start_cursor"] = cursor

        resp = client.databases.query(**kwargs)

        for r in resp.get("results", []):
            pid   = r["id"]
            props = r.get("properties", {})

            sport = ((props.get("Sport") or {}).get("select") or {}).get("name", "") or ""
            st    = props.get("Stat", {})
            if st.get("type") == "select" and st.get("select"):
                stat = (st["select"]["name"] or "").strip()
            else:
                stat = "".join(t.get("plain_text","") for t in st.get(st.get("type",""), [])).strip()

            # Prefer "Teams" multi-select; fall back to legacy Team 1 / Team 2
            if props.get("Teams", {}).get("type") == "multi_select":
                teams = [t["name"].strip().upper() for t in props["Teams"]["multi_select"]]
            else:
                t1 = props.get("Team 1", {}).get("title", [])
                t2 = props.get("Team 2", {}).get("rich_text", [])
                t1txt = t1[0]["plain_text"] if t1 else ""
                t2txt = t2[0]["plain_text"] if t2 else ""
                teams = [x.strip().upper() for x in (t1txt + "," + t2txt).split(",") if x.strip()]

            results.append({
                "page_id": pid,
                "sport": sport,
                "stat": stat,
                "teams": teams,
                "id_number": (
                    _prop_text_or_number(props, "ID-Number")
                    or _prop_text_or_number(props, "ID Number")
                    or _prop_text_or_number(props, "ID")
                ),
                "no_id": (
                    _prop_text_or_number(props, "No-ID")
                    or _prop_text_or_number(props, "No ID")
                    or _prop_text_or_number(props, "No")
                    or _prop_text_or_number(props, "No Order")
                ),
                "order": _prop_text_or_number(props, "Order"),
                "created_time": r.get("created_time"),
            })

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    # Sort: No Order → Order → Sport/Teams (stable)
    results.sort(key=lambda r: (
        _order_tuple(r.get("id_number")),  # 1) ID-Number (number or text OK)
        _order_tuple(r.get("no_id")),      # 2) No-ID / No / No Order
        _order_tuple(r.get("order")),      # 3) Order
        _created_ts(r),
        (r.get("sport") or "").upper(),
        ",".join(r.get("teams") or []).upper(),
    ))
    return results

def _load_psp_rows_cbb() -> List[Dict]:
    """Pull unprocessed PSP rows for CBB and sort PPG→APG→RPG→3PM."""
    if not PSP_DATABASE_ID:
        return []
    rows: List[Dict] = []
    cursor = None
    while True:
        kwargs = {
            "database_id": PSP_DATABASE_ID,
            "filter": {
                "and": [
                    {"property": "Processed", "select": {"equals": "no"}},
                    {"property": "Sport", "select": {"equals": "CBB"}},
                ]
            },
            "page_size": 100,
        }
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = client.databases.query(**kwargs)

        for r in resp.get("results", []):
            pid   = r["id"]
            props = r.get("properties", {})

            # Stat (select or rich_text/title fallback)
            st = props.get("Stat", {})
            if st.get("type") == "select" and st.get("select"):
                stat = (st["select"]["name"] or "").strip().upper()
            else:
                stat = "".join(t.get("plain_text","") for t in st.get(st.get("type",""), [])).strip().upper()

            # Teams (multi_select or text)
            teams_prop = props.get("Teams", {})
            if teams_prop.get("type") == "multi_select":
                teams = [t["name"].strip().upper() for t in teams_prop["multi_select"]]
            else:
                txt = "".join(t.get("plain_text","") for t in teams_prop.get(teams_prop.get("type",""), []))
                teams = [x.strip().upper() for x in txt.split(",") if x.strip()]

            # Prefer numeric Order if present; else allow textual “No Order” fallback
            order_num = None
            if props.get("Order", {}).get("type") == "number":
                order_num = props["Order"].get("number")
            no_order = None
            if props.get("No Order", {}):
                typ = props["No Order"].get("type")
                if typ in ("title","rich_text") and props["No Order"].get(typ):
                    no_order = "".join(t.get("plain_text","") for t in props["No Order"][typ]).strip()

            rows.append({
                "page_id": pid,
                "stat": stat,
                "teams": teams,
                "order_num": order_num,
                "no_order": no_order,
            })

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    # Sort by PSP stat order primarily, then by numeric Order if present (deterministic)
    rows.sort(key=lambda r: (PSP_STAT_RANK.get(r["stat"], 99),
                             float(r["order_num"]) if isinstance(r["order_num"], (int,float)) else 1e9,
                             r.get("no_order") or ""))
    return rows

def process_psp_cbb(df: pd.DataFrame, collector: Optional[List[PickRecord]] = None):
    """
    For each PSP row (CBB, unprocessed):
      - Filter CSV by Teams (if provided)
      - Bucket top 12 for the requested stat
      - Append 'CBB PSP - {STAT} leaders (this season)' + colors block
      - Mark PSP page Processed=Yes
    """
    rows = _load_psp_rows_cbb()
    if not rows:
        return

    # Normalize CSV columns once
    df["Player"] = df["Player"].astype(str).str.strip()
    df["Team"]   = df["Team"].astype(str).str.upper().str.strip()
    df = _remove_banned_from_df(df)

    for row in rows:
        page_id  = row["page_id"]
        stat_in  = (row["stat"] or "").upper()
        stat_col = STAT_MAP.get(stat_in) or stat_in
        if stat_col not in df.columns:
            # fallback to a sensible default
            for c in ["PPG","APG","RPG","3PM"]:
                if c in df.columns:
                    stat_col = c
                    break

        teams = [t.upper() for t in (row["teams"] or [])]
        use = df if not teams else df[df["Team"].isin(teams)]

        if use.empty or stat_col not in use.columns:
            g = y = r = p = []
        else:
            g, y, r, p = bucket_top12(use, stat_col)

        heading = f"CBB PSP - {stat_in or stat_col} leaders (this season)"
        _append([
            {"object":"block","type":"paragraph","paragraph":{
                "rich_text":[{"type":"text","text":{"content":heading}}]
            }},
            {"object":"block","type":"paragraph","paragraph":{
                "rich_text":[{"type":"text","text":{"content":_colors_block(g, y, r, p)}}]
            }},
            {"object":"block","type":"divider","divider":{}}
        ])
        _update(page_id, {"Processed": {"select": {"name": "Yes"}}})
        _record_pick(collector, {
            "category": "psp",
            "heading": heading,
            "stat": stat_in or stat_col,
            "teams": teams,
            "summary": _colors_block(g, y, r, p),
            "generated_at": datetime.utcnow().isoformat(),
            "buckets": {
                "green": g,
                "yellow": y,
                "red": r,
                "purple": p,
            },
        })
        print(f"[psp] ✅ PSP CBB — {stat_in} — teams={len(teams) if teams else 'ALL'}", file=sys.stderr)

def ensure_csv():
    if os.path.exists(CSV_PATH):
        return
    # If the CSV isn’t present, run the scraper inline
    import subprocess, sys
    print("[refresh] cbb_players_stats.csv not found — running cbb_scraper.py", file=sys.stderr)
    subprocess.check_call([sys.executable, "cbb_scraper.py"])

def _matchup_key(teams: List[str]) -> Tuple[str, str]:
    teams = [t for t in (teams or []) if t]
    if len(teams) >= 2:
        return (teams[0], teams[1])
    if len(teams) == 1:
        return (teams[0], "")
    return ("ALL", "TEAMS")

def main(collector: Optional[List[PickRecord]] = None):
    ensure_csv()
    df = pd.read_csv(CSV_PATH)
    df["Player"] = df["Player"].astype(str).str.strip()
    df["Team"]   = df["Team"].astype(str).str.upper().str.strip()
    df = _remove_banned_from_df(df)

    rows = fetch_unprocessed_rows(DATABASE_ID)

    grouped: Dict[Tuple[str,str], List[Dict]] = {}
    for r in rows:
        sport = (r["sport"] or "").strip().upper()
        if sport not in CBB_ALIASES:
            continue
        grouped.setdefault(_matchup_key(r["teams"]), []).append(r)

    def _group_sort_key(items: List[Dict]) -> Tuple:
        def _best(field: str):
            return min((_order_tuple(row.get(field)) for row in items), default=(2, float("inf"), ""))
        first = items[0] if items else {}
        return (
            _best("id_number"),
            _best("no_id"),
            _best("order"),
            min((_created_ts(row) for row in items), default=float("inf")),
            (first.get("sport") or "").upper(),
            ",".join(first.get("teams") or []).upper(),
        )

    # --- Regular CBB game rows ---
    for (t1, t2), game_rows in sorted(grouped.items(), key=lambda item: _group_sort_key(item[1])):
        stats_payload: List[Dict[str, Any]] = []
        def _canon_stat(row):
            s = (row["stat"] or "").strip().upper()
            return (STAT_MAP.get(s) or s).upper()
        game_rows.sort(key=lambda r: (STAT_RANK.get(_canon_stat(r), 99), _canon_stat(r)))

        heading_text = (
            "All Teams — CBB" if (t1, t2) == ("ALL","TEAMS")
            else f"{t1} vs {t2} — CBB" if t2
            else f"{t1} — CBB"
        )

        blocks = [_h3(heading_text)]
        pages_to_mark: List[str] = []

        for r in game_rows:
            page_id = r["page_id"]
            stat_in = (r["stat"] or "").strip().upper()
            stat_col = STAT_MAP.get(stat_in) or next((c for c in ["PPG","APG","RPG","3PM"] if c in df.columns), None)

            teams = [t.upper() for t in (r["teams"] or [])]
            use = df if not teams else df[df["Team"].isin(teams)]

            g: List[str] = []
            y: List[str] = []
            rr: List[str] = []
            p: List[str] = []
            if stat_col and not use.empty:
                g, y, rr, p = bucket_top12(use, stat_col)
            summary = _colors_block(g, y, rr, p)

            blocks.append(_para(f"{(stat_in or stat_col or '').upper()} — this season"))
            blocks.append(_para(summary))
            blocks.append({"object":"block","type":"divider","divider":{}})
            pages_to_mark.append(page_id)
            stats_payload.append({
                "stat": stat_in or stat_col or "",
                "teams": teams,
                "summary": summary,
                "generated_at": datetime.utcnow().isoformat(),
                "buckets": {
                    "green": g,
                    "yellow": y,
                    "red": rr,
                    "purple": p,
                },
            })

        _append(blocks)
        for pid in pages_to_mark:
            _update(pid, {"Processed": {"select": {"name": "Yes"}}})
        _record_pick(collector, {
            "category": "game",
            "heading": heading_text,
            "matchup": {"team1": t1, "team2": t2},
            "entries": stats_payload,
            "generated_at": datetime.utcnow().isoformat(),
        })
        print(f"[refresh] ✅ CBB group posted: {heading_text} ({len(game_rows)} stats)", file=sys.stderr)

    # --- PSP CBB (once, after regular) ---
    # Optional: add a single header for clarity
    # _append([_h3("CBB PSP — League Leaders")])
    process_psp_cbb(df, collector=collector)

def build_picks() -> List[PickRecord]:
    picks: List[PickRecord] = []
    main(collector=picks)
    _write_cache(picks)
    return picks

if __name__ == "__main__":
    picks = build_picks()
    print(json.dumps(picks))
