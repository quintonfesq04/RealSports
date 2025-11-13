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
import sys
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple
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
import unicodedata

import argparse
import logging

import argparse
import hashlib

import json
from pathlib import Path

_PAGE_CACHE: Dict[str, str] = {}

CACHE_FILE = Path(os.getenv("STATMUSE_CACHE_FILE", ".statmuse_cache.json"))
CACHE_TTL_SECS = int(os.getenv("STATMUSE_CACHE_TTL_SECS", "21600"))  # 6 hours
DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)
TEST2_CACHE = DATA_DIR / "test2.json"
SCHEDULE_JSON = Path(os.getenv("SCHEDULE_JSON", DATA_DIR / "schedule.json"))
PICKS_DATE = os.getenv("PICKS_DATE") or datetime.utcnow().strftime("%Y%m%d")
PickRecord = Dict[str, Any]

USE_NOTION_SOURCE = os.getenv("USE_NOTION_SOURCE", "0").strip().lower() in {"1", "true", "yes", "on"}
USE_LOCAL_SCHEDULE = not USE_NOTION_SOURCE

def _record_pick(collector: Optional[List[PickRecord]], entry: PickRecord) -> None:
    if collector is not None:
        collector.append(entry)

def _write_cache(payload: List[PickRecord], date: Optional[str] = None) -> None:
    try:
        TEST2_CACHE.write_text(json.dumps(payload), encoding="utf-8")
        if date:
            dated = DATA_DIR / f"picks_test2_{date}.json"
            dated.write_text(json.dumps(payload), encoding="utf-8")
    except Exception as exc:
        print(f"[cache] failed to write test2 picks cache: {exc}", file=sys.stderr)

# Feature flags (env can override; CLI can toggle too)
def _env_flag(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


HEADLESS_ENV = any(
    os.getenv(flag)
    for flag in (
        "CI",
        "CODESPACES",
        "SPACE_ID",
        "HF_TOKEN",
        "HF_HOME",
    )
)

DRY_RUN = _env_flag("DRY_RUN", "0")
DISABLE_SELENIUM = _env_flag("DISABLE_SELENIUM", "1" if HEADLESS_ENV else "0")
DISABLE_CACHE = _env_flag("DISABLE_CACHE", "0")
SAVE_BAD_HTML = _env_flag("SAVE_BAD_HTML", "0")
SAVE_BAD_HTML_DIR = Path(os.getenv("SAVE_BAD_HTML_DIR",".debug_html"))
if SAVE_BAD_HTML:
    SAVE_BAD_HTML_DIR.mkdir(parents=True, exist_ok=True)

# Simple fetch metrics
METRICS = {
    "cache_mem_hit": 0, "cache_disk_hit": 0,
    "requests_ok": 0, "requests_miss": 0,
    "selenium_ok": 0, "saved_html": 0,
}

TRY_PSP_STATMUSE = True  # PSP rows are just rollups; skip StatMuse scraping noise unless you set True
# How many CFB teams to combine per StatMuse query before chunking
MAX_CFB_COMBINED_TEAMS = 8
# How many teams we allow per StatMuse PSP query before chunking
MAX_PSP_COMBINED_TEAMS = {
    "NBA": 8,
    "WNBA": 8,
    "NHL": 10,
    "NFL": 10,
    "MLB": 12,
    "CBB": 8,
    "CFB": MAX_CFB_COMBINED_TEAMS,  # already handled specially
}

# PSP display ordering inside the final Notion post
_PSP_STAT_ORDER = {
    "MLB":  ["TB", "RBI", "K"],
    "WNBA": ["PPG", "APG", "RPG", "3PM"],
    "CFB":  ["TOTAL SCRIMMAGE YARDS", "RECEPTIONS", "TOTAL TOUCHDOWNS"],
    "NFL":  ["TOTAL SCRIMMAGE YARDS", "RECEPTIONS", "TOTAL TOUCHDOWNS"],
    "NBA":  ["PPG", "APG", "RPG", "3PM"],
    "CBB":  ["PPG", "APG", "RPG", "3PM"],
    "NHL":  ["SHOTS ON GOAL", "POINTS", "HITS", "SAVES"],
}
_PSP_SPORT_ORDER = ["MLB", "WNBA", "CFB", "NFL", "NHL", "NBA", "CBB"]

# Injury helpers (from injuries.py you added)
try:
    from injuries import get_injured_players_for_sport, remove_injured_rows
except Exception:
    # Fallback no-ops so the script still runs if the module isn't present
    def get_injured_players_for_sport(*args, **kwargs):
        return set()
    def remove_injured_rows(data, *args, **kwargs):
        return data

def _psp_stat_rank(sport_up: str, stat_up: str) -> int:
    order = _PSP_STAT_ORDER.get(sport_up, [])
    try:
        return order.index(stat_up)
    except ValueError:
        return 999

# ============================== ENV / Notion ===============================

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
    raise RuntimeError("NOTION_TOKEN is not set. Add it to your environment or .env")

if USE_NOTION_SOURCE:
    NOTION_TOKEN = _load_env_token()
    client = Client(auth=NOTION_TOKEN)
else:
    NOTION_TOKEN = ""
    client = None

# Verbose logging: set VERBOSE=1 in env to see mapping warnings, retries, etc.
VERBOSE = int(os.getenv("VERBOSE") or "0")
DRY_RUN = False

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

def football_season_year(today: Optional[date] = None) -> int:
    d = today or date.today()
    return d.year if d.month >= 8 else d.year - 1

SHOW_CFB_DEBUG_LINKS = False

# ============================== NFL controls ===============================

NFL_QUERY_MODE   = os.getenv("NFL_QUERY_MODE", "SEASON").upper()   # "SEASON" | "DATES" | "LAST_N_DAYS"
NFL_SEASON_YEAR = int(os.getenv("NFL_SEASON_YEAR") or football_season_year())
NFL_START_DATE   = os.getenv("NFL_START_DATE", "")  # e.g., "September 5, 2024"
NFL_END_DATE     = os.getenv("NFL_END_DATE",   "")  # e.g., "September 12, 2024"
NFL_LAST_N_DAYS  = int(os.getenv("NFL_LAST_N_DAYS") or "7")

# ============================== MLB controls ===============================

def baseball_season_year(today: Optional[date] = None) -> int:
    d = today or date.today()
    # MLB generally starts in Mar/Apr
    return d.year if d.month >= 3 else d.year - 1

MLB_SEASON_YEAR = int(os.getenv("MLB_SEASON_YEAR") or baseball_season_year())

# Restrict MLB to *this* postseason; override via .env if needed
MLB_PS_START_DATE = os.getenv("MLB_PS_START_DATE", f"October 1, {MLB_SEASON_YEAR}")
MLB_PS_END_DATE   = os.getenv("MLB_PS_END_DATE", "")  # blank -> today

# ============================== Injuries toggle ============================
# Load .env early (safe to call even if python-dotenv isn't installed)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from typing import Final

# Pylance-visible constant; controlled by .env USE_INJURIES=0/1 (or off/no/false)
INJURIES_ENABLED: Final[bool] = os.getenv("USE_INJURIES", "1").strip().lower() not in {
    "0", "false", "no", "off"
}

# ============================== HTTP session ===============================

try:
    from urllib3.util.retry import Retry
    from requests.adapters import HTTPAdapter
except Exception:
    Retry = None
    HTTPAdapter = None

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": DEFAULT_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    })
    if Retry and HTTPAdapter:
        retry = Retry(
            total=3, backoff_factor=0.3,
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

def _parse_date_any(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    for fmt in ("%B %d, %Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return datetime(dt.year, dt.month, dt.day)
        except Exception:
            pass
    return None

def _filter_rows_by_year(rows: List[Dict[str, str]], year: int) -> List[Dict[str, str]]:
    """If the table exposes a season/year column, keep only rows for that year."""
    if not rows:
        return rows
    YEAR_KEYS = {"SEASON", "YEAR", "YR"}
    year_col = next((k for k in rows[0].keys() if k.strip().upper() in YEAR_KEYS), None)
    if not year_col:
        return rows
    filtered = []
    for r in rows:
        val = re.sub(r"[^0-9]", "", str(r.get(year_col, "")))
        try:
            y = int(val)
        except Exception:
            y = None
        if y == year:
            filtered.append(r)
    return filtered

CFB_SEASON_YEAR = int(os.getenv("CFB_SEASON_YEAR") or football_season_year())

# ============================== Notion input ===============================

def fetch_unprocessed_rows(db_id: str) -> List[Dict]:
    if client is None:
        return []
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

def _schedule_file_for_date(date_str: str) -> Path:
    candidate = DATA_DIR / f"schedule_{date_str}.json"
    if candidate.exists():
        return candidate
    return SCHEDULE_JSON


def load_schedule_rows_from_json(date: str) -> List[Dict]:
    path = _schedule_file_for_date(date)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
    except Exception:
        return []
    rows: List[Dict] = []
    for idx, entry in enumerate(data):
        sport = (entry.get("sport") or "").strip().upper()
        stat = (entry.get("stat") or "").strip()
        teams = entry.get("teams") or []
        rows.append({
            "page_id": entry.get("page_id") or f"{sport}-{entry.get('event_id')}-{stat}-{idx}",
            "sport": sport,
            "stat": stat,
            "teams": teams,
            "order": idx,
            "start_time": entry.get("start_time"),
            "psp": bool(entry.get("psp")),
        })
    return rows


def load_game_rows() -> List[Dict]:
    if USE_LOCAL_SCHEDULE:
        rows = load_schedule_rows_from_json(PICKS_DATE)
    else:
        rows = fetch_unprocessed_rows(DATABASE_ID)
    return [r for r in rows if not r.get("psp")]


def load_psp_rows_from_json() -> List[Dict]:
    rows = load_schedule_rows_from_json(PICKS_DATE)
    return [r for r in rows if r.get("psp")]


def fetch_psp_rows_from_notion() -> List[Dict]:
    if client is None:
        return []
    resp = client.databases.query(
        database_id=PSP_DATABASE_ID,
        filter={"property":"Processed","select":{"equals":"no"}},
        page_size=100,
    )
    rows = resp.get("results", [])
    if not rows:
        return []

    by_sport: Dict[str, List[Dict]] = {}
    for r in rows:
        props = r.get("properties", {})
        sport = props["Sport"]["select"]["name"].strip()
        by_sport.setdefault(sport.upper(), []).append(r)

    ordered_rows: List[Dict] = []

    def _stat_text(props: dict) -> str:
        st = props.get("Stat")
        if not isinstance(st, dict):
            return ""
        typ = st.get("type")
        if typ == "select" and st.get("select"):
            return (st["select"].get("name") or "").strip()
        if typ == "rich_text" and st.get("rich_text"):
            return "".join(x.get("plain_text", "") for x in st["rich_text"]).strip()
        if typ == "title" and st.get("title"):
            return "".join(x.get("plain_text", "") for x in st["title"]).strip()
        return ""

    for sport_up in _PSP_SPORT_ORDER:
        group = by_sport.get(sport_up)
        if not group:
            continue
        def _stat_rank(row):
            return _psp_stat_rank(sport_up, _stat_text(row.get("properties", {})).upper())
        group.sort(key=_stat_rank)
        for r in group:
            props = r["properties"]
            sport = props["Sport"]["select"]["name"].strip()
            st = props["Stat"]
            if st["type"] == "select":
                stat = st["select"]["name"].strip()
            else:
                stat = "".join(t["plain_text"] for t in st.get("rich_text", [])).strip()

            teams_prop = props.get("Teams", {})
            if teams_prop.get("type") == "multi_select":
                teams = [o["name"] for o in teams_prop["multi_select"]] or None
            elif teams_prop.get("type") in ("rich_text","title"):
                raw = "".join(t["plain_text"] for t in teams_prop.get(teams_prop["type"], []))
                teams = [x.strip() for x in raw.split(",") if x.strip()]
            else:
                teams = None

            ordered_rows.append({
                "page_id": r["id"],
                "sport": sport.strip().upper(),
                "stat": stat,
                "teams": teams or [],
                "start_time": None,
                "psp": True,
            })
    return ordered_rows


def load_psp_rows() -> List[Dict]:
    if USE_LOCAL_SCHEDULE:
        return load_psp_rows_from_json()
    return fetch_psp_rows_from_notion()

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

def _disk_cache_get(url: str) -> Optional[str]:
    """Return cached HTML if fresh, else None."""
    try:
        if not CACHE_FILE.exists():
            return None
        blob = json.loads(CACHE_FILE.read_text("utf-8"))
        rec = blob.get(url)
        if not rec:
            return None
        # TTL check
        if time.time() - float(rec.get("ts", 0)) > CACHE_TTL_SECS:
            return None
        return rec.get("html") or None
    except Exception:
        return None

def _disk_cache_put(url: str, html: str) -> None:
    """Store HTML on disk with timestamp (best-effort, safe if it fails)."""
    try:
        blob = {}
        if CACHE_FILE.exists():
            blob = json.loads(CACHE_FILE.read_text("utf-8"))
        blob[url] = {"ts": time.time(), "html": html}

        # Simple pruning so file doesn’t grow forever
        if len(blob) > 5000:
            items = sorted(blob.items(), key=lambda kv: kv[1].get("ts", 0))
            for k, _ in items[:max(1, len(items)//10)]:
                blob.pop(k, None)

        CACHE_FILE.write_text(json.dumps(blob), encoding="utf-8")
    except Exception:
        pass

def _log(msg: str):
    if VERBOSE:
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] {msg}", file=sys.stderr)

def _safe_name_from_url(url: str) -> str:
    h = hashlib.md5(url.encode("utf-8")).hexdigest()[:10]
    return re.sub(r"[^A-Za-z0-9]+", "_", url)[:40] + "_" + h + ".html"

def _maybe_save_html(url: str, html: str, reason: str = "no-table"):
    if not SAVE_BAD_HTML or not html:
        return
    try:
        fname = SAVE_BAD_HTML_DIR / _safe_name_from_url(url)
        fname.write_text(html, encoding="utf-8")
        METRICS["saved_html"] += 1
        _log(f"[debug-html] saved {reason} → {fname}")
    except Exception:
        pass

def fetch_html(url: str, wait_css: str = "table", wait_seconds: int = 25) -> str:
    """
    Order: in-memory cache → disk cache → requests → selenium (unless disabled).
    Saves non-tabling pages to disk when SAVE_BAD_HTML=1 (or via CLI).
    """
    # 0) In-memory cache
    if not DISABLE_CACHE and url in _PAGE_CACHE:
        METRICS["cache_mem_hit"] += 1
        return _PAGE_CACHE[url]

    # 1) Disk cache (persistent)
    if not DISABLE_CACHE:
        cached = _disk_cache_get(url)
        if cached:
            METRICS["cache_disk_hit"] += 1
            _PAGE_CACHE[url] = cached
            return cached

    # 2) Fast path — requests
    try:
        r = SESSION.get(url, timeout=10)
        r.encoding = "utf-8"
        if r.ok:
            html = r.text
            if "<table" in html or "<TABLE" in html:
                METRICS["requests_ok"] += 1
                _PAGE_CACHE[url] = html
                if not DISABLE_CACHE and html:
                    _disk_cache_put(url, html)
                return html
            else:
                METRICS["requests_miss"] += 1
                _maybe_save_html(url, html, reason="requests-no-table")
        else:
            METRICS["requests_miss"] += 1
    except Exception:
        METRICS["requests_miss"] += 1

    # 3) Selenium (unless disabled)
    if DISABLE_SELENIUM:
        _log(f"[selenium] disabled; skipping for {url}")
        return ""

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
                pass  # still try to read page_source

            html = drv.page_source or ""
            _PAGE_CACHE[url] = html
            if not DISABLE_CACHE and html:
                _disk_cache_put(url, html)

            # Metrics + debug save
            if html:
                METRICS["selenium_ok"] += 1
                if "<table" not in html and "<TABLE" not in html:
                    _maybe_save_html(url, html, reason="selenium-no-table")

            # Recycle the driver every N pages to prevent hangs
            global _PAGES_FETCHED
            _PAGES_FETCHED += 1
            if _PAGES_FETCHED % _RECYCLE_EVERY == 0:
                _log("[webdriver] recycling chrome driver")
                _DriverPool.close()

            return html
        except WebDriverException as e:
            last_err = e
            _DriverPool.close()  # force a fresh driver next loop

    if VERBOSE and last_err:
        print(f"[webdriver] error for {url}: {last_err}", file=sys.stderr)
    return ""

from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_many(urls: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not urls:
        return out

    pending: List[str] = []
    for u in urls:
        if not DISABLE_CACHE and u in _PAGE_CACHE:
            METRICS["cache_mem_hit"] += 1
            out[u] = _PAGE_CACHE[u]
        elif not DISABLE_CACHE:
            cached = _disk_cache_get(u)
            if cached:
                METRICS["cache_disk_hit"] += 1
                _PAGE_CACHE[u] = cached
                out[u] = cached
            else:
                pending.append(u)
        else:
            pending.append(u)

    if not pending:
        return out

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(SESSION.get, u, timeout=10): u for u in pending}
        for fut in as_completed(futs):
            u = futs[fut]
            try:
                r = fut.result()
                if r.ok:
                    r.encoding = "utf-8"
                    html = r.text
                    if "<table" in html or "<TABLE" in html:
                        METRICS["requests_ok"] += 1
                    else:
                        METRICS["requests_miss"] += 1
                        _maybe_save_html(u, html, reason="fetch_many-no-table")
                    _PAGE_CACHE[u] = html
                    out[u] = html
                    if not DISABLE_CACHE and html:
                        _disk_cache_put(u, html)
                else:
                    METRICS["requests_miss"] += 1
                    _PAGE_CACHE[u] = ""
            except Exception:
                METRICS["requests_miss"] += 1
                _PAGE_CACHE[u] = ""

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

# --- small utility used by PSP split/merge across sports ---
def _chunks(seq, size):
    """
    Yield consecutive slices of 'seq' of length 'size'.
    Safe for None/empty inputs and goofy sizes.
    """
    items = list(seq or [])
    try:
        n = int(size)
    except Exception:
        n = 1
    n = max(1, n)
    for i in range(0, len(items), n):
        yield items[i:i+n]

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
            print(f"[warn] Team(s) not mapped for {sport_up}: {', '.join(unmapped)}", file=sys.stderr)
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
    stat_query = STAT_QUERY_OVERRIDES.get((sport_up, stat_up)) or stat
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
            print(f"[warn] Team(s) not mapped for CFB: {', '.join(unmapped)}", file=sys.stderr)

        # Label wording: TDs need the special phrase
        def _stat_label() -> str:
            return "total touchdowns" if stat_up in {"TD","TDS","TOUCHDOWNS","TOTAL TOUCHDOWNS"} else stat_query

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
            print("[parse] no CFB rows for:", debug_links, file=sys.stderr)

        # IMPORTANT: Do NOT post-filter CFB by team later; we want the union of results.
        return all_rows, True, season, debug_links

        # ----- NFL: separate timeframe controls; league-wide -----
    if _is_nfl(sport_up):
        # decide base query text once
        if NFL_QUERY_MODE == "SEASON":
            base_q = f"{stat_query} leaders nfl this season"
            used_season = True
        elif NFL_QUERY_MODE == "DATES" and NFL_START_DATE and NFL_END_DATE:
            base_q = f"{stat_query} leaders nfl from {NFL_START_DATE} to {NFL_END_DATE}"
            used_season = False
        else:
            used_season = False
            end = datetime.today()
            start = end - timedelta(days=max(1, NFL_LAST_N_DAYS - 1))
            # we'll pass dates via build_query_url
            base_q = f"{stat_query} leaders nfl"

        # chunk if PSP passed a long team list (len > 2 usually means PSP)
        limit = MAX_PSP_COMBINED_TEAMS.get("NFL", 10)
        team_list = teams or []
        all_rows: List[Dict[str, str]] = []
        debug_links: List[str] = []

        if team_list and len(team_list) > limit:
            for chunk in _chunks(team_list, limit):
                if NFL_QUERY_MODE == "SEASON":
                    url = build_query_url(base_q, chunk, "NFL", include_dates=False)
                elif NFL_QUERY_MODE == "DATES" and NFL_START_DATE and NFL_END_DATE:
                    url = build_query_url(base_q, chunk, "NFL", include_dates=False)
                else:
                    end = datetime.today()
                    start = end - timedelta(days=max(1, NFL_LAST_N_DAYS - 1))
                    url = build_query_url(base_q, chunk, "NFL", start, end, include_dates=True)
                debug_links.append(url)
                html = fetch_html(url)
                rows = parse_table(html)
                if rows:
                    all_rows.extend(rows)
        else:
            # normal single query path
            if NFL_QUERY_MODE == "SEASON":
                url = build_query_url(base_q, team_list, "NFL", include_dates=False)
            elif NFL_QUERY_MODE == "DATES" and NFL_START_DATE and NFL_END_DATE:
                url = build_query_url(base_q, team_list, "NFL", include_dates=False)
            else:
                end = datetime.today()
                start = end - timedelta(days=max(1, NFL_LAST_N_DAYS - 1))
                url = build_query_url(base_q, team_list, "NFL", start, end, include_dates=True)
            debug_links.append(url)
            all_rows = parse_table(fetch_html(url))

            # explicit-year fallback if season wording fails
            if not all_rows and NFL_QUERY_MODE == "SEASON":
                url2 = build_query_url(f"{stat_query} leaders nfl {NFL_SEASON_YEAR}", team_list, "NFL", include_dates=False)
                debug_links.append(url2)
                all_rows = parse_table(fetch_html(url2))

        return all_rows, used_season, (NFL_SEASON_YEAR if used_season else 0), debug_links

        # ----- Others: full-season (or MLB postseason) with teams; chunk & merge -----
    sport_limit = MAX_PSP_COMBINED_TEAMS.get(sport_up, 8)
    team_list = teams or []
    all_rows: List[Dict[str, str]] = []
    debug_links: List[str] = []

    # Build candidate queries depending on sport/timeframe
    if sport_up == "MLB":
        # STRICT: only this postseason
        # 1) Prefer explicit "this postseason" and "{year} postseason"
        candidates = [
            f"{stat_query} leaders mlb this postseason",
            f"{stat_query} leaders mlb postseason {MLB_SEASON_YEAR}",
        ]
        include_dates_flag = False

        # 2) Prepare a date-window fallback for *this* postseason only
        ps_start = _parse_date_any(MLB_PS_START_DATE) or datetime(MLB_SEASON_YEAR, 10, 1)
        ps_end   = _parse_date_any(MLB_PS_END_DATE) or datetime.today()
        ps_end   = datetime(ps_end.year, ps_end.month, ps_end.day)  # normalize to midnight
    else:
        # Everyone else = this season
        candidates = [f"{stat_query} leaders {sport.lower()} this season"]
        include_dates_flag = False

    # Try each candidate until we get rows
    for base_q in candidates:
        attempt_rows: List[Dict[str, str]] = []
        if team_list and len(team_list) > sport_limit:
            for chunk in _chunks(team_list, sport_limit):
                url = build_query_url(base_q, chunk, sport, include_dates=include_dates_flag)
                debug_links.append(url)
                html = fetch_html(url)
                rows = parse_table(html)
                if rows:
                    attempt_rows.extend(rows)
        else:
            url = build_query_url(base_q, team_list, sport, include_dates=include_dates_flag)
            debug_links.append(url)
            html = fetch_html(url)
            attempt_rows = parse_table(html)

        # For MLB, drop wrong-year rows if the table exposes a season/year column
        if sport_up == "MLB":
            attempt_rows = _filter_rows_by_year(attempt_rows, MLB_SEASON_YEAR)

        if attempt_rows:
            all_rows = attempt_rows
            break

    # MLB final fallback: explicit date window (no generic "postseason" text!)
    if sport_up == "MLB" and not all_rows:
        if team_list and len(team_list) > sport_limit:
            tmp: List[Dict[str, str]] = []
            for chunk in _chunks(team_list, sport_limit):
                url = build_query_url(f"{stat_query} leaders mlb", chunk, sport, ps_start, ps_end, include_dates=True)
                debug_links.append(url)
                html = fetch_html(url)
                rows = parse_table(html)
                if rows:
                    tmp.extend(rows)
            all_rows = tmp
        else:
            url = build_query_url(f"{stat_query} leaders mlb", team_list, sport, ps_start, ps_end, include_dates=True)
            debug_links.append(url)
            html = fetch_html(url)
            all_rows = parse_table(html)

        # Year filter again if a column exists
        all_rows = _filter_rows_by_year(all_rows, MLB_SEASON_YEAR)

    if not all_rows and VERBOSE:
        print(f"[parse] no data for {sport_up} statmuse (season/postseason mode; teams={len(team_list)})", file=sys.stderr)

    # season_year is only meaningful for MLB postseason display here
    return all_rows, False, (MLB_SEASON_YEAR if sport_up == "MLB" else 0), debug_links

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
    "DKMetcalf": "DK Metcalf",
    "Calvin Austin IIIC III": "Calvin Austin III",
    "Ray-Ray Mc Cloud IIIR III": "Ray-Ray McCloud III",
    "Ollie Gordon IIO II": "Ollie Gordon II",
    "Will Mc Donald IVW IV": "Will McDonald IV",
    "Tre Veyon Henderson": "TreVeyon Henderson",
    "De Mario Douglas": "DeMario Douglas",
    "Tetairoa Mc Millan": "Tetairoa McMillan",
    "DJMoore": "DJ Moore",
    "Luther Burden IIIL III": "Luther Burden III",
    "Stroud": "C.J. Stroud",
    "Cee Dee Lamb": "CeeDee Lamb",
    "RJHarvey": "RJ Harvey",
    "Ka Vontae Turpin": "KaVontae Turpin",
    "Dobbins": "J.K. Dobbins",
    "Chig Okonkwo": "Chigoziem Okonkwo",
    "Kenny Moore IIK II": "Kenny Moore II",
    "John Fitz Patrick": "John FitzPatrick",
    "Ronald Holland IIR II": "Ronald Holland II",
    "Caris Le Vert": "Caris LeVert",
    "AJGreen": "AJ Green",
    "OGAnunoby": "OG Anunoby",
    "Nikola JovićN Jović": "Nikola Jović",
    "Miles Mc Bride": "Miles McBride",
    "La Melo Ball": "LaMelo Ball",
    "Jaden Mc Daniels": "Jaden McDaniels",
    "Donte Di Vincenzo": "Donte DiVincenzo",
    "RJBarrett": "RJ Barrett",
    "Gradey Dick": "Gradey D.",
    "Dereck Lively IID II": "Dereck Lively II",
    "Luka DončićL Dončić": "Luka Dončić",
    "Zach La Vinet": "Zach LaVine",
    "Jake La Ravia": "Jake LaRavia",
    "Dario ŠarićD Šarić": "Dario Šarić",
    "CJMc Collum": "CJ McCollum",
    "Nathan Mac Kinnon": "Nathan MacKinnon",
    "Ryan Mc Donagh": "Ryan McDonagh",
    "Zach La Vine": "Zach LaVine",
    "De Mar Rozan": "DeMar DeRozan",
    "Terry Mc Laurin": "Terry McLaurin",
    "Jeremy Mc Nichols": "Jeremy McNichols",
    "Luke Mc Caffrey": "Luke McCaffrey",
    "VJEdgecombe": "VJ Edgecombe",
    "Nikola VučevićN Vučević": "Nikola Vučević",
    "Trey Murphy IIIT III": "Trey Murphy III",
    "Jusuf NurkićJ Nurkić": "Jusuf Nurkić",
    "Nikola JokićN Jokić": "Nikola Jokić",
    "Da Ron Holmes IID II": "DaRon Holmes II",
    "Jimmy Butler IIIJ III": "Jimmy Butler",
    "Washington": "P.J. Washington",
    "Connor Mc David": "Connor McDavid",
    "Devin Mc Cuinnn": "Devin McCuinn",
    "Ray JDennis Dennis": "RayJ Dennis",
    "Alex De Brincat": "Alex DeBrincat",
    "Michael Mc Carron": "Michael McCarron",
    "Bobby Mc Mannion": "Bobby McMann",
    "Jake De Brusk": "Jake DeBrusk",
    "Greer": "A.J. Greer",
    "Jake Mc Connachie": "Jake McConnachie",
    "Israel Polk": "I. Polk", 
    "JJPeterka": "JJ Peterka",
    "Ryan Mc Leod": "Ryan McLeod",
    "Tony De Angelo": "Tony DeAngelo",
    "Charlie Mc Avoy": "Charlie McAvoy",
    "Miller": "J.T. Miller",
    "Dylan De Melo": "Dylan DeMelo",
    "Marvin Bagley IIIM III": "Marvin Bagley III",
    "Joseph Himon IIJ II": "Joseph Himon II",
    "Hayden Eligon IIH II": "Hayden Eligon II",
    "Lake Mc Ree": "Lake McRee",
}

def _name_key(s: str) -> str:
    """
    Canonical key for name matching: case/diacritic/punct/space-insensitive,
    and ignores common suffixes like Jr/Sr/II/III/IV/V.
    """
    if not s:
        return ""
    # Remove accents (Dončić -> Doncic)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()

    # Normalize punctuation/spaces (incl. curly apostrophes & HTML '&apos;')
    s = (s.replace("&apos;", "")
           .replace("’", "")
           .replace("'", "")
           .replace(".", "")
           .replace("-", " "))

    # Drop common suffixes
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", s)

    # Strip all non-alphanumerics and collapse
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

BANNED_PLAYERS = {
    "Ray-Ray Mc Cloud III",
    "K'Lavon Chaisson",
    "Marcus Jones",
    "Jalyx Hunt",
    "Jordan Davis",
    "Sydney Brown",
    "Ja'Tavion Sanders",
    "Brycen Tremayne",
    "Tommy Tremble",
    "Jimmy Horn Jr",
    "Mitchell Evans",
    "Nahshon Wright",
    "Tylan Wallace",
    "Luke Farrell",
    "Will Anderson Jr",
    "Jaylin Noel",
    "Devaughn Vele",
    "Jack Stoll",
    "Jordan Howden",
    "Taysom Hill",
    "Ryan Flournoy",
    "Tyler Lockett",
    "David Martin-Robinson",
    "Connor Heyward",
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
    "TOTAL GOALS": {"TOTAL GOALS", "GOALS"},
    "SHOTS ON GOAL": {"SHOTS ON GOAL", "SHOTS"},
    "SHOTS": {"SHOTS", "SHOTS ON GOAL"},
    "HITS": {"HITS"},
    "SAVES": {"SAVES"},
}

STAT_QUERY_OVERRIDES = {
    ("NHL", "TOTAL GOALS"): "goals",
    ("NHL", "SHOTS ON GOAL"): "shots on goal",
    ("NHL", "SHOTS"): "shots on goal",
    ("NHL", "POINTS"): "points",
    ("NHL", "HITS"): "hits",
    ("NHL", "SAVES"): "saves",
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
    seen, out = set(), []
    for rec in rows:
        key = _name_key(rec.get(name_col, ""))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(rec)
    return out

def remove_banned_rows(rows: List[Dict[str, str]], banned: set) -> List[Dict[str, str]]:
    if not rows or not banned:
        return rows

    # Build ban keys from both raw and cleaned variants
    ban_keys = {_name_key(n) for n in banned}
    ban_keys |= {_name_key(clean_name(n)) for n in banned}

    name_col = "NAME" if "NAME" in rows[0] else "PLAYER" if "PLAYER" in rows[0] else list(rows[0].keys())[0]
    out = []
    for rec in rows:
        nm = rec.get(name_col, "")
        k1 = _name_key(nm)
        k2 = _name_key(clean_name(nm))
        if k1 in ban_keys or k2 in ban_keys:
            if VERBOSE:
                print(f"[ban-final] {nm}", file=sys.stderr)
            continue
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
            print(f"[injuries-mlb] fetch error: {e}", file=sys.stderr)
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
            print(f"[trades-web] fetch error: {e}", file=sys.stderr)
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
    teams_set = set(_normalize_abbr(t) for t in (teams or []))

    # Normalize keys once for speed
    traded_key_map = { _name_key(k): _normalize_abbr(v)
                       for k, v in (traded_players or {}).items() }
    banned_keys = { _name_key(n) for n in (banned_players or set()) }

    out = []
    for rec in data:
        # choose name column
        name_col = "NAME" if "NAME" in rec else "PLAYER" if "PLAYER" in rec else list(rec.keys())[0]
        nm_raw = rec.get(name_col, "")
        key    = _name_key(nm_raw)
        key_c  = _name_key(clean_name(nm_raw))
        if key in banned_keys or key_c in banned_keys:
            if VERBOSE:
                print(f"[filter] banned: {nm_raw}", file=sys.stderr)
            continue

        # row's team code
        team_code = _normalize_abbr(_get_team_from_row(rec))

        # traded team mismatch? (only if we know both)
        mapped_team = traded_key_map.get(key)
        if mapped_team and team_code and team_code != mapped_team:
            if VERBOSE:
                print(f"[filter] traded-mismatch: {nm_raw} row_team={team_code} mapped={mapped_team}", file=sys.stderr)
            continue

        # explicit team filter (if provided)
        if teams_set and team_code and team_code not in teams_set:
            continue

        out.append(rec)
    return out

# ============================== Notion helpers =============================

def notion_append_blocks(blocks: List[Dict]):
    if client is None:
        return
    if DRY_RUN:
        _log(f"[dry-run] would append {len(blocks)} blocks to poll page {POLL_PAGE_ID}")
        return
    for attempt in range(3):
        try:
            client.blocks.children.append(block_id=POLL_PAGE_ID, children=blocks); return
        except Exception as e:
            msg = str(e)
            if "Rate limited" in msg or "429" in msg:
                time.sleep(2 ** attempt); continue
            print(f"[notion] append error: {e}", file=sys.stderr); return

def notion_update_page(page_id: str, props: Dict):
    if client is None:
        return
    if DRY_RUN:
        _log(f"[dry-run] would update page {page_id} props={list(props.keys())}")
        return
    for attempt in range(3):
        try:
            client.pages.update(page_id=page_id, properties=props); return
        except Exception as e:
            msg = str(e)
            if "Rate limited" in msg or "429" in msg:
                time.sleep(2 ** attempt); continue
            print(f"[notion] update error: {e}", file=sys.stderr); return

def _link_para(text: str, url: str) -> Dict:
    return {
        "object":"block","type":"paragraph",
        "paragraph":{"rich_text":[{"type":"text","text":{"content":text,"link":{"url":url}}}]}
    }

def _post_colors_only_block(page_id: str, heading_text: str):
    summary = _colors_only_summary()
    if client is None:
        return
    notion_append_blocks([
        {"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":heading_text}}]}},
        {"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":summary}}]}},
        {"object":"block","type":"divider","divider":{}}
    ])
    notion_update_page(page_id, {"Processed":{"select":{"name":"Yes"}}})

def post_run_log(message: str):
    if client is None:
        return
    if not os.getenv("POST_RUN_LOG", "0").strip() in {"1","true","yes","on"}:
        return
    notion_append_blocks([
        {
          "object":"block","type":"callout",
          "callout":{"icon":{"emoji":"🧪"},"rich_text":[{"type":"text","text":{"content":message}}]}
        },
        {"object":"block","type":"divider","divider":{}}
    ])

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


# --- Replacement for process_games() ---

def process_games(collector: Optional[List[PickRecord]] = None):
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
    rows = load_game_rows()
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
        game_entries: List[Dict[str, Any]] = []

        # We’ll mark Processed for each row we actually handle
        pages_to_mark: List[str] = []
        for r in game_rows:
            page_id = r["page_id"]
            sport   = r["sport"]
            stat    = r["stat"]
            teams   = r["teams"]
            stat_up = (stat or "").strip().upper()
            sport_up_local = (sport or "").strip().upper()

            # Colors-only for strikeouts
            if stat_up in {"K", "SO", "STRIKEOUTS"}:
                # section header line (season/postseason wording)
                _when = "this postseason" if sport_up_local == "MLB" else "this season"
                summary = _colors_only_summary()
                blocks_for_game.append({
                    "object":"block","type":"paragraph",
                    "paragraph":{"rich_text":[{"type":"text","text":{"content":f"{stat} — {_when}"}}]}
                })
                # colors-only grid
                blocks_for_game.append({
                    "object":"block","type":"paragraph",
                    "paragraph":{"rich_text":[{"type":"text","text":{"content":summary}}]}
                })

                blocks_for_game.append({"object":"block","type":"divider","divider":{}})
                pages_to_mark.append(page_id)
                game_entries.append({
                    "stat": stat,
                    "suffix": _when,
                    "summary": summary,
                    "teams": teams,
                    "sport": sport_up_local,
                    "generated_at": datetime.utcnow().isoformat(),
                    "debug_links": [],
                    "buckets": {
                        "green": [],
                        "yellow": [],
                        "red": [],
                        "purple": [],
                    },
                })
                continue

            traded_players = {}
            if sport_up_local == "MLB":
                traded_players = fetch_trades_past_week()

            # Scrape StatMuse (regular games always scrape)
            data, used_season_mode, season_year, debug_links = scrape_statmuse_data(stat, sport, teams)

            # Post-filters (with centralized injury filter)
            injured = get_injured_players_for_sport(
                sport_up_local, session=SESSION, verbose=bool(VERBOSE), clean_name_fn=clean_name
            ) if INJURIES_ENABLED else set()

            if sport_up_local == "MLB":
                data = filter_traded_banned_and_teams(
                    data, teams, traded_players=traded_players, banned_players=BANNED_PLAYERS
                )
                if data and injured:
                    data = remove_injured_rows(
                        data, injured, clean_name_fn=clean_name, name_key_fn=_name_key, verbose=bool(VERBOSE)
                    )
                if data:
                    data = dedupe_by_player(data)

            elif sport_up_local == "NFL":
                data = filter_traded_banned_and_teams(data, teams, banned_players=BANNED_PLAYERS)
                if data and injured:
                    data = remove_injured_rows(
                        data, injured, clean_name_fn=clean_name, name_key_fn=_name_key, verbose=bool(VERBOSE)
                    )

            elif sport_up_local in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
                # keep union; optional injury drop
                if data and injured:
                    data = remove_injured_rows(
                        data, injured, clean_name_fn=clean_name, name_key_fn=_name_key, verbose=bool(VERBOSE)
                    )

            else:
                data = filter_traded_banned_and_teams(data, teams, banned_players=BANNED_PLAYERS)
                if data and injured:
                    data = remove_injured_rows(
                        data, injured, clean_name_fn=clean_name, name_key_fn=_name_key, verbose=bool(VERBOSE)
                    )
                if data:
                    data = dedupe_by_player(data)

            # Final safety sweep: drop anything still matching the ban list
            data = remove_banned_rows(data, BANNED_PLAYERS)

            # Build summary
            if not data:
                summary = _colors_only_summary()
                g_list: List[str] = []
                y_list: List[str] = []
                rd_list: List[str] = []
                p_list: List[str] = []
            else:
                data = dedupe_by_player(data)
                desired_key = "TD" if (sport_up_local in {"CFB","NCAAF","COLLEGE FOOTBALL"} and stat_up in {"TD","TDS","TOUCHDOWNS"}) else stat
                stat_key    = pick_stat_key(data, desired_key)
                g, y, rd, p = bucket_top12(data, stat_key)
                g_list, y_list, rd_list, p_list = g, y, rd, p
                if sport_up_local in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
                    summary = _format_buckets_cfb(g, y, rd, p) or "(no qualifying leaders found)"
                else:
                    summary = _format_buckets_default(g, y, rd, p)

            # Suffix
            if sport_up_local in {"CFB","NCAAF","COLLEGE FOOTBALL"} and used_season_mode:
                suffix = f"{season_year}"
            elif sport_up_local == "NFL":
                if NFL_QUERY_MODE == "SEASON":
                    suffix = f"{NFL_SEASON_YEAR}"
                elif NFL_QUERY_MODE == "DATES" and NFL_START_DATE and NFL_END_DATE:
                    suffix = f"{NFL_START_DATE} → {NFL_END_DATE}"
                else:
                    suffix = f"last {NFL_LAST_N_DAYS} days"
            elif sport_up_local == "MLB":
                suffix = f"this postseason {MLB_SEASON_YEAR}"
            else:
                suffix = "this season"

            # Section blocks for this stat
            blocks_for_game.append({
                "object":"block","type":"paragraph",
                "paragraph":{"rich_text":[{"type":"text","text":{"content":f"{stat} — {suffix}"}}]}
            })
            blocks_for_game.append({
                "object":"block","type":"paragraph",
                "paragraph":{"rich_text":[{"type":"text","text":{"content":summary}}]}
            })
            sampled_links: List[str] = []
            if SHOW_CFB_DEBUG_LINKS and debug_links:
                seen = set()
                for u in debug_links:
                    if not u or u in seen:
                        continue
                    seen.add(u)
                    sampled_links.append(u)
                    if len(sampled_links) >= 6:
                        break

            # After blocks_for_game.append(...) that writes the summary
            if SHOW_CFB_DEBUG_LINKS and debug_links:
                # Dedup & cap to something reasonable
                seen = set()
                shown = 0
                for u in debug_links:
                    if not u or u in seen:
                        continue
                    seen.add(u)
                    shown += 1
                    blocks_for_game.append(_link_para(f"View query {shown}", u))
                    if shown >= 6:  # cap so the post doesn't get too long
                        break

            blocks_for_game.append({"object":"block","type":"divider","divider":{}})

            pages_to_mark.append(page_id)
            game_entries.append({
                "stat": stat,
                "suffix": suffix,
                "summary": summary,
                "teams": teams,
                "sport": sport_up_local,
                "generated_at": datetime.utcnow().isoformat(),
                "debug_links": sampled_links,
                "buckets": {
                    "green": g_list,
                    "yellow": y_list,
                    "red": rd_list,
                    "purple": p_list,
                },
            })

        # Push one append per game (Notion only)
        if client is not None:
            notion_append_blocks(blocks_for_game)
            for pid in pages_to_mark:
                notion_update_page(pid, {"Processed": {"select": {"name": "Yes"}}})

        # Console
        try:
            print(f"[refresh] ✅ Posted grouped game: {heading_text} ({len(game_rows)} stats)", file=sys.stderr)
        except Exception:
            print(f"[refresh] ✅ Posted grouped game: {sport_up} ({len(game_rows)} stats)", file=sys.stderr)
        _record_pick(collector, {
            "category": "game",
            "sport": sport_up,
            "heading": heading_text,
            "matchup": {"team1": t1, "team2": t2},
            "teams": game_rows[0]["teams"] if game_rows and game_rows[0].get("teams") else [],
            "entries": game_entries,
            "generated_at": datetime.utcnow().isoformat(),
        })

def process_psp_rows(collector: Optional[List[PickRecord]] = None):
    rows = load_psp_rows()
    if not rows:
        return

    rows_by_sport: Dict[str, List[Dict]] = {}
    for r in rows:
        rows_by_sport.setdefault((r.get("sport") or "").strip().upper(), []).append(r)

    notion_enabled = client is not None and USE_NOTION_SOURCE

    for sport_up in _PSP_SPORT_ORDER:
        group = rows_by_sport.get(sport_up)
        if not group:
            continue

        group.sort(key=lambda row: _psp_stat_rank(sport_up, (row.get("stat") or "").strip().upper()))

        blocks_for_sport: List[Dict] = []
        pages_to_mark: List[str] = []

        for r in group:
            pid = r.get("page_id")
            sport_up_local = (r.get("sport") or "").strip().upper()
            stat = r.get("stat") or ""
            stat_up = stat.strip().upper()
            teams = r.get("teams") or []

            if sport_up_local == "MLB" and stat_up in {"K","SO","STRIKEOUTS"}:
                heading = f"{sport_up_local} PSP - {stat_up} leaders (this postseason)"
                summary = _colors_only_summary()
                blocks_for_sport.append({"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":heading}}]}})
                blocks_for_sport.append({"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":summary}}]}})
                if pid:
                    pages_to_mark.append(pid)
                _record_pick(collector, {
                    "category": "psp",
                    "sport": sport_up_local,
                    "heading": heading,
                    "stat": stat_up,
                    "teams": teams,
                    "summary": summary,
                    "suffix": "this postseason",
                    "generated_at": datetime.utcnow().isoformat(),
                    "debug_links": [],
                    "buckets": {"green": [], "yellow": [], "red": [], "purple": []},
                })
                continue

            if TRY_PSP_STATMUSE:
                data, used_season_mode, season_year, debug_links = scrape_statmuse_data(stat, sport_up_local, teams)
            else:
                data, used_season_mode, season_year, debug_links = [], False, 0, []

            injured = get_injured_players_for_sport(
                sport_up_local, session=SESSION, verbose=bool(VERBOSE), clean_name_fn=clean_name
            ) if INJURIES_ENABLED else set()

            if sport_up_local == "MLB":
                if data:
                    data = dedupe_by_player(data)
                if data and injured:
                    data = remove_injured_rows(
                        data, injured, clean_name_fn=clean_name, name_key_fn=_name_key, verbose=bool(VERBOSE)
                    )
                data = filter_traded_banned_and_teams(
                    data, teams, traded_players=fetch_trades_past_week(), banned_players=BANNED_PLAYERS
                )
            elif sport_up_local == "NFL":
                data = filter_traded_banned_and_teams(data, teams, banned_players=BANNED_PLAYERS)
                if data and injured:
                    data = remove_injured_rows(
                        data, injured, clean_name_fn=clean_name, name_key_fn=_name_key, verbose=bool(VERBOSE)
                    )
            elif sport_up_local in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
                if data and injured:
                    data = remove_injured_rows(
                        data, injured, clean_name_fn=clean_name, name_key_fn=_name_key, verbose=bool(VERBOSE)
                    )
            else:
                data = filter_traded_banned_and_teams(data, teams, banned_players=BANNED_PLAYERS)
                if data and injured:
                    data = remove_injured_rows(
                        data, injured, clean_name_fn=clean_name, name_key_fn=_name_key, verbose=bool(VERBOSE)
                    )
                if data:
                    data = dedupe_by_player(data)

            if not data:
                summary = _colors_only_summary()
                g_list: List[str] = []
                y_list: List[str] = []
                rd_list: List[str] = []
                p_list: List[str] = []
            else:
                data = dedupe_by_player(data)
                desired_key = "TD" if (sport_up_local in {"CFB","NCAAF","COLLEGE FOOTBALL"} and stat_up in {"TD","TDS","TOUCHDOWNS"}) else stat_up
                stat_key = pick_stat_key(data, desired_key)
                g, y, rd, p = bucket_top12(data, stat_key)
                g_list, y_list, rd_list, p_list = g, y, rd, p
                if sport_up_local in {"CFB","NCAAF","COLLEGE FOOTBALL"}:
                    summary = _format_buckets_cfb(g, y, rd, p) or "(no qualifying leaders found)"
                else:
                    summary = _format_buckets_default(g, y, rd, p)

            if sport_up_local in {"CFB","NCAAF","COLLEGE FOOTBALL"} and used_season_mode:
                suffix = f"{season_year}"
            elif sport_up_local == "NFL":
                if NFL_QUERY_MODE == "SEASON":
                    suffix = f"{NFL_SEASON_YEAR}"
                elif NFL_QUERY_MODE == "DATES" and NFL_START_DATE and NFL_END_DATE:
                    suffix = f"{NFL_START_DATE} → {NFL_END_DATE}"
                else:
                    suffix = "past week" if NFL_LAST_N_DAYS >= 7 else f"last {NFL_LAST_N_DAYS} days"
            elif sport_up_local == "MLB":
                suffix = f"this postseason {MLB_SEASON_YEAR}"
            else:
                suffix = "this season"

            heading = f"{sport_up_local} PSP - {stat_up} leaders ({suffix})"
            blocks_for_sport.append({"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":heading}}]}})
            blocks_for_sport.append({"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":summary}}]}})
            sampled_links: List[str] = []
            if SHOW_CFB_DEBUG_LINKS and debug_links:
                seen = set()
                for u in debug_links:
                    if not u or u in seen:
                        continue
                    seen.add(u)
                    sampled_links.append(u)
                    if len(sampled_links) >= 6:
                        break
            if SHOW_CFB_DEBUG_LINKS and debug_links:
                seen = set(); shown = 0
                for u in debug_links:
                    if not u or u in seen: continue
                    seen.add(u); shown += 1
                    blocks_for_sport.append(_link_para(f"View query {shown}", u))
                    if shown >= 6: break

            if pid:
                pages_to_mark.append(pid)
            _record_pick(collector, {
                "category": "psp",
                "sport": sport_up_local,
                "heading": heading,
                "stat": stat_up,
                "teams": teams,
                "summary": summary,
                "suffix": suffix,
                "generated_at": datetime.utcnow().isoformat(),
                "debug_links": sampled_links,
                "buckets": {
                    "green": g_list,
                    "yellow": y_list,
                    "red": rd_list,
                    "purple": p_list,
                },
            })

        if blocks_for_sport and notion_enabled:
            notion_append_blocks(blocks_for_sport)

        if notion_enabled:
            for pid in pages_to_mark:
                notion_update_page(pid, {"Processed":{"select":{"name":"Yes"}}})

        print(f"[refresh] ✅ PSP updated (grouped) for {sport_up}", file=sys.stderr)

# ============================== Main =======================================

def process_all(only: str = "both", collector: Optional[List[PickRecord]] = None):
    if only in ("both", "games"):
        process_games(collector=collector)
    if only in ("both", "psp"):
        process_psp_rows(collector=collector)

def reset_cache():
    try:
        if CACHE_FILE.exists():
            CACHE_FILE.unlink()
            _log("[cache] reset cache file")
    except Exception as e:
        _log(f"[cache] reset failed: {e}")

def _cli():
    parser = argparse.ArgumentParser(description="StatMuse → Notion updater")
    parser.add_argument("--games-only", action="store_true", help="Process regular game rows only")
    parser.add_argument("--psp-only", action="store_true", help="Process PSP rows only")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logs")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to Notion")
    parser.add_argument("--no-selenium", action="store_true", help="Skip Selenium fallback")
    parser.add_argument("--no-cache", action="store_true", help="Ignore disk/memory cache")
    parser.add_argument("--save-html", action="store_true", help="Save non-tabling HTML to .debug_html/")
    parser.add_argument("--reset-cache", action="store_true", help="Delete the disk cache file")

    args = parser.parse_args()

    # Apply flags to globals
    global VERBOSE, DRY_RUN, DISABLE_SELENIUM, DISABLE_CACHE, SAVE_BAD_HTML
    if args.verbose: VERBOSE = 1
    if args.dry_run: DRY_RUN = True
    if args.no_selenium: DISABLE_SELENIUM = True
    if args.no_cache: DISABLE_CACHE = True
    if args.save_html: SAVE_BAD_HTML = True
    if args.reset_cache: reset_cache()

    t0 = time.time()
    only = "both"
    if args.games_only:
        only = "games"
    elif args.psp_only:
        only = "psp"

    picks = build_picks(only=only)
    _log(f"[done] {time.time()-t0:.1f}s | metrics={METRICS}")
    print(json.dumps(picks))

def build_picks(only: str = "both") -> List[PickRecord]:
    picks: List[PickRecord] = []
    process_all(only=only, collector=picks)
    _write_cache(picks, date=PICKS_DATE)
    return picks

if __name__ == "__main__":
    _cli()
