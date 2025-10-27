# injuries.py
from __future__ import annotations

import os
import json
import time
import re
from typing import Iterable, Optional, Set, Dict, Any

import requests
from bs4 import BeautifulSoup

# ---------------------- dotenv (optional) ----------------------
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# ---------------------- Config / Defaults ----------------------
DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
SPORT_TO_CBS_PATH = {
    "NFL": "nfl",
    "MLB": "mlb",
    "NBA": "nba",
    "WNBA": "wnba",
    "NHL": "nhl",
    "CFB": "college-football",
}

CACHE_FILE = os.getenv("INJURIES_CACHE_FILE") or os.path.join(
    os.path.dirname(__file__), "injuries_cache.json"
)
CACHE_TTL_MIN = int(os.getenv("INJURIES_CACHE_TTL_MINUTES", "120"))

# ---------------------- Small name normalizers ----------------------
def clean_name(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    # Drop common suffixes spacing variants for consistency
    s = re.sub(r"\b(Jr\.?|Sr\.?|II|III|IV|V)\b", lambda m: m.group(0).replace(".", ""), s)
    return s.strip()

def name_key(s: str) -> str:
    s = (s or "").lower()
    s = s.replace("’", "'").replace(".", "").replace("-", " ")
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

# ---------------------- Session w/ retries ----------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": DEFAULT_UA})
    try:
        from urllib3.util.retry import Retry  # type: ignore
        from requests.adapters import HTTPAdapter  # type: ignore
        retry = Retry(
            total=3, backoff_factor=0.3,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://", HTTPAdapter(max_retries=retry))
    except Exception:
        pass
    return s

# ---------------------- CBS scraping ----------------------
def _cbs_url_for_sport(sport_up: str) -> Optional[str]:
    p = SPORT_TO_CBS_PATH.get(sport_up)
    if not p:
        return None
    return f"https://www.cbssports.com/{p}/injuries/"

def _parse_cbs_injuries_html(html: str, verbose: bool = False) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")
    injured: Set[str] = set()

    # New CBS layout uses shadowed tables per team
    sections = soup.find_all("div", class_="TableBase-shadows")
    if sections:
        for section in sections:
            table = section.find("table")
            if not table:
                continue
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if not tds:
                    continue
                a_tags = tds[0].find_all("a")
                if len(a_tags) >= 2:
                    name = a_tags[1].get_text(strip=True)
                elif a_tags:
                    name = a_tags[0].get_text(strip=True)
                else:
                    name = tds[0].get_text(strip=True)
                name = clean_name(name)
                if name:
                    injured.add(name)

    # Older fallback: any tables on the page
    if not injured:
        for table in soup.find_all("table"):
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if not tds:
                    continue
                txt = tds[0].get_text(strip=True)
                txt = clean_name(txt)
                if txt:
                    injured.add(txt)

    if verbose:
        print(f"[injuries] parsed {len(injured)} names from CBS")
    return injured

def _fetch_injuries_from_web(sport_up: str, session: Optional[requests.Session] = None, verbose: bool = False) -> Set[str]:
    url = _cbs_url_for_sport(sport_up)
    if not url:
        if verbose:
            print(f"[injuries] no CBS url for sport={sport_up}")
        return set()
    s = session or make_session()
    try:
        r = s.get(url, timeout=15)
        r.encoding = "utf-8"
        if not r.ok:
            if verbose:
                print(f"[injuries] {sport_up} fetch failed: HTTP {r.status_code}")
            return set()
        return _parse_cbs_injuries_html(r.text, verbose=verbose)
    except Exception as e:
        if verbose:
            print(f"[injuries] {sport_up} fetch error: {e}")
        return set()

# ---------------------- Cache helpers ----------------------
def _load_cache() -> Dict[str, Any]:
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_cache(obj: Dict[str, Any]) -> None:
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[injuries] cache save error: {e}")

def _cache_fresh(ts: float) -> bool:
    age_min = (time.time() - ts) / 60.0
    return age_min <= max(1, CACHE_TTL_MIN)

# ---------------------- Public API ----------------------
def get_injured_players_for_sport(
    sport: str,
    *,
    session: Optional[requests.Session] = None,
    verbose: bool = False,
    clean_name_fn=None
) -> Set[str]:
    """
    Returns a set of injured player names for the given sport (uppercased key).
    Uses cache if fresh; otherwise refreshes from the web and writes cache.
    """
    sport_up = (sport or "").strip().upper()
    cache = _load_cache()
    bucket = cache.get("sports", {})
    ts = float(cache.get("ts", 0))

    if sport_up in bucket and _cache_fresh(ts):
        names = set(bucket.get(sport_up, []))
        return {clean_name_fn(n) if clean_name_fn else clean_name(n) for n in names}

    # fetch & write back
    names = _fetch_injuries_from_web(sport_up, session=session, verbose=verbose)
    if names:
        bucket[sport_up] = sorted(names)
        cache = {"ts": time.time(), "sports": bucket}
        _save_cache(cache)

    return {clean_name_fn(n) if clean_name_fn else clean_name(n) for n in names}

def remove_injured_rows(
    rows: Iterable[Dict[str, Any]],
    injured_names: Set[str],
    *,
    clean_name_fn=None,
    name_key_fn=None,
    verbose: bool = False
):
    """
    Remove any row whose player name appears in injured_names.
    Tries columns NAME / PLAYER / first column as the player name.
    """
    ck = name_key_fn or name_key
    cn = clean_name_fn or clean_name

    injured_keys = {ck(n) for n in injured_names}
    out = []
    rows = list(rows or [])
    if not rows:
        return rows

    # infer the name column
    def _name_col(r: Dict[str, Any]) -> str:
        if "NAME" in r: return "NAME"
        if "PLAYER" in r: return "PLAYER"
        return list(r.keys())[0]

    name_col = _name_col(rows[0])

    for rec in rows:
        nm = cn(rec.get(name_col, ""))
        if not nm:
            out.append(rec)
            continue
        if ck(nm) in injured_keys:
            if verbose:
                print(f"[injuries] drop injured: {nm}")
            continue
        out.append(rec)
    return out

def refresh_injury_cache(
    sports: Optional[Iterable[str]] = None,
    *,
    session: Optional[requests.Session] = None,
    verbose: bool = False
) -> Dict[str, Any]:
    """
    Fetch injuries for a list of sports and write the cache file.
    Returns the cache dict.
    """
    s = session or make_session()
    sports = list(sports or SPORT_TO_CBS_PATH.keys())
    bucket: Dict[str, Any] = {}

    for sp in sports:
        names = sorted(_fetch_injuries_from_web(sp.upper(), session=s, verbose=verbose))
        bucket[sp.upper()] = names
        if verbose:
            print(f"[injuries] {sp.upper()}: {len(names)} names")

    cache = {"ts": time.time(), "sports": bucket}
    _save_cache(cache)
    return cache

# ---------------------- CLI ----------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Injuries helper: fetch & cache injured players by sport.")
    parser.add_argument("--sport", help="Sport (NFL, MLB, NBA, WNBA, NHL, CFB). If omitted, all sports are processed.")
    parser.add_argument("--dump", choices=["names", "json"], help="How to print output.")
    parser.add_argument("--write-cache", action="store_true", help="Write injuries_cache.json after fetching.")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging.")
    args = parser.parse_args()

    sess = make_session()

    if args.sport:
        names = get_injured_players_for_sport(args.sport, session=sess, verbose=args.verbose)
        if args.dump == "json":
            print(json.dumps(sorted(list(names)), ensure_ascii=False, indent=2))
        else:
            print(f"{args.sport.upper()} injured players: {len(names)}")
            for n in sorted(names):
                print(f"- {n}")
        if args.write_cache:
            # ensure single-sport result is merged into cache
            cache = _load_cache()
            bucket = cache.get("sports", {})
            bucket[args.sport.upper()] = sorted(list(names))
            cache = {"ts": time.time(), "sports": bucket}
            _save_cache(cache)
    else:
        cache = refresh_injury_cache(verbose=args.verbose)
        if args.dump == "json":
            print(json.dumps(cache, ensure_ascii=False, indent=2))
        else:
            for sp, names in cache.get("sports", {}).items():
                print(f"{sp}: {len(names)}")
        # write-cache implied if no --sport (we just refreshed everything)