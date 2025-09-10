#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CFB_scraper.py
--------------
Scrape NCAA FBS season leaders for:
  - Receiving Yards
  - Receptions (totals)
  - Rushing Yards
  - Total Touchdowns (non-QB)

Data source: NCAA FBS leaders pages (server-rendered lists).
"""

import os
import re
import sys
import time
import math
import argparse
import random
import unicodedata
from typing import List, Tuple, Optional, Dict

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ----------------------------
# Config
# ----------------------------
BASE = "https://www.ncaa.com"
FBS_HUB = f"{BASE}/stats/football/fbs"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}
DEFAULT_TOP = 50   # how many rows to keep per category
MAX_PAGES = 10     # safety for pagination crawl

# Categories to collect: link text patterns (case-insensitive) on the NCAA leaders pages
CATEGORY_PATTERNS = {
    "receiving_yards": re.compile(r"^Receiving Yards$", re.I),
    "receptions": re.compile(r"^Receptions$", re.I),
    "rushing_yards": re.compile(r"^Rushing Yards$", re.I),
    "total_td": re.compile(r"^Total Touchdowns$", re.I),
}

# ----------------------------
# Helpers
# ----------------------------
def normspace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _season_default() -> int:
    # NCAA labels season by year it ends (football: current year is correct during season)
    from datetime import date
    today = date.today()
    return today.year

def _request(url: str, params: Optional[dict] = None, max_tries: int = 6, base_sleep: float = 1.4) -> requests.Response:
    """GET with polite backoff and randomized jitter."""
    last_err = None
    for attempt in range(1, max_tries + 1):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=20)
            if resp.status_code == 429:
                # backoff on rate limiting
                sleep_s = base_sleep * attempt + random.uniform(0.2, 0.9)
                print(f"[cfb] warn: 429 @ {url} — backing off {sleep_s:.1f}s...")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_err = e
            sleep_s = base_sleep * attempt + random.uniform(0.2, 0.8)
            print(f"[cfb] warn: request error — {url}: {e}; retrying in {sleep_s:.1f}s")
            time.sleep(sleep_s)
    raise last_err or RuntimeError(f"request failed for {url}")

def _build_season_hub(season: int) -> str:
    return f"{FBS_HUB}/current" if season == _season_default() else f"{FBS_HUB}/{season}"

def _find_category_link(hub_html: str, pattern: re.Pattern) -> Optional[str]:
    """
    From the FBS hub page HTML, find the link whose text matches the category pattern.
    """
    soup = BeautifulSoup(hub_html, "html.parser")
    # The hub page shows a long list of categories under "INDIVIDUAL STATISTICS"
    for a in soup.find_all("a", href=True):
        text = normspace(a.get_text())
        if pattern.search(text):
            href = a["href"]
            if href.startswith("/"):
                return BASE + href
            elif href.startswith("http"):
                return href
    return None

def _parse_table_from_page(html: str) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Parse a leaders table from a category page and find the 'next' page URL if present.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the first <table> that looks like a leaders table
    table = soup.find("table")
    if not table:
        # Sometimes NCAA renders as lists; try to convert a list into a DataFrame as fallback
        rows = []
        # Attempt a generic extraction from list items showing rank - player - school - value
        for li in soup.select("li a"):
            t = normspace(li.get_text())
            if re.match(r"^\d+\.\s", t):
                rows.append([t])
        if rows:
            df = pd.DataFrame(rows, columns=["RAW"])
        else:
            df = pd.DataFrame()
    else:
        df = pd.read_html(str(table))[0]
        # Normalize headers
        df.columns = [normspace(str(c)) for c in df.columns]

    # Find next page
    next_link = None
    for a in soup.select("a"):
        txt = normspace(a.get_text())
        if re.search(r"^(Next|Next Page|Next »)$", txt, re.I):
            href = a.get("href")
            if href:
                next_link = href if href.startswith("http") else BASE + href
            break

    return df, next_link

def _collect_category(season: int, cat_name: str, cat_url: str, keep_top: int) -> pd.DataFrame:
    """
    Crawl category pages (handle pagination) and collect up to keep_top rows.
    """
    rows: List[pd.DataFrame] = []
    url = cat_url
    seen = set()
    page = 1
    while url and page <= MAX_PAGES and len(pd.concat(rows, ignore_index=True)) if rows else 0 < keep_top:
        if url in seen:
            break
        seen.add(url)

        resp = _request(url)
        df, next_url = _parse_table_from_page(resp.text)

        if df.empty:
            break

        # Heuristic clean-up: standardize common columns if present
        rename_map = {}
        for c in df.columns:
            cu = c.upper()
            if cu in {"RK", "RANK", "NO.", "#"}:
                rename_map[c] = "RANK"
            elif cu in {"PLAYER", "NAME"}:
                rename_map[c] = "PLAYER"
            elif cu in {"SCHOOL", "TEAM"}:
                rename_map[c] = "TEAM"
            elif cu in {"POS", "POSITION"}:
                rename_map[c] = "POS"
            elif cu in {"REC", "RECEPTIONS"}:
                rename_map[c] = "REC"
            elif "REC YDS" in cu or cu == "YDS" or "RECEIVING YDS" in cu:
                rename_map[c] = "REC_YDS"
            elif "RUSH YDS" in cu or (cu == "YDS" and cat_name == "rushing_yards"):
                rename_map[c] = "RUSH_YDS"
            elif cu in {"TD", "TDS"}:
                rename_map[c] = "TD"
            elif cu in {"TOT TD", "TOTAL TD", "TOTAL TOUCHDOWNS"}:
                rename_map[c] = "TOT_TD"
        if rename_map:
            df = df.rename(columns=rename_map)

        # Add CATEGORY/SEASON for traceability
        df["CATEGORY"] = cat_name
        df["SEASON"] = season

        rows.append(df)

        # Follow pagination if present
        url = next_url
        page += 1

        # Be polite
        time.sleep(0.8 + random.uniform(0.1, 0.6))

    if not rows:
        return pd.DataFrame()

    out = pd.concat(rows, ignore_index=True)

    # Trim to top N if a clear rank column exists
    if "RANK" in out.columns:
        # Make sure RANK is numeric for sorting
        with pd.option_context("mode.chained_assignment", None):
            out["RANK"] = pd.to_numeric(out["RANK"], errors="coerce")
        out = out.sort_values(["RANK"], na_position="last").dropna(subset=["RANK"])
        out = out.head(keep_top).reset_index(drop=True)
    else:
        out = out.head(keep_top).reset_index(drop=True)

    return out

def _filter_non_qb_total_td(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # Try to keep only players with POS not equal to QB; if no POS col, keep all.
    if "POS" in df.columns:
        return df[df["POS"].astype(str).str.upper() != "QB"].reset_index(drop=True)
    return df

def _print_preview(title: str, df: pd.DataFrame, cols: List[str]):
    print(f"\n— {title} (top {min(len(df), 10) or 0}) —")
    if df.empty:
        print("(no rows)")
        return
    show = [c for c in cols if c in df.columns]
    print(df[show].head(10).to_string(index=False))

# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Scrape NCAA FBS season leaders (Receiving Yards, Receptions, Rushing Yards, Total TD non-QB).")
    parser.add_argument("--season", type=int, default=_season_default(), help="Season year (default: current year)")
    parser.add_argument("--out", type=str, default=".", help="Output directory (default: current directory)")
    parser.add_argument("--top", type=int, default=DEFAULT_TOP, help="How many rows per category to keep")
    args = parser.parse_args()

    season = args.season
    outdir = os.path.abspath(args.out)
    os.makedirs(outdir, exist_ok=True)

    print("\n=== CFB Season Leaders (NCAA) ===")
    print(f"Season: {season}")
    print(f"Out: {outdir}")

    hub_url = _build_season_hub(season)
    hub_resp = _request(hub_url)
    hub_html = hub_resp.text

    # Discover category pages from the hub by link text
    cat_urls: Dict[str, Optional[str]] = {}
    for key, patt in CATEGORY_PATTERNS.items():
        cat_urls[key] = _find_category_link(hub_html, patt)

    # If a category wasn’t linked on the hub, we can still try the conventional path pattern:
    # /stats/football/fbs/{season}/individual/<id>
    # But IDs vary, so we only proceed with discovered links to avoid 404s/rate-limit loops.

    # Collect data per category
    results: Dict[str, pd.DataFrame] = {}
    for cname, url in cat_urls.items():
        if not url:
            print(f"[cfb] warn: category link not found on hub for '{cname}'. Skipping.")
            results[cname] = pd.DataFrame()
            continue
        try:
            df = _collect_category(season, cname, url, keep_top=args.top)
            results[cname] = df
        except Exception as e:
            print(f"[cfb] warn: failed '{cname}' — {e}")
            results[cname] = pd.DataFrame()

    # Post-process: filter QBs from total TD
    if "total_td" in results:
        results["total_td"] = _filter_non_qb_total_td(results["total_td"])

    # Save CSVs and print previews
    outs = {
        "receiving_yards": os.path.join(outdir, f"cfb_{season}_leaders_receiving_yards.csv"),
        "receptions":     os.path.join(outdir, f"cfb_{season}_leaders_receptions.csv"),
        "rushing_yards":  os.path.join(outdir, f"cfb_{season}_leaders_rushing_yards.csv"),
        "total_td":       os.path.join(outdir, f"cfb_{season}_leaders_total_td_non_qb.csv"),
    }

    # Receiving Yards
    ry = results.get("receiving_yards", pd.DataFrame())
    if not ry.empty:
        ry.to_csv(outs["receiving_yards"], index=False)
        print(f"[cfb] saved: {outs['receiving_yards']}")
    _print_preview("Receiving Yards", ry, ["RANK", "PLAYER", "TEAM", "POS", "REC_YDS"])

    # Receptions (totals)
    rec = results.get("receptions", pd.DataFrame())
    if not rec.empty:
        rec.to_csv(outs["receptions"], index=False)
        print(f"[cfb] saved: {outs['receptions']}")
    _print_preview("Receptions", rec, ["RANK", "PLAYER", "TEAM", "POS", "REC"])

    # Rushing Yards
    rush = results.get("rushing_yards", pd.DataFrame())
    if not rush.empty:
        rush.to_csv(outs["rushing_yards"], index=False)
        print(f"[cfb] saved: {outs['rushing_yards']}")
    _print_preview("Rushing Yards", rush, ["RANK", "PLAYER", "TEAM", "POS", "RUSH_YDS"])

    # Total Touchdowns (non-QB)
    ttd = results.get("total_td", pd.DataFrame())
    if not ttd.empty:
        ttd.to_csv(outs["total_td"], index=False)
        print(f"[cfb] saved: {outs['total_td']}")
    _print_preview("Total TD (Non-QB)", ttd, ["RANK", "PLAYER", "TEAM", "POS", "TOT_TD"])

    print("\nDone.")

if __name__ == "__main__":
    sys.exit(main())