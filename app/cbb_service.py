from __future__ import annotations

import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "cbb_players_stats.csv"
SCRAPER_PATH = BASE_DIR / "cbb_scraper.py"

STAT_MAP = {
    "PTS": "PPG",
    "PPG": "PPG",
    "POINTS": "PPG",
    "AST": "APG",
    "APG": "APG",
    "ASSISTS": "APG",
    "REB": "RPG",
    "RPG": "RPG",
    "REBOUNDS": "RPG",
    "3P": "3PM",
    "3PM": "3PM",
    "3PT": "3PM",
}


def _ensure_csv() -> None:
    if CSV_PATH.exists():
        return
    result = subprocess.run(
        ["python3", str(SCRAPER_PATH)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        tail = (result.stderr or "").strip()[-400:]
        raise RuntimeError(f"cbb_scraper.py failed: {tail}")


def _load_dataframe() -> pd.DataFrame:
    _ensure_csv()
    df = pd.read_csv(CSV_PATH)
    df["Player"] = df["Player"].astype(str).str.strip()
    df["Team"] = df["Team"].astype(str).str.upper().str.strip()
    return df


def _clean_name(name: str) -> str:
    name = (name or "").strip()
    return " ".join(name.split())


def _bucket_top12(df: pd.DataFrame, stat_col: str) -> Tuple[List[str], List[str], List[str], List[str]]:
    tmp = df[["Player", stat_col]].copy()
    tmp = tmp[pd.to_numeric(tmp[stat_col], errors="coerce").notna()]
    tmp[stat_col] = tmp[stat_col].astype(float)
    tmp = tmp.sort_values(stat_col, ascending=False).head(12)
    names = [_clean_name(n) for n in tmp["Player"].tolist()]
    return names[0:3], names[3:6], names[6:9], names[9:12]


def _format_summary(buckets: Dict[str, List[str]]) -> str:
    def line(label: str, players: List[str]) -> str:
        if not players:
            return f"{label} —"
        return f"{label} {', '.join(players)}"

    return "\n".join(
        [
            line("🟢", buckets["green"]),
            line("🟡", buckets["yellow"]),
            line("🔴", buckets["red"]),
            line("🟣", buckets["purple"]),
        ]
    )


def compute_cbb_summary(team1: str, team2: Optional[str], stat: str) -> Dict[str, object]:
    df = _load_dataframe()
    teams = [t.strip().upper() for t in [team1 or "", team2 or ""] if t and t.strip()]
    stat_key = STAT_MAP.get((stat or "").upper(), (stat or "").upper())
    if stat_key not in df.columns:
        raise ValueError(f"Unknown stat {stat}. Available: PPG, APG, RPG, 3PM")
    filtered = df if not teams else df[df["Team"].isin(teams)]
    if filtered.empty:
        raise ValueError("No players found for the provided team(s).")
    green, yellow, red, purple = _bucket_top12(filtered, stat_key)
    buckets = {"green": green, "yellow": yellow, "red": red, "purple": purple}
    summary = _format_summary(buckets)
    heading = f"{team1.upper()} vs {team2.upper()}" if len(teams) == 2 else (team1.upper() if teams else "All Teams")
    return {
        "heading": heading,
        "stat": stat_key,
        "teams": teams or ["ALL"],
        "summary": summary,
        "buckets": buckets,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }
