import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.cbb_service import compute_cbb_summary
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "picks.db"
PICKS_DIR = DATA_DIR

SCRIPT_MAP = {
    "cbb": BASE_DIR / ("cbb_picks.py" if (BASE_DIR / "cbb_picks.py").exists() else "picks_cbb.py"),
    "test2": BASE_DIR / "test2.py",
}
JSON_FALLBACK = {
    "cbb": DATA_DIR / "cbb.json",
    "test2": DATA_DIR / "test2.json",
}

UTILITY_SCRIPTS: Dict[str, Dict[str, Any]] = {
    "schedule_fetch": {
        "label": "Schedule Fetcher",
        "description": "Pulls ESPN scoreboards and populates data/schedule.json.",
        "path": BASE_DIR / "schedule_fetch.py",
    },
    "injuries": {
        "label": "Injuries Cache",
        "description": "Scrapes CBS injuries for all sports and refreshes injuries_cache.json.",
        "path": BASE_DIR / "injuries.py",
    },
    "cbb_scraper": {
        "label": "CBB Stat Scraper",
        "description": "Rebuilds cbb_players_stats.csv from ESPN leaderboards.",
        "path": BASE_DIR / "cbb_scraper.py",
    },
}
JOB_METADATA = [
    {"key": key, "label": meta["label"], "description": meta["description"]}
    for key, meta in UTILITY_SCRIPTS.items()
]


ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()

app = FastAPI(title="RealSports Picks", version="0.1.0")
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "app" / "static")), name="static")


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS picks (
                kind TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS script_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                stdout TEXT,
                stderr TEXT,
                exit_code INTEGER NOT NULL,
                ran_at TEXT NOT NULL
            )
            """
        )


init_db()


def _picks_file(kind: str, date_str: str) -> Path:
    return PICKS_DIR / f"picks_{kind}_{date_str}.json"


def list_pick_dates(kind: str) -> List[str]:
    pattern = f"picks_{kind}_*.json"
    dates: List[str] = []
    for path in PICKS_DIR.glob(pattern):
        try:
            date_part = path.stem.split("_")[-1]
            int(date_part)
            dates.append(date_part)
        except Exception:
            continue
    dates.sort()
    return dates


def list_schedule_dates() -> List[str]:
    dates: List[str] = []
    for path in DATA_DIR.glob("schedule_*.json"):
        try:
            date_part = path.stem.split("_")[-1]
            int(date_part)
            dates.append(date_part)
        except Exception:
            continue
    dates.sort()
    return dates


def load_picks_for_kind(kind: str, date: Optional[str] = None) -> Dict[str, Any]:
    if kind == "test2" and date:
        file_path = _picks_file(kind, date)
        if file_path.exists():
            return {"kind": kind, "data": json.loads(file_path.read_text()), "updated_at": date}
    default = _get_cached(kind) or _default_payload(kind)
    if kind == "test2" and date and default["updated_at"] != date:
        # fallback empty structure with requested date tag
        return {"kind": kind, "data": [], "updated_at": date}
    return default


def list_job_history(limit: int = 50) -> List[Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT name, stdout, stderr, exit_code, ran_at
            FROM script_runs
            ORDER BY ran_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    history = []
    for name, stdout, stderr, exit_code, ran_at in rows:
        history.append(
            {
                "name": name,
                "stdout": stdout or "",
                "stderr": stderr or "",
                "exit_code": exit_code,
                "ran_at": ran_at,
            }
        )
    return history


class CBBFetchRequest(BaseModel):
    team1: str
    team2: Optional[str] = None
    stat: str


def _ensure_kind(kind: str) -> str:
    if kind not in SCRIPT_MAP:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown kind")
    return kind


def _extract_token(request: Request) -> Optional[str]:
    header = request.headers.get("x-admin-token")
    query = request.query_params.get("token")
    return header or query


def _require_admin(token: Optional[str]) -> None:
    # Token enforcement removed; endpoints are open.
    return


def _run_script_to_json(kind: str, extra_env: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    script_path = SCRIPT_MAP[kind]
    if not script_path.exists():
        raise RuntimeError(f"Script {script_path} does not exist")

    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    result = subprocess.run(
        ["python3", str(script_path)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        env=env,
    )

    if result.returncode != 0:
        tail = (result.stderr or "").strip()[-400:]
        raise RuntimeError(f"{script_path.name} failed ({result.returncode}): {tail}")

    stdout = (result.stdout or "").strip()
    data: Optional[List[Dict[str, Any]]] = None
    if stdout:
        try:
            data = json.loads(stdout)
        except json.JSONDecodeError:
            data = None

    if data is None:
        fallback = JSON_FALLBACK[kind]
        if fallback.exists():
            data = json.loads(fallback.read_text(encoding="utf-8"))

    if data is None:
        raise RuntimeError(f"{script_path.name} produced no JSON output")

    if not isinstance(data, list):
        raise RuntimeError(f"{script_path.name} output must be a JSON list")

    return data


def _save_payload(kind: str, payload: List[Dict[str, Any]], updated_at: str) -> None:
    encoded = json.dumps(payload)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO picks(kind, payload, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(kind) DO UPDATE SET
                payload=excluded.payload,
                updated_at=excluded.updated_at
            """,
            (kind, encoded, updated_at),
        )


def _default_payload(kind: str) -> Dict[str, Any]:
    return {"kind": kind, "data": [], "updated_at": None}


def _get_cached(kind: str) -> Optional[Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute("SELECT payload, updated_at FROM picks WHERE kind=?", (kind,))
        row = cursor.fetchone()
    if not row:
        return None
    payload = json.loads(row[0])
    return {"kind": kind, "data": payload, "updated_at": row[1]}


def _refresh_kind(kind: str, target_date: Optional[str] = None, run_prereqs: bool = True) -> Dict[str, Any]:
    _ensure_kind(kind)
    if kind == "test2":
        extra_env = {"PICKS_DATE": target_date} if target_date else None
        if run_prereqs:
            try:
                _run_utility_script("schedule_fetch")
            except Exception as exc:
                print(f"[pipeline] schedule_fetch failed: {exc}", file=sys.stderr)
            try:
                _run_utility_script("injuries")
            except Exception as exc:
                print(f"[pipeline] injuries script failed: {exc}", file=sys.stderr)
        payload = _run_script_to_json(kind, extra_env=extra_env)
    else:
        payload = _run_script_to_json(kind)
    if target_date:
        try:
            dt = datetime.strptime(target_date, "%Y%m%d")
            updated_at = dt.strftime("%Y-%m-%dT00:00:00Z")
        except Exception:
            updated_at = target_date
    else:
        updated_at = datetime.utcnow().isoformat() + "Z"
    _save_payload(kind, payload, updated_at)
    return {"kind": kind, "data": payload, "updated_at": updated_at}


async def refresh_kind_async(kind: str, target_date: Optional[str] = None, run_prereqs: bool = True) -> Dict[str, Any]:
    return await run_in_threadpool(_refresh_kind, kind, target_date, run_prereqs)


def _record_script_run(name: str, stdout: str, stderr: str, exit_code: int) -> Dict[str, Any]:
    ran_at = datetime.utcnow().isoformat() + "Z"
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO script_runs(name, stdout, stderr, exit_code, ran_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (name, stdout, stderr, exit_code, ran_at),
        )
    return {"name": name, "stdout": stdout, "stderr": stderr, "exit_code": exit_code, "ran_at": ran_at}


def _latest_script_run(name: str) -> Optional[Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            SELECT stdout, stderr, exit_code, ran_at
            FROM script_runs
            WHERE name=?
            ORDER BY ran_at DESC
            LIMIT 1
            """,
            (name,),
        )
        row = cursor.fetchone()
    if not row:
        return None
    return {"name": name, "stdout": row[0] or "", "stderr": row[1] or "", "exit_code": row[2], "ran_at": row[3]}


def _run_utility_script(name: str) -> Dict[str, Any]:
    meta = UTILITY_SCRIPTS.get(name)
    if not meta:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown job")
    path = Path(meta["path"])
    if not path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"{path.name} not found")
    result = subprocess.run(
        ["python3", str(path)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
    )
    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    return _record_script_run(name, stdout, stderr, result.returncode)


async def run_utility_script_async(name: str) -> Dict[str, Any]:
    return await run_in_threadpool(_run_utility_script, name)


def _run_full_pipeline() -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for job in ("schedule_fetch", "injuries"):
        results[job] = _run_utility_script(job)
    dates = list_schedule_dates() or list_pick_dates("test2")
    if not dates:
        dates = [datetime.utcnow().strftime("%Y%m%d")]
    for date in dates:
        try:
            results[f"test2_{date}"] = _refresh_kind("test2", target_date=date, run_prereqs=False)
        except Exception as exc:
            results[f"test2_{date}_error"] = str(exc)
    try:
        results["cbb"] = _refresh_kind("cbb")
    except Exception as exc:
        results["cbb_error"] = str(exc)
    return results


async def run_full_pipeline_async() -> Dict[str, Any]:
    return await run_in_threadpool(_run_full_pipeline)


@app.get("/", response_class=RedirectResponse)
async def root() -> RedirectResponse:
    return RedirectResponse(url="/picks/test2", status_code=status.HTTP_302_FOUND)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    initial_payload = {
        kind: _get_cached(kind) or _default_payload(kind)
        for kind in SCRIPT_MAP
    }
    job_state = {
        name: _latest_script_run(name)
        for name in UTILITY_SCRIPTS
    }
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "initial_data": initial_payload,
            "job_metadata": JOB_METADATA,
            "initial_jobs": job_state,
        },
    )


@app.get("/picks/test2", response_class=HTMLResponse)
async def picks_test2_page(request: Request, date: Optional[str] = None) -> HTMLResponse:
    available_dates = list_schedule_dates() or list_pick_dates("test2")
    if not available_dates:
        available_dates = [datetime.utcnow().strftime("%Y%m%d")]
    selected = date if date in available_dates else available_dates[-1]
    payload = load_picks_for_kind("test2", selected)
    return templates.TemplateResponse(
        "picks_test2.html",
        {
            "request": request,
            "payload": payload,
            "selected_date": selected,
            "available_dates": available_dates,
        },
    )


@app.get("/picks/cbb", response_class=HTMLResponse)
async def picks_cbb_page(request: Request) -> HTMLResponse:
    payload = load_picks_for_kind("cbb")
    scraper_job = _latest_script_run("cbb_scraper")
    return templates.TemplateResponse(
        "picks_cbb.html",
        {
            "request": request,
            "payload": payload,
            "scraper_job": scraper_job,
        },
    )


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request) -> HTMLResponse:
    job_state = {name: _latest_script_run(name) for name in UTILITY_SCRIPTS}
    available_dates = list_schedule_dates()
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "job_state": job_state,
            "job_labels": {name: meta["label"] for name, meta in UTILITY_SCRIPTS.items()},
            "available_dates": available_dates,
        },
    )


@app.get("/settings/jobs", response_class=HTMLResponse)
async def settings_jobs(request: Request) -> HTMLResponse:
    history = list_job_history()
    return templates.TemplateResponse(
        "settings_jobs.html",
        {"request": request, "history": history},
    )


@app.get("/api/picks/{kind}")
async def api_picks(kind: str, request: Request) -> JSONResponse:
    kind = _ensure_kind(kind)
    date = request.query_params.get("date")
    if kind == "test2" and date:
        payload = load_picks_for_kind(kind, date)
        return JSONResponse(payload)
    cached = _get_cached(kind) or _default_payload(kind)
    return JSONResponse(cached)


@app.get("/api/picks")
async def api_all_picks() -> JSONResponse:
    return JSONResponse({kind: _get_cached(kind) or _default_payload(kind) for kind in SCRIPT_MAP})


@app.get("/api/picks/{kind}/dates")
async def api_pick_dates(kind: str) -> JSONResponse:
    kind = _ensure_kind(kind)
    if kind == "test2":
        return JSONResponse({"dates": list_schedule_dates() or list_pick_dates("test2")})
    return JSONResponse({"dates": []})


@app.get("/api/jobs")
async def api_jobs() -> JSONResponse:
    return JSONResponse({name: _latest_script_run(name) for name in UTILITY_SCRIPTS})


@app.post("/api/cbb/fetch")
async def api_cbb_fetch(payload: CBBFetchRequest) -> JSONResponse:
    try:
        data = await run_in_threadpool(compute_cbb_summary, payload.team1, payload.team2, payload.stat)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    return JSONResponse(data)


@app.post("/admin/refresh/{kind}")
async def refresh_kind(kind: str, request: Request) -> JSONResponse:
    kind = _ensure_kind(kind)
    token = _extract_token(request)
    _require_admin(token)
    date = request.query_params.get("date")
    try:
        data = await refresh_kind_async(kind, target_date=date)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    return JSONResponse({"status": "ok", **data})


@app.post("/admin/run/{name}")
async def run_job(name: str, request: Request) -> JSONResponse:
    token = _extract_token(request)
    _require_admin(token)
    try:
        result = await run_utility_script_async(name)
    except HTTPException:
        raise
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    return JSONResponse({"status": "ok", **result})


@app.post("/admin/run-all")
async def run_all(request: Request) -> JSONResponse:
    token = _extract_token(request)
    _require_admin(token)
    results = await run_full_pipeline_async()
    return JSONResponse({"status": "ok", "results": results})


async def _refresh_all(token: Optional[str]) -> Dict[str, Any]:
    _require_admin(token)
    refreshed: Dict[str, Any] = {}
    for kind in SCRIPT_MAP:
        try:
            refreshed[kind] = await refresh_kind_async(kind)
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    return refreshed


@app.post("/admin/refresh-all")
async def refresh_all_api(request: Request) -> JSONResponse:
    token = _extract_token(request)
    refreshed = await _refresh_all(token)
    return JSONResponse({"status": "ok", "results": refreshed})


@app.get("/admin/refresh-all")
async def refresh_all_redirect(request: Request) -> RedirectResponse:
    token = _extract_token(request)
    await _refresh_all(token)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
