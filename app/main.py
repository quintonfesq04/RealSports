import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.cbb_service import compute_cbb_summary, compute_cbb_psp
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "picks.db"
PICKS_DIR = DATA_DIR

_APP_TIMEZONE = os.getenv("APP_TIMEZONE", "US/Eastern")
try:
    APP_TZ = ZoneInfo(_APP_TIMEZONE)
except Exception:
    APP_TZ = ZoneInfo("UTC")

AUTO_REFRESH_ENABLED = os.getenv("AUTO_REFRESH_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
AUTO_REFRESH_HOUR = int(os.getenv("AUTO_REFRESH_HOUR", "5"))
AUTO_REFRESH_MINUTE = int(os.getenv("AUTO_REFRESH_MINUTE", "0"))

def _now_local() -> datetime:
    return datetime.now(APP_TZ)

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
JOB_LABELS = {key: meta["label"] for key, meta in UTILITY_SCRIPTS.items()}
JOB_LABELS["picks_refresh"] = "Picks Refresh"


ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()

app = FastAPI(title="RealSports Picks", version="0.1.0")
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "app" / "static")), name="static")

PIPELINE_STATE: Dict[str, Any] = {
    "running": False,
    "last_error": None,
    "last_finished_at": None,
    "queued_at": None,
    "stage": "idle",
    "current_date": None,
    "processed_dates": [],
    "last_message": None,
}
PIPELINE_LOG: deque = deque(maxlen=200)
AUTO_REFRESH_TASK: Optional[asyncio.Task] = None


def _update_pipeline_state(**fields: Any) -> None:
    PIPELINE_STATE.update(fields)


def _pipeline_log(message: str) -> None:
    entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "message": message,
    }
    PIPELINE_LOG.appendleft(entry)
    PIPELINE_STATE["last_message"] = message


def _seconds_until_next_run() -> float:
    now = _now_local()
    target = now.replace(
        hour=AUTO_REFRESH_HOUR,
        minute=AUTO_REFRESH_MINUTE,
        second=0,
        microsecond=0,
    )
    if now >= target:
        target += timedelta(days=1)
    delta = (target - now).total_seconds()
    return max(delta, 60.0)


JOB_RUNTIME_NAMES = ("schedule_fetch", "injuries", "cbb_scraper", "picks_refresh")
JOB_RUNTIME_STATE: Dict[str, Dict[str, Any]] = {
    name: {"running": False, "last_message": None, "last_error": None, "last_finished_at": None}
    for name in JOB_RUNTIME_NAMES
}
JOB_RUNTIME_LOG: Dict[str, deque] = {name: deque(maxlen=100) for name in JOB_RUNTIME_NAMES}


def _job_state(name: str) -> Dict[str, Any]:
    if name not in JOB_RUNTIME_STATE:
        JOB_RUNTIME_STATE[name] = {"running": False, "last_message": None, "last_error": None, "last_finished_at": None}
    return JOB_RUNTIME_STATE[name]


def _job_log(name: str, message: str) -> None:
    if name not in JOB_RUNTIME_LOG:
        JOB_RUNTIME_LOG[name] = deque(maxlen=100)
    entry = {"timestamp": datetime.utcnow().isoformat() + "Z", "message": message}
    JOB_RUNTIME_LOG[name].appendleft(entry)
    _job_state(name)["last_message"] = message


def _job_start(name: str, message: str) -> None:
    state = _job_state(name)
    state.update({"running": True, "last_error": None})
    _job_log(name, message)


def _job_finish(name: str, success_message: Optional[str] = None, error: Optional[str] = None) -> None:
    state = _job_state(name)
    state["running"] = False
    state["last_finished_at"] = datetime.utcnow().isoformat() + "Z"
    if error:
        state["last_error"] = error
        _job_log(name, f"Error: {error}")
    elif success_message:
        _job_log(name, success_message)

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
    future_dates: List[str] = []
    today_int = int(_now_local().strftime("%Y%m%d"))
    for path in DATA_DIR.glob("schedule_*.json"):
        try:
            date_part = path.stem.split("_")[-1]
            date_int = int(date_part)
            dates.append(date_part)
            if date_int >= today_int:
                future_dates.append(date_part)
        except Exception:
            continue
    target = future_dates or dates
    target.sort()
    return target


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

class CBBPspRequest(BaseModel):
    teams: Optional[str] = ""
    stats: str

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
        if target_date:
            dates = [target_date]
        else:
            dates = list_schedule_dates()
            if not dates:
                dates = [_now_local().strftime("%Y%m%d")]
        return _refresh_test2_dates(dates, run_prereqs, target_date)

    payload = _run_script_to_json(kind)
    updated_at = datetime.utcnow().isoformat() + "Z"
    _save_payload(kind, payload, updated_at)
    return {"kind": kind, "data": payload, "updated_at": updated_at}


def _default_test2_date(dates: List[str]) -> str:
    if not dates:
        return _now_local().strftime("%Y%m%d")
    sorted_dates = sorted(dates)
    today = _now_local()
    tomorrow_str = (today + timedelta(days=1)).strftime("%Y%m%d")
    for candidate in sorted_dates:
        if candidate >= tomorrow_str:
            return candidate
    today_str = today.strftime("%Y%m%d")
    for candidate in sorted_dates:
        if candidate >= today_str:
            return candidate
    return sorted_dates[-1]


def _iso_from_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%Y%m%d")
        return dt.strftime("%Y-%m-%dT00:00:00Z")
    except Exception:
        return date_str


def _refresh_test2_dates(dates: List[str], run_prereqs: bool, target_date: Optional[str]) -> Dict[str, Any]:
    unique_dates = sorted({d for d in dates if d})
    if not unique_dates:
        unique_dates = [_now_local().strftime("%Y%m%d")]

    if run_prereqs:
        try:
            _run_utility_script("schedule_fetch")
        except Exception as exc:
            print(f"[pipeline] schedule_fetch failed: {exc}", file=sys.stderr)
        try:
            _run_utility_script("injuries")
        except Exception as exc:
            print(f"[pipeline] injuries script failed: {exc}", file=sys.stderr)

    payload_by_date: Dict[str, List[Dict[str, Any]]] = {}
    last_success_date: Optional[str] = None
    for date_str in unique_dates:
        payload_by_date[date_str] = _run_script_to_json("test2", extra_env={"PICKS_DATE": date_str})
        last_success_date = date_str

    default_date = target_date or _default_test2_date(unique_dates)
    payload = payload_by_date.get(default_date)
    if payload is None and last_success_date:
        payload = payload_by_date.get(last_success_date, [])
        default_date = last_success_date
    updated_at = _iso_from_date(default_date)
    _save_payload("test2", payload or [], updated_at)
    return {
        "kind": "test2",
        "data": payload or [],
        "updated_at": updated_at,
        "default_date": default_date,
        "processed_dates": unique_dates,
    }


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


def _run_full_pipeline(progress: Optional[Callable[[str, Optional[str], Optional[str]], None]] = None) -> Dict[str, Any]:
    def report(stage: str, current: Optional[str] = None, message: Optional[str] = None) -> None:
        if progress:
            progress(stage, current, message)

    results: Dict[str, Any] = {}
    report("schedule_fetch", message="Fetching schedules for upcoming slates…")
    results["schedule_fetch"] = _run_utility_script("schedule_fetch")
    report("injuries", message="Refreshing multi-sport injury cache…")
    results["injuries"] = _run_utility_script("injuries")
    dates = list_schedule_dates() or list_pick_dates("test2")
    if not dates:
        dates = [_now_local().strftime("%Y%m%d")]
    total = len(dates)
    for idx, date in enumerate(dates, start=1):
        try:
            report("test2", date, f"Building picks for {date} ({idx}/{total})…")
            result = _refresh_kind("test2", target_date=date, run_prereqs=False)
            results[f"test2_{date}"] = result
            data_blocks = (result.get("data") if isinstance(result, dict) else None) or []
            for block in data_blocks:
                heading = block.get("heading") or block.get("matchup") or "Matchup"
                sport = block.get("sport") or block.get("category") or ""
                label = f"{sport} — {heading}".strip(" —")
                report("test2", date, f"{date}: {label}")
            report("test2_complete", date, f"Finished picks for {date}.")
        except Exception as exc:
            report("test2_error", date, f"Failed picks for {date}: {exc}")
            results[f"test2_{date}_error"] = str(exc)
    include_cbb = os.getenv("PIPELINE_INCLUDE_CBB", "1").strip().lower() in {"1", "true", "yes"}
    if include_cbb:
        report("cbb", message="Refreshing CBB picks…")
        try:
            results["cbb"] = _refresh_kind("cbb")
        except Exception as exc:
            report("cbb_error", message=f"CBB refresh failed: {exc}")
            results["cbb_error"] = str(exc)

    had_errors = any(key.endswith("_error") for key in results)
    if had_errors:
        report("done", message="Pipeline finished with errors.")
    else:
        report("done", message="Pipeline finished.")
    return results


def _run_full_pipeline_background() -> None:
    _update_pipeline_state(
        running=True,
        last_error=None,
        stage="queued",
        current_date=None,
        processed_dates=[],
    )
    _pipeline_log("Pipeline started.")
    processed_dates: List[str] = []

    def progress(stage: str, current: Optional[str], message: Optional[str]) -> None:
        nonlocal processed_dates
        state: Dict[str, Any] = {"stage": stage, "current_date": current}
        if stage in {"test2_complete", "test2_error"} and current:
            if current not in processed_dates:
                processed_dates.append(current)
            state["processed_dates"] = processed_dates.copy()
        _update_pipeline_state(**state)
        if message:
            _pipeline_log(message)

    try:
        _run_full_pipeline(progress=progress)
    except Exception as exc:
        _update_pipeline_state(last_error=str(exc))
        _pipeline_log(f"Pipeline failed: {exc}")
        print(f"[pipeline] background run failed: {exc}", file=sys.stderr)
    finally:
        _update_pipeline_state(
            running=False,
            current_date=None,
            stage="idle",
            last_finished_at=datetime.utcnow().isoformat() + "Z",
        )


def _legacy_run_full_pipeline() -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for job in ("schedule_fetch", "injuries"):
        results[job] = _run_utility_script(job)
    dates = list_schedule_dates() or list_pick_dates("test2")
    if not dates:
        dates = [_now_local().strftime("%Y%m%d")]
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
    return await run_in_threadpool(_legacy_run_full_pipeline)


async def _auto_refresh_loop() -> None:
    await asyncio.sleep(5)
    while AUTO_REFRESH_ENABLED:
        wait_seconds = _seconds_until_next_run()
        _pipeline_log(f"Auto-refresh scheduled in {int(wait_seconds // 60)} minutes.")
        try:
            await asyncio.sleep(wait_seconds)
        except asyncio.CancelledError:
            break
        if not AUTO_REFRESH_ENABLED:
            break
        if PIPELINE_STATE.get("running"):
            _pipeline_log("Auto-refresh skipped: pipeline already running.")
            continue
        _pipeline_log("Auto-refresh starting pipeline run.")
        try:
            await asyncio.to_thread(_run_full_pipeline_background)
        except Exception as exc:
            _pipeline_log(f"Auto-refresh encountered an error: {exc}")
            continue


@app.on_event("startup")
async def startup_events() -> None:
    global AUTO_REFRESH_TASK
    if AUTO_REFRESH_ENABLED and AUTO_REFRESH_TASK is None:
        AUTO_REFRESH_TASK = asyncio.create_task(_auto_refresh_loop())


@app.on_event("shutdown")
async def shutdown_events() -> None:
    global AUTO_REFRESH_TASK
    if AUTO_REFRESH_TASK:
        AUTO_REFRESH_TASK.cancel()
        AUTO_REFRESH_TASK = None


@app.get("/dashboard", response_class=RedirectResponse)
@app.get("/", response_class=RedirectResponse)
async def root_to_picks() -> RedirectResponse:
    return RedirectResponse(url="/picks/test2", status_code=status.HTTP_302_FOUND)


@app.get("/picks/test2", response_class=HTMLResponse)
async def picks_test2_page(request: Request, date: Optional[str] = None) -> HTMLResponse:
    available_dates = list_schedule_dates() or list_pick_dates("test2")
    if not available_dates:
        available_dates = [_now_local().strftime("%Y%m%d")]
    selected = date if date in available_dates else _default_test2_date(available_dates)
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
    job_runtime = {
        name: {**_job_state(name), "log": list(JOB_RUNTIME_LOG.get(name, []))}
        for name in JOB_RUNTIME_NAMES
    }
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "job_state": job_state,
            "job_labels": JOB_LABELS,
            "job_runtime": job_runtime,
        },
    )


@app.get("/settings/jobs", response_class=HTMLResponse)
async def settings_jobs(request: Request) -> HTMLResponse:
    history = list_job_history()
    job_runtime = {
        name: {**_job_state(name), "log": list(JOB_RUNTIME_LOG.get(name, []))}
        for name in JOB_RUNTIME_NAMES
    }
    return templates.TemplateResponse(
        "settings_jobs.html",
        {
            "request": request,
            "history": history,
            "pipeline": PIPELINE_STATE,
            "pipeline_log": list(PIPELINE_LOG),
            "job_labels": JOB_LABELS,
            "job_runtime": job_runtime,
        },
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
    jobs = {name: _latest_script_run(name) for name in UTILITY_SCRIPTS}
    jobs["pipeline"] = PIPELINE_STATE.copy()
    jobs["pipeline_log"] = list(PIPELINE_LOG)
    jobs["job_runtime"] = {
        name: {**_job_state(name), "log": list(JOB_RUNTIME_LOG.get(name, []))}
        for name in JOB_RUNTIME_NAMES
    }
    return JSONResponse(jobs)


@app.get("/api/jobs/history")
async def api_job_history() -> JSONResponse:
    return JSONResponse(
        {
            "history": list_job_history(),
            "pipeline": PIPELINE_STATE.copy(),
            "log": list(PIPELINE_LOG),
            "runtime": {
                name: {**_job_state(name), "log": list(JOB_RUNTIME_LOG.get(name, []))}
                for name in JOB_RUNTIME_NAMES
            },
        }
    )


@app.post("/api/cbb/fetch")
async def api_cbb_fetch(payload: CBBFetchRequest) -> JSONResponse:
    try:
        data = await run_in_threadpool(compute_cbb_summary, payload.team1, payload.team2, payload.stat)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    return JSONResponse(data)


@app.post("/api/cbb/psp")
async def api_cbb_psp(payload: CBBPspRequest) -> JSONResponse:
    try:
        data = await run_in_threadpool(compute_cbb_psp, payload.teams or "", payload.stats or "")
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
    job_name: Optional[str] = None
    if kind == "test2" and not date:
        job_name = "picks_refresh"
    if job_name:
        message = "Refreshing all picks dates…" if not date else f"Refreshing picks for {date}…"
        _job_start(job_name, message)
    try:
        data = await refresh_kind_async(kind, target_date=date)
    except RuntimeError as exc:
        if job_name:
            _job_finish(job_name, error=str(exc))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    else:
        if job_name:
            _job_finish(job_name, success_message="Finished refreshing picks.")
    return JSONResponse({"status": "ok", **data})


@app.post("/admin/run/{name}")
async def run_job(name: str, request: Request) -> JSONResponse:
    token = _extract_token(request)
    _require_admin(token)
    runtime_name = name if name in JOB_RUNTIME_NAMES else None
    if runtime_name:
        label = JOB_LABELS.get(runtime_name, runtime_name.replace("_", " ").title())
        _job_start(runtime_name, f"Running {label}…")
    try:
        result = await run_utility_script_async(name)
    except HTTPException as exc:
        if runtime_name:
            _job_finish(runtime_name, error=str(exc.detail or exc))
        raise
    except RuntimeError as exc:
        if runtime_name:
            _job_finish(runtime_name, error=str(exc))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    else:
        if runtime_name:
            _job_finish(runtime_name, success_message="Job completed.")
    return JSONResponse({"status": "ok", **result})


@app.post("/admin/run-all")
async def run_all(request: Request, background_tasks: BackgroundTasks) -> JSONResponse:
    token = _extract_token(request)
    _require_admin(token)
    if PIPELINE_STATE.get("running"):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Pipeline already running")
    PIPELINE_STATE["queued_at"] = datetime.utcnow().isoformat() + "Z"
    background_tasks.add_task(_run_full_pipeline_background)
    return JSONResponse({"status": "started"}, status_code=status.HTTP_202_ACCEPTED)


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
