---
title: RealSports Picks
emoji: 🏀
colorFrom: blue
colorTo: green
sdk: docker
pinned: false
---

The goal behind this project was to help speed up my research process when filling out polls on RealSports.io

How to start:
1. download all files - at minimum download all python files
2. run the following python codes to get the player stats:
   - cbb_database.py
   - nba_database.py
   - nhl_database.py
   - mlb_database.py
3. once you run those codes, then go to Universal_Sports_Analyzer.py --this file is a Machine Learning python code that analyzes the stats based on the teams you provide. When you run the code you will be prompted to choose the sport you want,
   followed by the team name(s), next you will enter the stat you want, and lastly you will enter your "target value" - this value is the number you want your play to hit (i.e., Player to get 6+ rebounds), lastly the code will output what I call
   the green yellow red method, which is around 9 players broken into three categories. Here are the plays and their meanings:
      - 🟢 plays, these are your best bets - a player that is not a clear favorite or underdog
      - 🟡 plays, these are your favorites - a player that is likely to win, but the karma is lower
      - 🔴 plays, these are your underdogs - a player that is a long shot with a high payout if it hits

I hope anyone who uses RealSports likes this code and if you have any issues feel free to message me on Real @quintonfesq and I will try to help

## FastAPI web app

1. Install the dependencies (FastAPI + Jinja + Uvicorn) and start the server:

   ```bash
   pip install -r requirements.txt
   uvicorn app.main:app --host 0.0.0.0 --port 8000
   python schedule_fetch.py, python injuries.py, python test2.py locally so data/ and data/picks.db 
   ```

2. Visit [http://localhost:8000/picks/test2](http://localhost:8000/picks/test2) for the multi-sport picks (with the date selector at the top).
   - Use the drop-down to jump between today and the next few days of cached results.
   - “Copy group” copies the four-line summary exactly as you’d paste it; “Copy All” grabs the entire page.
3. Visit [/picks/cbb](http://localhost:8000/picks/cbb) for college basketball.
   - The page shows the cached `picks_cbb.py` output plus the custom query form so you can run ad-hoc matchups.
4. Use [/settings](http://localhost:8000/settings) if you need to re-run the schedule fetch, injuries cache, CBB scraper, or the entire pipeline manually. There’s also a link to the job-status page so you can confirm the 5 AM automation ran.

What’s included in the new UI:
- Picks pages only (Picks + CBB) with a Settings area; casual users never see the background controls.
- Multi-day cache: the schedule fetcher pulls today + the next four days, StatMuse results are stored per-date, and the 5 AM job runs the entire pipeline so the evening copy/paste is instant.
- Feed-level “Copy” buttons that flatten each pick into the 🟢/🟡/🔴/🟣 text you share with the team.
- A CBB custom query form embedded on the CBB picks page; the scraper button sits there too.
- Settings page for manual runs (schedule, injuries, pipeline, CBB scraper) and a separate job-status page for debugging.
- GitHub Actions only needs an `APP_URL` secret now—the endpoints are open, so there’s no admin token to manage.
- Notion is no longer required: the scheduler hits ESPN scoreboards, injuries scrape CBS, and the Picks feed consumes `data/schedule.json` directly.
- Dedicated picks pages live at `/picks/test2` and `/picks/cbb`, each with “refresh / copy group / copy all” controls for sharing.
- Settings page keeps the manual tools + job logs; job history now lives at `/settings/jobs` (legacy dashboard removed).
- Picks refreshes now process today plus the upcoming schedule window, and the UI defaults to tomorrow’s slate (while keeping today’s plays one click away).
- A “Quick lookup” form on the Picks page lets you filter the cached picks by sport/stat/teams without leaving the browser.
- A PSP lookup block on the Picks page surfaces the cached PSP rows by sport/stat if you need to grab them manually.
- The CBB page includes both the single-matchup query and a PSP query that takes comma-separated teams/stats (including combo stats like `PRA` or `PPG+RPG+APG`) for multi-school pulls.
- Set `APP_TIMEZONE` (default `US/Eastern`) if you need the “today/tomorrow” logic to align with a different local day boundary.
- Use `PIPELINE_INCLUDE_CBB=1` only if your environment has the Notion credentials required by `picks_cbb.py`; it defaults to off so HF runs don’t error.
- Set `AUTO_REFRESH_ENABLED=1` (plus optional `AUTO_REFRESH_HOUR` / `AUTO_REFRESH_MINUTE`) to let the server kick off the full pipeline daily without GitHub Actions.
- Selenium is disabled automatically in headless runtimes (`DISABLE_SELENIUM=1`); set it back to `0` locally if you rely on browser scraping.

## Deployment (free hosting)

The FastAPI layer lives in `app/main.py`, reads cached JSON/SQLite from `data/`, and exposes everything through Uvicorn. Deta Space shut down in 2024, so the current no-card-required options are below.

### 1. Hugging Face Spaces (Docker)

Create a new **Space → Docker** and point it at this repo (or copy/paste the code). The included `Dockerfile` builds the app:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 7860
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-7860}"]
```

Spaces default to port 7860; once the build finishes the app is instantly public. Add the same env vars from the “Variables” tab (they map to `os.getenv` in the scripts).

### 2. Koyeb (GitHub deploy)

1. Push this repo to GitHub.
2. In Koyeb: **Create Service → Deploy from GitHub**.
3. Start command: `uvicorn app.main:app --host 0.0.0.0 --port 8080`.
4. Add env vars (`ADMIN_TOKEN`, `NOTION_TOKEN`, `DATABASE_ID`, `POLL_PAGE_ID`, `PSP_DATABASE_ID`) in the service configuration.

Koyeb gives you a permanent free tier (512 MB, single container). Use it for the read-only viewer while keeping scrapers on a separate schedule or cron run.

### 3. Render (free Web Service)

Render’s free Web Service tier sleeps after 15 minutes of inactivity but is perfect for a public dashboard:

1. Push this repo to GitHub.
2. In Render: **New Web Service → Connect Repo → Build with Docker** (Render auto-detects `Dockerfile`).
3. Set the environment to Docker and keep the default start command (Render runs the container, which already starts Uvicorn).
4. Add the same env vars/secrets under “Environment”.

Render assigns a public URL like `https://realsports-picks.onrender.com`; first requests trigger a cold boot (~30s).

### Pre-deploy checklist

- Run `python schedule_fetch.py`, `python injuries.py`, and `python test2.py` locally so `data/` has fresh caches before pushing to any host.
- Commit the generated JSON/SQLite if you want instant content on cold start.
- Verify `uvicorn app.main:app --host 0.0.0.0 --port 8000` works locally and that `requirements.txt` only lists packages you really need on the public host.
