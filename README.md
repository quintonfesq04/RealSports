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
   ```

2. Visit [http://localhost:8000/picks/test2](http://localhost:8000/picks/test2) for the multi-sport picks (with the date selector at the top).
   - Use the drop-down to jump between today and the next few days of cached results.
   - “Copy group” copies the four-line summary exactly as you’d paste it; “Copy All” grabs the entire page.
3. Visit [/picks/cbb](http://localhost:8000/picks/cbb) for college basketball.
   - The page shows the cached `picks_cbb.py` output plus the custom query form so you can run ad-hoc matchups.
4. Use [/settings](http://localhost:8000/settings) if you need to re-run the schedule fetch, injuries cache, CBB scraper, or the entire pipeline manually. There’s also a link to the job-status page so you can confirm the 5 AM automation ran.

What’s included in the new UI:
- Picks pages only (Test2 + CBB) with a Settings area; casual users never see the background controls.
- Multi-day cache: the schedule fetcher pulls today + the next four days, StatMuse results are stored per-date, and the 5 AM job runs the entire pipeline so the evening copy/paste is instant.
- Feed-level “Copy” buttons that flatten each pick into the 🟢/🟡/🔴/🟣 text you share with the team.
- A CBB custom query form embedded on the CBB picks page; the scraper button sits there too.
- Settings page for manual runs (schedule, injuries, pipeline, CBB scraper) and a separate job-status page for debugging.
- GitHub Actions only needs an `APP_URL` secret now—the endpoints are open, so there’s no admin token to manage.
- Notion is no longer required: the scheduler hits ESPN scoreboards, injuries scrape CBS, and Test2 consumes `data/schedule.json` directly.
- Dedicated picks pages live at `/picks/test2` and `/picks/cbb`, each with “refresh / copy group / copy all” controls for sharing.
- Settings page keeps the manual tools + job logs; `/dashboard` (legacy view) is still available from there if you ever need it.
