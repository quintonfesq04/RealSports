import asyncio
import sys
import os
import subprocess
import pandas as pd
from datetime import datetime
from notion_client import Client

# Add the RealSports directory to the Python path so we can import Universal_Sports_Analyzer.
sys.path.append("/Users/Q/Documents/Documents/RealSports")
import Universal_Sports_Analyzer as USA

# -------------------------
# CONFIGURATION
# -------------------------
NOTION_TOKEN = "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"

DATABASE_ID = "1aa71b1c-663e-8035-bc89-fb1e84a2d919"
PSP_DATABASE_ID = "1ac71b1c663e808e9110eee23057de0e"
POLL_PAGE_ID = "18e71b1c663e80cdb8a0fe5e8aeee5a9"

client = Client(auth=NOTION_TOKEN)

# -------------------------
# Notion Database Functions
# -------------------------
def extract_team_name(prop):
    """ Extracts the plain text of a team name from Notion properties. """
    if not prop:
        return ""
    if prop.get("type") == "title":
        return "".join(part.get("plain_text", "") for part in prop.get("title", [])).strip().upper()
    if prop.get("type") == "rich_text":
        return "".join(part.get("plain_text", "") for part in prop.get("rich_text", [])).strip().upper()
    return ""

def extract_teams_from_notion(props):
    """ Extracts Team 1 and Team 2 from either 'Team 1'/'Team 2' fields or from a combined 'Teams' field. """
    team1 = extract_team_name(props.get("Team 1", {}))
    team2 = extract_team_name(props.get("Team 2", {}))

    # If Team 1 and Team 2 are missing, try extracting from 'Teams'
    if not team1 or not team2:
        teams_prop = props.get("Teams", {})
        teams_raw = extract_team_name(teams_prop)

        if " vs " in teams_raw:  # Case where teams are stored like "MIN vs NYY"
            team1, team2 = [t.strip().upper() for t in teams_raw.split(" vs ")]
        elif "," in teams_raw:  # Case where teams are comma-separated
            teams_list = [t.strip().upper() for t in teams_raw.split(",") if t.strip()]
            if len(teams_list) >= 2:
                team1, team2 = teams_list[:2]  # Take first two teams

    return team1, team2

def fetch_unprocessed_rows(database_id):
    try:
        response = client.databases.query(
            database_id=database_id,
            filter={
                "property": "Processed",
                "select": {"equals": "no"}
            },
            sort=[{
                "property": "Order",
                "direction": "ascending"
            }]
        )
    except Exception as e:
        print("Error querying database:", e)
        return []
    
    rows = []
    for result in response.get("results", []):
        page_id = result["id"]
        created_time = result.get("created_time", "")
        props = result.get("properties", {})

        # Extract teams
        team1, team2 = extract_teams_from_notion(props)
        print(f"DEBUG: Extracted from Notion -> Team 1: '{team1}', Team 2: '{team2}'")  # Debugging print

        if not team1 or not team2:
            print(f"❌ ERROR: Only found {len([t for t in [team1, team2] if t])} team(s) for row: {props}")

        sport = props.get("Sport", {}).get("select", {}).get("name", "")
        stat = extract_team_name(props.get("Stat", {}))
        target_value = extract_team_name(props.get("Target", {}))

        order_val = props.get("Order", {}).get("unique_id", {}).get("number")

        rows.append({
            "page_id": page_id,
            "team1": team1,
            "team2": team2,
            "teams": [team1, team2] if team1 and team2 else [],
            "sport": sport,
            "stat": stat,
            "target": target_value,
            "created_time": created_time,
            "Order": order_val,
            "psp": (database_id == PSP_DATABASE_ID)
        })
    
    rows.sort(key=lambda x: float(x.get("Order") if x.get("Order") is not None else float('inf')))
    return rows

async def mark_row_as_processed(page_id):
    try:
        await asyncio.to_thread(client.pages.update,
                                page_id=page_id,
                                properties={"Processed": {"select": {"name": "Yes"}}})
    except Exception as e:
        print(f"Error marking row {page_id} as processed: {e}")

def update_psp_files():
    try:
        subprocess.run(["python", "psp_database.py"], capture_output=True, text=True)
    except Exception as e:
        print("Error running psp_database.py:", e)

# -------------------------
# Analyzer Function
# -------------------------
def run_universal_sports_analyzer_programmatic(row):
    sport_upper = row["sport"].upper()
    teams = row["teams"]

    if len(teams) < 2:
        return f"❌ Two teams are required. (Found: {teams})"

    target_val = row["target"]
    if target_val and target_val.lower() not in ["", "none"]:
        try:
            target_val = float(target_val)
        except ValueError:
            target_val = None

    if row.get("psp", False):
        update_psp_files()
        return "PSP processing not implemented for this sport."

    elif sport_upper == "MLB":
        df = USA.integrate_mlb_data()
        used_stat = row["stat"].upper() if row["stat"].strip() else "RBI"
        return USA.analyze_mlb_noninteractive(df, teams, used_stat)

    elif sport_upper == "NBA":
        df = USA.integrate_nba_data('nba_player_stats.csv', 'nba_injury_report.csv')
        used_stat = row["stat"].upper() if row["stat"].strip() else "PPG"
        return USA.analyze_sport_noninteractive(df, USA.STAT_CATEGORIES_NBA, "PLAYER", "TEAM", teams, used_stat, target_val)

    elif sport_upper == "CBB":
        df = USA.load_player_stats()
        used_stat = row["stat"].upper() if row["stat"].strip() else "PPG"
        return USA.analyze_sport_noninteractive(df, USA.STAT_CATEGORIES_CBB, "Player", "Team", teams, used_stat, target_val)

    elif sport_upper == "NHL":
        df = USA.integrate_nhl_data("nhl_player_stats.csv", "nhl_injuries.csv")
        nhl_stat = row["stat"].upper() if row["stat"].strip() else "GOALS"
        return USA.analyze_nhl_noninteractive(df, teams, nhl_stat, target_val)

    return "Sport not recognized."

# -------------------------
# Main Process
# -------------------------
async def process_rows():
    main_rows = fetch_unprocessed_rows(DATABASE_ID)
    psp_rows = fetch_unprocessed_rows(PSP_DATABASE_ID)
    all_rows = main_rows + psp_rows

    if not all_rows:
        print("No unprocessed rows found in any database.")
        return

    poll_entries = []
    for row in all_rows:
        result = run_universal_sports_analyzer_programmatic(row)
        title = f"Game: {row.get('team1','')} vs {row.get('team2','')} ({row['sport']}, {row['stat']}, Target: {row['target']})"
        poll_entries.append({"title": title, "output": result})
        await mark_row_as_processed(row["page_id"])

    print("Poll page updated successfully.")

def main():
    asyncio.run(process_rows())

if __name__ == "__main__":
    main()