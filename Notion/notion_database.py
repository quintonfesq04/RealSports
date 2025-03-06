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
POLL_PAGE_ID = "18e71b1c663e80cdb8a0fe5e8aeee5a9"  # The page where outputs will be added

client = Client(auth=NOTION_TOKEN)

# -------------------------
# Notion Database Functions
# -------------------------
def extract_team_name(prop):
    """Extracts the plain text of a team name from Notion properties."""
    if not prop:
        return ""
    if prop.get("type") == "title":
        return "".join(part.get("plain_text", "") for part in prop.get("title", [])).strip().upper()
    if prop.get("type") == "rich_text":
        return "".join(part.get("plain_text", "") for part in prop.get("rich_text", [])).strip().upper()
    return ""

def extract_teams_from_notion(props):
    """
    Extracts Team 1 and Team 2 from either 'Team 1'/'Team 2' fields or from a combined 'Teams' field.
    This enhanced version checks if both fields are available. If not, it tries to parse the combined field
    using common delimiters such as " vs " or commas.
    """
    team1 = extract_team_name(props.get("Team 1", {}))
    team2 = extract_team_name(props.get("Team 2", {}))

    # If either Team 1 or Team 2 is missing, try extracting from the combined 'Teams' field.
    if not team1 or not team2:
        teams_prop = props.get("Teams", {})
        teams_raw = extract_team_name(teams_prop)
        if teams_raw:
            # Try splitting by " vs " (case-insensitive)
            if " vs " in teams_raw.lower():
                parts = teams_raw.lower().split(" vs ")
                if len(parts) >= 2:
                    team1 = team1 or parts[0].strip().upper()
                    team2 = team2 or parts[1].strip().upper()
            # Fallback: try splitting by comma
            elif "," in teams_raw:
                teams_list = [t.strip().upper() for t in teams_raw.split(",") if t.strip()]
                if len(teams_list) >= 2:
                    team1 = team1 or teams_list[0]
                    team2 = team2 or teams_list[1]
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

        # Extract teams using the enhanced function
        team1, team2 = extract_teams_from_notion(props)
        print(f"DEBUG: Extracted from Notion -> Team 1: '{team1}', Team 2: '{team2}'")

        if not team1 or not team2:
            print(f"❌ ERROR: Only found {len([t for t in [team1, team2] if t])} team(s) for row: {props}")

        sport = props.get("Sport", {}).get("select", {}).get("name", "")
        stat = extract_team_name(props.get("Stat", {}))
        
        # --- NEW TARGET EXTRACTION ---
        target_value = None
        target_prop = props.get("Target", {})
        if target_prop.get("type") == "number":
            target_value = target_prop.get("number")
        else:
            target_value = extract_team_name(target_prop)
        # --------------------------------

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
    max_attempts = 3
    attempt = 0
    while attempt < max_attempts:
        try:
            await asyncio.to_thread(
                client.pages.update,
                page_id=page_id,
                properties={"Processed": {"select": {"name": "Yes"}}}
            )
            return  # Exit if successful.
        except Exception as e:
            attempt += 1
            print(f"Error marking row {page_id} as processed on attempt {attempt}: {e}")
            await asyncio.sleep(2)  # Wait 2 seconds before retrying.
    print(f"Failed to mark row {page_id} as processed after {max_attempts} attempts.")

def update_psp_files():
    try:
        subprocess.run(["python", "psp_database.py"], capture_output=True, text=True)
    except Exception as e:
        print("Error running psp_database.py:", e)

def update_poll_page(entries):
    """
    Updates the designated Notion poll page by appending results.
    For each game, this function creates:
      - A heading block (game title)
      - A paragraph block (analysis output)
      - A divider block
    If there are more than 100 blocks, it sends them in batches.
    """
    blocks = []
    for entry in entries:
        # Create a heading block for the game title.
        blocks.append({
            "object": "block",
            "type": "heading_3",
            "heading_3": {
                "rich_text": [{
                    "type": "text",
                    "text": {"content": entry["title"]}
                }]
            }
        })
        # Create a paragraph block for the output.
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{
                    "type": "text",
                    "text": {"content": entry["output"]}
                }]
            }
        })
        # Create a divider block to separate games.
        blocks.append({
            "object": "block",
            "type": "divider",
            "divider": {}
        })
    
    # Notion API limits appending to 100 blocks per request.
    chunk_size = 100
    for i in range(0, len(blocks), chunk_size):
        chunk = blocks[i:i+chunk_size]
        try:
            client.blocks.children.append(block_id=POLL_PAGE_ID, children=chunk)
        except Exception as e:
            print("Error updating poll page:", e)
    print("Poll page updated successfully.")

# -------------------------
# Analyzer Function
# -------------------------
def run_universal_sports_analyzer_programmatic(row):
    sport_upper = row["sport"].upper()

    # PSP processing branch:
    if row.get("psp", False):
        update_psp_files()
        # Optionally, wait a short period if needed (e.g., time.sleep(2))
        sport_lower = row["sport"].lower()
        stat_val = row["stat"].strip()
        stat_formatted = stat_val.lower().replace(" ", "_") if stat_val else "default"
        filename = os.path.join("PSP", f"{sport_lower}_{stat_formatted}_psp_data.csv")
        if not os.path.exists(filename):
            return "PSP data processed but CSV not found."
        try:
            df_psp = pd.read_csv(filename)
        except Exception as e:
            return f"PSP data processed but error reading CSV: {e}"
        teams = row["teams"]
        if sport_upper == "MLB":
            used_stat = row["stat"].upper() if row["stat"].strip() else "RBI"
            return USA.analyze_mlb_noninteractive(df_psp, teams, used_stat)
        elif sport_upper == "NBA":
            target_val = row.get("target")
            if not target_val:
                target_val = 20  # adjust default as needed
            else:
                try:
                    target_val = float(target_val)
                except:
                    target_val = 20
            used_stat = row["stat"].upper() if row["stat"].strip() else "PPG"
            
            # --- FIX FOR MISSING TEAM COLUMN ---
            print("PSP DataFrame columns before TEAM fix:", df_psp.columns.tolist())
            if "TEAM" not in df_psp.columns:
                if "Team" in df_psp.columns:
                    df_psp.rename(columns={"Team": "TEAM"}, inplace=True)
                    print("Renamed 'Team' to 'TEAM'.")
                else:
                    if teams:
                        df_psp["TEAM"] = teams[0]
                        print("Added default TEAM column with value:", teams[0])
                    else:
                        return "❌ Error: No team information found."
            # --------------------------------------
            
            # Note: The CSV contains a "NAME" column (not "PLAYER")
            return USA.analyze_sport_noninteractive(df_psp, USA.STAT_CATEGORIES_NBA, "NAME", "TEAM", teams, used_stat, target_val)
        elif sport_upper == "CBB":
            target_val = row.get("target")
            if not target_val:
                target_val = 10  # default for CBB (adjust as needed)
            else:
                try:
                    target_val = float(target_val)
                except:
                    target_val = 10
            used_stat = row["stat"].upper() if row["stat"].strip() else "PPG"
            return USA.analyze_sport_noninteractive(df_psp, USA.STAT_CATEGORIES_CBB, "Player", "Team", teams, used_stat, target_val)
        elif sport_upper == "NHL":
            used_stat = row["stat"].upper() if row["stat"].strip() else "GOALS"
            if used_stat in ["S", "SHOTS"]:
                target_val = row.get("target")
                if not target_val:
                    target_val = 10  # default for shots (adjust as needed)
                else:
                    try:
                        target_val = float(target_val)
                    except:
                        target_val = 10
            else:
                target_val = None
            # Ensure the PSP DataFrame has a "Team" column.
            if "Team" not in df_psp.columns:
                df_psp["Team"] = teams[0]  # or combine teams if preferred
            if "Player" not in df_psp.columns and "NAME" in df_psp.columns:
                df_psp.rename(columns={"NAME": "Player"}, inplace=True)
            return USA.analyze_nhl_noninteractive(df_psp, teams, used_stat, target_val)
        else:
            return "PSP analysis not available for this sport."

    # Non-PSP (regular) processing below:
    teams = row["teams"]
    if len(teams) < 2:
        return f"❌ Two teams are required. (Found: {teams})"

    if sport_upper == "MLB":
        target_val = None
        df = USA.integrate_mlb_data()
        used_stat = row["stat"].upper() if row["stat"].strip() else "RBI"
        return USA.analyze_mlb_noninteractive(df, teams, used_stat)
    elif sport_upper == "NBA":
        target_val = row["target"]
        if isinstance(target_val, (int, float)):
            pass
        elif target_val and isinstance(target_val, str) and target_val.lower() not in ["", "none"]:
            try:
                target_val = float(target_val)
            except ValueError:
                target_val = None
        if target_val is None or target_val == 0:
            return "Target value required and must be nonzero."
        df = USA.integrate_nba_data('nba_player_stats.csv', 'nba_injury_report.csv')
        used_stat = row["stat"].upper() if row["stat"].strip() else "PPG"
        return USA.analyze_sport_noninteractive(df, USA.STAT_CATEGORIES_NBA, "PLAYER", "TEAM", teams, used_stat, target_val)
    elif sport_upper == "CBB":
        target_val = row["target"]
        if isinstance(target_val, (int, float)):
            pass
        elif target_val and isinstance(target_val, str) and target_val.lower() not in ["", "none"]:
            try:
                target_val = float(target_val)
            except ValueError:
                target_val = None
        if target_val is None or target_val == 0:
            return "Target value required and must be nonzero."
        df = USA.load_player_stats()
        used_stat = row["stat"].upper() if row["stat"].strip() else "PPG"
        return USA.analyze_sport_noninteractive(df, USA.STAT_CATEGORIES_CBB, "Player", "Team", teams, used_stat, target_val)
    elif sport_upper == "NHL":
        df = USA.integrate_nhl_data("nhl_player_stats.csv", "nhl_injuries.csv")
        used_stat = row["stat"].upper() if row["stat"].strip() else "GOALS"
        if used_stat in ["S", "SHOTS"]:
            target_val = row["target"]
            if isinstance(target_val, (int, float)):
                pass
            elif target_val and isinstance(target_val, str) and target_val.lower() not in ["", "none"]:
                try:
                    target_val = float(target_val)
                except ValueError:
                    target_val = None
            if target_val is None or target_val == 0:
                return "Target value required for Shots and must be nonzero."
        else:
            target_val = None
        return USA.analyze_nhl_noninteractive(df, teams, used_stat, target_val)
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
    
    # Update the poll page with the collected results.
    update_poll_page(poll_entries)

def main():
    asyncio.run(process_rows())

if __name__ == "__main__":
    main()