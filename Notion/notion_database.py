import asyncio
import sys
import os
import subprocess
import pandas as pd
from datetime import datetime
from notion_client import Client

# Add the RealSports directory to the Python path so we can import Universal_Sports_Analyzer.
sys.path.append("/Users/Q/Documents/Documents/RealSports")
from Universal_Sports_Analyzer import is_banned  # Updated banned logic is in this module
import Universal_Sports_Analyzer as USA

# -------------------------
# CONFIGURATION
# -------------------------
NOTION_TOKEN = "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"

# Main polls database ID (for regular game polls)
DATABASE_ID = "1aa71b1c-663e-8035-bc89-fb1e84a2d919"
# PSP database ID (for your PSP queries)
PSP_DATABASE_ID = "1ac71b1c663e808e9110eee23057de0e"
POLL_PAGE_ID = "18e71b1c663e80cdb8a0fe5e8aeee5a9"

client = Client(auth=NOTION_TOKEN)

# -------------------------
# Notion Database Functions
# -------------------------
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

        # Extract teams from the "Teams" property if available, otherwise use "Team 1" and "Team 2"
        if "Teams" in props:
            team_prop = props["Teams"]
            if team_prop.get("type") == "title":
                team_parts = team_prop.get("title", [])
            else:
                team_parts = team_prop.get("rich_text", [])
            teams_raw = "".join(part.get("plain_text", "") for part in team_parts)
            teams_list = [t.strip().upper() for t in teams_raw.split(",") if t.strip()]
            team1 = teams_list[0] if teams_list else ""
            team2 = teams_list[1] if len(teams_list) > 1 else ""
            row_teams = teams_list
        else:
            team1_data = props.get("Team 1", {})
            if team1_data.get("type") == "title":
                team1_parts = team1_data.get("title", [])
            else:
                team1_parts = team1_data.get("rich_text", [])
            team1 = "".join(part.get("plain_text", "") for part in team1_parts).strip().upper()
            team2_parts = props.get("Team 2", {}).get("rich_text", [])
            team2 = "".join(part.get("plain_text", "") for part in team2_parts).strip().upper()
            row_teams = [team1, team2] if team2 else [team1]

        # Sport and Stat
        sport_select = props.get("Sport", {}).get("select", {})
        sport = sport_select.get("name", "") if sport_select else ""
        stat_prop = props.get("Stat", {})
        if stat_prop.get("type") == "select":
            stat = stat_prop.get("select", {}).get("name", "")
        elif stat_prop.get("type") == "rich_text":
            stat = "".join(part.get("plain_text", "") for part in stat_prop.get("rich_text", []))
        else:
            stat = ""
        target_prop = props.get("Target", {})
        if target_prop.get("type") == "number":
            target_value = str(target_prop.get("number", ""))
        elif target_prop.get("type") == "rich_text":
            target_value = "".join(part.get("plain_text", "") for part in target_prop.get("rich_text", []))
        else:
            target_value = ""

        # Extract the Order value from the nested unique_id structure.
        order_val = None
        if "Order" in props:
            order_prop = props["Order"]
            if order_prop.get("type") == "unique_id":
                order_val = order_prop.get("unique_id", {}).get("number")
        
        is_psp = (database_id == PSP_DATABASE_ID)
        rows.append({
            "page_id": page_id,
            "team1": team1,
            "team2": team2,
            "teams": row_teams,
            "sport": sport,
            "stat": stat,
            "target": target_value,
            "created_time": created_time,
            "Order": order_val,
            "psp": is_psp
        })
    rows.sort(key=lambda x: float(x.get("Order") if x.get("Order") is not None else float('inf')))
    return rows

async def append_poll_entries_to_page(entries):
    blocks = []
    for entry in entries:
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": entry["title"]}}]
            }
        })
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": entry["output"]}}]
            }
        })
        blocks.append({"object": "block", "type": "divider", "divider": {}})
    max_blocks = 100
    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]
    responses = []
    for block_chunk in chunk_list(blocks, max_blocks):
        try:
            response = await asyncio.to_thread(client.blocks.children.append,
                                               block_id=POLL_PAGE_ID,
                                               children=block_chunk)
            responses.append(response)
        except Exception as e:
            print(f"Error updating poll page with a block chunk: {e}")
            return None
    return responses

async def mark_row_as_processed(page_id):
    try:
        await asyncio.to_thread(client.pages.update,
                                page_id=page_id,
                                properties={"Processed": {"select": {"name": "Yes"}}})
    except Exception as e:
        if "Conflict occurred while saving" in str(e):
            print(f"Conflict error while marking row {page_id} as processed. Retrying...")
            await asyncio.sleep(1)
            await mark_row_as_processed(page_id)
        else:
            print(f"Error marking row {page_id} as processed: {e}")

def update_psp_files():
    try:
        result = subprocess.run(["python", "psp_database.py"], capture_output=True, text=True)
        print(result.stdout)
    except Exception as e:
        print("Error running psp_database.py:", e)

# -------------------------
# Helper Functions: PSP Analyzers
# -------------------------
def analyze_nhl_psp(file_path, stat_key):
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return f"Error reading PSP CSV: {e}"
    df.columns = [col.upper() for col in df.columns]
    PSP_MAPPING = {
        "GOALS": "G",
        "HITS": "HIT",
        "POINTS": "P",
        "SAVES": "SV",
        "SHOTS": "S"
    }
    mapped_stat = PSP_MAPPING.get(stat_key)
    if mapped_stat is None:
        return f"❌ Invalid NHL stat choice."
    if mapped_stat not in df.columns:
        return f"Stat column '{mapped_stat}' not found in CSV."
    try:
        df[mapped_stat] = pd.to_numeric(df[mapped_stat].replace({',': ''}, regex=True), errors='coerce')
    except Exception as e:
        return f"Error converting stat column: {e}"
    try:
        df_inj = pd.read_csv("NHL/nhl_injuries.csv")
        df_inj["playerName"] = df_inj["playerName"].str.strip()
        injured_names = set(df_inj["playerName"].dropna().unique())
        df = df[~df["NAME"].isin(injured_names)]
    except Exception as e:
        return f"Error loading or processing NHL injuries CSV: {e}"
    sorted_df = df.sort_values(by=mapped_stat, ascending=False).reset_index(drop=True)
    yellow = sorted_df.iloc[0:3]
    green = sorted_df.iloc[6:9]
    red = sorted_df.iloc[12:15]
    player_col = "NAME" if "NAME" in sorted_df.columns else None
    if player_col is None:
        return "Player column not found in CSV."
    output = f"🟢 {', '.join(str(x) for x in green[player_col].tolist() if not is_banned(str(x), stat_key))}\n"
    output += f"🟡 {', '.join(str(x) for x in yellow[player_col].tolist() if not is_banned(str(x), stat_key))}\n"
    output += f"🔴 {', '.join(str(x) for x in red[player_col].tolist() if not is_banned(str(x), stat_key))}"
    return output

def analyze_nba_psp(file_path, stat_key):
    try:
        df = pd.read_csv(file_path)
        df.columns = [col.upper() for col in df.columns]
    except Exception as e:
        return f"Error reading PSP CSV: {e}"
    try:
        df_inj = pd.read_csv("NBA/nba_injury_report.csv")
        df_inj["playerName"] = df_inj["playerName"].str.strip()
        injured_names = set(df_inj["playerName"].dropna().unique())
        df = df[~df["NAME"].isin(injured_names)]
    except Exception as e:
        return f"Error loading or processing NBA injuries CSV: {e}"
    sorted_df = df.sort_values(by=stat_key, ascending=False).reset_index(drop=True)
    sorted_df = sorted_df.drop_duplicates(subset=["NAME"]).reset_index(drop=True)
    green = sorted_df.iloc[0:3]
    yellow = sorted_df.iloc[3:6]
    red = sorted_df.iloc[6:9]
    player_col = "NAME" if "NAME" in sorted_df.columns else None
    if player_col is None:
        return "Player column not found in CSV."
    output = f"🟢 {', '.join(str(x) for x in green[player_col].tolist() if not is_banned(str(x), stat_key))}\n"
    output += f"🟡 {', '.join(str(x) for x in yellow[player_col].tolist() if not is_banned(str(x), stat_key))}\n"
    output += f"🔴 {', '.join(str(x) for x in red[player_col].tolist() if not is_banned(str(x), stat_key))}"
    return output

# -------------------------
# Analyzer Function: Calls the appropriate analyzer
# -------------------------
def run_universal_sports_analyzer_programmatic(row):
    sport_upper = row["sport"].upper()
    teams = row.get("teams", [])
    if not teams:
        teams = [team.strip().upper() for team in [row.get("team1", ""), row.get("team2", "")] if team]
    
    def parse_target(target):
        t = target.strip().lower()
        if t in ["", "none"]:
            return None
        try:
            return float(t)
        except Exception:
            return None

    target_val = parse_target(row["target"])
    
    if row.get("psp", False):
        update_psp_files()
        if sport_upper == "NHL":
            stat_key = row["stat"].upper()
            file_path = os.path.join("PSP", f"nhl_{row['stat'].lower()}_psp_data.csv")
            return analyze_nhl_psp(file_path, stat_key)
        elif sport_upper == "NBA":
            stat_key = row["stat"].upper()
            if stat_key == "FG3M":
                stat_key = "3PM"
            if stat_key not in USA.STAT_CATEGORIES_NBA:
                return f"❌ Invalid NBA stat choice."
            file_path = os.path.join("PSP", f"nba_{row['stat'].lower()}_psp_data.csv")
            return analyze_nba_psp(file_path, stat_key)
        elif sport_upper == "CBB":
            stat_key = row["stat"].upper()
            if stat_key not in USA.STAT_CATEGORIES_CBB:
                return f"❌ Invalid CBB stat choice."
            # Use the dedicated analyzer for CBB from USA
            df = USA.integrate_cbb_data("cbb_players_stats.csv", "cbb_injuries.csv")
            if df.empty:
                return "❌ CBB stats not found or empty."
            return USA.analyze_cbb_noninteractive(df, row.get("teams", []), stat_key, target_val, stat_key)
        else:
            return "PSP processing not configured for this sport."
    else:
        if sport_upper == "NBA":
            df = USA.integrate_nba_data('nba_player_stats.csv', 'nba_injury_report.csv')
            used_stat = row["stat"].upper() if row["stat"].strip() else "PPG"
            player_col = "PLAYER" if "PLAYER" in df.columns else "NAME"
            return USA.analyze_sport_noninteractive(
                df, USA.STAT_CATEGORIES_NBA, player_col, "TEAM", teams, used_stat, target_val, used_stat
            )
        elif sport_upper == "CBB":
            player_stats_file = "cbb_players_stats.csv"
            if not os.path.exists(player_stats_file):
                return f"❌ '{player_stats_file}' file not found."
            try:
                df = USA.integrate_cbb_data(player_stats_file=player_stats_file)
            except FileNotFoundError:
                return f"❌ '{player_stats_file}' file not found."
            used_stat = row["stat"].upper() if row["stat"].strip() else "PPG"
            return USA.analyze_cbb_noninteractive(
                df, teams, used_stat, target_val, used_stat
            )
        elif sport_upper == "MLB":
            df = USA.integrate_mlb_data()
            if df.empty or "TEAM" not in df.columns:
                return "❌ 'TEAM' column not found in the MLB data."
            used_stat = row["stat"].upper() if row["stat"].strip() else "RBI"
            return USA.analyze_mlb_noninteractive(
                df, teams, used_stat, used_stat
            )
        elif sport_upper == "NHL":
            df = USA.integrate_nhl_data("nhl_player_stats.csv", "nhl_injuries.csv")
            nhl_stat = row["stat"].upper() if row["stat"].strip() else "GOALS"
            return USA.analyze_nhl_noninteractive(df, teams, nhl_stat, target_val, nhl_stat)
        else:
            return "Sport not recognized."

# -------------------------
# Main Process
# -------------------------
async def process_rows():
    main_rows = fetch_unprocessed_rows(DATABASE_ID)
    psp_rows = fetch_unprocessed_rows(PSP_DATABASE_ID)
    all_rows = main_rows + psp_rows
    
    poll_entries = []
    for row in all_rows:
        result = run_universal_sports_analyzer_programmatic(row)
        title = f"Game: {row.get('team1','')} vs {row.get('team2','')} ({row['sport']}, {row['stat']}, Target: {row['target']})"
        poll_entries.append({
            "title": title,
            "output": result
        })
        await mark_row_as_processed(row["page_id"])
    
    await append_poll_entries_to_page(poll_entries)

def main():
    asyncio.run(process_rows())

if __name__ == "__main__":
    main()