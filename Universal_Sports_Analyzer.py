#!/usr/bin/env python3
"""
Unified Sports Analyzer with Notion & PSP Integration

This script demonstrates:
  • A unified analysis function that computes a "Success_Rate" for most sports 
    (using a target value) or simply orders by a stat for MLB (which doesn’t require a target).
  • An interactive mode and a Notion integration mode.
  • A branch for PSP processing – if a row’s "psp" property is True, a specialized PSP analyzer is used.
  
Before running, please ensure:
  - CSV files (e.g. "nba_player_stats.csv", "cbb_players_stats.csv", "mlb_stats.csv", etc.) exist.
  - Your Notion API token, database IDs, and page IDs are correctly set.
  - Required packages (pandas, numpy, notion-client, asyncio) are installed.
"""

import pandas as pd
import numpy as np
import os
import re
import asyncio
from notion_client import Client

# ---------------------------
# Configuration: Stat Categories
# ---------------------------
STAT_CATEGORIES_NBA = {
    "PPG": "PTS",
    "APG": "AST",
    "RPG": "REB",
    "3PM": "FG3M"
}

STAT_CATEGORIES_CBB = {
    "PPG": "PPG",
    "APG": "APG",
    "RPG": "RPG",
    "3PM": "3PM"
}

STAT_CATEGORIES_NHL = {
    "GOALS": "G",
    "ASSISTS": "A",    # Assumes per-game conversion is handled before analysis
    "POINTS": "PTS",   # Assumes per-game conversion is handled before analysis
    "S": "shotsPerGame"
}

STAT_CATEGORIES_MLB = {
    "RBI": "RBI",
    "G": "G",
    "AB": "AB",
    "R": "R",
    "H": "H",
    "AVG": "AVG",
    "OBP": "OBP",
    "OPS": "OPS"
}

# ---------------------------
# Configuration: Banned Players
# ---------------------------
GLOBAL_BANNED_PLAYERS = [
    "Bobby Portis",
    "Jonas Valančiūnas",
    "Ethen Frank",
    "Killian Hayes",
    "Khris Middleton",
    "Bradley Beal"
]
GLOBAL_BANNED_PLAYERS_SET = {p.strip().lower() for p in GLOBAL_BANNED_PLAYERS}

def is_banned(player_name, stat=None):
    """
    Returns True if the player is in the banned list.
    The optional stat parameter allows for future stat-specific logic.
    """
    return player_name.strip().lower() in GLOBAL_BANNED_PLAYERS_SET

# ---------------------------
# Unified Analysis Function
# ---------------------------
def analyze_poll(df, player_col, stat_col, target_value=None, mode="threshold", use_target=True):
    """
    Analyzes a poll DataFrame by computing a "Success_Rate" (if use_target is True) 
    or using the raw stat for ordering (if use_target is False), then grouping players.
    
    Parameters:
      df          : DataFrame with player data.
      player_col  : Column name for player names.
      stat_col    : Column name for the stat (e.g. "PTS", "PPG", etc.).
      target_value: Numeric target used for calculating success rate (if use_target=True).
      mode        : "threshold" (default) uses categorization based on thresholds,
                    "slicing" uses a simple top-9 slicing approach.
      use_target  : Boolean indicating whether to compute a success rate (True for NBA/CBB/NHL) 
                    or ignore the target (False for MLB).
                    
    Returns:
      A formatted string:
        🟢 [Green Plays]
        🟡 [Yellow Plays]
        🔴 [Red Plays]
    """
    # Convert stat column to numeric and drop invalid rows
    df[stat_col] = pd.to_numeric(df[stat_col], errors='coerce')
    df = df.dropna(subset=[stat_col])
    
    if use_target:
        # Compute success rate using the provided target value
        df["Success_Rate"] = ((df[stat_col] / target_value) * 100).round(1)
    else:
        # For sports like MLB, ignore target and use the raw stat for ordering
        df["Success_Rate"] = df[stat_col]
    
    # If using thresholds (and target), assign categories (only if use_target is True)
    if use_target:
        conditions = [
            df["Success_Rate"] >= 120,
            (df["Success_Rate"] >= 100) & (df["Success_Rate"] < 120),
            df["Success_Rate"] < 100
        ]
        choices = ["🟡 Favorite", "🟢 Best Bet", "🔴 Underdog"]
        df["Category"] = np.select(conditions, choices, default="Uncategorized")
    else:
        # For MLB, we simply order by the raw stat and then use slicing later.
        df["Category"] = "Ordered"
    
    # Remove banned players and duplicates (by player name)
    df = df[~df[player_col].apply(is_banned)]
    df = df.drop_duplicates(subset=[player_col])
    
    # Group players based on the selected mode
    if mode == "threshold" and use_target:
        green = df[df["Category"] == "🟢 Best Bet"].nlargest(3, "Success_Rate")
        yellow = df[df["Category"] == "🟡 Favorite"].nlargest(3, "Success_Rate")
        red = df[df["Category"] == "🔴 Underdog"].nsmallest(3, "Success_Rate")
    elif mode == "slicing":
        sorted_df = df.sort_values(by="Success_Rate", ascending=False)
        players = sorted_df[player_col].tolist()[:9]
        # For MLB the order is: positions 1-3 = Yellow, 4-6 = Green, 7-9 = Red
        yellow = pd.DataFrame(players[:3], columns=[player_col])
        green = pd.DataFrame(players[3:6], columns=[player_col])
        red = pd.DataFrame(players[6:9], columns=[player_col])
    else:
        raise ValueError("Invalid mode specified. Use 'threshold' or 'slicing'.")
    
    green_list = green[player_col].tolist() if not green.empty else []
    yellow_list = yellow[player_col].tolist() if not yellow.empty else []
    red_list = red[player_col].tolist() if not red.empty else []
    
    output = f"🟢 {', '.join(green_list) if green_list else 'No Green Plays'}\n"
    output += f"🟡 {', '.join(yellow_list) if yellow_list else 'No Yellow Plays'}\n"
    output += f"🔴 {', '.join(red_list) if red_list else 'No Red Plays'}"
    return output

# ---------------------------
# PSP Analyzer Functions
# ---------------------------
def analyze_nhl_psp(file_path, stat_key):
    """
    Reads the NHL PSP CSV file and returns formatted groups.
    This is a simplified version; adjust filtering (e.g. injuries) as needed.
    """
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return f"Error reading PSP CSV: {e}"
    df.columns = [col.upper() for col in df.columns]
    
    try:
        df[stat_key] = pd.to_numeric(df[stat_key].replace({',': ''}, regex=True), errors='coerce')
    except Exception as e:
        return f"Error converting stat column: {e}"
    df = df.dropna(subset=[stat_key])
    sorted_df = df.sort_values(by=stat_key, ascending=False).reset_index(drop=True)
    yellow = sorted_df.iloc[0:3]
    green = sorted_df.iloc[6:9]
    red = sorted_df.iloc[12:15]
    player_col = "NAME" if "NAME" in sorted_df.columns else None
    if player_col is None:
        return "Player column not found in PSP CSV."
    green_list = [x for x in green[player_col].tolist() if not is_banned(str(x), stat_key)]
    yellow_list = [x for x in yellow[player_col].tolist() if not is_banned(str(x), stat_key)]
    red_list = [x for x in red[player_col].tolist() if not is_banned(str(x), stat_key)]
    output = f"🟢 {', '.join(green_list)}\n"
    output += f"🟡 {', '.join(yellow_list)}\n"
    output += f"🔴 {', '.join(red_list)}"
    return output

def analyze_nba_psp(file_path, stat_key):
    """
    Reads the NBA PSP CSV file and returns formatted groups.
    This is a simplified version.
    """
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return f"Error reading PSP CSV: {e}"
    df.columns = [col.upper() for col in df.columns]
    try:
        df[stat_key] = pd.to_numeric(df[stat_key].replace({',': ''}, regex=True), errors='coerce')
    except Exception as e:
        return f"Error converting stat column: {e}"
    df = df.dropna(subset=[stat_key])
    sorted_df = df.sort_values(by=stat_key, ascending=False).reset_index(drop=True)
    yellow = sorted_df.iloc[0:3]
    green = sorted_df.iloc[3:6]
    red = sorted_df.iloc[6:9]
    player_col = "NAME" if "NAME" in sorted_df.columns else None
    if player_col is None:
        return "Player column not found in PSP CSV."
    green_list = [x for x in green[player_col].tolist() if not is_banned(str(x), stat_key)]
    yellow_list = [x for x in yellow[player_col].tolist() if not is_banned(str(x), stat_key)]
    red_list = [x for x in red[player_col].tolist() if not is_banned(str(x), stat_key)]
    output = f"🟢 {', '.join(green_list)}\n"
    output += f"🟡 {', '.join(yellow_list)}\n"
    output += f"🔴 {', '.join(red_list)}"
    return output

# ---------------------------
# Interactive Analysis Functions
# ---------------------------
def analyze_sport_interactive(df, stat_categories, player_col, team_col):
    """
    Interactive prompt to select teams, stat choice, and (if needed) target value.
    Then calls analyze_poll and prints the result.
    """
    while True:
        teams_input = input("\nEnter team names separated by commas (or type 'exit' to quit): ")
        if teams_input.lower() == "exit":
            break
        team_list = [t.strip().upper() for t in teams_input.split(",") if t.strip()]
        filtered_df = df[df[team_col].astype(str).str.upper().isin(team_list)]
        if filtered_df.empty:
            print("❌ No matching teams found. Please check the team names.")
            continue
        
        stat_choice = input("\nEnter stat to sort by: ").strip().upper()
        if stat_choice not in stat_categories:
            print("❌ Invalid stat choice. Please try again.")
            continue
        mapped_stat = stat_categories[stat_choice]
        
        # For MLB, we do not require a target value.
        if team_list and ("mlb" in df.columns.str.lower().tolist() or player_col == "PLAYER" and stat_categories == STAT_CATEGORIES_MLB):
            use_target = False
            target_value = None
            mode = "slicing"
        else:
            use_target = True
            target_input = input(f"\nEnter target {stat_choice} value (per game): ").strip()
            try:
                target_value = float(target_input)
            except Exception:
                print("❌ Invalid target value.")
                continue
            mode = "threshold"
        
        result = analyze_poll(filtered_df.copy(), player_col, mapped_stat, target_value, mode=mode, use_target=use_target)
        print(f"\nPlayer Performance Based on Target {target_input if use_target else 'N/A'} {stat_choice}:")
        print(result)

def analyze_sport_noninteractive(df, stat_categories, player_col, team_col, teams, stat_choice, target_value=None, use_target=True, mode="threshold"):
    """
    Non-interactive version that expects parameters and returns the formatted output.
    """
    if isinstance(teams, str):
        team_list = [t.strip().upper() for t in teams.split(",") if t.strip()]
    else:
        team_list = teams
    filtered_df = df[df[team_col].astype(str).str.upper().isin(team_list)]
    if filtered_df.empty:
        return "❌ No matching teams found."
    mapped_stat = stat_categories.get(stat_choice)
    if mapped_stat is None:
        return "❌ Invalid stat choice."
    result = analyze_poll(filtered_df.copy(), player_col, mapped_stat, target_value, mode=mode, use_target=use_target)
    return result

# ---------------------------
# Notion Integration Functions
# ---------------------------
# Notion API configuration (adjust these values as needed)
NOTION_TOKEN = "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"
DATABASE_ID = "1aa71b1c-663e-8035-bc89-fb1e84a2d919"   # Main polls database ID
PSP_DATABASE_ID = "1ac71b1c663e808e9110eee23057de0e"      # PSP database ID
POLL_PAGE_ID = "18e71b1c663e80cdb8a0fe5e8aeee5a9"         # Page where results are appended

client = Client(auth=NOTION_TOKEN)

def fetch_unprocessed_rows(database_id):
    """
    Queries the Notion database for rows where Processed equals "no".
    Returns a list of dictionaries containing row information.
    """
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
        props = result.get("properties", {})
        
        # Extract teams from "Teams" property if present; else use "Team 1" and "Team 2"
        if "Teams" in props:
            team_prop = props["Teams"]
            team_parts = team_prop.get("title", []) if team_prop.get("type") == "title" else team_prop.get("rich_text", [])
            teams_raw = "".join(part.get("plain_text", "") for part in team_parts)
            teams_list = [t.strip().upper() for t in teams_raw.split(",") if t.strip()]
            team1 = teams_list[0] if teams_list else ""
            team2 = teams_list[1] if len(teams_list) > 1 else ""
        else:
            team1_data = props.get("Team 1", {})
            team1_parts = team1_data.get("title", []) if team1_data.get("type") == "title" else team1_data.get("rich_text", [])
            team1 = "".join(part.get("plain_text", "") for part in team1_parts).strip().upper()
            team2_parts = props.get("Team 2", {}).get("rich_text", [])
            team2 = "".join(part.get("plain_text", "") for part in team2_parts).strip().upper()
            teams_list = [team1, team2] if team2 else [team1]
        
        # Sport, Stat, and Target
        sport = props.get("Sport", {}).get("select", {}).get("name", "")
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
        
        # Check if this row is meant for PSP processing (assumes a "PSP" property exists)
        psp_flag = props.get("PSP", {}).get("checkbox", False)
        
        # Optional: Order value for sorting
        order_val = None
        if "Order" in props and props["Order"].get("type") == "unique_id":
            order_val = props["Order"].get("unique_id", {}).get("number")
        
        rows.append({
            "page_id": page_id,
            "teams": ", ".join(teams_list),
            "sport": sport,
            "stat": stat,
            "target": target_value,
            "team1": team1,
            "team2": team2,
            "psp": psp_flag,
            "Order": order_val
        })
    rows.sort(key=lambda x: float(x.get("Order")) if x.get("Order") is not None else float('inf'))
    return rows

def mark_row_as_processed(page_id):
    """Marks the given Notion page (row) as processed by setting its Processed property to 'Yes'."""
    try:
        client.pages.update(
            page_id=page_id,
            properties={"Processed": {"select": {"name": "Yes"}}}
        )
    except Exception as e:
        print(f"Error marking row {page_id} as processed: {e}")

async def append_poll_entries_to_page(entries):
    """
    Appends blocks (each representing a poll entry) to the Notion page.
    """
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
    
    max_blocks = 100  # Notion limits per request
    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]
    responses = []
    for block_chunk in chunk_list(blocks, max_blocks):
        try:
            response = await asyncio.to_thread(
                client.blocks.children.append,
                block_id=POLL_PAGE_ID,
                children=block_chunk
            )
            responses.append(response)
        except Exception as e:
            print(f"Error updating poll page with a block chunk: {e}")
            return None
    return responses

def run_unified_analysis(row):
    """
    Uses parameters from a Notion row to load the appropriate CSV,
    filter by teams, and call the unified analyzer.
    
    If the row has a PSP flag, it calls the PSP analyzer (for NBA or NHL).
    Otherwise, it uses the standard analysis.
    """
    teams = row.get("teams", "")
    sport = row.get("sport", "").upper()
    stat_choice = row.get("stat", "").upper()
    
    # If PSP flag is set, use PSP analyzers.
    if row.get("psp", False):
        if sport == "NHL":
            stat_key = stat_choice  # Assuming the PSP CSV uses the same stat key.
            file_path = os.path.join("PSP", f"nhl_{stat_choice.lower()}_psp_data.csv")
            return analyze_nhl_psp(file_path, stat_key)
        elif sport == "NBA":
            stat_key = stat_choice
            if stat_key == "FG3M":
                stat_key = "3PM"
            file_path = os.path.join("PSP", f"nba_{stat_choice.lower()}_psp_data.csv")
            return analyze_nba_psp(file_path, stat_key)
        else:
            return "PSP processing not configured for this sport."
    
    # For non-PSP rows, determine CSV, column names, and analysis parameters.
    try:
        if sport == "NBA":
            df = pd.read_csv("nba_player_stats.csv")
            player_col = "PLAYER"
            # Use "TEAM" if available, else try "Team"
            if "TEAM" in df.columns:
                team_col = "TEAM"
            elif "Team" in df.columns:
                team_col = "Team"
            else:
                return "❌ 'TEAM' column not found in NBA CSV."
            mapped_stat = STAT_CATEGORIES_NBA.get(stat_choice, "PTS")
            use_target = True
        elif sport == "CBB":
            df = pd.read_csv("cbb_players_stats.csv")
            player_col = "Player"
            if "Team" in df.columns:
                team_col = "Team"
            elif "TEAM" in df.columns:
                team_col = "TEAM"
            else:
                return "❌ 'Team' column not found in CBB CSV."
            mapped_stat = STAT_CATEGORIES_CBB.get(stat_choice, "PPG")
            use_target = True
        elif sport == "NHL":
            df = pd.read_csv("nhl_player_stats.csv")
            player_col = "Player"
            if "Team" in df.columns:
                team_col = "Team"
            elif "TEAM" in df.columns:
                team_col = "TEAM"
            else:
                return "❌ 'Team' column not found in NHL CSV."
            mapped_stat = STAT_CATEGORIES_NHL.get(stat_choice, "G")
            use_target = True
        elif sport == "MLB":
            df = pd.read_csv("mlb_stats.csv")
            player_col = "PLAYER"
            if "TEAM" in df.columns:
                team_col = "TEAM"
            elif "Team" in df.columns:
                team_col = "Team"
            else:
                return "❌ 'TEAM' column not found in MLB CSV."
            mapped_stat = STAT_CATEGORIES_MLB.get(stat_choice, "RBI")
            # For MLB, we ignore target values and use slicing mode.
            use_target = False
        else:
            return "❌ Sport not recognized."
    except Exception as e:
        return f"❌ Error loading CSV for {sport}: {e}"
    
    # Parse teams into a list.
    if isinstance(teams, str):
        team_list = [t.strip().upper() for t in teams.split(",") if t.strip()]
    else:
        team_list = teams
    filtered_df = df[df[team_col].astype(str).str.upper().isin(team_list)]
    if filtered_df.empty:
        return "❌ No matching teams found."
    
    # For sports that use target (NBA, CBB, NHL), parse the target value.
    if use_target:
        try:
            target_value = float(row.get("target", 0))
        except Exception:
            return "❌ Invalid target value."
        mode = "threshold"
    else:
        target_value = None
        mode = "slicing"
    
    result = analyze_poll(filtered_df.copy(), player_col, mapped_stat, target_value, mode=mode, use_target=use_target)
    return result

async def process_rows():
    """
    Main async function for Notion integration.
    Fetches unprocessed rows, processes each row with run_unified_analysis,
    appends the results to the designated Notion page, and marks rows as processed.
    """
    rows = fetch_unprocessed_rows(DATABASE_ID)
    poll_entries = []
    for row in rows:
        result = run_unified_analysis(row)
        title = f"Game: {row.get('team1','')} vs {row.get('team2','')} ({row.get('sport','')}, {row.get('stat','')}, Target: {row.get('target','')})"
        poll_entries.append({
            "title": title,
            "output": result
        })
        mark_row_as_processed(row["page_id"])
    await append_poll_entries_to_page(poll_entries)

# ---------------------------
# Main Program: Mode Selection
# ---------------------------
def main():
    mode = input("Select mode: (1) Interactive, (2) Notion Integration: ").strip()
    if mode == "1":
        print("Interactive Mode Selected")
        sport_choice = input("Enter sport (NBA, CBB, NHL, MLB): ").strip().upper()
        try:
            if sport_choice == "NBA":
                df = pd.read_csv("nba_player_stats.csv")
                analyze_sport_interactive(df, STAT_CATEGORIES_NBA, "PLAYER", "TEAM")
            elif sport_choice == "CBB":
                df = pd.read_csv("cbb_players_stats.csv")
                analyze_sport_interactive(df, STAT_CATEGORIES_CBB, "Player", "Team")
            elif sport_choice == "NHL":
                df = pd.read_csv("nhl_player_stats.csv")
                analyze_sport_interactive(df, STAT_CATEGORIES_NHL, "Player", "Team")
            elif sport_choice == "MLB":
                df = pd.read_csv("mlb_stats.csv")
                analyze_sport_interactive(df, STAT_CATEGORIES_MLB, "PLAYER", "TEAM")
            else:
                print("Invalid sport selection.")
        except Exception as e:
            print(f"Error loading stats file: {e}")
    elif mode == "2":
        print("Notion Integration Mode Selected")
        asyncio.run(process_rows())
    else:
        print("Invalid mode selected.")

if __name__ == "__main__":
    main()