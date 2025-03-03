import sys
import pandas as pd
from notion_client import Client
import time
import test as USA

# ---------------
# CONFIGURATION
# ---------------
NOTION_TOKEN = "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"
DATABASE_ID = "1aa71b1c663e8035bc89fb1e84a2d919"  
POLL_PAGE_ID = "18e71b1c663e80cdb8a0fe5e8aeee5a9"

client = Client(auth=NOTION_TOKEN)

# ---------------
# RUN UNIVERSAL SPORTS ANALYZER (Programmatic Mode)
# ---------------
def run_universal_sports_analyzer_programmatic(team1, team2, sport, stat, target):
    sport_upper = sport.upper()
    teams = [team1.strip().upper(), team2.strip().upper()]
    
    if sport_upper == "NBA":
        df = USA.integrate_nba_data('nba_player_stats.csv', 'nba_injury_report.csv')
        used_stat = stat.upper() if stat.strip() else "PPG"
        used_target = float(target) if target.strip() else 0
        result = USA.analyze_sport_noninteractive(
            df, USA.STAT_CATEGORIES_NBA, "PLAYER", "TEAM", teams, used_stat, used_target
        )
    elif sport_upper == "CBB":
        df = USA.load_player_stats()
        used_stat = stat.upper() if stat.strip() else "PPG"
        used_target = float(target) if target.strip() else 0
        result = USA.analyze_sport_noninteractive(
            df, USA.STAT_CATEGORIES_CBB, "Player", "Team", teams, used_stat, used_target
        )
    elif sport_upper == "MLB":
        df = USA.integrate_mlb_data()
        # For MLB, we ignore target and simply return the top 9 players.
        result = USA.analyze_mlb_by_team(df)
    elif sport_upper == "NHL":
        df = USA.integrate_nhl_data("nhl_player_stats.csv", "nhl_injuries.csv")
        if stat.strip():
            nhl_stat = stat.upper()
        else:
            nhl_stat = "GOALS"
        if nhl_stat == "S":
            used_target = float(target) if target.strip() else None
            result = USA.analyze_nhl_noninteractive(df, teams, nhl_stat, used_target)
        else:
            result = USA.analyze_nhl_noninteractive(df, teams, nhl_stat, None)
    else:
        result = "Sport not recognized."
    return result

# ---------------
# FETCH UNPROCESSED ROWS FROM NOTION DATABASE
# ---------------
def fetch_unprocessed_rows():
    """
    Query the database for rows where the Processed property equals "no".
    Expects properties: Team 1, Team 2, Sport, Stat, Target.
    """
    try:
        response = client.databases.query(
            database_id=DATABASE_ID,
            filter={
                "property": "Processed",
                "select": {"equals": "no"}
            }
        )
    except Exception as e:
        print("Error querying database. Ensure your inline database has a select property named 'Processed' with an option 'no'.", e)
        return []
    
    rows = []
    for result in response.get("results", []):
        page_id = result["id"]
        created_time = result.get("created_time", "")
        props = result.get("properties", {})
        
        # Extract "Team 1"
        team1_data = props.get("Team 1", {})
        if team1_data.get("type") == "title":
            team1_parts = team1_data.get("title", [])
        else:
            team1_parts = team1_data.get("rich_text", [])
        team1 = "".join(part.get("plain_text", "") for part in team1_parts)
        
        # Extract "Team 2"
        team2_parts = props.get("Team 2", {}).get("rich_text", [])
        team2 = "".join(part.get("plain_text", "") for part in team2_parts)
        
        # Extract "Sport"
        sport_select = props.get("Sport", {}).get("select", {})
        sport = sport_select.get("name", "") if sport_select else ""
        
        # Extract "Stat"
        stat_value = ""
        stat_prop = props.get("Stat", {})
        if "select" in stat_prop:
            stat_select = stat_prop.get("select")
            stat_value = stat_select.get("name", "") if stat_select else ""
        elif "rich_text" in stat_prop:
            stat_rich = stat_prop.get("rich_text", [])
            stat_value = "".join(part.get("plain_text", "") for part in stat_rich)
        
        # Extract "Target"
        target_parts = props.get("Target", {}).get("rich_text", [])
        target_value = "".join(part.get("plain_text", "") for part in target_parts)
        
        rows.append({
            "page_id": page_id,
            "team1": team1,
            "team2": team2,
            "sport": sport,
            "stat": stat_value,
            "target": target_value,
            "created_time": created_time
        })
    
    rows.sort(key=lambda x: x["created_time"])
    return rows

# ---------------
# APPEND POLL ENTRIES TO THE POLL PAGE AS SEPARATE BLOCKS
# ---------------
def append_poll_entries_to_page(entries):
    """
    Append each poll entry as three separate blocks:
      - Title block (game title)
      - Output block (analysis/picks)
      - Divider block
    If total blocks exceed 100, split into chunks.
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
        blocks.append({
            "object": "block",
            "type": "divider",
            "divider": {}
        })
    
    max_blocks = 100
    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]
    
    responses = []
    for block_chunk in chunk_list(blocks, max_blocks):
        try:
            response = client.blocks.children.append(
                block_id=POLL_PAGE_ID,
                children=block_chunk
            )
            responses.append(response)
        except Exception as e:
            print(f"Error updating poll page with a block chunk: {e}")
            return None
    return responses

# ---------------
# UPDATE ROW TO MARK AS PROCESSED (OPTIONAL)
# ---------------
def mark_row_as_processed(page_id):
    try:
        client.pages.update(
            page_id=page_id,
            properties={
                "Processed": {"select": {"name": "Yes"}}
            }
        )
    except Exception as e:
        print(f"Error marking row {page_id} as processed: {e}")

# ---------------
# MAIN WORKFLOW
# ---------------
def main():
    rows = fetch_unprocessed_rows()
    if not rows:
        print("No unprocessed rows found.")
        return
    
    poll_entries = []
    for row in rows:
        page_id = row["page_id"]
        team1 = row["team1"]
        team2 = row["team2"]
        sport = row["sport"]
        stat = row["stat"]
        target = row["target"]
        
        picks_output = run_universal_sports_analyzer_programmatic(team1, team2, sport, stat, target)
        title = f"Game: {team1} vs {team2} ({sport}, {stat}, Target: {target})"
        poll_entries.append({
            "title": title,
            "output": picks_output
        })
        
        mark_row_as_processed(page_id)
        print(f"Processed row for {team1} vs {team2} ({sport}, {stat})")
    
    response = append_poll_entries_to_page(poll_entries)
    print("Poll Entries:")
    for entry in poll_entries:
        print(entry["title"])
        print(entry["output"])
        print("-----")
    if response:
        print("Poll page updated successfully.")
    else:
        print("Failed to update poll page.")

if __name__ == "__main__":
    main()