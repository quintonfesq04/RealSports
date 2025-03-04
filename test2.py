import sys
import pandas as pd
from notion_client import Client
import time
from datetime import datetime
import test as USA  # Import our analyzer module

# ---------------
# CONFIGURATION
# ---------------
NOTION_TOKEN = "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"
DATABASE_ID = "1aa71b1c663e8035bc89fb1e84a2d919"  
POLL_PAGE_ID = "18e71b1c663e80cdb8a0fe5e8aeee5a9"

client = Client(auth=NOTION_TOKEN)

def fetch_unprocessed_rows():
    try:
        response = client.databases.query(
            database_id=DATABASE_ID,
            filter={
                "property": "Processed",
                "select": {"equals": "no"}
            },
            sort=[{
                "property": "Order",    # Sorting by your custom "Order" property
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
        
        team1_data = props.get("Team 1", {})
        if team1_data.get("type") == "title":
            team1_parts = team1_data.get("title", [])
        else:
            team1_parts = team1_data.get("rich_text", [])
        team1 = "".join(part.get("plain_text", "") for part in team1_parts)
        
        team2_parts = props.get("Team 2", {}).get("rich_text", [])
        team2 = "".join(part.get("plain_text", "") for part in team2_parts)
        
        sport_select = props.get("Sport", {}).get("select", {})
        sport = sport_select.get("name", "") if sport_select else ""
        
        stat_value = ""
        stat_prop = props.get("Stat", {})
        if stat_prop.get("type") == "select":
            stat_select = stat_prop.get("select")
            stat_value = stat_select.get("name", "") if stat_select else ""
        elif stat_prop.get("type") == "rich_text":
            stat_rich = stat_prop.get("rich_text", [])
            stat_value = "".join(part.get("plain_text", "") for part in stat_rich)
        
        target_prop = props.get("Target", {})
        if target_prop.get("type") == "number":
            target_value = str(target_prop.get("number", ""))
        elif target_prop.get("type") == "rich_text":
            target_rich = target_prop.get("rich_text", [])
            target_value = "".join(part.get("plain_text", "") for part in target_rich)
        else:
            target_value = ""
        
        rows.append({
            "page_id": page_id,
            "team1": team1,
            "team2": team2,
            "sport": sport,
            "stat": stat_value,
            "target": target_value,
            "created_time": created_time
        })
    
    # The query sorts by Order, but if the returned order is the reverse of what you expect,
    # you can reverse the list:
    rows = list(reversed(rows))
    
    # Optionally, if you want to verify by created_time (comment out if not needed):
    # try:
    #     rows.sort(key=lambda x: datetime.fromisoformat(x["created_time"].replace("Z", "+00:00")))
    # except Exception as e:
    #     print("Error sorting rows by created_time:", e)
    return rows

def append_poll_entries_to_page(entries):
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

def run_universal_sports_analyzer_programmatic(team1, team2, sport, stat, target):
    sport_upper = sport.upper()
    teams = [team1.strip().upper(), team2.strip().upper()]
    
    def parse_target(target):
        t = target.strip().lower()
        if t in ["", "none"]:
            return None
        try:
            return float(t)
        except Exception:
            return None

    if sport_upper == "NBA":
        df = USA.integrate_nba_data('nba_player_stats.csv', 'nba_injury_report.csv')
        used_stat = stat.upper() if stat.strip() else "PPG"
        used_target = parse_target(target)
        result = USA.analyze_sport_noninteractive(
            df, USA.STAT_CATEGORIES_NBA, "PLAYER", "TEAM", teams, used_stat, used_target
        )
    elif sport_upper == "CBB":
        df = USA.load_player_stats()
        used_stat = stat.upper() if stat.strip() else "PPG"
        used_target = parse_target(target)
        result = USA.analyze_sport_noninteractive(
            df, USA.STAT_CATEGORIES_CBB, "Player", "Team", teams, used_stat, used_target
        )
    elif sport_upper == "MLB":
        df = USA.integrate_mlb_data()
        used_stat = stat.upper() if stat.strip() else "RBI"
        if parse_target(target) is not None:
            result = USA.analyze_sport_noninteractive(
                df, USA.STAT_CATEGORIES_MLB, "PLAYER", "TEAM", teams, used_stat, parse_target(target)
            )
        else:
            result = USA.analyze_mlb_noninteractive(df, teams, used_stat)
    elif sport_upper == "NHL":
        df = USA.integrate_nhl_data("nhl_player_stats.csv", "nhl_injuries.csv")
        if stat.strip():
            nhl_stat = stat.upper()
        else:
            nhl_stat = "GOALS"
        if nhl_stat == "S":
            used_target = parse_target(target)
            result = USA.analyze_nhl_noninteractive(df, teams, nhl_stat, used_target)
        else:
            result = USA.analyze_nhl_noninteractive(df, teams, nhl_stat, None)
    else:
        result = "Sport not recognized."
    return result

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