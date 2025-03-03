import sys
import pandas as pd
from notion_client import Client
import pexpect
import time

# ---------------
# CONFIGURATION
# ---------------
NOTION_TOKEN = "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"
DATABASE_ID = "1aa71b1c663e8035bc89fb1e84a2d919"  
POLL_PAGE_ID = "18e71b1c663e80cdb8a0fe5e8aeee5a9"

client = Client(auth=NOTION_TOKEN)

# Mapping from Sport name to expected sport number.
sports_mapping = {
    "CBB": "1",
    "NBA": "2",
    "NHL": "3",
    "MLB": "4"
}

# ---------------
# RUN UNIVERSAL SPORTS ANALYZER VIA PEXPECT (SIMULATED INTERACTIVE INPUT)
# ---------------
def run_universal_sports_analyzer(team1, team2, sport, stat, target):
    sports_num = sports_mapping.get(sport.upper(), "5")
    teams = f"{team1}, {team2}"
    
    try:
        child = pexpect.spawn("python Universal_Sports_Analyzer.py", encoding="utf-8", timeout=30)
        child.logfile = sys.stdout  # for debugging
        
        # Common start: select sport and send team names.
        child.expect(r"Choose an option", timeout=30)
        time.sleep(0.05)
        child.sendline(sports_num)
        
        child.expect(r"team names", timeout=30)
        time.sleep(0.05)
        child.sendline(teams)
        
        # Branch by sport.
        if sport.upper() == "MLB":
            # MLB flow: after teams, wait for a second team names prompt then exit.
            child.expect(r"team names", timeout=30)
            time.sleep(0.05)
            child.sendline("exit")
            child.expect(r"Choose an option", timeout=30)
            time.sleep(0.05)
            child.sendline("5")
            
        elif sport.upper() == "NHL":
            # NHL flow: after teams, the analyzer asks for stat.
            child.expect(r"stat", timeout=30)
            time.sleep(0.05)
            child.sendline(stat)
            # NHL does not use a target value.
            child.expect(r"team names", timeout=30)
            time.sleep(0.05)
            child.sendline("exit")
            child.expect(r"Choose an option", timeout=30)
            time.sleep(0.05)
            child.sendline("5")
                
        elif sport.upper() == "NBA":
            # NBA flow: after sending team names, the analyzer asks for a stat.
            child.expect(r"stat", timeout=30)
            time.sleep(0.05)
            child.sendline(stat)
            
            # For NBA, use expect_exact if the prompt is predictable:
            if target.strip():
                prompt = f"Enter target {stat} value (per game):"
                try:
                    child.expect_exact(prompt, timeout=30)
                except pexpect.TIMEOUT:
                    print(f"Timeout waiting for NBA target prompt: {prompt}")
                    # Optionally, you can decide to send a default value:
                    child.sendline("0")
                else:
                    time.sleep(0.05)
                    child.sendline(target)
            else:
                # If no target was provided, send a default (e.g. 0)
                child.expect_exact(f"Enter target {stat} value (per game):", timeout=30)
                time.sleep(0.05)
                child.sendline("0")
            
            # Wait for the analysis output by matching one of the pick emojis.
            child.expect([r"🟢", r"🟡", r"🔴"], timeout=30)
            time.sleep(0.05)
            
            # Once the analysis is printed, exit the input loop.
            child.expect(r"team names", timeout=30)
            time.sleep(0.05)
            child.sendline("exit")
            
            # Exit the main menu.
            child.expect(r"Choose an option", timeout=30)
            time.sleep(0.05)
            child.sendline("5")
            
        elif sport.upper() == "CBB":
            # College Basketball flow: similar logic using expect_exact.
            child.expect(r"stat", timeout=30)
            time.sleep(0.05)
            child.sendline(stat)
            
            # Build the exact prompt string.
            prompt = f"Enter target {stat} value (per game):"
            try:
                child.expect_exact(prompt, timeout=30)
            except pexpect.TIMEOUT:
                print(f"Timeout waiting for CBB target prompt: {prompt}")
                # Optionally send a default value.
                child.sendline("0")
            else:
                time.sleep(0.05)
                child.sendline(target if target.strip() else "0")
            
            # Wait for the next expected prompt (for example, team names) to know the analyzer is done.
            child.expect(r"team names", timeout=30)
            time.sleep(0.05)
            child.sendline("exit")
            
            # Exit the main menu.
            child.expect(r"Choose an option", timeout=30)
            time.sleep(0.05)
            child.sendline("5")
            
        else:
            # Default fallback flow.
            child.expect(r"team names", timeout=30)
            time.sleep(0.05)
            child.sendline("exit")
            child.expect(r"Choose an option", timeout=30)
            time.sleep(0.05)
            child.sendline("exit")
        
        child.expect(pexpect.EOF, timeout=30)
    except pexpect.exceptions.TIMEOUT as te:
        print("Universal_Sports_Analyzer.py timed out:", te)
        return ""
    except pexpect.exceptions.EOF as eof:
        print("Unexpected EOF:", eof)
        return ""
    
    # Capture only the lines that start with one of the pick emojis.
    result_lines = []
    for line in child.before.splitlines():
        stripped = line.strip()
        if stripped.startswith("🟢") or stripped.startswith("🟡") or stripped.startswith("🔴"):
            result_lines.append(stripped)
    return "\n".join(result_lines)

# ---------------
# FETCH UNPROCESSED ROWS FROM NOTION DATABASE
# ---------------
def fetch_unprocessed_rows():
    """
    Query the inline database for rows where the Processed property (select) equals "no".
    Assumes your database has:
      - Team 1 (rich_text or title)
      - Team 2 (rich_text)
      - Sport (select)
      - Stat (rich_text or select)
      - Target (rich_text)
      - Processed (select) with options "yes" and "no"
      
    Also extracts the created_time so we can sort the rows from top to bottom.
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
        
        # Extract "Sport" (select)
        sport_select = props.get("Sport", {}).get("select", {})
        sport = sport_select.get("name", "") if sport_select else ""
        
        # Extract "Stat" (select or rich_text)
        stat_value = ""
        stat_prop = props.get("Stat", {})
        if "select" in stat_prop:
            stat_select = stat_prop.get("select")
            stat_value = stat_select.get("name", "") if stat_select else ""
        elif "rich_text" in stat_prop:
            stat_rich = stat_prop.get("rich_text", [])
            stat_value = "".join(part.get("plain_text", "") for part in stat_rich)
        
        # Extract "Target" (rich_text)
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
    
    # Sort rows in ascending order by created_time (top-to-bottom).
    rows.sort(key=lambda x: x["created_time"])
    return rows

# ---------------
# APPEND POLL ENTRIES TO THE POLL PAGE AS SEPARATE BLOCKS
# ---------------
def append_poll_entries_to_page(entries):
    """
    Append each poll entry as two separate blocks:
      - One block for the game title.
      - One block for the overall output (the picks).
    A divider block is added between games.
    """
    blocks = []
    for entry in entries:
        # Title block.
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": entry["title"]}}]
            }
        })
        # Output block.
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": entry["output"]}}]
            }
        })
        # Divider block.
        blocks.append({
            "object": "block",
            "type": "divider",
            "divider": {}
        })
    
    try:
        response = client.blocks.children.append(
            block_id=POLL_PAGE_ID,
            children=blocks
        )
        return response
    except Exception as e:
        print(f"Error updating poll page with entries: {e}")
        return None

# ---------------
# UPDATE ROW TO MARK AS PROCESSED (OPTIONAL)
# ---------------
def mark_row_as_processed(page_id):
    """
    Update the row to mark it as processed by setting the Processed property to "Yes".
    """
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
        
        picks_output = run_universal_sports_analyzer(team1, team2, sport, stat, target)
        title = f"Game: {team1} vs {team2} ({sport}, {stat}, Target: {target})"
        overall_output = picks_output
        
        poll_entries.append({
            "title": title,
            "output": overall_output
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