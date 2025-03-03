import sys
import pandas as pd
from notion_client import Client
import pexpect
import time

# ---------------
# CONFIGURATION
# ---------------
NOTION_TOKEN = "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"
DATABASE_ID = "1aa71b1c663e8035bc89fb1e84a2d919"  # Your inline database ID
POLL_PAGE_ID = "18e71b1c663e80cdb8a0fe5e8aeee5a9"  # Replace with your actual poll page ID

client = Client(auth=NOTION_TOKEN)

# Mapping from Sport name to expected sport number.
sports_mapping = {
    "CBB": "1",
    "NBA": "2",
    "NHL": "3",
    "MLB": "4"
}

# ---------------
# SPLIT TEXT HELPER
# ---------------
def split_text(text, max_length=2000):
    """
    Split a long text into a list of strings, each of maximum max_length characters.
    Splits at newline boundaries if possible.
    """
    if len(text) <= max_length:
        return [text]
    lines = text.splitlines()
    chunks = []
    current_chunk = ""
    for line in lines:
        if len(current_chunk) + len(line) + 1 > max_length:
            chunks.append(current_chunk)
            current_chunk = line
        else:
            if current_chunk:
                current_chunk += "\n" + line
            else:
                current_chunk = line
    if current_chunk:
        chunks.append(current_chunk)
    return chunks

# ---------------
# RUN UNIVERSAL SPORTS ANALYZER VIA PEXPECT (SIMULATED INTERACTIVE INPUT)
# ---------------
def run_universal_sports_analyzer(team1, team2, sport, stat, target):
    """
    Uses pexpect to simulate interactive input for Universal_Sports_Analyzer.py.
    
    For MLB, the expected interactive flow is:
      1. When prompted "Choose an option ..." send the sport number (e.g. "4").
      2. When prompted "Enter team names ..." send the team names (e.g. "NYY, ATL").
      3. Wait for the next "team names" prompt—which indicates the output (the picks) has been printed—and capture that output.
      4. Then send "exit" to quit that loop.
      5. Optionally, send "5" to exit the main menu.
      
    A 5‑second delay is inserted before each send.
    """
    sports_num = sports_mapping.get(sport.upper(), "5")
    teams = f"{team1}, {team2}"
    
    try:
        child = pexpect.spawn("python Universal_Sports_Analyzer.py", encoding="utf-8", timeout=30)
        # Log output for debugging.
        child.logfile = sys.stdout
        
        # 1. Wait for the "Choose an option" prompt and send the sport number.
        child.expect(r"Choose an option", timeout=30)
        time.sleep(5)
        child.sendline(sports_num)
        
        # 2. Wait for the prompt that mentions "team names" and send the team names.
        child.expect(r"team names", timeout=30)
        time.sleep(5)
        child.sendline(teams)
        
        # 3. Wait for the next occurrence of the "team names" prompt.
        child.expect(r"team names", timeout=30)
        # Capture the output produced before this prompt (this should contain the pick lines).
        result_output = child.before
        time.sleep(5)
        child.sendline("exit")
        
        # 4. Optionally, wait for a "Choose an option" prompt and send "5" to exit.
        child.expect(r"Choose an option", timeout=30)
        time.sleep(5)
        child.sendline("5")
        
        # 5. Wait for EOF.
        child.expect(pexpect.EOF, timeout=30)
        
    except pexpect.exceptions.TIMEOUT as te:
        print("Universal_Sports_Analyzer.py timed out:", te)
        return "", "", ""
    except pexpect.exceptions.EOF as eof:
        print("Unexpected EOF:", eof)
        return "", "", ""
    
    # Parse the captured result_output for lines starting with the pick emojis.
    green, yellow, red = "", "", ""
    for line in result_output.splitlines():
        stripped = line.strip()
        if stripped.startswith("🟢"):
            green = stripped[len("🟢"):].strip()
        elif stripped.startswith("🟡"):
            yellow = stripped[len("🟡"):].strip()
        elif stripped.startswith("🔴"):
            red = stripped[len("🔴"):].strip()
    return green, yellow, red

# ---------------
# FETCH UNPROCESSED ROWS FROM NOTION DATABASE
# ---------------
def fetch_unprocessed_rows():
    """
    Query the inline database for rows where the Processed property (select) equals "no".
    Assumes your database has the following properties:
      - Team 1 (rich_text or title)
      - Team 2 (rich_text)
      - Sport (select)
      - Stat (rich_text or select)
      - Target (rich_text)
      - Processed (select) with options "yes" and "no"
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
            "target": target_value
        })
    
    return rows

# ---------------
# APPEND POLL TEXT TO A DESIGNATED NOTION PAGE (CHUNKED)
# ---------------
def append_poll_text_to_page(poll_text):
    """
    Append the poll text to the designated poll page.
    If poll_text is longer than 2000 characters, split it into chunks.
    """
    def split_text(text, max_length=2000):
        if len(text) <= max_length:
            return [text]
        lines = text.splitlines()
        chunks = []
        current_chunk = ""
        for line in lines:
            if len(current_chunk) + len(line) + 1 > max_length:
                chunks.append(current_chunk)
                current_chunk = line
            else:
                if current_chunk:
                    current_chunk += "\n" + line
                else:
                    current_chunk = line
        if current_chunk:
            chunks.append(current_chunk)
        return chunks

    chunks = split_text(poll_text, 2000)
    responses = []
    for chunk in chunks:
        try:
            response = client.blocks.children.append(
                block_id=POLL_PAGE_ID,
                children=[
                    {
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{"type": "text", "text": {"content": chunk}}]
                        }
                    }
                ]
            )
            responses.append(response)
        except Exception as e:
            print(f"Error updating poll page for chunk: {e}")
    return responses

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
    
    poll_lines = []
    for row in rows:
        page_id = row["page_id"]
        team1 = row["team1"]
        team2 = row["team2"]
        sport = row["sport"]
        stat = row["stat"]
        target = row["target"]
        
        green_picks, yellow_picks, red_picks = run_universal_sports_analyzer(team1, team2, sport, stat, target)
        
        poll_entry = (f"{team1} vs {team2} ({sport}, {stat}, Target: {target}):\n"
                      f"  🟢 {green_picks}\n"
                      f"  🟡 {yellow_picks}\n"
                      f"  🔴 {red_picks}\n")
        poll_lines.append(poll_entry)
        
        mark_row_as_processed(page_id)
        print(f"Processed row for {team1} vs {team2} ({sport}, {stat})")
    
    poll_text = "\n".join(poll_lines)
    print("Poll Text to Append:")
    print(poll_text)
    
    responses = append_poll_text_to_page(poll_text)
    if responses:
        print("Poll page updated successfully.")
    else:
        print("Failed to update poll page.")

if __name__ == "__main__":
    main()