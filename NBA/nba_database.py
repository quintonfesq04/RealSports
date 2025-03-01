import requests
import pandas as pd
from bs4 import BeautifulSoup
import re  # Import the re module

# URLs for the NBA Stats API and Injury Report
stats_url = "https://stats.nba.com/stats/leagueLeaders"
injury_url = "https://www.cbssports.com/nba/injuries/"
params = {
    "LeagueID": "00",
    "PerMode": "PerGame",
    "Scope": "S",
    "Season": "2024-25",
    "SeasonType": "Regular Season",
    "StatCategory": "PTS"
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Referer": "https://stats.nba.com/",
    "Origin": "https://stats.nba.com"
}

def fetch_nba_player_stats():
    # Fetch data from the API
    response = requests.get(stats_url, params=params, headers=headers)
    data = response.json()

    # Extract the relevant player stats
    player_stats = data["resultSet"]["rowSet"]
    columns = data["resultSet"]["headers"]

    # Save data to CSV
    output_file = "nba_player_stats.csv"
    df = pd.DataFrame(player_stats, columns=columns)
    df.to_csv(output_file, index=False)
    print(f"💾 Saved player stats to {output_file}")
    return df

def extract_player_data(url):
    """
    Extracts player data from the CBS Sports injury report.

    Args:
        url: The URL of the injury report webpage.

    Returns:
        A DataFrame containing injury data.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        soup = BeautifulSoup(response.content, 'html.parser')
        table_shadows_divs = soup.find_all('div', class_='TableBase-shadows')

        if not table_shadows_divs:
            print("TableBase-shadows divs not found.")
            return pd.DataFrame()

        data = []

        for table_shadows_div in table_shadows_divs:
            # Get the team name from the previous sibling h4 element
            team_name_element = table_shadows_div.find_previous_sibling('h4', class_='TableBase-title')
            team_name = team_name_element.get_text(strip=True) if team_name_element else 'Unknown'

            # Extract injury data from the table
            rows = table_shadows_div.find_all('tr')
            for row in rows[1:]:  # Skip the header row
                cols = row.find_all('td')
                if len(cols) >= 5:
                    player_name = cols[0].find('span', class_='CellPlayerName--long').get_text(strip=True) if cols[0].find('span', class_='CellPlayerName--long') else cols[0].get_text(strip=True)
                    print(f"Original player_name: {player_name}")  # Debugging print statement

                    position = cols[1].get_text(strip=True)
                    updated = cols[2].get_text(strip=True)
                    injury = cols[3].get_text(strip=True)
                    injury_status = cols[4].get_text(strip=True)
                    data.append({
                        'teamName': team_name,
                        'playerName': player_name,
                        'position': position,
                        'updated': updated,
                        'injury': injury,
                        'injuryStatus': injury_status
                    })
                    print(f"Extracted data: {team_name}, {player_name}, {position}, {updated}, {injury}, {injury_status}")  # Debugging print statement

        return pd.DataFrame(data)

    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()

def merge_stats_with_injuries(stats_df, injuries_df):
    """
    Merges the player stats DataFrame with the injury DataFrame.

    Args:
        stats_df: DataFrame containing player stats.
        injuries_df: DataFrame containing injury data.

    Returns:
        A DataFrame with injured players removed.
    """
    # Ensure player names are in a consistent format
    stats_df['PLAYER'] = stats_df['PLAYER'].str.strip()
    injuries_df['playerName'] = injuries_df['playerName'].str.strip()
    # Merge DataFrames on player name and exclude injured players
    merged_df = pd.merge(stats_df, injuries_df, left_on='PLAYER', right_on='playerName', how='left')
    healthy_players_df = merged_df[merged_df['injury'].isnull()]

    return healthy_players_df

if __name__ == "__main__":
    # Fetch NBA player stats
    stats_df = fetch_nba_player_stats()

    # Extract NBA injury data
    injuries_df = extract_player_data(injury_url)

    if not injuries_df.empty:
        print("💾 Saved injury report data")
        injuries_df.to_csv('nba_injury_report.csv', index=False)
    else:
        print("❌ No injury data found")

    # Merge stats with injury data and remove injured players
    healthy_players_df = merge_stats_with_injuries(stats_df, injuries_df)
    healthy_output_file = "nba_healthy_player_stats.csv"
    healthy_players_df.to_csv(healthy_output_file, index=False)
    print(f"💾 Saved healthy player stats to {healthy_output_file}")
