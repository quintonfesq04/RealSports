import pandas as pd
import os
import re

# Define stat categories for NBA, CBB, NHL, and MLB
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
    "PTS": "P",
    "G": "G",
    "A": "A",
    "S": "shotsPerGame"
}

# For MLB we expect the cleaned CSV to include these desired columns.
DESIRED_MLB_COLS = ["PLAYER", "TEAM", "G", "AB", "R", "H", "RBI", "AVG", "OBP", "OPS"]
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

def clean_header(header):
    """
    Cleans a header string by removing duplicate text and then checking for desired abbreviations.
    For example, if header is "PLAYERPLAYER", returns "PLAYER". If the header contains "RBI"
    (even within extra text), returns "RBI".
    """
    header = header.strip()
    # Check for duplicated halves (e.g., "PLAYERPLAYER")
    if header.isupper() and len(header) % 2 == 0:
        mid = len(header) // 2
        if header[:mid] == header[mid:]:
            header = header[:mid]
    # Check for our desired keys in order of descending length.
    keys = sorted(["PLAYER", "TEAM", "RBI", "AVG", "OBP", "OPS", "AB", "R", "H", "G"], key=len, reverse=True)
    for key in keys:
        if key.lower() in header.lower():
            return key
    return header

def fix_mlb_player_name(name):
    """
    Fixes concatenated MLB player names.
    Removes digits and extra characters then extracts words that start with a capital letter.
    Returns the first and last word (assumed to be the first and last name).
    For example, "278CadeC BunnellBunnell3B26" becomes "Cade Bunnell".
    """
    name = re.sub(r'\d+', '', name).strip()
    parts = re.findall(r'[A-ZÀ-ÖØ-Ý][a-zà-öø-ÿ]+', name)
    if parts and len(parts) >= 2:
        return f"{parts[0]} {parts[-1]}"
    elif parts:
        return parts[0]
    return name

def categorize_players(df, stat_choice, target_value, player_col, team_col):
    """Categorize players based on success rate and assign a category."""
    if df.empty:
        print("❌ DataFrame is empty. Check if the team abbreviations and CSV data are correct.")
        return pd.DataFrame()

    df["Success_Rate"] = ((df[stat_choice] / target_value) * 100).round(1)
    
    if stat_choice == "3PM" and target_value == 1:
        df.loc[df[stat_choice] > 0, "Category"] = "🟢 Best Bet"
        df.loc[df[stat_choice] == 0, "Category"] = "🔴 Underdog"
    elif stat_choice == "FG3M" and target_value == 1:
        df.loc[df["Success_Rate"] >= 265, "Category"] = "🟡 Favorite"
        df.loc[(df["Success_Rate"] >= 180) & (df["Success_Rate"] < 265), "Category"] = "🟢 Best Bet"
        df.loc[(df["Success_Rate"] >= 100) & (df["Success_Rate"] < 180), "Category"] = "🔴 Underdog"
    elif stat_choice == "3PM" and target_value == 2:
        df.loc[df["Success_Rate"] >= 150, "Category"] = "🟡 Favorite"
        df.loc[(df["Success_Rate"] >= 100) & (df["Success_Rate"] < 150), "Category"] = "🟢 Best Bet"
        df.loc[(df["Success_Rate"] >= 50) & (df["Success_Rate"] < 100), "Category"] = "🔴 Underdog"
    else:
        df.loc[df["Success_Rate"] >= 110, "Category"] = "🟡 Favorite"
        df.loc[(df["Success_Rate"] >= 85) & (df["Success_Rate"] < 110), "Category"] = "🟢 Best Bet"
        df.loc[df["Success_Rate"] < 85, "Category"] = "🔴 Underdog"

    df = df.drop_duplicates(subset=[player_col, team_col])

    red_players = df[df["Category"] == "🔴 Underdog"].nlargest(3, "Success_Rate")
    if len(red_players) < 3:
        extra = df[df["Success_Rate"] < 100].nlargest(3 - len(red_players), "Success_Rate")
        red_players = pd.concat([red_players, extra]).drop_duplicates().nlargest(3, "Success_Rate")
        
    green_players = df[df["Category"] == "🟢 Best Bet"].nlargest(3, "Success_Rate")
    if len(green_players) < 3:
        extra = df[df["Success_Rate"] >= 100].nlargest(3 - len(green_players), "Success_Rate")
        green_players = pd.concat([green_players, extra]).drop_duplicates().nlargest(3, "Success_Rate")
        
    yellow_players = df[df["Category"] == "🟡 Favorite"].nlargest(3, "Success_Rate")
    if len(yellow_players) < 3:
        extra = df[df["Success_Rate"] >= 120].nlargest(3 - len(yellow_players), "Success_Rate")
        yellow_players = pd.concat([yellow_players, extra]).drop_duplicates().nlargest(3, "Success_Rate")
    
    final_df = pd.concat([green_players, yellow_players, red_players]).drop_duplicates(subset=[player_col, team_col]).reset_index(drop=True)
    final_df = pd.concat([
        final_df[final_df["Category"] == "🟢 Best Bet"].sort_values(by="Success_Rate", ascending=False),
        final_df[final_df["Category"] == "🟡 Favorite"].sort_values(by="Success_Rate", ascending=False),
        final_df[final_df["Category"] == "🔴 Underdog"].sort_values(by="Success_Rate", ascending=True)
    ]).reset_index(drop=True)
    
    return final_df.head(9)

def analyze_teams(df, stat_categories, player_col, team_col):
    """Analyze player performance based on input teams and selected stat."""
    while True:
        teams = input("\nEnter team names separated by commas (or 'exit' to return to main menu): ").replace(" ", "").upper()
        if teams.lower() == 'exit':
            break

        team_list = teams.split(",")
        filtered_df = df[df[team_col].isin(team_list)].copy()

        if filtered_df.empty:
            print("❌ No matching teams found in the data. Please check the team names.")
            continue

        print("\nAvailable stats to analyze:")
        for key in stat_categories:
            print(f"- {key}")

        stat_choice = input("\nEnter stat to sort by (PPG, APG, RPG, 3PM, PTS, G, A, S): ").strip().upper()
        if stat_choice not in stat_categories:
            print("❌ Invalid stat choice. Please try again.")
            continue

        target_value = float(input(f"\nEnter target {stat_choice} value: "))
        mapped_stat = stat_categories[stat_choice]
        result_df = categorize_players(filtered_df, mapped_stat, target_value, player_col, team_col)

        if result_df.empty:
            print("❌ No players found for the given criteria.")
        else:
            print(f"\n🏀 Player Performance Based on Target {target_value} {stat_choice} 🏀")
            print(result_df[[player_col, team_col, mapped_stat, "Success_Rate", "Category"]].to_string(index=False))
            green_players = result_df[result_df["Category"] == "🟢 Best Bet"][player_col].tolist()
            yellow_players = result_df[result_df["Category"] == "🟡 Favorite"][player_col].tolist()
            red_players = result_df[result_df["Category"] == "🔴 Underdog"][player_col].tolist()

            print("\n🟢 " + ", ".join(green_players[:3]))
            print("🟡 " + ", ".join(yellow_players[:3]))
            print("🔴 " + ", ".join(red_players[:3]))

def analyze_mlb_by_team(df):
    """
    For MLB, filter by team names and then display the top 9 RBI leaders.
    Player names are fixed using fix_mlb_player_name().
    Only the columns PLAYER, TEAM, RBI, AVG, OBP, and OPS are displayed.
    The top 3 are labeled as "🟢", the next 3 as "🟡", and the final 3 as "🔴".
    """
    teams = input("\nEnter team names separated by commas (or 'exit' to return to main menu): ").replace(" ", "").upper()
    if teams.lower() == 'exit':
        return
    team_list = teams.split(",")
    try:
        filtered_df = df[df["TEAM"].isin(team_list)].copy()
    except Exception as e:
        print("Error filtering by TEAM:", e)
        return
    if filtered_df.empty:
        print("❌ No matching teams found. Please check the team names.")
        return
    try:
        filtered_df["RBI"] = pd.to_numeric(filtered_df["RBI"], errors='coerce')
    except Exception as e:
        print("Error converting RBI to numeric:", e)
        return
    sorted_df = filtered_df.sort_values(by="RBI", ascending=False)
    top9 = sorted_df.head(9)
    # Keep only desired columns for MLB poll
    display_cols = ["PLAYER", "TEAM", "RBI", "AVG", "OBP", "OPS"]
    top9 = top9[display_cols]
    # Split top9 into three groups (top 3, next 3, last 3)
    green = top9.iloc[:3]
    yellow = top9.iloc[3:6]
    red = top9.iloc[6:9]
    
    print("\nMLB Top 9 RBI Leaders for teams (" + ", ".join(team_list) + "):")
    print(top9.to_string(index=False))
    print("\n🟢 " + ", ".join(green["PLAYER"].tolist()))
    print("🟡 " + ", ".join(yellow["PLAYER"].tolist()))
    print("🔴 " + ", ".join(red["PLAYER"].tolist()))

def load_player_stats():
    # Load College Basketball stats
    file_path = "/Users/Q/Documents/Documents/RealSports/CBB/cbb_players_stats.csv"
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found. Please ensure the CSV exists or update the path.")
        return pd.DataFrame()
    return pd.read_csv(file_path)

def load_nba_player_stats(file_path):
    return pd.read_csv(file_path)

def load_nba_injury_report(file_path):
    return pd.read_csv(file_path)

def merge_nba_stats_with_injuries(stats_df, injuries_df):
    stats_df['PLAYER'] = stats_df['PLAYER'].str.strip()
    injuries_df['playerName'] = injuries_df['playerName'].str.strip()
    merged_df = pd.merge(stats_df, injuries_df, left_on='PLAYER', right_on='playerName', how='left')
    healthy_players_df = merged_df[merged_df['injury'].isnull()]
    return healthy_players_df

def integrate_nba_data(player_stats_file, injury_report_file):
    stats_df = load_nba_player_stats(player_stats_file)
    injuries_df = load_nba_injury_report(injury_report_file)
    return merge_nba_stats_with_injuries(stats_df, injuries_df)

def load_nhl_player_stats(file_path):
    return pd.read_csv(file_path)

def load_nhl_injury_data(file_path):
    return pd.read_csv(file_path)

def integrate_nhl_data(player_stats_file, injury_data_file):
    stats_df = load_nhl_player_stats(file_path=player_stats_file)
    injuries_df = load_nhl_injury_data(file_path=injury_data_file)
    integrated_data = pd.merge(stats_df, injuries_df, how='left', left_on='Player', right_on='player')
    integrated_data = integrated_data[integrated_data['injuryStatus'].isnull()]
    return integrated_data

def load_and_clean_mlb_stats():
    """
    Loads and cleans MLB stats from mlb_stats.csv.
    This function removes extra descriptive text from headers and forces a desired header mapping.
    The resulting DataFrame will have the columns:
       ["PLAYER", "TEAM", "G", "AB", "R", "H", "RBI", "AVG", "OBP", "OPS"]
    """
    file_path = "mlb_stats.csv"
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error loading MLB stats from {file_path}: {e}")
        return pd.DataFrame()
    # Clean raw headers
    raw_cols = [clean_header(col) for col in df.columns]
    df.columns = raw_cols
    # Remove duplicate columns if any.
    df = df.loc[:, ~df.columns.duplicated()]
    # Ensure all desired columns exist; if missing, add them as empty.
    for col in DESIRED_MLB_COLS:
        if col not in df.columns:
            print(f"Missing desired column: {col}")
            df[col] = None
    # Reorder the DataFrame to contain only the desired columns.
    df = df.reindex(columns=DESIRED_MLB_COLS)
    # Fix player names using fix_mlb_player_name.
    df["PLAYER"] = df["PLAYER"].apply(fix_mlb_player_name)
    return df

def analyze_mlb_stats(df, stat_choice, target_value):
    """
    Analyzes MLB stats based on a chosen stat and target value.
    Converts the chosen stat column to numeric, calculates Success_Rate,
    and sorts the DataFrame by this rate.
    """
    try:
        df[stat_choice] = pd.to_numeric(df[stat_choice], errors='coerce')
    except Exception as e:
        print("Error converting stat column to numeric:", e)
        return df
    df["Success_Rate"] = ((df[stat_choice] / target_value) * 100).round(1)
    sorted_df = df.sort_values(by="Success_Rate", ascending=False)
    return sorted_df

def main():
    print("✅ Files loaded successfully")

    while True:
        print("\nSelect Sport:")
        print("1️⃣ College Basketball (CBB)")
        print("2️⃣ NBA")
        print("3️⃣ NHL")
        print("4️⃣ MLB")
        print("5️⃣ Exit")
        choice = input("Choose an option (1/2/3/4/5): ").strip()

        if choice == '1':
            print("\n📊 Selected: College Basketball (CBB)")
            df_cbb = load_player_stats()
            if df_cbb.empty:
                continue
            analyze_teams(df_cbb, STAT_CATEGORIES_CBB, "Player", "Team")
        elif choice == '2':
            print("\n📊 Selected: NBA")
            player_stats_file = 'nba_player_stats.csv'
            injury_report_file = 'nba_injury_report.csv'
            df_nba = integrate_nba_data(player_stats_file, injury_report_file)
            analyze_teams(df_nba, STAT_CATEGORIES_NBA, "PLAYER", "TEAM")
        elif choice == '3':
            print("\n📊 Selected: NHL")
            df_nhl = integrate_nhl_data("nhl_player_stats.csv", "nhl_injuries.csv")
            analyze_teams(df_nhl, STAT_CATEGORIES_NHL, "Player", "Team")
        elif choice == '4':
            print("\n📊 Selected: MLB")
            df_mlb = load_and_clean_mlb_stats()
            if df_mlb.empty:
                print("MLB stats CSV not found or empty.")
                continue
            print("Available MLB columns:", df_mlb.columns.tolist())
            print("\nAvailable stats to analyze:")
            for key in STAT_CATEGORIES_MLB:
                print(f"- {key}")
            # For MLB, simply prompt for team names.
            analyze_mlb_by_team(df_mlb)
        elif choice == '5':
            print("👋 Exiting... Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please select 1, 2, 3, 4, or 5.")

if __name__ == "__main__":
    main()