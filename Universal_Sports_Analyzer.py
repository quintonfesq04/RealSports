import pandas as pd
import os
import re

# --------------------------------------------------
# Define stat categories for NBA, CBB, NHL, and MLB
# --------------------------------------------------
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

# For NHL, we now include total Goals (G) and per-game values for Assists, Points, and Shots.
# In our analysis we want to show:
#   - GOALS (total)
#   - ASSISTS per game (calculated from raw 'A' and games played)
#   - POINTS per game (calculated from raw 'P' and games played)
#   - S: shots per game (calculated from raw 'S' and games played)
STAT_CATEGORIES_NHL = {
    "GOALS": "G",
    "ASSISTS": "A",   # We'll convert A to per-game value
    "POINTS": "PTS",   # We'll convert P to per-game value
    "S": "shotsPerGame"  # Custom stat: shots per game
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

# --------------------------------------------------
# Utility Functions: Header Cleaning and MLB Name Fixing
# --------------------------------------------------
def clean_header(header):
    header = header.strip()
    if header.isupper() and len(header) % 2 == 0:
        mid = len(header) // 2
        if header[:mid] == header[mid:]:
            header = header[:mid]
    keys = sorted(["PLAYER", "TEAM", "RBI", "AVG", "OBP", "OPS", "AB", "R", "H", "G"], key=len, reverse=True)
    for key in keys:
        if key.lower() in header.lower():
            return key
    return header

def fix_mlb_player_name(name):
    name = re.sub(r'\d+', '', name).strip()
    parts = re.findall(r'[A-ZÀ-ÖØ-Ý][a-zà-öø-ÿ]+', name)
    if parts and len(parts) >= 2:
        return f"{parts[0]} {parts[-1]}"
    elif parts:
        return parts[0]
    return name

# --------------------------------------------------
# NHL Per-Game Stat Calculation Functions
# --------------------------------------------------
def calculate_per_game_stat(df, raw_stat, new_stat_name, games_column="GP"):
    """Calculate a per-game statistic by dividing the raw stat by games played."""
    if games_column in df.columns:
        games = pd.to_numeric(df[games_column], errors='coerce')
    elif "G" in df.columns:
        games = pd.to_numeric(df["G"], errors='coerce')
    else:
        games = pd.Series([1] * len(df))
    stat_values = pd.to_numeric(df[raw_stat], errors='coerce')
    df[new_stat_name] = stat_values / games.replace(0, pd.NA)
    return df

def calculate_nhl_per_game_stats(df):
    # Calculate per-game assists ("A"), points ("PTS"), and shots per game ("shotsPerGame")
    df = calculate_per_game_stat(df, "A", "A")
    df = calculate_per_game_stat(df, "P", "PTS")
    df = calculate_per_game_stat(df, "S", "shotsPerGame")
    return df

# --------------------------------------------------
# Categorization Function (Used by All Sports)
# --------------------------------------------------
def categorize_players(df, stat_choice, target_value, player_col, team_col):
    if df.empty:
        print("❌ DataFrame is empty. Check if the CSV data are correct.")
        return pd.DataFrame()
    try:
        df[stat_choice] = pd.to_numeric(df[stat_choice], errors='coerce')
    except Exception as e:
        print("Error converting stat column to numeric:", e)
        return df
    df["Success_Rate"] = ((df[stat_choice] / target_value) * 100).round(1)
    
    # Generic thresholds:
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
    # Order as GREEN, YELLOW, then RED.
    final_df = pd.concat([
        final_df[final_df["Category"] == "🟢 Best Bet"].sort_values(by="Success_Rate", ascending=False),
        final_df[final_df["Category"] == "🟡 Favorite"].sort_values(by="Success_Rate", ascending=False),
        final_df[final_df["Category"] == "🔴 Underdog"].sort_values(by="Success_Rate", ascending=True)
    ]).reset_index(drop=True)
    
    return final_df.head(9)

# --------------------------------------------------
# NHL Integration: Stats, Injury Filtering, and Per-Game Calculations
# --------------------------------------------------
def load_nhl_player_stats(file_path):
    return pd.read_csv(file_path)

def load_nhl_injury_data(file_path):
    return pd.read_csv(file_path)

def integrate_nhl_data(player_stats_file, injury_data_file):
    stats_df = load_nhl_player_stats(player_stats_file)
    injuries_df = load_nhl_injury_data(injury_data_file)
    # Rename if needed so the merge key matches
    if "playerName" in injuries_df.columns:
        injuries_df.rename(columns={"playerName": "Player"}, inplace=True)
    try:
        integrated_data = pd.merge(stats_df, injuries_df, how='left', on='Player')
    except Exception as e:
        print("Merge error for NHL data:", e)
        return stats_df
    # Filter out injured players
    integrated_data = integrated_data[integrated_data['injuryStatus'].isnull()]
    # If "Team" is missing, restore it from the original stats DataFrame
    if "Team" not in integrated_data.columns:
        integrated_data["Team"] = stats_df["Team"]
    integrated_data = calculate_nhl_per_game_stats(integrated_data)
    if "GP" in integrated_data.columns:
        games = pd.to_numeric(integrated_data["GP"], errors='coerce')
    elif "G" in integrated_data.columns:
        games = pd.to_numeric(integrated_data["G"], errors='coerce')
    else:
        games = pd.Series([1] * len(integrated_data))
    integrated_data = integrated_data[games >= 15]
    return integrated_data

# --------------------------------------------------
# MLB Integration: Stats and Injury Filtering
# --------------------------------------------------
def load_and_clean_mlb_stats():
    file_path = "mlb_stats.csv"
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error loading MLB stats from {file_path}: {e}")
        return pd.DataFrame()
    raw_cols = [clean_header(col) for col in df.columns]
    df.columns = raw_cols
    df = df.loc[:, ~df.columns.duplicated()]
    for col in DESIRED_MLB_COLS:
        if col not in df.columns:
            df[col] = None
    df = df.reindex(columns=DESIRED_MLB_COLS)
    df["PLAYER"] = df["PLAYER"].apply(fix_mlb_player_name)
    return df

def integrate_mlb_data():
    try:
        df_stats = load_and_clean_mlb_stats()
    except Exception as e:
        print("Error loading and cleaning MLB stats:", e)
        return pd.DataFrame()
    try:
        df_inj = pd.read_csv("mlb_injuries.csv")
    except Exception as e:
        print("Error loading mlb_injuries.csv:", e)
        return df_stats
    df_inj["playerName"] = df_inj["playerName"].str.strip()
    df_inj["playerName_clean"] = df_inj["playerName"].apply(fix_mlb_player_name)
    injured_names = set(df_inj["playerName_clean"].dropna().unique())
    healthy_df = df_stats[~df_stats["PLAYER"].isin(injured_names)].copy()
    return healthy_df[DESIRED_MLB_COLS]

# --------------------------------------------------
# Generic Analysis Function for All Sports (Using Green/Yellow/Red)
# --------------------------------------------------
def analyze_sport(df, stat_categories, player_col, team_col):
    """
    Analyze a sport's data using the given stat categories.
    For NBA/CBB (using STAT_CATEGORIES_NBA/STAT_CATEGORIES_CBB), it assumes data is already per game.
    For NHL (using STAT_CATEGORIES_NHL), it will calculate per-game stats if applicable.
    """
    # For NHL, recalc per-game stats if needed:
    if stat_categories == STAT_CATEGORIES_NHL:
        if "GP" in df.columns and "S" in df.columns:
            df = df.loc[df["GP"] >= 10]
            df = df.assign(shotsPerGame = df["S"] / df["GP"])
        # For NHL, if the stat is one we want per-game (like "A", "P" for points, etc.)
        # we assume that the CSV has raw totals and we calculate per-game via separate functions.
        # (Your calculate_nhl_per_game_stats function should be applied when using NHL data.)
        # Otherwise, leave as-is.
    else:
        # For NBA/CBB, assume the CSV already has per-game stats (like PPG, APG, RPG, 3PM).
        if "gamesPlayed" in df.columns and "shots" in df.columns:
            df = df.loc[df["gamesPlayed"] >= 10]
            df = df.assign(shotsPerGame = df["shots"] / df["gamesPlayed"])
    
    while True:
        teams = input("\nEnter team names separated by commas (or 'exit' to return to main menu): ").replace(" ", "").upper()
        if teams.lower() == 'exit':
            break
        team_list = teams.split(",")
        filtered_df = df[df[team_col].isin(team_list)].copy()
        if filtered_df.empty:
            print("❌ No matching teams found. Please check the team names.")
            continue
        print("\nAvailable stats to analyze:")
        for key in stat_categories:
            print(f"- {key}")
        stat_choice = input("\nEnter stat to sort by (choose from above): ").strip().upper()
        if stat_choice not in stat_categories:
            print("❌ Invalid stat choice. Please try again.")
            continue
        
        mapped_stat = stat_categories[stat_choice]
        # For NBA/CBB, do not recalculate per-game stats if already present.
        if stat_categories != STAT_CATEGORIES_NHL:
            df_mode = filtered_df.copy()
        else:
            # For NHL, if using stats like ASSISTS, POINTS, or S, assume they've been calculated.
            df_mode = filtered_df.copy()
        
        try:
            df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
        except Exception as e:
            print("Error converting stat column to numeric:", e)
            continue
        
        target_value = float(input(f"\nEnter target {stat_choice} value (per game): "))
        result_df = categorize_players(df_mode, mapped_stat, target_value, player_col, team_col)
        if result_df.empty:
            print("❌ No players found for the given criteria.")
        else:
            print(f"\nPlayer Performance Based on Target {target_value} {stat_choice}:")
            print(result_df[[player_col, team_col, mapped_stat, "Success_Rate", "Category"]].to_string(index=False))
            green_players = result_df[result_df["Category"] == "🟢 Best Bet"][player_col].tolist()
            yellow_players = result_df[result_df["Category"] == "🟡 Favorite"][player_col].tolist()
            red_players = result_df[result_df["Category"] == "🔴 Underdog"][player_col].tolist()
            print("\n🟢 " + ", ".join(green_players))
            print("🟡 " + ", ".join(yellow_players))
            print("🔴 " + ", ".join(red_players))

# --------------------------------------------------
# NHL Analysis Main Flow
# --------------------------------------------------
def analyze_nhl_flow(df):
    """
    Analyze NHL data for a given set of teams.
    - If exactly 2 teams are entered, use "Games Mode" which ranks the sorted players as:
      • Yellow: ranks 1–3
      • Green: ranks 4–6
      • Red: ranks 15–17
    - If more than 2 teams are entered, use fixed ranking slices (PSP Mode).
    """
    while True:
        teams = input("\nEnter NHL team names separated by commas (or 'exit' to return to main menu): ").replace(" ", "").upper()
        if teams.lower() == "exit":
            break
        team_list = teams.split(",")
        filtered_df = df[df["Team"].isin(team_list)].copy()
        if filtered_df.empty:
            print("❌ No matching teams found. Please check the team names.")
            continue

        print("\nAvailable NHL stats to analyze:")
        for key in STAT_CATEGORIES_NHL:
            print(f"- {key}")
        stat_choice = input("\nEnter NHL stat to analyze (choose from above): ").strip().upper()
        if stat_choice not in STAT_CATEGORIES_NHL:
            print("❌ Invalid NHL stat choice.")
            continue

        # For stats that need per-game calculation, update the data.
        if stat_choice in ["ASSISTS", "POINTS", "S"]:
            df_mode = calculate_nhl_per_game_stats(filtered_df.copy())
        else:
            df_mode = filtered_df.copy()

        mapped_stat = STAT_CATEGORIES_NHL[stat_choice]
        try:
            df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
        except Exception as e:
            print("Error converting stat column to numeric:", e)
            continue

        # Decide ranking method based on number of teams entered.
        if len(team_list) == 2:
            # Games Mode using your ranking system:
            sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
            yellow = sorted_df.iloc[0:3]
            green = sorted_df.iloc[3:6]
            red = sorted_df.iloc[14:17]
            print(f"\nNHL {stat_choice} Analysis (Games Mode) for teams {', '.join(team_list)}:")
            print("🟢 " + ", ".join(green["Player"].tolist()))
            print("\n🟡 " + ", ".join(yellow["Player"].tolist()))
            print("🔴 " + ", ".join(red["Player"].tolist()))
        else:
            # Fixed Ranking Mode for more than 2 teams.
            if stat_choice == "S":
                if "GP" in df_mode.columns and "S" in df_mode.columns:
                    df_mode = df_mode.assign(shotsPerGame = pd.to_numeric(df_mode["S"], errors="coerce") / pd.to_numeric(df_mode["GP"], errors="coerce"))
                    sorted_df = df_mode.sort_values(by="shotsPerGame", ascending=False)
                else:
                    print("Required raw data for shots per game is missing.")
                    continue
            elif stat_choice == "POINTS":
                try:
                    df_mode["PTS"] = pd.to_numeric(df_mode["PTS"], errors="coerce")
                except Exception as e:
                    print("Error converting points to numeric:", e)
                    continue
                sorted_df = df_mode.sort_values(by="PTS", ascending=False)
            else:
                sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
            
            yellow = sorted_df.iloc[:3]
            green = sorted_df.iloc[7:10]
            red = sorted_df.iloc[22:25]
            print(f"\nNHL {stat_choice} Analysis (Fixed Ranking) for teams {', '.join(team_list)}:")
            print("\n🟢 " + ", ".join(green["Player"].tolist()))
            print("🟡 " + ", ".join(yellow["Player"].tolist()))
            print("🔴 " + ", ".join(red["Player"].tolist()))

# --------------------------------------------------
# MLB Analysis Function (Using Green/Yellow/Red)
# --------------------------------------------------
def analyze_nhl_flow(df):
    """
    Analyze NHL data for a given set of teams.
    - If exactly 2 teams are entered (Games Mode):
        • If the chosen stat is "S" (shots), prompt for a target value and use the target to calculate picks.
        • Otherwise, use your ranking system: 
            - Yellow: ranks 1–3
            - Green: ranks 4–6
            - Red: ranks 15–17
    - If more than 2 teams are entered, use fixed ranking slices.
    """
    while True:
        teams = input("\nEnter NHL team names separated by commas (or 'exit' to return to main menu): ").replace(" ", "").upper()
        if teams.lower() == "exit":
            break
        team_list = teams.split(",")
        filtered_df = df[df["Team"].isin(team_list)].copy()
        if filtered_df.empty:
            print("❌ No matching teams found. Please check the team names.")
            continue

        print("\nAvailable NHL stats to analyze:")
        for key in STAT_CATEGORIES_NHL:
            print(f"- {key}")
        stat_choice = input("\nEnter NHL stat to analyze (choose from above): ").strip().upper()
        if stat_choice not in STAT_CATEGORIES_NHL:
            print("❌ Invalid NHL stat choice.")
            continue

        # For stats requiring per-game calculations, update the DataFrame.
        if stat_choice in ["ASSISTS", "POINTS", "S"]:
            df_mode = calculate_nhl_per_game_stats(filtered_df.copy())
        else:
            df_mode = filtered_df.copy()

        mapped_stat = STAT_CATEGORIES_NHL[stat_choice]
        try:
            df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
        except Exception as e:
            print("Error converting stat column to numeric:", e)
            continue

        # Decide mode based on the number of teams entered.
        if len(team_list) == 2:
            # Games Mode for exactly 2 teams.
            sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
            if stat_choice == "S":
                # For Shots, prompt for a target value.
                target_value = float(input(f"\nEnter target {stat_choice} value (per game): "))
                result_df = categorize_players(df_mode, mapped_stat, target_value, "Player", "Team")
                if result_df.empty:
                    print("❌ No players found for the given criteria.")
                else:
                    print(f"\nNHL {stat_choice} Analysis (Games Mode) based on target {target_value}:")
                    print(result_df[["Player", "Team", mapped_stat, "Success_Rate", "Category"]].to_string(index=False))
                    green_players = result_df[result_df["Category"] == "🟢 Best Bet"]["Player"].tolist()
                    yellow_players = result_df[result_df["Category"] == "🟡 Favorite"]["Player"].tolist()
                    red_players = result_df[result_df["Category"] == "🔴 Underdog"]["Player"].tolist()
                    print("\n🟢 " + ", ".join(green_players))
                    print("🟡 " + ", ".join(yellow_players))
                    print("🔴 " + ", ".join(red_players))
            else:
                # For all other stats, use your ranking slices.
                yellow = sorted_df.iloc[0:3]
                green = sorted_df.iloc[3:6]
                red = sorted_df.iloc[14:17]
                print(f"\nNHL {stat_choice} Analysis (Games Mode) for teams {', '.join(team_list)}:")
                print("\n🟢 " + ", ".join(green["Player"].tolist()))
                print("🟡 " + ", ".join(yellow["Player"].tolist()))
                print("🔴 " + ", ".join(red["Player"].tolist()))
        else:
            # Fixed Ranking Mode for more than 2 teams.
            if stat_choice == "S":
                if "GP" in df_mode.columns and "S" in df_mode.columns:
                    df_mode = df_mode.assign(shotsPerGame = pd.to_numeric(df_mode["S"], errors="coerce") / pd.to_numeric(df_mode["GP"], errors="coerce"))
                    sorted_df = df_mode.sort_values(by="shotsPerGame", ascending=False)
                else:
                    print("Required raw data for shots per game is missing.")
                    continue
            elif stat_choice == "POINTS":
                try:
                    df_mode["PTS"] = pd.to_numeric(df_mode["PTS"], errors="coerce")
                except Exception as e:
                    print("Error converting points to numeric:", e)
                    continue
                sorted_df = df_mode.sort_values(by="PTS", ascending=False)
            else:
                sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
            
            yellow = sorted_df.iloc[:3]
            green = sorted_df.iloc[7:10]
            red = sorted_df.iloc[22:25]
            print(f"\nNHL {stat_choice} Analysis (Fixed Ranking) for teams {', '.join(team_list)}:")
            print("\n🟢 " + ", ".join(green["Player"].tolist()))
            print("🟡 " + ", ".join(yellow["Player"].tolist()))
            print("🔴 " + ", ".join(red["Player"].tolist()))
# --------------------------------------------------
# Data Loading Functions for Other Sports
# --------------------------------------------------
def load_player_stats():
    file_path = "/Users/Q/Documents/Documents/RealSports/CBB/cbb_players_stats.csv"
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found. Please update the path.")
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

# --------------------------------------------------
# Main Menu and Sport Selection
# --------------------------------------------------
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
            analyze_sport(df_cbb, STAT_CATEGORIES_CBB, "Player", "Team")
        elif choice == '2':
            print("\n📊 Selected: NBA")
            player_stats_file = 'nba_player_stats.csv'
            injury_report_file = 'nba_injury_report.csv'
            df_nba = integrate_nba_data(player_stats_file, injury_report_file)
            analyze_sport(df_nba, STAT_CATEGORIES_NBA, "PLAYER", "TEAM")
        elif choice == '3':
            print("\n📊 Selected: NHL")
            df_nhl = integrate_nhl_data("nhl_player_stats.csv", "nhl_injuries.csv")
            analyze_nhl_flow(df_nhl)
        elif choice == '4':
            print("\n📊 Selected: MLB")
            df_mlb = integrate_mlb_data()
            if df_mlb.empty:
                print("MLB stats CSV not found or empty.")
                continue
            print("Available MLB columns:", df_mlb.columns.tolist())
            print("\nAvailable stats to analyze:")
            for key in STAT_CATEGORIES_MLB:
                print(f"- {key}")
            analyze_mlb_by_team(df_mlb)
        elif choice == '5':
            print("👋 Exiting... Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please select 1, 2, 3, 4, or 5.")

if __name__ == "__main__":
    main()