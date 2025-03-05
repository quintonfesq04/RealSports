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

# For NHL, include totals and per-game stats.
STAT_CATEGORIES_NHL = {
    "GOALS": "G",
    "ASSISTS": "A",   # to be converted to per-game
    "POINTS": "PTS",   # to be converted to per-game
    "S": "shotsPerGame"  # shots per game
}

# For MLB, desired columns.
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
# Team Name Normalization
# --------------------------------------------------
# Define a dictionary mapping alternate names to the canonical name.
TEAM_ALIASES = {
    "QUC": "QUOC",
    "AZ": "ARI",
    "ARZ": "ARI"
}

def normalize_team_name(team):
    team = team.strip().upper()
    return TEAM_ALIASES.get(team, team)

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
    """
    Fix an MLB player name by:
      1. Removing any digits.
      2. Removing extra spaces.
      3. Removing an extra trailing letter if present.
    """
    name = re.sub(r'\d+', '', name).strip()
    name = re.sub(r'\s+', ' ', name)
    match = re.match(r"^(.*[A-Za-z])([A-Za-z])\.$", name)
    if match:
        name = match.group(2).strip()
    return name

# --------------------------------------------------
# NHL Per-Game Stat Calculation Functions
# --------------------------------------------------
def calculate_per_game_stat(df, raw_stat, new_stat_name, games_column="GP"):
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
    if target_value is None or target_value == 0:
        return pd.DataFrame()
    df["Success_Rate"] = ((df[stat_choice] / target_value) * 100).round(1)
    
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
    
    green_list = final_df[final_df["Category"] == "🟢 Best Bet"][player_col].tolist()
    yellow_list = final_df[final_df["Category"] == "🟡 Favorite"][player_col].tolist()
    red_list = final_df[final_df["Category"] == "🔴 Underdog"][player_col].tolist()
    
    green_output = ", ".join(green_list) if green_list else "No Green Plays"
    yellow_output = ", ".join(yellow_list) if yellow_list else "No Yellow Plays"
    red_output = ", ".join(red_list) if red_list else "No Red Plays"
    
    output = f"🟢 {green_output}\n"
    output += f"🟡 {yellow_output}\n"
    output += f"🔴 {red_output}"
    return output

# --------------------------------------------------
# NHL Integration Functions
# --------------------------------------------------
def load_nhl_player_stats(file_path):
    return pd.read_csv(file_path)

def load_nhl_injury_data(file_path):
    return pd.read_csv(file_path)

def integrate_nhl_data(player_stats_file, injury_data_file):
    stats_df = load_nhl_player_stats(player_stats_file)
    injuries_df = load_nhl_injury_data(injury_data_file)
    if "playerName" in injuries_df.columns:
        injuries_df.rename(columns={"playerName": "Player"}, inplace=True)
    try:
        integrated_data = pd.merge(stats_df, injuries_df, how='left', on='Player')
    except Exception as e:
        print("Merge error for NHL data:", e)
        return stats_df
    integrated_data = integrated_data[integrated_data['injuryStatus'].isnull()]
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
# MLB Integration Functions
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
# MLB Noninteractive Analysis (Ranking Slices)
# --------------------------------------------------
def analyze_mlb_noninteractive(df, teams, stat_choice):
    if teams:
        team_list = [normalize_team_name(t) for t in teams.split(",") if t.strip()] if isinstance(teams, str) else [normalize_team_name(t) for t in teams]
        print("Normalized CSV team names:", df["TEAM"].astype(str).apply(normalize_team_name).unique())
        print("Team list from input:", team_list)
        filtered_df = df[df["TEAM"].astype(str).apply(normalize_team_name).isin(team_list)].copy()
    else:
        filtered_df = df.copy()
    if filtered_df.empty:
        return "❌ No matching teams found."
    mapped_stat = STAT_CATEGORIES_MLB.get(stat_choice)
    if mapped_stat is None:
        return "❌ Invalid stat choice."
    try:
        filtered_df[mapped_stat] = pd.to_numeric(filtered_df[mapped_stat], errors='coerce')
    except Exception as e:
        return f"Error converting stat column: {e}"
    sorted_df = filtered_df.sort_values(by=mapped_stat, ascending=False)
    yellow = sorted_df.iloc[0:3]
    green = sorted_df.iloc[3:6]
    red = sorted_df.iloc[6:9]
    output = "🟢 " + ", ".join(green["PLAYER"].tolist()) + "\n"
    output += "🟡 " + ", ".join(yellow["PLAYER"].tolist()) + "\n"
    output += "🔴 " + ", ".join(red["PLAYER"].tolist())
    return output

# --------------------------------------------------
# Non-interactive (Programmatic) Interfaces for NBA/CBB and NHL
# --------------------------------------------------
def analyze_sport_noninteractive(df, stat_categories, player_col, team_col, teams, stat_choice, target_value):
    if isinstance(teams, str):
        team_list = [normalize_team_name(t) for t in teams.split(",") if t.strip()]
    else:
        team_list = [normalize_team_name(t) for t in teams]
    
    print("Normalized CSV team names:", df[team_col].astype(str).apply(normalize_team_name).unique())
    print("Team list from input:", team_list)
    
    filtered_df = df[df[team_col].astype(str).apply(normalize_team_name).isin(team_list)].copy()
    if filtered_df.empty:
        return "❌ No matching teams found."
    mapped_stat = stat_categories.get(stat_choice)
    if mapped_stat is None:
        return "❌ Invalid stat choice."
    df_mode = filtered_df.copy()
    try:
        df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
    except Exception as e:
        return f"Error converting stat column: {e}"
    if target_value is None or target_value == 0:
        return "Target value required and must be nonzero."
    df_mode["Success_Rate"] = ((df_mode[mapped_stat] / target_value) * 100).round(1)
    df_mode.loc[df_mode["Success_Rate"] >= 110, "Category"] = "🟡 Favorite"
    df_mode.loc[(df_mode["Success_Rate"] >= 85) & (df_mode["Success_Rate"] < 110), "Category"] = "🟢 Best Bet"
    df_mode.loc[df_mode["Success_Rate"] < 85, "Category"] = "🔴 Underdog"
    df_mode = df_mode.drop_duplicates(subset=[player_col, team_col])
    red_players = df_mode[df_mode["Category"] == "🔴 Underdog"].nlargest(3, "Success_Rate")
    if len(red_players) < 3:
        extra = df_mode[df_mode["Success_Rate"] < 100].nlargest(3 - len(red_players), "Success_Rate")
        red_players = pd.concat([red_players, extra]).drop_duplicates().nlargest(3, "Success_Rate")
    green_players = df_mode[df_mode["Category"] == "🟢 Best Bet"].nlargest(3, "Success_Rate")
    if len(green_players) < 3:
        extra = df_mode[df_mode["Success_Rate"] >= 100].nlargest(3 - len(green_players), "Success_Rate")
        green_players = pd.concat([green_players, extra]).drop_duplicates().nlargest(3, "Success_Rate")
    yellow_players = df_mode[df_mode["Category"] == "🟡 Favorite"].nlargest(3, "Success_Rate")
    if len(yellow_players) < 3:
        extra = df_mode[df_mode["Success_Rate"] >= 120].nlargest(3 - len(yellow_players), "Success_Rate")
        yellow_players = pd.concat([yellow_players, extra]).drop_duplicates().nlargest(3, "Success_Rate")
    
    final_df = pd.concat([green_players, yellow_players, red_players]).drop_duplicates(subset=[player_col, team_col]).reset_index(drop=True)
    final_df = pd.concat([
        final_df[final_df["Category"] == "🟢 Best Bet"].sort_values(by="Success_Rate", ascending=False),
        final_df[final_df["Category"] == "🟡 Favorite"].sort_values(by="Success_Rate", ascending=False),
        final_df[final_df["Category"] == "🔴 Underdog"].sort_values(by="Success_Rate", ascending=True)
    ]).reset_index(drop=True)
    
    green_list = final_df[final_df["Category"] == "🟢 Best Bet"][player_col].tolist()
    yellow_list = final_df[final_df["Category"] == "🟡 Favorite"][player_col].tolist()
    red_list = final_df[final_df["Category"] == "🔴 Underdog"][player_col].tolist()
    
    green_output = ", ".join(green_list) if green_list else "No Green Plays"
    yellow_output = ", ".join(yellow_list) if yellow_list else "No Yellow Plays"
    red_output = ", ".join(red_list) if red_list else "No Red Plays"
    
    output = f"🟢 {green_output}\n"
    output += f"🟡 {yellow_output}\n"
    output += f"🔴 {red_output}"
    return output

def analyze_nhl_noninteractive(df, teams, stat_choice, target_value=None):
    filtered_df = df[df["Team"].isin(teams)].copy()
    if filtered_df.empty:
        return "❌ No matching teams found."
    if stat_choice in ["ASSISTS", "POINTS", "S"]:
        df_mode = calculate_nhl_per_game_stats(filtered_df.copy())
    else:
        df_mode = filtered_df.copy()
    mapped_stat = STAT_CATEGORIES_NHL.get(stat_choice)
    if mapped_stat is None:
        return "❌ Invalid NHL stat choice."
    try:
        df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
    except Exception as e:
        return f"Error converting stat column: {e}"
    if len(teams) == 2 and stat_choice == "S":
        if target_value is None:
            return "Target value required for Shots analysis."
        result = categorize_players(df_mode, mapped_stat, target_value, "Player", "Team")
        return f"{result}"
    else:
        if stat_choice == "S":
            if "GP" in df_mode.columns and "S" in df_mode.columns:
                df_mode = df_mode.assign(shotsPerGame = pd.to_numeric(df_mode["S"], errors="coerce") / pd.to_numeric(df_mode["GP"], errors="coerce"))
                sorted_df = df_mode.sort_values(by="shotsPerGame", ascending=False)
            else:
                return "Required raw data for shots per game is missing."
        elif stat_choice == "POINTS":
            try:
                df_mode["PTS"] = pd.to_numeric(df_mode["PTS"], errors='coerce')
            except Exception as e:
                return f"Error converting points: {e}"
            sorted_df = df_mode.sort_values(by="PTS", ascending=False)
        else:
            sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
        
        yellow = sorted_df.iloc[:3]
        green = sorted_df.iloc[7:10]
        red = sorted_df.iloc[22:25]
        output = "🟢 " + ", ".join(green["Player"].tolist()) + "\n"
        output += "🟡 " + ", ".join(yellow["Player"].tolist()) + "\n"
        output += "🔴 " + ", ".join(red["Player"].tolist())
        return output

# --------------------------------------------------
# Interactive Functions
# --------------------------------------------------
def analyze_sport(df, stat_categories, player_col, team_col):
    while True:
        teams_input = input("\nEnter team names separated by commas (or 'exit' to return to main menu): ")
        if teams_input.lower() == 'exit':
            break
        team_list = [normalize_team_name(t) for t in teams_input.split(",") if t.strip()]
        print("Normalized CSV team names:", df[team_col].astype(str).apply(normalize_team_name).unique())
        print("Team list from input:", team_list)
        filtered_df = df[df[team_col].astype(str).apply(normalize_team_name).isin(team_list)].copy()
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
        df_mode = filtered_df.copy()
        try:
            df_mode[mapped_stat] = pd.to_numeric(df_mode[mapped_stat], errors='coerce')
        except Exception as e:
            print("Error converting stat column to numeric:", e)
            continue
        
        target_value = input(f"\nEnter target {stat_choice} value (per game): ").strip()
        if not target_value:
            print("❌ Target value is required.")
            continue
        try:
            target_value = float(target_value)
        except Exception as e:
            print("❌ Invalid target value.", e)
            continue
        
        result = categorize_players(df_mode, mapped_stat, target_value, player_col, team_col)
        print(f"\nPlayer Performance Based on Target {target_value} {stat_choice}:")
        print(result)

def analyze_nhl_flow(df):
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

        if len(team_list) == 2:
            sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
            if stat_choice == "S":
                target_value = input(f"\nEnter target {stat_choice} value (per game): ").strip()
                if not target_value:
                    print("❌ Target value is required for Shots.")
                    continue
                try:
                    target_value = float(target_value)
                except Exception as e:
                    print("❌ Invalid target value.", e)
                    continue
                result = categorize_players(df_mode, mapped_stat, target_value, "Player", "Team")
                print(result)
            else:
                yellow = sorted_df.iloc[0:3]
                green = sorted_df.iloc[3:6]
                red = sorted_df.iloc[14:17]
                print("🟢 " + ", ".join(green["Player"].tolist()))
                print("🟡 " + ", ".join(yellow["Player"].tolist()))
                print("🔴 " + ", ".join(red["Player"].tolist()))
        else:
            if stat_choice == "S":
                if "GP" in df_mode.columns and "S" in df_mode.columns:
                    df_mode = df_mode.assign(shotsPerGame = pd.to_numeric(df_mode["S"], errors="coerce") / pd.to_numeric(df_mode["GP"], errors="coerce"))
                    sorted_df = df_mode.sort_values(by="shotsPerGame", ascending=False)
                else:
                    print("Required raw data for shots per game is missing.")
                    continue
            elif stat_choice == "POINTS":
                try:
                    df_mode["PTS"] = pd.to_numeric(df_mode["PTS"], errors='coerce')
                except Exception as e:
                    print("Error converting points to numeric:", e)
                    continue
                sorted_df = df_mode.sort_values(by="PTS", ascending=False)
            else:
                sorted_df = df_mode.sort_values(by=mapped_stat, ascending=False)
            
            yellow = sorted_df.iloc[:3]
            green = sorted_df.iloc[7:10]
            red = sorted_df.iloc[22:25]
            print("🟢 " + ", ".join(green["Player"].tolist()))
            print("🟡 " + ", ".join(yellow["Player"].tolist()))
            print("🔴 " + ", ".join(red["Player"].tolist()))

def analyze_mlb_by_team_interactive(df, mapped_stat):
    if df.empty:
        print("MLB stats CSV not found or empty.")
        return

    print("Available MLB columns:", df.columns.tolist())
    print("\nTop MLB Players (filtered by team if provided):")
    
    teams_input = input("Enter MLB team names separated by commas (or press Enter to show all): ").strip().upper()
    if teams_input:
        team_list = [x.strip() for x in teams_input.split(",")]
        filtered_df = df[df["TEAM"].astype(str).apply(normalize_team_name).isin(team_list)]
        print("Normalized CSV team names:", df["TEAM"].astype(str).apply(normalize_team_name).unique())
        print("Team list from input:", team_list)
    else:
        filtered_df = df

    if filtered_df.empty:
        print("❌ No matching teams found.")
        return

    sorted_df = filtered_df.sort_values(by=mapped_stat, ascending=False)
    yellow = sorted_df.iloc[0:3]
    green = sorted_df.iloc[4:7]
    red = sorted_df.iloc[9:12]
    print("🟢 " + ", ".join(green["PLAYER"].tolist()))
    print("🟡 " + ", ".join(yellow["PLAYER"].tolist()))
    print("🔴 " + ", ".join(red["PLAYER"].tolist()))

def load_player_stats():
    file_path = "/Users/Q/Documents/Documents/RealSports/CBB/cbb_players_stats.csv"
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found. Please update the path.")
        return pd.DataFrame()
    return pd.read_csv(file_path)

# --------------------------------------------------
# NBA Integration Functions
# --------------------------------------------------
def integrate_nba_data(player_stats_file, injury_report_file):
    stats_df = load_nba_player_stats(player_stats_file)
    injuries_df = load_nba_injury_report(injury_report_file)
    return merge_nba_stats_with_injuries(stats_df, injuries_df)

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

# --------------------------------------------------
# Main Menu (Interactive)
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
            analyze_mlb_by_team_interactive(df_mlb, mapped_stat="RBI")
        elif choice == '5':
            print("👋 Exiting... Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please select 1, 2, 3, 4, or 5.")

if __name__ == "__main__":
    main()