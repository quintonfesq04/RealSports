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
TEAM_ALIASES = {
    "QUC": "QUOC",
    "AZ": "ARI",
    "NCSU": "NCST",
    "PRES": "PRE",
    "LR": "UALR",
    "BOIS": "BSU",
    "HPU": "HP",
    "CAM": "CAMP",
    "HCU": "HBU",
    "ATH": "OAK"
}

def normalize_team_name(team):
    team = team.strip().upper()
    return TEAM_ALIASES.get(team, team)

# --------------------------------------------------
# Banned Players Handling
# --------------------------------------------------
BANNED_PLAYERS = [
    "Bobby Portis",
    "Jonas Valančiūnas",
    "Ethen Frank",
    "Killian Hayes",
    "Khris Middleton"
]

BANNED_PLAYERS_SET = {p.strip().lower() for p in BANNED_PLAYERS}

def is_banned(player_name, banned_set=None):
    """
    Return True if player_name (normalized) is in banned_set.
    If banned_set is not provided, use the global BANNED_PLAYERS_SET.
    """
    if banned_set is None:
        banned_set = BANNED_PLAYERS_SET
    return player_name.strip().lower() in banned_set

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
    name = re.sub(r'^\d+', '', name)
    name = re.sub(r'\d+$', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    tokens = re.split(r'[ \.]+', name)
    fixed_tokens = []
    known_positions = {"RF", "CF", "LF", "SS", "C", "1B", "2B", "3B", "OF", "DH"}
    def remove_trailing_duplicate_substring(token):
        for i in range(1, len(token)):
            prefix = token[:i]
            if token.endswith(prefix) and len(token) - i > 0:
                if token[:-i].startswith(prefix):
                    return token[:-i]
        return token
    for token in tokens:
        if not token:
            continue
        token = re.sub(r'[^A-Za-z]+$', '', token)
        for pos in known_positions:
            if token.upper().endswith(pos):
                token = token[:-len(pos)].strip()
        token = re.sub(r'(?<=[a-z])([A-Z])$', '', token)
        if len(token) % 2 == 0:
            half = len(token) // 2
            if token[:half].lower() == token[half:].lower():
                token = token[:half]
        token = remove_trailing_duplicate_substring(token)
        fixed_tokens.append(token)
    seen = set()
    unique_tokens = []
    for token in fixed_tokens:
        token_lower = token.lower()
        if token_lower not in seen:
            unique_tokens.append(token)
            seen.add(token_lower)
    return " ".join(unique_tokens)

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
        return "❌ DataFrame is empty. Check if the CSV data are correct."
    
    # Remove banned players
    df = df[~df[player_col].apply(lambda x: is_banned(x))]
    
    # Convert stat column to numeric and drop rows with NaN stat values, if the column exists.
    if stat_choice in df.columns:
        try:
            df[stat_choice] = pd.to_numeric(df[stat_choice], errors='coerce')
        except Exception as e:
            print("Error converting stat column to numeric:", e)
            return f"Error converting stat column: {e}"
        df = df.dropna(subset=[stat_choice])
    else:
        # This is expected for non-numeric columns like "GOALS" when the numeric version ("G") is used elsewhere.
        print(f"Warning: Column '{stat_choice}' not found in DataFrame. Skipping numeric conversion.")
    
    # Aggressively drop duplicate players (adjust subset if team matters)
    df = df.drop_duplicates(subset=[player_col])
    
    if target_value is None or target_value == 0:
        return pd.DataFrame()
    df["Success_Rate"] = ((df[stat_choice] / target_value) * 100).round(1)
    
    df.loc[df["Success_Rate"] >= 110, "Category"] = "🟡 Favorite"
    df.loc[(df["Success_Rate"] >= 85) & (df["Success_Rate"] < 110), "Category"] = "🟢 Best Bet"
    df.loc[df["Success_Rate"] < 85, "Category"] = "🔴 Underdog"
    
    # Drop duplicates one more time (if necessary)
    df = df.drop_duplicates(subset=[player_col, team_col])
    
    # Select the top three players for each category
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
    try:
        stats_df = load_nhl_player_stats(player_stats_file)
    except FileNotFoundError:
        print(f"Error: The file {player_stats_file} was not found.")
        return pd.DataFrame()
    try:
        injuries_df = load_nhl_injury_data(injury_data_file)
    except FileNotFoundError:
        print(f"Error: The file {injury_data_file} was not found.")
        return stats_df
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
    integrated_data.columns = [col.strip() for col in integrated_data.columns]
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
    
    if df.empty:
        print(f"Error: The file {file_path} is empty.")
        return pd.DataFrame()
    
    raw_cols = [clean_header(col) for col in df.columns]
    df.columns = raw_cols
    df = df.loc[:, ~df.columns.duplicated()]
    
    for col in DESIRED_MLB_COLS:
        if col not in df.columns:
            df[col] = None
    
    df = df.reindex(columns=DESIRED_MLB_COLS)
    df["PLAYER"] = df["PLAYER"].apply(fix_mlb_player_name)
    
    if "TEAM" not in df.columns:
        print("Error: 'TEAM' column not found in the MLB stats CSV.")
        return pd.DataFrame()
    
    df["TEAM"] = df["TEAM"].astype(str).apply(normalize_team_name)
    return df

def integrate_mlb_data():
    try:
        df_stats = load_and_clean_mlb_stats()
        if "TEAM" not in df_stats.columns:
            print("Error: 'TEAM' column not found in the MLB stats CSV.")
            return pd.DataFrame()
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
def analyze_mlb_noninteractive(df, teams, stat_choice, banned_players):
    if "TEAM" not in df.columns:
        return "❌ 'TEAM' column not found in the DataFrame."
    if teams:
        team_list = ([normalize_team_name(t) for t in teams.split(",") if t.strip()]
                     if isinstance(teams, str) else [normalize_team_name(t) for t in teams])
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
    # Remove duplicate player entries based on the PLAYER column.
    sorted_df = sorted_df.drop_duplicates(subset=["PLAYER"])
    sorted_df = sorted_df[~sorted_df["PLAYER"].apply(lambda x: is_banned(x, banned_players))]
    
    non_banned = sorted_df["PLAYER"].tolist()
    players_to_use = non_banned[:9] if len(non_banned) >= 9 else non_banned
    yellow_list = players_to_use[0:3]
    green_list = players_to_use[3:6]
    red_list = players_to_use[6:9]
    
    output = "🟢 " + ", ".join(green_list) + "\n"
    output += "🟡 " + ", ".join(yellow_list) + "\n"
    output += "🔴 " + ", ".join(red_list)
    return output

# --------------------------------------------------
# Non-interactive (Programmatic) Interfaces for NBA/CBB and NHL
# --------------------------------------------------
def analyze_sport_noninteractive(df, stat_categories, player_col, team_col, teams, stat_choice, target_value, banned_set_param):
    if team_col not in df.columns:
        if "Team" in df.columns:
            team_col = "Team"
        else:
            print(f"❌ '{team_col}' column not found in the DataFrame.")
            return "❌ 'Team' column not found in the DataFrame."
    if isinstance(teams, str):
        team_list = [normalize_team_name(t) for t in teams.split(",") if t.strip()]
    else:
        team_list = [normalize_team_name(t) for t in teams]
    
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
        return f"Error converting stat column to numeric: {e}"
    if target_value is None or target_value == 0:
        return "Target value required and must be nonzero."
    df_mode["Success_Rate"] = ((df_mode[mapped_stat] / target_value) * 100).round(1)
    df_mode.loc[df_mode["Success_Rate"] >= 110, "Category"] = "🟡 Favorite"
    df_mode.loc[(df_mode["Success_Rate"] >= 85) & (df_mode["Success_Rate"] < 110), "Category"] = "🟢 Best Bet"
    df_mode.loc[df_mode["Success_Rate"] < 85, "Category"] = "🔴 Underdog"
    
    # Drop duplicate entries using player_col (and team_col if needed)
    df_mode = df_mode.drop_duplicates(subset=[player_col])
    # Remove rows where the stat value is nan using the correct mapped column, then drop duplicates by player name.
    df_mode = df_mode.dropna(subset=[mapped_stat])
    df_mode = df_mode.drop_duplicates(subset=[player_col])
    
    if stat_categories == STAT_CATEGORIES_NBA:
        sorted_overall = df_mode.sort_values(by="Success_Rate", ascending=False)
        # Remove duplicate player names
        sorted_overall = sorted_overall.drop_duplicates(subset=[player_col])
        all_non_banned = [player for player in sorted_overall[player_col].tolist() 
                            if not is_banned(player, banned_set_param)]
        non_banned = all_non_banned[:9]
        yellow_list = non_banned[0:3]
        green_list = non_banned[3:6]
        red_list = non_banned[6:9]
    else:
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
        non_banned = [player for player in final_df[player_col].tolist() if not is_banned(player, banned_set_param)]
        if len(non_banned) < 9:
            all_non_banned = [player for player in df_mode[player_col].tolist() if not is_banned(player, banned_set_param)]
            non_banned = all_non_banned[:9]
        else:
            non_banned = non_banned[:9]
        green_list = non_banned[0:3]
        yellow_list = non_banned[3:6]
        red_list = non_banned[6:9]
    
    yellow_output = ", ".join(yellow_list) if yellow_list else "No Yellow Plays"
    green_output = ", ".join(green_list) if green_list else "No Green Plays"
    red_output = ", ".join(red_list) if red_list else "No Red Plays"

    output = f"🟢 {green_output}\n"
    output += f"🟡 {yellow_output}\n"
    output += f"🔴 {red_output}"
    return output

def analyze_nhl_noninteractive(df, teams, stat_choice, target_value=None, banned_players=[]):
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
    
    # Drop rows with missing values in the mapped column
    df_mode = df_mode.dropna(subset=[mapped_stat])
    
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
        
        # Remove duplicate entries based on the Player column.
        sorted_df = sorted_df.drop_duplicates(subset=["Player"])
        sorted_df = sorted_df[~sorted_df["Player"].apply(lambda x: is_banned(x, banned_players))]
        non_banned = sorted_df["Player"].tolist()
        players_to_use = non_banned[:9] if len(non_banned) >= 9 else non_banned
        yellow = players_to_use[0:3]
        green = players_to_use[3:6]
        red = players_to_use[6:9]
        output = "🟢 " + ", ".join(green) + "\n"
        output += "🟡 " + ", ".join(yellow) + "\n"
        output += "🔴 " + ", ".join(red)
        return output

def analyze_cbb_noninteractive(df, teams, stat_choice, target_value, banned_players):
    team_col = "Team" if "Team" in df.columns else "TEAM"
    if team_col not in df.columns:
        return "❌ 'Team' column not found in the DataFrame."
    
    if isinstance(teams, str):
        team_list = [normalize_team_name(t) for t in teams.split(",") if t.strip()]
    else:
        team_list = [normalize_team_name(t) for t in teams]
    
    filtered_df = df[df[team_col].astype(str).apply(normalize_team_name).isin(team_list)].copy()
    if filtered_df.empty:
        return "❌ No matching teams found."
    
    mapped_stat = STAT_CATEGORIES_CBB.get(stat_choice)
    if mapped_stat is None:
        return "❌ Invalid stat choice."
    
    try:
        filtered_df[mapped_stat] = pd.to_numeric(filtered_df[mapped_stat], errors='coerce')
    except Exception as e:
        return f"Error converting stat column: {e}"
    
    if target_value is None or target_value == 0:
        return "Target value required and must be nonzero."
    
    filtered_df["Success_Rate"] = ((filtered_df[mapped_stat] / target_value) * 100).round(1)
    filtered_df.loc[filtered_df["Success_Rate"] >= 110, "Category"] = "🟡 Favorite"
    filtered_df.loc[(filtered_df["Success_Rate"] >= 85) & (filtered_df["Success_Rate"] < 110), "Category"] = "🟢 Best Bet"
    filtered_df.loc[filtered_df["Success_Rate"] < 85, "Category"] = "🔴 Underdog"
    
    filtered_df = filtered_df.drop_duplicates(subset=["Player", team_col])
    
    # For green players:
    green_players = filtered_df[filtered_df["Category"] == "🟢 Best Bet"].nlargest(3, "Success_Rate")
    if len(green_players) < 3:
        extra = filtered_df[filtered_df["Success_Rate"] >= 100].nlargest(3 - len(green_players), "Success_Rate")
        green_players = pd.concat([green_players, extra]).drop_duplicates().nlargest(3, "Success_Rate")
    
    # For yellow players:
    yellow_players = filtered_df[filtered_df["Category"] == "🟡 Favorite"].nlargest(3, "Success_Rate")
    if len(yellow_players) < 3:
        extra = filtered_df[filtered_df["Success_Rate"] >= 120].nlargest(3 - len(yellow_players), "Success_Rate")
        yellow_players = pd.concat([yellow_players, extra]).drop_duplicates().nlargest(3, "Success_Rate")
    
    # For red plays, select the 3 players with the lowest success rates.
    red_players = filtered_df.sort_values(by="Success_Rate", ascending=True).head(3)
    # Force their category to "🔴 Underdog"
    red_players["Category"] = "🔴 Underdog"
    
    # Build the output lists from these selections.
    green_list = [player for player in green_players["Player"].tolist() if not is_banned(player, banned_players)]
    yellow_list = [player for player in yellow_players["Player"].tolist() if not is_banned(player, banned_players)]
    red_list = [player for player in red_players["Player"].tolist() if not is_banned(player, banned_players)]
    
    green_output = ", ".join(green_list) if green_list else "No Green Plays"
    yellow_output = ", ".join(yellow_list) if yellow_list else "No Yellow Plays"
    red_output = ", ".join(red_list) if red_list else "No Red Plays"
    
    output = f"🟢 {green_output}\n"
    output += f"🟡 {yellow_output}\n"
    output += f"🔴 {red_output}"
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
        filtered_df = df[df[team_col].astype(str).apply(normalize_team_name).isin(team_list)].copy()
        if filtered_df.empty:
            print("❌ No matching teams found. Please check the team names.")
            continue
        stat_choice = input("\nEnter stat to sort by: ").strip().upper()
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

        stat_choice = input("\nEnter NHL stat to analyze: ").strip().upper()
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
            else:
                yellow = sorted_df.iloc[0:3]
                green = sorted_df.iloc[3:6]
                red = sorted_df.iloc[14:17]
                result = f"🟢 {', '.join(green['Player'].tolist())}\n"
                result += f"🟡 {', '.join(yellow['Player'].tolist())}\n"
                result += f"🔴 {', '.join(red['Player'].tolist())}"
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
            
            sorted_df = sorted_df[~sorted_df["Player"].apply(lambda x: is_banned(x, BANNED_PLAYERS_SET))]
            non_banned = sorted_df["Player"].tolist()
            if len(non_banned) < 9:
                players_to_use = non_banned
            else:
                players_to_use = non_banned[:9]
            yellow = players_to_use[0:3]
            green = players_to_use[3:6]
            red = players_to_use[6:9]
            result = f"🟢 {', '.join(green)}\n"
            result += f"🟡 {', '.join(yellow)}\n"
            result += f"🔴 {', '.join(red)}"
        
        print("\n" + result)

def analyze_mlb_by_team_interactive(df, mapped_stat):
    if df.empty:
        print("MLB stats CSV not found or empty.")
        return

    print("\nTop MLB Players (filtered by team if provided):")
    
    teams_input = input("Enter MLB team names separated by commas (or press Enter to show all): ").strip().upper()
    if teams_input:
        team_list = [x.strip() for x in teams_input.split(",")]
        filtered_df = df[df["TEAM"].astype(str).apply(normalize_team_name).isin(team_list)]
    else:
        filtered_df = df

    if filtered_df.empty:
        print("❌ No matching teams found.")
        return

    sorted_df = filtered_df.sort_values(by=[mapped_stat], ascending=False)
    sorted_df = sorted_df[~sorted_df["PLAYER"].apply(lambda x: is_banned(x, BANNED_PLAYERS_SET))]
    non_banned = sorted_df["PLAYER"].tolist()
    if len(non_banned) < 9:
        players_to_use = non_banned
    else:
        players_to_use = non_banned[:9]
    yellow = players_to_use[0:3]
    green = players_to_use[3:6]
    red = players_to_use[6:9]
    
    print("🟢 " + ", ".join(green))
    print("🟡 " + ", ".join(yellow))
    print("🔴 " + ", ".join(red))

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
# CBB Integration Functions
# --------------------------------------------------
def integrate_cbb_data(player_stats_file="cbb_player_stats.csv", injury_data_file="cbb_injuries.csv"):
    try:
        stats_df = pd.read_csv(player_stats_file)
    except FileNotFoundError:
        print(f"Error: The file {player_stats_file} was not found.")
        return pd.DataFrame()
    
    try:
        injuries_df = pd.read_csv(injury_data_file)
    except FileNotFoundError:
        print(f"Error: The file {injury_data_file} was not found.")
        return stats_df

    # Rename the player name column to "Player"
    if "playerName" in injuries_df.columns:
        injuries_df.rename(columns={"playerName": "Player"}, inplace=True)
    elif "col_0" in injuries_df.columns:
        injuries_df.rename(columns={"col_0": "Player"}, inplace=True)
    
    # Rename the injury status column to "injuryStatus" if needed
    if "injuryStatus" not in injuries_df.columns and "col_2" in injuries_df.columns:
        injuries_df.rename(columns={"col_2": "injuryStatus"}, inplace=True)
    
    try:
        integrated_data = pd.merge(stats_df, injuries_df, how='left', on='Player')
    except Exception as e:
        print("Merge error for CBB data:", e)
        return stats_df

    # Filter out players whose injuryStatus indicates they're out
    if "injuryStatus" in integrated_data.columns:
        mask = (
            integrated_data["injuryStatus"].fillna("")
            .str.lower()
            .str.contains("out indefinitely") |
            integrated_data["injuryStatus"].fillna("")
            .str.lower()
            .str.contains("out for season")
        )
        integrated_data = integrated_data[~mask]

    # Ensure the "Team" column exists
    if "Team" not in integrated_data.columns:
        integrated_data["Team"] = stats_df["Team"]

    # Normalize column names
    integrated_data.columns = [col.strip() for col in integrated_data.columns]
    return integrated_data

# --------------------------------------------------
# Interactive Main Menu
# --------------------------------------------------
def main():
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
            df_cbb = integrate_cbb_data(player_stats_file="cbb_players_stats.csv", injury_data_file="cbb_injuries.csv")
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