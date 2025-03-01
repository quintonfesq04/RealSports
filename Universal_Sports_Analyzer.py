import pandas as pd
import os

# Define stat categories for NBA, CBB, and NHL
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

def categorize_players(df, stat_choice, target_value, player_col, team_col):
    """Categorize players based on success rate and assign a category."""
    if df.empty:
        print("❌ DataFrame is empty. Check if the team abbreviations and CSV data are correct.")
        return pd.DataFrame()

    # Compute success rate as a percentage.
    df["Success_Rate"] = ((df[stat_choice] / target_value) * 100).round(1)
    
    # Special branch for CBB 3PM when target value is 1
    if stat_choice == "3PM" and target_value == 1:
        # For college basketball: assign Best Bet for nonzero 3PM, Underdog for 0.
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
        teams = input("\nEnter team names separated by commas (or 'exit' to return): ").replace(" ", "").upper()
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

def load_player_stats():
    # Attempt to load the CBB CSV; adjust the path if needed.
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
    stats_df = load_nhl_player_stats(player_stats_file)
    injuries_df = load_nhl_injury_data(injury_data_file)
    integrated_data = pd.merge(stats_df, injuries_df, how='left', left_on='Player', right_on='player')
    integrated_data = integrated_data[integrated_data['injuryStatus'].isnull()]
    return integrated_data

def main():
    print("✅ Files loaded successfully")

    while True:
        print("\nSelect Sport:")
        print("1️⃣ College Basketball (CBB)")
        print("2️⃣ NBA")
        print("3️⃣ NHL")
        print("4️⃣ Exit")
        choice = input("Choose an option (1/2/3/4): ").strip()

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
            print("👋 Exiting... Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please select 1, 2, 3, or 4.")

if __name__ == "__main__":
    import os
    main()