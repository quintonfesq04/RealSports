import pandas as pd
import joblib
from sklearn.preprocessing import StandardScaler

# Load the trained ML model
MODEL_PATH = "ml_player_model.pkl"
model, scaler = joblib.load(MODEL_PATH)

STAT_CATEGORIES = {
    "PPG": "avgPoints",
    "APG": "avgAssists",
    "RPG": "avgRebounds",
    "3PM": "avgThreePointFieldGoalsMade"
}

def dynamic_category_adjustment(df, stat_choice, target_value):
    """Dynamically adjusts category thresholds to ensure exactly 3 players in each group."""
    if df.empty:
        print("⚠️ No players available for categorization.")
        return df

    # Compute success rate
    df["Success_Rate"] = ((df[stat_choice] / target_value) * 100).round(1)

    # **Special Case: 3PM with Target 1**
    if stat_choice == "3PM" and target_value == 1:
        df.loc[df["Success_Rate"] >= 175, "Category"] = "🟡 Favorite"
        df.loc[(df["Success_Rate"] >= 105) & (df["Success_Rate"] < 175), "Category"] = "🟢 Best Bet"
        df.loc[(df["Success_Rate"] >= 70) & (df["Success_Rate"] < 105), "Category"] = "🔴 Underdog"
    else:
        # Default categorization for other stats
        df.loc[df["Success_Rate"] >= 100, "Category"] = "🟡 Favorite"
        df.loc[(df["Success_Rate"] >= 80) & (df["Success_Rate"] < 100), "Category"] = "🟢 Best Bet"
        df.loc[df["Success_Rate"] < 70, "Category"] = "🔴 Underdog"

    # Remove duplicates before filtering
    df = df.drop_duplicates(subset=["Player", "Team"])

    # **Ensure Exactly 3 Players Per Category**
    red_players = df[df["Category"] == "🔴 Underdog"].nlargest(3, "Success_Rate")
    if len(red_players) < 3:
        extra_reds = df[df["Success_Rate"] < 80].nlargest(3 - len(red_players), "Success_Rate")
        red_players = pd.concat([red_players, extra_reds]).drop_duplicates().nlargest(3, "Success_Rate")

    green_players = df[df["Category"] == "🟢 Best Bet"].nlargest(3, "Success_Rate")
    if len(green_players) < 3:
        extra_greens = df[df["Success_Rate"] >= 80].nlargest(3 - len(green_players), "Success_Rate")
        green_players = pd.concat([green_players, extra_greens]).drop_duplicates().nlargest(3, "Success_Rate")

    yellow_players = df[df["Category"] == "🟡 Favorite"].nlargest(3, "Success_Rate")
    if len(yellow_players) < 3:
        extra_yellows = df[df["Success_Rate"] >= 100].nlargest(3 - len(yellow_players), "Success_Rate")
        yellow_players = pd.concat([yellow_players, extra_yellows]).drop_duplicates().nlargest(3, "Success_Rate")

    # **Ensure Exactly 9 Unique Players**
    final_df = pd.concat([red_players, green_players, yellow_players]).drop_duplicates(subset=["Player", "Team"]).reset_index(drop=True)

    # **Sort: Green (descending) → Yellow (descending) → Red (descending)**
    final_df = pd.concat([
        final_df[final_df["Category"] == "🟢 Best Bet"].sort_values(by="Success_Rate", ascending=False),
        final_df[final_df["Category"] == "🟡 Favorite"].sort_values(by="Success_Rate", ascending=False),
        final_df[final_df["Category"] == "🔴 Underdog"].sort_values(by="Success_Rate", ascending=False)
    ]).reset_index(drop=True)

    return final_df.head(9)  # Ensure final output is exactly 9 players

# Load data at startup
df = pd.read_excel("cbb_player_stats.xlsx")

def find_best_stat_players():
    """Finds the best players for a given stat using pre-loaded data."""
    teams = input("Enter team abbreviations separated by commas: ").replace(" ", "").upper().split(",")

    # Filter data
    filtered_df = df[df["Team"].isin(teams)].copy()
    
    print("\nAvailable stats to analyze:")
    for key in STAT_CATEGORIES:
        print(f"- {key}")

    stat_choice = input("Enter stat to sort by (PPG, APG, RPG, 3PM): ").strip().upper()
    if stat_choice not in STAT_CATEGORIES:
        print("❌ Invalid stat choice. Defaulting to RPG.")
        stat_choice = "RPG"

    target_value = float(input(f"Enter target {stat_choice} value: "))

    # Sort and categorize
    filtered_df = filtered_df.sort_values(by=stat_choice, ascending=False)
    categorized_df = dynamic_category_adjustment(filtered_df, stat_choice, target_value)

    print(f"\n🏀 **ML-Powered Player Betting Recommendations for {stat_choice}** 🏀")
    print(categorized_df.to_string(index=False))

def main():
    """Main function to run the program."""
    while True:
        find_best_stat_players()

if __name__ == "__main__":
    main()