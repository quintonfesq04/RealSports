import pandas as pd
import joblib
from sklearn.preprocessing import StandardScaler

# Load the trained NBA ML model
MODEL_PATH = "nba_ml_model.pkl"
model, scaler = joblib.load(MODEL_PATH)

# Stat categories
STAT_CATEGORIES = {
    "PPG": "avgPoints",
    "APG": "avgAssists",
    "RPG": "RPG",
    "3PM": "avgThreePointFieldGoalsMade"
}

# -------------------------------------------
# ✅ Dynamic Category Adjustment Function
# -------------------------------------------
def dynamic_category_adjustment(df, stat_choice, target_value):
    """Adjust category thresholds dynamically for NBA ML model predictions."""
    if df.empty:
        print("⚠️ No players available for categorization.")
        return df

    # ✅ Normalize RPG values if necessary
    if stat_choice == "RPG":
        df[stat_choice] = df[stat_choice].apply(lambda x: x / 10 if x > 20 else x)

    # 🔢 Calculate Success Rate
    df["Success_Rate"] = ((df[stat_choice] / target_value) * 100).round(1)

    # ✅ Categorize players
    df.loc[df["Success_Rate"] >= 90, "Category"] = "🟢 Best Bet"
    df.loc[df["Success_Rate"] > 120, "Category"] = "🟡 Favorite"
    df.loc[df["Success_Rate"] < 90, "Category"] = "🔴 Underdog"

    # ✅ Ensure correct category count (3 players each)
    red_players = df[df["Category"] == "🔴 Underdog"].nlargest(3, "Success_Rate")
    green_players = df[df["Category"] == "🟢 Best Bet"].nlargest(3, "Success_Rate")
    yellow_players = df[df["Category"] == "🟡 Favorite"].nlargest(3, "Success_Rate")

    # Fill missing spots if needed
    if len(red_players) < 3:
        red_players = pd.concat([red_players, df[df["Success_Rate"] < 90].nlargest(3 - len(red_players), "Success_Rate")])

    if len(green_players) < 3:
        green_players = pd.concat([green_players, df[df["Success_Rate"] >= 90].nlargest(3 - len(green_players), "Success_Rate")])

    if len(yellow_players) < 3:
        yellow_players = pd.concat([yellow_players, df[df["Success_Rate"] > 120].nlargest(3 - len(yellow_players), "Success_Rate")])

    # ✅ Combine results and return final dataframe
    final_df = pd.concat([green_players, yellow_players, red_players]).reset_index(drop=True)
    return final_df.head(9)


# -------------------------------------------
# ✅ Load NBA Player Stats
# -------------------------------------------
df = pd.read_csv("/Users/Q/Documents/Documents/RealSports/nba_player_stats.csv")


# -------------------------------------------
# ✅ Find Best Players Function
# -------------------------------------------
def find_best_stat_players():
    """Finds the best NBA players for a given stat."""
    teams = input("\nEnter team abbreviations separated by commas: ").replace(" ", "").upper().split(",")
    filtered_df = df[df["Team"].isin(teams)].copy()

    # ✅ Display available stats
    print("\nAvailable stats to analyze:")
    for key in STAT_CATEGORIES:
        print(f"- {key}")

    # ✅ Choose a stat
    stat_choice = input("\nEnter stat to sort by (PPG, APG, RPG, 3PM): ").strip().upper()
    if stat_choice not in STAT_CATEGORIES:
        print("❌ Invalid stat choice. Defaulting to RPG.")
        stat_choice = "RPG"

    # ✅ Input target value
    target_value = float(input(f"\nEnter target {stat_choice} value: "))

    # ✅ Sort and categorize players
    filtered_df = filtered_df.sort_values(by=STAT_CATEGORIES[stat_choice], ascending=False)
    categorized_df = dynamic_category_adjustment(filtered_df, STAT_CATEGORIES[stat_choice], target_value)

    # ✅ Print final results
    print(f"\n🏀 **ML-Powered Player Betting Recommendations for {stat_choice}** 🏀")
    print(categorized_df[["Player", "Team", STAT_CATEGORIES[stat_choice], "Success_Rate", "Category"]].to_string(index=False))


# -------------------------------------------
# ✅ Main Function
# -------------------------------------------
def main():
    """Main function to run the NBA ML script."""
    while True:
        print("\n1️⃣ Analyze Player Stats\n2️⃣ Exit")
        choice = input("\nChoose an option (1/2): ").strip()

        if choice == "1":
            find_best_stat_players()
        elif choice == "2":
            print("\n👋 Exiting program.")
            break
        else:
            print("\n❌ Invalid choice. Please enter 1 or 2.")


# -------------------------------------------
# ✅ Run the Program
# -------------------------------------------
if __name__ == "__main__":
    main()