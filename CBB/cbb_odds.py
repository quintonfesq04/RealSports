import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import StandardScaler

# Load the trained ML model
MODEL_PATH = "ml_player_model.pkl"
model, scaler = joblib.load(MODEL_PATH)

# Load team stats from the Excel file
team_stats_df = pd.read_excel("team_stats.xlsx")

def predict_game_outcomes(team1, team2):
    """Predicts game outcomes including win probability, spread, and total score."""
    team1_stats = team_stats_df[team_stats_df["Team"] == team1]
    team2_stats = team_stats_df[team_stats_df["Team"] == team2]

    if team1_stats.empty or team2_stats.empty:
        print(f"❌ Error: Missing data for {team1} or {team2}")
        return None

    # Extract relevant stats
    team1_ppg = team1_stats["PPG"].values[0]
    team1_opp_ppg = team1_stats["Opponent PPG"].values[0]
    team2_ppg = team2_stats["PPG"].values[0]
    team2_opp_ppg = team2_stats["Opponent PPG"].values[0]

    # Compute expected score
    team1_expected_score = (team1_ppg + team2_opp_ppg) / 2
    team2_expected_score = (team2_ppg + team1_opp_ppg) / 2

    # Compute predicted spread
    predicted_spread = team1_expected_score - team2_expected_score

    # Compute total points (over/under estimate)
    predicted_total = team1_expected_score + team2_expected_score

    # Compute win probability (logistic function approximation)
    win_prob = 1 / (1 + np.exp(-0.1 * predicted_spread))  # Adjusted logistic function

    # Convert to moneyline equivalent
    moneyline = round(-100 / win_prob) if win_prob > 0.5 else round(100 / (1 - win_prob))

    print(f"\n🏀 **Game Prediction: {team1} vs {team2}** 🏀")
    print(f"🔹 **Predicted Score:** {team1} {team1_expected_score:.1f} - {team2} {team2_expected_score:.1f}")
    print(f"📈 **Predicted Spread:** {team1} by {abs(predicted_spread):.1f}")
    print(f"📊 **Projected Total Score:** {predicted_total:.1f}")
    print(f"💰 **Win Probability:** {team1} ({win_prob*100:.1f}%) | Moneyline Estimate: {moneyline}")

def main():
    """Main function to get user input and compute game predictions."""
    while True:
        teams = input("Enter two team names separated by a comma: ").strip().split(",")
        if len(teams) != 2:
            print("❌ Please enter exactly two team names.")
            continue
        team1, team2 = teams[0].strip(), teams[1].strip()
        predict_game_outcomes(team1, team2)

if __name__ == "__main__":
    main()