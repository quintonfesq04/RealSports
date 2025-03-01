import requests
import pandas as pd

def fetch_data():
    base_url = "https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-college-basketball/statistics/byathlete"
    
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    while True:
        print("\nChoose a mode:")
        print("1 - Compare Two Teams (Sort by One Stat)")
        print("2 - Find Top 10 Players for Each Stat (Among Multiple Teams)")
        print("Type 'exit' to quit the program.")
        mode = input("Enter 1 or 2: ").strip()

        if mode.lower() == "exit":
            print("🚪 Exiting program. Goodbye!")
            break

        elif mode == "1":
            while True:
                team1 = input("Enter first team abbreviation (or 'exit' to return to mode selection): ").strip().upper()
                if team1.lower() == "exit":
                    break

                team2 = input("Enter second team abbreviation: ").strip().upper()
                if team2.lower() == "exit":
                    break

                team_filter = [team1, team2]

                stat_options = {
                    "PPG": ("Points Per Game", "offensive", 0),
                    "APG": ("Assists Per Game", "offensive", 10),
                    "RPG": ("Rebounds Per Game", "general", 12),
                    "3PM": ("Three-Pointers Made Per Game", "offensive", 4)
                }

                print("\nAvailable stats to sort by:")
                for key, value in stat_options.items():
                    print(f"- {key}: {value[0]}")

                stat_choice = input("\nEnter stat to sort by (PPG, APG, RPG, 3PM): ").strip().upper()
                if stat_choice not in stat_options:
                    print("❌ Invalid stat choice. Defaulting to PPG.")
                    stat_choice = "PPG"

                stat_name, stat_category, stat_index = stat_options[stat_choice]

                all_players = []
                page = 1
                limit = 50

                while True:
                    url = f"{base_url}?region=us&lang=en&contentorigin=espn&isqualified=true&page={page}&limit={limit}&sort=offensive.avgPoints:desc"
                    response = requests.get(url, headers=headers)

                    if response.status_code != 200:
                        break

                    data = response.json()
                    players = data.get("athletes", [])
                    if not players:
                        break

                    for player in players:
                        athlete_info = player.get("athlete", {})
                        name = athlete_info.get("displayName", "Unknown")

                        teams_info = athlete_info.get("teams", [{}])
                        team_abbreviation = teams_info[0].get("abbreviation", "Unknown")

                        if team_abbreviation not in team_filter:
                            continue

                        stats = {category["name"]: category.get("totals", []) for category in player.get("categories", [])}
                        stat_value = float(stats.get(stat_category, [0])[stat_index])

                        all_players.append([name, stat_value])

                    page += 1

                df = pd.DataFrame(all_players, columns=["Player", stat_choice])
                df = df.sort_values(by=stat_choice, ascending=False)

                print(f"\n🏀 **Top Players from {team1} and {team2}, Sorted by {stat_name}** 🏀")
                print(df.to_string(index=False))

        elif mode == "2":
            while True:
                teams_input = input("Enter team abbreviations (comma-separated) or 'exit' to return to mode selection: ").strip()
                if teams_input.lower() == "exit":
                    break

                team_filter = [team.strip().upper() for team in teams_input.split(",")]

                all_players = []
                page = 1
                limit = 50

                while True:
                    url = f"{base_url}?region=us&lang=en&contentorigin=espn&isqualified=true&page={page}&limit={limit}&sort=offensive.avgPoints:desc"
                    response = requests.get(url, headers=headers)

                    if response.status_code != 200:
                        break

                    data = response.json()
                    players = data.get("athletes", [])
                    if not players:
                        break

                    for player in players:
                        athlete_info = player.get("athlete", {})
                        name = athlete_info.get("displayName", "Unknown")

                        teams_info = athlete_info.get("teams", [{}])
                        team_abbreviation = teams_info[0].get("abbreviation", "Unknown")

                        if team_abbreviation not in team_filter:
                            continue

                        stats = {category["name"]: category.get("totals", []) for category in player.get("categories", [])}

                        player_stats = {
                            "PPG": float(stats.get("offensive", [0] * 13)[0]),
                            "APG": float(stats.get("offensive", [0] * 13)[10]),
                            "RPG": float(stats.get("general", [0] * 13)[12]),
                        }

                        all_players.append([name, team_abbreviation, player_stats["PPG"], player_stats["APG"], player_stats["RPG"]])

                    page += 1

                df = pd.DataFrame(all_players, columns=["Player", "Team", "PPG", "APG", "RPG"])
                
                for stat in ["PPG", "APG", "RPG"]:
                    df_sorted = df.sort_values(by=stat, ascending=False).head(10)
                    print(f"\n🏀 **Top 10 Players for {stat} Among Selected Teams** 🏀")
                    print(df_sorted.to_string(index=False))
        
        else:
            print("❌ Invalid mode. Please enter 1 or 2.")

if __name__ == "__main__":
    fetch_data()
