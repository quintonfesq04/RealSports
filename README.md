The goal behind this project was to help speed up my research process when filling out polls on RealSports.io

How to start:
1. download all files - at minimum download all python files
2. run the following python codes to get the player stats:
   - cbb_database.py
   - nba_database.py
   - nhl_database.py
   - mlb_database.py
3. once you run those codes, then go to Universal_Sports_Analyzer.py --this file is a Machine Learning python code that analyzes the stats based on the teams you provide. When you run the code you will be prompted to choose the sport you want,
   followed by the team name(s), next you will enter the stat you want, and lastly you will enter your "target value" - this value is the number you want your play to hit (i.e., Player to get 6+ rebounds), lastly the code will output what I call
   the green yellow red method, which is around 9 players broken into three categories. Here are the plays and their meanings:
     🟢 plays, these are your best bets - a player that is not a clear favorite or underdog
     🟡 plays, these are your favorites - a player that is likely to win, but the karma is lower
     🔴 plays, these are your underdogs - a player that is a long shot with a high payout if it hits

I hope anyone who uses RealSports likes this code and if you have any issues feel free to message me on Real @quintonfesq and I will try to help
