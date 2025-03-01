import pandas as pd

# Load the poll data
df = pd.read_csv("notion_poll_data.csv")

# Process the data (example: calculate total votes)
total_votes = df['Votes'].sum()
print(f"Total Votes: {total_votes}")

# Save the processed data
df.to_csv("processed_poll_data.csv", index=False)
print("💾 Saved processed poll data to CSV")
