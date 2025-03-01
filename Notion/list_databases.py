import requests

NOTION_TOKEN = "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"
DATABASE_ID = "18e71b1c663e80cdb8a0fe5e8aeee5a9"
headers = {
    "Authorization": "Bearer " + NOTION_TOKEN,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

def retrieve_database_info():
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print("Database information retrieved successfully.")
        print(response.json())
    else:
        print(f"Error: {response.status_code}")
        print(response.json())

retrieve_database_info()
