import os
import praw
import pytz
import pandas as pd
from datetime import datetime
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import argparse

from utils import fetch_comments, analyze_ticker_trends
from credentials import CLIENT_ID, CLIENT_SECRET, USER_AGENT, TRADING_FOLDER_ID

CREDS_PATH = os.path.join(os.getcwd(), 'Trading Access.json')

# Load Google Drive API credentials
def authenticate_google_services(creds_path):
    """Authenticate with Google Sheets and Drive API."""
    scope = ["https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]

    creds = Credentials.from_service_account_file(creds_path, scopes=scope)
    gspread_client = gspread.authorize(creds)
    drive_service = build('drive', 'v3', credentials=creds)

    return gspread_client, drive_service

def create_spreadsheet_in_folder(sheet_name, folder_id, creds_path):
    """
    Creates a new Google Spreadsheet inside a specific Google Drive folder.
    """
    gspread_client, drive_service = authenticate_google_services(creds_path)

    # Create a blank spreadsheet
    file_metadata = {
        'name': sheet_name,
        'mimeType': 'application/vnd.google-apps.spreadsheet',
        'parents': [folder_id]  # Assign the "Trading" folder as the parent
    }

    spreadsheet = drive_service.files().create(
        body=file_metadata,
        fields='id'
    ).execute()

    sheet_id = spreadsheet.get('id')
    print(f"✅ Created spreadsheet '{sheet_name}' in Trading folder: https://docs.google.com/spreadsheets/d/{sheet_id}")

    return gspread_client.open_by_key(sheet_id)

def get_existing_spreadsheet(sheet_name, folder_id, creds_path):
    """
    Check if a Google Spreadsheet with the given name exists in the specified folder.
    Returns the spreadsheet object if found, otherwise returns None.
    """
    _, drive_service = authenticate_google_services(creds_path)

    # Search for the file in the specific folder
    query = f"name='{sheet_name}' and mimeType='application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents"
    
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if files:
        file_id = files[0]["id"]
        print(f"📄 Spreadsheet '{sheet_name}' already exists: https://docs.google.com/spreadsheets/d/{file_id}")
        return file_id  # Return the existing spreadsheet ID
    else:
        return None  # File does not exist


def update_google_sheet(comments, target_date, creds_path):
    """
    Store comments in Google Sheets (Google Drive). 
    Creates a new sheet for the date if it doesn't exist, otherwise appends data.
    """

    sheet_name = 'pennystocks_subreddit_' + target_date
    spreadsheet_id = get_existing_spreadsheet(sheet_name, TRADING_FOLDER_ID, CREDS_PATH)

    if spreadsheet_id == None:
        print(f"❌ Spreadsheet '{sheet_name}' not found. Creating a new one...")
        spreadsheet = create_spreadsheet_in_folder(sheet_name, TRADING_FOLDER_ID, CREDS_PATH)  # Create a new spreadsheet
    else:
        gspread_client, _ = authenticate_google_services(creds_path)
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)

    # Try opening an existing spreadsheet or create a new one
    try:
        sheet = spreadsheet.worksheet(target_date)
        existing_data = sheet.get_all_values()
        existing_ids = set(row[0] for row in existing_data[1:])  # Get all existing comment IDs, skipping the header
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=target_date, rows="1000", cols="12")
        sheet.append_row(["ID", "Parent ID", "Timestamp", "Subreddit", "Link", "Author", 
                          "Author Fullname", "Blocked", "Score", "Ups", "Downs", "Likes", 
                          "Body", "NSFW"])
        existing_ids = set()

    # Convert list of dictionaries to rows
    new_rows = [[
        comment["id"], comment["parent_id"], comment["timestamp"], comment["subreddit"],
        comment["link"], comment["author"], comment["author_fullname"], comment["author_is_blocked"],
        comment["score"], comment["ups"], comment["downs"], comment["likes"],
        comment["body"], comment["over_18"]
    ] for comment in comments if comment["id"] not in existing_ids]

    # Append new data
    if new_rows:
        sheet.append_rows(new_rows, value_input_option="RAW")
        print(f"📌 Updated Google Sheet: {sheet_name} -> {target_date} with {len(new_rows)} new comments.")
    else:
        print(f"✅ No new comments to add. All comments already exist in the sheet.")

# Initialize Reddit API
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT,
)

# Select subreddit
subreddit = reddit.subreddit("pennystocks")

# Stock ticker pattern (U.S. stock tickers are usually all caps, 1-5 letters)
TICKER_PATTERN = r'\b[A-Z]{1,5}\b'

ISRAEL_TZ = pytz.timezone("Asia/Jerusalem")

# Main function
def main(interval_minutes=5):
    comments = fetch_comments(interval_minutes, reddit, subreddit, ISRAEL_TZ)

    if comments:
        # Extract target date
        target_date = datetime.now(ISRAEL_TZ).strftime('%Y-%m-%d')

        # Update Google Sheets with new comments
        update_google_sheet(comments, target_date, CREDS_PATH)
    else:
        print("⚠️ No new comments found.")

# Run script
if __name__ == "__main__":
    ## run it using python data_fetch/fetch_reddit_comments.py --interval 1 ###

    # Use argparse to allow date specification from command line
    parser = argparse.ArgumentParser(description="Fetch Reddit comments and analyze stock ticker mentions.")
    parser.add_argument("--interval", type=int, default=1, help="Time interval in minutes for bucketing data (default: 1).")

    args = parser.parse_args()
    main(args.interval)