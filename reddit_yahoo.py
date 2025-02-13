import praw
import re
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict
from yahoo_fin import stock_info as si
import yfinance as yf
import pandas as pd
import os
import pytz
import argparse

from credentials import CLIENT_ID, CLIENT_SECRET, USER_AGENT



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

# Define timezones
UTC = pytz.utc
ISRAEL_TZ = pytz.timezone("Asia/Jerusalem")

def log_rate_limits():
    limits = reddit.auth.limits
    print(f"🚦 API Requests Left: {limits['remaining']}, Resets in: {limits['reset_timestamp']} sec")

# Function to fetch comments from today
def fetch_today_comments(target_date):
    """
    Fetch comments for a specific date in Israel time (00:00 to 23:59).
    """
    print(f"\n🔄 Fetching all comments for {target_date} (Israel Time)...")
    
    target_date_dt = datetime.strptime(target_date, "%Y-%m-%d")
    start_time_ist = ISRAEL_TZ.localize(datetime(target_date_dt.year, target_date_dt.month, target_date_dt.day, 0, 0, 0))
    end_time_ist = start_time_ist + timedelta(days=1)

    # Convert to UTC timestamps
    start_time_utc = start_time_ist.astimezone(UTC).timestamp()
    end_time_utc = end_time_ist.astimezone(UTC).timestamp()

    comments = []
    
    for comment in subreddit.comments(limit=None):
        comment_time = datetime.fromtimestamp(comment.created_utc, UTC)
        if start_time_utc <= comment.created_utc <= end_time_utc:
            comment_time_ist = comment_time.astimezone(ISRAEL_TZ)
            comments.append((comment.body, comment_time_ist))
    
    print(f"✅ Found {len(comments)} comments for {target_date} (Israel Time).")
    log_rate_limits()
    return comments

# Function to extract stock tickers from comments
def extract_tickers(comments):
    tickers_with_time = []
    
    for text, timestamp in comments:
        words = set(re.findall(TICKER_PATTERN, text))  # Remove duplicates within each comment
        for word in words:
            tickers_with_time.append((word, timestamp))
    
    return tickers_with_time

def validate_tickers(ticker_list):
    """Validate extracted tickers using Yahoo Finance"""
    all_tickers = set(si.tickers_dow() + si.tickers_sp500() + si.tickers_nasdaq())
    valid_tickers = set()

    for ticker in ticker_list:
        if ticker in all_tickers:
            valid_tickers.add(ticker)
        else:
            stock = yf.Ticker(ticker)
            if stock.info and stock.info.get("shortName"):
                valid_tickers.add(ticker)
    
    return valid_tickers

def bucket_by_time(tickers_with_time, interval_minutes=5):
    """Group ticker mentions by date and time intervals"""

    # Create an empty dictionary where each key is an interval
    time_buckets = defaultdict(list)

    for ticker, timestamp in tickers_with_time:
        # Extract the date
        comment_date = timestamp.date().isoformat()  # "YYYY-MM-DD"

        # Calculate minutes since start of the day in Israel time
        midnight = datetime(timestamp.year, timestamp.month, timestamp.day, tzinfo=ISRAEL_TZ)
        minutes_since_start = int((timestamp - midnight).total_seconds() // 60)
        bucket = (minutes_since_start // interval_minutes) * interval_minutes  # Round down to nearest interval

        time_buckets[(comment_date, bucket)].append(ticker)

    return time_buckets

def analyze_ticker_trends(comments, interval_minutes=5):
    """Analyze stock ticker mentions over time"""
    tickers_with_time = extract_tickers(comments)
    valid_tickers = validate_tickers([ticker for ticker, _ in tickers_with_time])
    
    time_buckets = bucket_by_time(tickers_with_time, interval_minutes)

    rows = []
    
    # Count mentions in each time bucket
    print(f"\n📊 Stock Ticker Mentions in {interval_minutes}-Minute Intervals:")
    for (date, bucket) in sorted(time_buckets.keys()):
        start_time = (datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=ISRAEL_TZ) + timedelta(minutes=bucket)).strftime("%H:%M")
        end_time = (datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=ISRAEL_TZ) + timedelta(minutes=bucket + interval_minutes)).strftime("%H:%M")

        ticker_counts = Counter(time_buckets[(date, bucket)])
        filtered_counts = {ticker: count for ticker, count in ticker_counts.items() if ticker in valid_tickers}

        for ticker, count in filtered_counts.items():
            rows.append({"date": date, "start_time": start_time, "end_time": end_time, "ticker": ticker, "mentions": count})

    # Convert to DataFrame
    results = pd.DataFrame(rows, columns=["date", "start_time", "end_time", "ticker", "mentions"])

    return results

# Main function
def main(target_date, interval_minutes=5):
    comments = fetch_today_comments(target_date)
    df_ticker_mentions = analyze_ticker_trends(comments, interval_minutes=5)
    df_ticker_mentions["start_datetime"] = pd.to_datetime(df_ticker_mentions["date"] + " " + df_ticker_mentions["start_time"])
    df_ticker_mentions["end_datetime"] = pd.to_datetime(df_ticker_mentions["date"] + " " + df_ticker_mentions["end_time"])
    df_ticker_mentions.sort_values(by=['date', 'start_time', 'end_time', 'ticker'], inplace=True)

    start_time_str = df_ticker_mentions["start_datetime"].min().strftime("%Y-%m-%d_%H-%M-%S")
    end_time_str = df_ticker_mentions["end_datetime"].max().strftime("%Y-%m-%d_%H-%M-%S")

    data_dir = os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)), 'data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    file_path = os.path.join(data_dir, f"ticker_mentions_{start_time_str}_to_{end_time_str}.csv.gz")
    df_ticker_mentions.to_csv(file_path, compression='gzip', index=False)

# Run script
if __name__ == "__main__":
    # ## run it using python reddit_yahoo.py 2025-02-12 --interval 10 ###

    # target_date = '2025-02-12'
    # interval_minutes=5
    # main(target_date, interval_minutes)
    # Use argparse to allow date specification from command line
    parser = argparse.ArgumentParser(description="Fetch Reddit comments and analyze stock ticker mentions.")
    parser.add_argument("date", type=str, help="Date in YYYY-MM-DD format for fetching comments.")
    parser.add_argument("--interval", type=int, default=5, help="Time interval in minutes for bucketing data (default: 5).")

    args = parser.parse_args()
    main(args.date, args.interval)
