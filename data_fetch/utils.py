import os
import pandas as pd
from datetime import datetime, timedelta
import pytz
import re
from collections import Counter
from yahoo_fin import stock_info as si
import yfinance as yf
from collections import defaultdict


def log_rate_limits(reddit):
    limits = reddit.auth.limits
    print(f"🚦 API Requests Left: {limits['remaining']}, Resets in: {limits['reset_timestamp']} sec")

def fetch_comments(interval_minutes, reddit, subreddit, ISRAEL_TZ):
    """
    Fetch comments for a specific date in Israel time based on selected interval.
    """

    target_time = datetime.now(pytz.utc).astimezone(ISRAEL_TZ).replace(second=0, microsecond=0)
    end_time_ist = target_time
    start_time_ist = end_time_ist - timedelta(minutes=interval_minutes)
    print(f"\n🔄 Fetching all comments between {start_time_ist.strftime('%Y-%m-%d %H:%M:%S')} and {end_time_ist.strftime('%Y-%m-%d %H:%M:%S')} (Israel Time)...")

    # Convert to UTC timestamps
    start_time_utc = start_time_ist.astimezone(pytz.utc).timestamp()
    end_time_utc = end_time_ist.astimezone(pytz.utc).timestamp()

    comments = []

    for comment in subreddit.comments(limit=None):
        comment_time = datetime.fromtimestamp(comment.created_utc, pytz.utc).astimezone(ISRAEL_TZ)
        if start_time_utc <= comment.created_utc <= end_time_utc:
            comment_time_ist = comment_time.astimezone(ISRAEL_TZ)
            comments.append({
                'id': str(comment.id),
                'parent_id': str(comment.parent_id),
                'timestamp': datetime.fromtimestamp(comment.created_utc, pytz.utc).astimezone(ISRAEL_TZ).strftime('%Y-%m-%d %H:%M:%S'), 
                'subreddit': str(comment.subreddit), 
                'link': os.path.join(comment.link_permalink, comment.id), 
                'author': str(comment.author), 
                'author_fullname': str(comment.author_fullname), 
                'author_is_blocked': comment.author_is_blocked, 
                'score': comment.score, 
                'ups': comment.ups, 
                'downs': comment.downs, 
                'likes': comment.likes, 
                'body': comment.body, 
                'over_18': comment.over_18
                })
    
    print(f"✅ Found {len(comments)} comments between {start_time_ist.strftime('%Y-%m-%d %H:%M:%S')} and {end_time_ist.strftime('%Y-%m-%d %H:%M:%S')} (Israel Time).")
    log_rate_limits(reddit)
    return comments

# Function to extract stock tickers from comments
def extract_tickers(comments, TICKER_PATTERN):
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

def bucket_by_time(tickers_with_time, ISRAEL_TZ, interval_minutes=5):
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

def analyze_ticker_trends(comments, ISRAEL_TZ, interval_minutes=5):
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
