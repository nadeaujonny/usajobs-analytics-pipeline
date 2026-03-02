"""
collect.py — Stage 1: Data Collection
Calls the USAJobs Search API for each keyword, handles pagination and retries,
and saves raw JSON responses to data/raw/
"""

import requests
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
API_KEY = os.getenv("USAJOBS_API_KEY")
EMAIL = os.getenv("USAJOBS_EMAIL")
BASE_URL = "https://data.usajobs.gov/api/Search"
RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")

HEADERS = {
    "Authorization-Key": API_KEY,
    "User-Agent": EMAIL,
    "Host": "data.usajobs.gov"
}

# Search keywords for data/analytics/tech roles
KEYWORDS = [
    "data analyst",
    "data scientist",
    "data engineer",
    "business intelligence",
    "business analyst",
    "statistician",
    "program analyst",
    "management analyst",
    "IT specialist",
]

RESULTS_PER_PAGE = 500  # Max allowed by the API


def fetch_keyword(keyword, max_retries=3, delay=5):
    """
    Fetch all job postings for a given keyword.
    Handles pagination and retries.
    Returns a list of job posting items.
    """
    all_items = []
    page = 1

    while True:
        params = {
            "Keyword": keyword,
            "ResultsPerPage": RESULTS_PER_PAGE,
            "Page": page,
        }

        # Retry logic
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                break  # Success — exit retry loop
            except (requests.RequestException, json.JSONDecodeError) as e:
                print(f"    Attempt {attempt}/{max_retries} failed for '{keyword}' page {page}: {e}")
                if attempt < max_retries:
                    time.sleep(delay)
                else:
                    print(f"    SKIPPING '{keyword}' page {page} after {max_retries} failed attempts.")
                    return all_items  # Return whatever we collected so far

        # Extract job items from response
        items = data.get("SearchResult", {}).get("SearchResultItems", [])
        if not items:
            break  # No more results — exit pagination loop

        all_items.extend(items)

        # Check if there are more pages
        total_results = int(data.get("SearchResult", {}).get("SearchResultCountAll", 0))
        fetched_so_far = page * RESULTS_PER_PAGE

        print(f"    Page {page}: got {len(items)} items (total available: {total_results})")

        if fetched_so_far >= total_results:
            break  # All pages collected

        page += 1
        time.sleep(1)  # Be polite to the API between pages

    return all_items


def collect_all():
    """
    Main collection function.
    Loops through all keywords, collects results, and saves raw JSON.
    Returns a summary dict.
    """
    # Create raw data directory if it doesn't exist
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    all_results = {}
    total_collected = 0

    print(f"=== Collection started at {timestamp} ===\n")

    for keyword in KEYWORDS:
        print(f"Collecting: '{keyword}'...")
        items = fetch_keyword(keyword)
        all_results[keyword] = items
        total_collected += len(items)
        print(f"  → Collected {len(items)} postings for '{keyword}'\n")

    # Save all results to a single JSON file
    output_file = os.path.join(RAW_DATA_DIR, f"raw_{timestamp}.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    print(f"=== Collection complete ===")
    print(f"Total postings collected: {total_collected}")
    print(f"Saved to: {output_file}")

    return {
        "timestamp": timestamp,
        "total_collected": total_collected,
        "output_file": output_file,
        "by_keyword": {kw: len(items) for kw, items in all_results.items()}
    }


# Allow running this script directly for testing
if __name__ == "__main__":
    summary = collect_all()
    print("\nSummary by keyword:")
    for kw, count in summary["by_keyword"].items():
        print(f"  {kw}: {count}")