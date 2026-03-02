"""
seed_historical.py — Seed the database with historical job posting data
Uses the USAJobs Historical JOA API (no authentication required)
to backfill the database with older postings.
"""

import requests
import json
import time
import os
from datetime import datetime
from transform import parse_salary, classify_role
from load import load_records


HISTORICAL_URL = "https://data.usajobs.gov/api/historicjoa"
PAGE_SIZE = 500

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

# Date ranges to pull (month by month for the past year)
DATE_RANGES = []
for year in [2025]:
    for month in range(1, 13):
        start = f"{year}-{month:02d}-01"
        if month == 12:
            end = f"{year + 1}-01-01"
        else:
            end = f"{year}-{month + 1:02d}-01"
        DATE_RANGES.append((start, end))

# Also add Jan and Feb 2026
DATE_RANGES.append(("2026-01-01", "2026-02-01"))
DATE_RANGES.append(("2026-02-01", "2026-03-01"))


def flatten_historical_job(item, keyword):
    """Flatten a historical API job record into our schema."""
    salary_min = parse_salary(str(item.get("MinimumPay", "")))
    salary_max = parse_salary(str(item.get("MaximumPay", "")))

    # Annualize hourly
    pay_interval = item.get("PayIntervalDescription", "")
    if pay_interval and "hour" in pay_interval.lower():
        if salary_min:
            salary_min = round(salary_min * 2087, 2)
        if salary_max:
            salary_max = round(salary_max * 2087, 2)

    salary_mid = None
    if salary_min and salary_max:
        salary_mid = round((salary_min + salary_max) / 2, 2)

    position_title = item.get("PositionTitle", "")

    return {
        "control_number": item.get("USAJOBSControlNumber", ""),
        "position_title": position_title,
        "organization_name": item.get("AgencyName", ""),
        "department_name": item.get("DepartmentName", ""),
        "sub_agency": item.get("SubAgencyName", ""),
        "job_grade": item.get("Grade", ""),
        "pay_plan": item.get("PayPlan", ""),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_mid": salary_mid,
        "salary_interval": pay_interval,
        "location_name": item.get("PositionLocation", ""),
        "city": "",
        "state": item.get("StateCode", ""),
        "country": "United States",
        "latitude": None,
        "longitude": None,
        "position_url": f"https://www.usajobs.gov/job/{item.get('USAJOBSControlNumber', '')}",
        "open_date": item.get("PositionOpenDate", ""),
        "close_date": item.get("PositionCloseDate", ""),
        "role_category": classify_role(position_title, keyword),
        "search_keyword": keyword,
        "collected_date": datetime.now().strftime("%Y-%m-%d"),
    }


def fetch_historical(keyword, start_date, end_date, max_retries=3, delay=5):
    """Fetch historical postings for a keyword within a date range."""
    all_items = []
    page = 1

    while True:
        params = {
            "Title": keyword,
            "StartPositionOpenDate": start_date,
            "EndPositionOpenDate": end_date,
            "PageSize": PAGE_SIZE,
            "PageNumber": page,
        }

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(HISTORICAL_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                break
            except (requests.RequestException, json.JSONDecodeError) as e:
                print(f"      Attempt {attempt}/{max_retries} failed: {e}")
                if attempt < max_retries:
                    time.sleep(delay)
                else:
                    return all_items

        items = data if isinstance(data, list) else data.get("data", [])
        if not items:
            break

        all_items.extend(items)

        if len(items) < PAGE_SIZE:
            break

        page += 1
        time.sleep(0.5)

    return all_items


def seed_historical():
    """Main function to seed historical data."""
    print("=" * 60)
    print("SEEDING HISTORICAL DATA")
    print("=" * 60)

    all_records = []
    seen = set()

    for start_date, end_date in DATE_RANGES:
        print(f"\n--- Period: {start_date} to {end_date} ---")

        for keyword in KEYWORDS:
            items = fetch_historical(keyword, start_date, end_date)
            if items:
                for item in items:
                    record = flatten_historical_job(item, keyword)
                    cn = record["control_number"]
                    if cn and cn not in seen:
                        seen.add(cn)
                        all_records.append(record)
                print(f"  {keyword}: {len(items)} found, {len(all_records)} unique total")
            time.sleep(0.5)

    print(f"\n--- LOADING {len(all_records)} HISTORICAL RECORDS ---")
    if all_records:
        summary = load_records(all_records, source="historical_seed")
        return summary
    else:
        print("No historical records found.")
        return None


if __name__ == "__main__":
    seed_historical()