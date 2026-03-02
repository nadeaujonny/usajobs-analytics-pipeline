"""
transform.py — Stage 2: Data Transformation
Reads raw JSON from collect.py, flattens nested fields, cleans data,
deduplicates, and returns a list of clean records ready for the database.
"""

import json
import os
import re
from datetime import datetime


def get_latest_raw_file(raw_dir=None):
    """Find the most recent raw JSON file in data/raw/"""
    if raw_dir is None:
        raw_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")

    json_files = [f for f in os.listdir(raw_dir) if f.startswith("raw_") and f.endswith(".json")]
    if not json_files:
        raise FileNotFoundError("No raw JSON files found in data/raw/")

    latest = sorted(json_files)[-1]  # Alphabetical sort works because of timestamp format
    return os.path.join(raw_dir, latest)


def parse_salary(salary_str):
    """Extract numeric salary from string like '$72,553.00' or '$0.00'"""
    if not salary_str:
        return None
    cleaned = re.sub(r'[^\d.]', '', salary_str)
    try:
        value = float(cleaned)
        return value if value > 0 else None
    except (ValueError, TypeError):
        return None


def classify_role(title, keyword):
    """
    Classify a job posting into a role category based on its title and search keyword.
    Returns a broad category string.
    """
    title_lower = title.lower() if title else ""

    # Check title first for more specific matching
    if "data scientist" in title_lower or "data science" in title_lower:
        return "Data Scientist"
    elif "data engineer" in title_lower:
        return "Data Engineer"
    elif "data analyst" in title_lower or "data analysis" in title_lower:
        return "Data Analyst"
    elif "business intelligence" in title_lower or " bi " in title_lower:
        return "Business Intelligence"
    elif "business analyst" in title_lower:
        return "Business Analyst"
    elif "statistician" in title_lower or "statistics" in title_lower:
        return "Statistician"
    elif "program analyst" in title_lower:
        return "Program Analyst"
    elif "management analyst" in title_lower:
        return "Management Analyst"
    elif "it specialist" in title_lower or "information technology" in title_lower:
        return "IT Specialist"

    # Fall back to search keyword
    keyword_map = {
        "data analyst": "Data Analyst",
        "data scientist": "Data Scientist",
        "data engineer": "Data Engineer",
        "business intelligence": "Business Intelligence",
        "business analyst": "Business Analyst",
        "statistician": "Statistician",
        "program analyst": "Program Analyst",
        "management analyst": "Management Analyst",
        "IT specialist": "IT Specialist",
    }
    return keyword_map.get(keyword, "Other")


def flatten_job(item, keyword):
    """
    Flatten a single job posting from the nested API JSON structure
    into a flat dictionary matching our database schema.
    """
    matched = item.get("MatchedObjectDescriptor", {})
    user_area = matched.get("UserArea", {}).get("Details", {})

    # Extract location (take first location if multiple)
    locations = matched.get("PositionLocation", [])
    first_location = locations[0] if locations else {}

    # Extract salary
    salary_min_str = matched.get("PositionRemuneration", [{}])[0].get("MinimumRange", "") if matched.get("PositionRemuneration") else ""
    salary_max_str = matched.get("PositionRemuneration", [{}])[0].get("MaximumRange", "") if matched.get("PositionRemuneration") else ""
    salary_interval = matched.get("PositionRemuneration", [{}])[0].get("Description", "") if matched.get("PositionRemuneration") else ""

    salary_min = parse_salary(salary_min_str)
    salary_max = parse_salary(salary_max_str)

    # Annualize if salary is listed as "Per Hour"
    if salary_interval and "hour" in salary_interval.lower():
        if salary_min:
            salary_min = round(salary_min * 2087, 2)  # OPM standard work hours/year
        if salary_max:
            salary_max = round(salary_max * 2087, 2)

    # Calculate midpoint
    salary_mid = None
    if salary_min and salary_max:
        salary_mid = round((salary_min + salary_max) / 2, 2)

    position_title = matched.get("PositionTitle", "")

    return {
        "control_number": matched.get("PositionID", ""),
        "position_title": position_title,
        "organization_name": matched.get("OrganizationName", ""),
        "department_name": matched.get("DepartmentName", ""),
        "sub_agency": user_area.get("SubAgency", ""),
        "job_grade": matched.get("JobGrade", [{}])[0].get("Code", "") if matched.get("JobGrade") else "",
        "pay_plan": user_area.get("PayPlan", ""),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_mid": salary_mid,
        "salary_interval": salary_interval,
        "location_name": first_location.get("LocationName", ""),
        "city": first_location.get("CityName", ""),
        "state": first_location.get("CountrySubDivisionCode", ""),
        "country": first_location.get("CountryCode", ""),
        "latitude": first_location.get("Latitude", None),
        "longitude": first_location.get("Longitude", None),
        "position_url": matched.get("PositionURI", ""),
        "open_date": matched.get("PositionStartDate", ""),
        "close_date": matched.get("PositionEndDate", ""),
        "role_category": classify_role(position_title, keyword),
        "search_keyword": keyword,
        "collected_date": datetime.now().strftime("%Y-%m-%d"),
    }


def transform_raw_file(filepath=None):
    """
    Main transformation function.
    Reads the raw JSON file, flattens all records, and deduplicates.
    Returns a list of clean record dictionaries.
    """
    if filepath is None:
        filepath = get_latest_raw_file()

    print(f"Transforming: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    all_records = []
    seen_control_numbers = set()

    for keyword, items in raw_data.items():
        for item in items:
            record = flatten_job(item, keyword)

            # Deduplicate by control number
            cn = record["control_number"]
            if cn and cn not in seen_control_numbers:
                seen_control_numbers.add(cn)
                all_records.append(record)

    print(f"  Raw items: {sum(len(v) for v in raw_data.values())}")
    print(f"  After dedup: {len(all_records)}")
    print(f"  Duplicates removed: {sum(len(v) for v in raw_data.values()) - len(all_records)}")

    return all_records


# Allow running directly for testing
if __name__ == "__main__":
    records = transform_raw_file()
    print(f"\nSample record:")
    if records:
        for key, value in records[0].items():
            print(f"  {key}: {value}")