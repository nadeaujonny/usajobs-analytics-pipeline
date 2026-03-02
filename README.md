# Federal Job Market Analytics Pipeline

An automated ETL pipeline that collects federal job postings daily from the USAJobs API, stores them in a SQLite database, and visualizes insights through an interactive Streamlit dashboard.

**[Live Dashboard](https://usajobs-analytics-pipeline.streamlit.app)**

## Project Overview

This pipeline tracks data and analytics-related federal job postings across 9 role categories, collecting new data daily via GitHub Actions. The dashboard provides insights into salary trends, geographic demand, agency hiring patterns, and more.

As of the initial collection, the database contains 1,131 unique job postings across 155 federal agencies in 54 states and territories, with an average midpoint salary of $115,731.

## Architecture
```
GitHub Actions (Daily 6AM UTC)
        |
        v
  collect.py       USAJobs Search API (9 keywords, pagination, retries)
        |
        v
  transform.py     Flatten JSON, deduplicate, classify roles, clean salaries
        |
        v
  load.py          SQLite upserts, pipeline logging
        |
        v
  jobs.db          SQLite database (committed to repo)
        |
        v
  app.py           Streamlit dashboard (hosted on Streamlit Community Cloud)
```

## Role Categories Tracked

- Data Analyst
- Data Scientist
- Data Engineer
- Business Intelligence
- Business Analyst
- Statistician
- Program Analyst
- Management Analyst
- IT Specialist

## Project Structure
```
usajobs-analytics-pipeline/
├── .github/workflows/
│   └── daily_collect.yml       GitHub Actions daily automation
├── data/
│   └── jobs.db                 SQLite database
├── scripts/
│   ├── collect.py              Stage 1 - API data collection
│   ├── transform.py            Stage 2 - Data transformation
│   ├── load.py                 Stage 3 - Database loading
│   └── seed_historical.py      Historical data seeding (experimental)
├── app.py                      Streamlit dashboard
├── run_pipeline.py             Pipeline orchestrator
├── requirements.txt            Python dependencies
├── .env.example                Environment variable template
└── .gitignore
```

## Tech Stack

Python, USAJobs API, SQLite, Streamlit, Plotly, Pandas, GitHub Actions

## Dashboard Pages

**Executive Overview** — KPI cards, role distribution, department breakdown, salary distribution, top states

**Salary Analysis** — Median and mean salary by role, state, and job grade

**Geographic Demand** — Interactive US map, state and city rankings, location type breakdown

**Agency Analysis** — Top hiring organizations, department comparisons, role-department heatmap

**Data Explorer** — Searchable data table with CSV download

## Setup

1. Clone the repo
2. Copy `.env.example` to `.env` and add your USAJobs API credentials
3. Install dependencies: `pip install -r requirements.txt`
4. Run the pipeline: `python run_pipeline.py`
5. Launch the dashboard: `streamlit run app.py`

## Daily Automation

The pipeline runs automatically via GitHub Actions at 6:00 AM UTC daily. It collects new postings, deduplicates against existing data, and commits the updated database back to the repository. The Streamlit dashboard reflects new data on each visit.

## Author

Jonathan Nadeau — [Portfolio](https://nadeaujonny.github.io) | [GitHub](https://github.com/nadeaujonny)