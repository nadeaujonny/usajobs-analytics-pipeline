"""
run_pipeline.py — Pipeline Orchestrator
Single entry point that runs the full ETL pipeline:
collect → transform → load
"""

from scripts.collect import collect_all
from scripts.transform import transform_raw_file
from scripts.load import load_records
import os


def run_pipeline():
    """Run the complete pipeline."""
    print("=" * 60)
    print("FEDERAL JOB MARKET ANALYTICS PIPELINE")
    print("=" * 60)

    # Stage 1: Collect
    print("\n--- STAGE 1: COLLECTION ---")
    collection_summary = collect_all()
    raw_file = collection_summary["output_file"]

    # Stage 2: Transform
    print("\n--- STAGE 2: TRANSFORMATION ---")
    records = transform_raw_file(raw_file)

    # Stage 3: Load
    print(f"\n--- STAGE 3: LOADING ---")
    load_summary = load_records(records)

    # Final summary
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Collected: {collection_summary['total_collected']} raw postings")
    print(f"  Transformed: {len(records)} unique records")
    print(f"  New inserts: {load_summary['inserted']}")
    print(f"  Duplicates skipped: {load_summary['skipped']}")
    print(f"  Total in database: {load_summary['total_in_db']}")


if __name__ == "__main__":
    run_pipeline()