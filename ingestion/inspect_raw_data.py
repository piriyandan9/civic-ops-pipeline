"""
inspect_raw_data.py
-------------------
Quality inspection for raw NYC 311 JSON files.

Reads the latest file in data/raw/, reports on data shape and content,
and asserts critical quality rules. Exits with code 1 if any rule fails.

Run from the project root:
    python ingestion/inspect_raw_data.py
"""

import json
import logging
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

# --- Configuration -----------------------------------------------------------

RAW_DIR = Path("data/raw")
REPORTS_DIR = Path("data/quality_reports")

VALID_BOROUGHS = {
    "MANHATTAN",
    "BROOKLYN",
    "QUEENS",
    "BRONX",
    "STATEN ISLAND",
    "Unspecified",
}

# Columns we consider critical for downstream analytics
CRITICAL_COLUMNS = ["unique_key", "created_date", "complaint_type", "borough"]

# --- Logging setup -----------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def find_latest_raw_file() -> Path:
    """Find the most recent JSON file in data/raw/."""
    
    files = list(RAW_DIR.glob("nyc311_*.json"))
    if not files:
        logger.error(f"No raw files found in {RAW_DIR}")
        sys.exit(1)
    
    latest = max(files, key=lambda f: f.stat().st_mtime)
    logger.info(f"Inspecting latest file: {latest.name}")
    return latest


def load_records(path: Path) -> list[dict]:
    """Read JSON file and return list of records."""
    
    with open(path, "r", encoding="utf-8") as f:
        records = json.load(f)
    
    logger.info(f"Loaded {len(records)} records")
    return records


def inspect_data(records: list[dict]) -> dict:
    """
    Look at the records and return a report dictionary describing
    what's in the data. Pure observation, no assertions yet.
    """
    
    report = {}
    
    # 1. Total record count
    report["total_records"] = len(records)
    
    # 2. All columns that appear across all records
    all_columns = set()
    for record in records:
        all_columns.update(record.keys())
    report["columns"] = sorted(all_columns)
    report["column_count"] = len(all_columns)
    
    # 3. Null/missing rate for each critical column
    null_rates = {}
    for col in CRITICAL_COLUMNS:
        missing = sum(1 for r in records if not r.get(col))
        null_rates[col] = {
            "missing": missing,
            "percent": round(100 * missing / len(records), 2),
        }
    report["null_rates"] = null_rates
    
    # 4. Unique values for borough (categorical field)
    borough_counts = Counter(r.get("borough", "MISSING") for r in records)
    report["borough_distribution"] = dict(borough_counts)
    
    # 5. Date range
    dates = [r["created_date"] for r in records if r.get("created_date")]
    if dates:
        report["earliest_created_date"] = min(dates)
        report["latest_created_date"] = max(dates)
    
    # 6. Duplicate primary key check
    keys = [r.get("unique_key") for r in records]
    duplicates = len(keys) - len(set(keys))
    report["duplicate_unique_keys"] = duplicates
    
    return report


def print_report(report: dict) -> None:
    """Pretty-print the inspection report to logs."""
    
    logger.info("-" * 60)
    logger.info("DATA INSPECTION REPORT")
    logger.info("-" * 60)
    logger.info(f"Total records: {report['total_records']}")
    logger.info(f"Total columns found: {report['column_count']}")
    logger.info("")
    logger.info("Null rates for critical columns:")
    for col, stats in report["null_rates"].items():
        logger.info(f"  {col}: {stats['missing']} missing ({stats['percent']}%)")
    logger.info("")
    logger.info("Borough distribution:")
    for borough, count in report["borough_distribution"].items():
        logger.info(f"  {borough}: {count}")
    logger.info("")
    logger.info(f"Date range: {report.get('earliest_created_date')} to {report.get('latest_created_date')}")
    logger.info(f"Duplicate unique_keys: {report['duplicate_unique_keys']}")
    logger.info("-" * 60)

def find_duplicate_keys(records: list[dict]) -> list[dict]:
    """
    Find records that share unique_keys with other records.
    Returns the offending records grouped by their key.
    """
    
    key_to_records = {}
    for r in records:
        key = r.get("unique_key")
        if key not in key_to_records:
            key_to_records[key] = []
        key_to_records[key].append(r)
    
    # Keep only keys that appear more than once
    duplicates = {k: rs for k, rs in key_to_records.items() if len(rs) > 1}
    return duplicates

def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting data quality inspection")
    logger.info("=" * 60)
    
    latest_file = find_latest_raw_file()
    records = load_records(latest_file)
    
    report = inspect_data(records)
    print_report(report)
    save_report(report, latest_file)
    run_assertions(records, report)

    # Investigate any duplicate keys
    if report["duplicate_unique_keys"] > 0:
        logger.warning(f"Found duplicates — investigating...")
        duplicates = find_duplicate_keys(records)
        for key, dup_records in duplicates.items():
            logger.warning(f"Key '{key}' appears {len(dup_records)} times")
            for i, dup in enumerate(dup_records, 1):
                logger.warning(
                    f"  Copy {i}: created_date={dup.get('created_date')}, "
                    f"complaint_type={dup.get('complaint_type')}, "
                    f"borough={dup.get('borough')}, "
                    f"status={dup.get('status')}"
                )

def run_assertions(records: list[dict], report: dict) -> None:
    """
    Hard quality rules. Exits with code 1 if any rule fails.
    """
    
    failures = []
    total = report["total_records"]
    
    # Rule 1: every record must have a unique_key
    if report["null_rates"]["unique_key"]["missing"] > 0:
        failures.append(
            f"Found records with missing unique_key "
            f"({report['null_rates']['unique_key']['missing']} records)"
        )
    
    # Rule 2: duplicate rate must be under 1%
    dup_count = report["duplicate_unique_keys"]
    dup_rate = 100 * dup_count / total
    if dup_rate > 1.0:
        failures.append(
            f"Duplicate rate too high: {dup_count} duplicates ({dup_rate:.3f}%)"
        )
    elif dup_count > 0:
        logger.warning(
            f"Found {dup_count} duplicates ({dup_rate:.3f}%) "
            f"- within tolerance (<1%), will be deduplicated downstream"
        )
    
    # Rule 3: all boroughs must be from the valid set
    invalid_boroughs = set(report["borough_distribution"].keys()) - VALID_BOROUGHS
    if invalid_boroughs:
        failures.append(f"Unexpected borough values: {invalid_boroughs}")
    
    # Rule 4: created_date must be parseable
    unparseable = 0
    for r in records:
        try:
            datetime.fromisoformat(r["created_date"].replace("Z", "+00:00"))
        except (ValueError, TypeError, AttributeError):
            unparseable += 1
    if unparseable > 0:
        failures.append(f"{unparseable} records have unparseable created_date")
    
    # Final verdict
    if failures:
        logger.error("=" * 60)
        logger.error("DATA QUALITY ASSERTIONS FAILED")
        logger.error("=" * 60)
        for f in failures:
            logger.error(f"  - {f}")
        sys.exit(1)
    else:
        logger.info("=" * 60)
        logger.info("ALL QUALITY ASSERTIONS PASSED ✓")
        logger.info("=" * 60)

def save_report(report: dict, source_file: Path) -> Path:
    """Save the inspection report as a timestamped text file."""
    
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = REPORTS_DIR / f"quality_report_{timestamp}.txt"
    
    lines = []
    lines.append("=" * 60)
    lines.append("NYC 311 DATA QUALITY REPORT")
    lines.append("=" * 60)
    lines.append(f"Source file: {source_file.name}")
    lines.append(f"Generated:   {datetime.now().isoformat()}")
    lines.append("")
    lines.append(f"Total records:        {report['total_records']}")
    lines.append(f"Total columns found:  {report['column_count']}")
    lines.append(f"Duplicate keys:       {report['duplicate_unique_keys']}")
    lines.append(f"Date range:           {report.get('earliest_created_date')} to {report.get('latest_created_date')}")
    lines.append("")
    lines.append("Null rates (critical columns):")
    for col, stats in report["null_rates"].items():
        lines.append(f"  {col:<20} {stats['missing']:>6} missing ({stats['percent']}%)")
    lines.append("")
    lines.append("Borough distribution:")
    for borough, count in sorted(report["borough_distribution"].items(), key=lambda x: -x[1]):
        pct = round(100 * count / report["total_records"], 2)
        lines.append(f"  {borough:<20} {count:>6} ({pct}%)")
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    
    logger.info(f"Saved quality report to {report_path}")
    return report_path

if __name__ == "__main__":
    main()