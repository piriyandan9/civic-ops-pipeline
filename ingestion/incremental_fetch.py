"""
incremental_fetch.py
--------------------
Production-shaped ingestion for NYC 311 service requests.

Features:
- Pagination via $offset to handle large result sets
- Incremental loading using a watermark (created_date)
- Watermark stored in data/state/last_run.json
- Retry logic on transient network failures
- Bounded first run (last 7 days) to avoid huge backfills

Run from the project root:
    python ingestion/incremental_fetch.py
"""

import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

# --- Configuration -----------------------------------------------------------

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 500
MAX_PAGES = 50
TIMEOUT_SECONDS = 60
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

RAW_DIR = Path("data/raw")
STATE_DIR = Path("data/state")
STATE_FILE = STATE_DIR / "last_run.json"

DEFAULT_LOOKBACK_DAYS = 7

# --- Logging setup -----------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# --- State management --------------------------------------------------------

def read_watermark() -> str:
    """
    Read the last successful run's max created_date from state file.
    On first run (no state file), default to DEFAULT_LOOKBACK_DAYS ago.
    Returns ISO 8601 datetime string (the format NYC 311 API expects).
    """

    if STATE_FILE.exists():
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        watermark = state["last_created_date"]
        logger.info(f"Loaded watermark from state: {watermark}")
        return watermark

    fallback = datetime.now() - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    watermark = fallback.strftime("%Y-%m-%dT%H:%M:%S.000")
    logger.info(f"No state file found. Using {DEFAULT_LOOKBACK_DAYS}-day fallback: {watermark}")
    return watermark


def write_watermark(new_watermark: str) -> None:
    """Persist the new watermark to state file."""

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state = {
        "last_created_date": new_watermark,
        "updated_at": datetime.now().isoformat(),
    }
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)
    logger.info(f"Watermark updated: {new_watermark}")


# --- API fetch ---------------------------------------------------------------

def fetch_page(watermark: str, offset: int) -> list[dict]:
    """
    Fetch one page of records created after the watermark.
    Retries on transient errors. Exits on persistent failure.
    """

    params = {
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$where": f"created_date > '{watermark}'",
        "$order": "created_date ASC",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.warning(f"Page offset={offset} attempt {attempt}/{MAX_RETRIES}: timed out")
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Page offset={offset} attempt {attempt}/{MAX_RETRIES}: connection error: {e}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"API HTTP error: {e}")
            sys.exit(1)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY_SECONDS)

    logger.error(f"Page offset={offset}: all retries exhausted")
    sys.exit(1)


def fetch_all_pages(watermark: str) -> list[dict]:
    """
    Loop through pages until we hit an empty page or MAX_PAGES.
    Returns all records as a single list.
    """

    all_records = []

    for page_num in range(MAX_PAGES):
        offset = page_num * PAGE_SIZE
        logger.info(f"Fetching page {page_num + 1} (offset={offset})")

        records = fetch_page(watermark, offset)

        if not records:
            logger.info(f"Empty page received. Pagination complete.")
            break

        all_records.extend(records)
        logger.info(f"Page {page_num + 1}: got {len(records)} records (total so far: {len(all_records)})")

        # Optimization: if API returned fewer than PAGE_SIZE, we're done
        if len(records) < PAGE_SIZE:
            logger.info(f"Partial page received. No more data.")
            break
    else:
        logger.warning(f"Hit MAX_PAGES safety limit ({MAX_PAGES}). More data may exist.")

    return all_records


# --- Disk I/O ----------------------------------------------------------------

def save_records(records: list[dict]) -> Path:
    """Save records as a timestamped JSON file in data/raw/."""

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = RAW_DIR / f"nyc311_{timestamp}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    size_kb = output_path.stat().st_size / 1024
    logger.info(f"Saved {len(records)} records to {output_path} ({size_kb:.1f} KB)")
    return output_path


# --- Orchestration -----------------------------------------------------------

def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting NYC 311 incremental ingestion")
    logger.info("=" * 60)

    watermark = read_watermark()
    records = fetch_all_pages(watermark)

    if not records:
        logger.info("No new records since last run. Exiting cleanly.")
        return

    save_records(records)

    # Update watermark to max created_date from this batch
    new_watermark = max(r["created_date"] for r in records)
    write_watermark(new_watermark)

    logger.info(f"Ingestion complete. {len(records)} new records processed.")


if __name__ == "__main__":
    main()