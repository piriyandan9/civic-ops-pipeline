"""
fetch_311_data.py
-----------------
Ingest a batch of NYC 311 service request records and save to local disk
as a timestamped JSON file in data/raw/.

Run from the project root:
    python ingestion/fetch_311_data.py
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

# --- Configuration -----------------------------------------------------------

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
BATCH_SIZE = 500
TIMEOUT_SECONDS = 60
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
OUTPUT_DIR = Path("data/raw")

# --- Logging setup -----------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# --- Core functions ----------------------------------------------------------

def fetch_records(limit: int) -> list[dict]:
    """
    Fetch a batch of records from the NYC 311 API.
    Retries up to MAX_RETRIES times on timeout or connection errors.
    """

    logger.info(f"Requesting {limit} records from NYC 311 API")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                API_URL,
                params={"$limit": limit},
                timeout=TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            records = response.json()
            logger.info(f"Received {len(records)} records (attempt {attempt})")
            return records

        except requests.exceptions.Timeout:
            logger.warning(
                f"Attempt {attempt}/{MAX_RETRIES}: timed out after {TIMEOUT_SECONDS}s"
            )
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES}: connection error: {e}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"API returned HTTP error: {e}")
            sys.exit(1)

        if attempt < MAX_RETRIES:
            logger.info(f"Waiting {RETRY_DELAY_SECONDS}s before retry...")
            time.sleep(RETRY_DELAY_SECONDS)

    logger.error(f"All {MAX_RETRIES} attempts failed. Giving up.")
    sys.exit(1)


def save_to_disk(records: list[dict]) -> Path:
    """Save records as a timestamped JSON file in data/raw/."""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"nyc311_{timestamp}.json"
    output_path = OUTPUT_DIR / filename

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    file_size_kb = output_path.stat().st_size / 1024
    logger.info(f"Saved {len(records)} records to {output_path} ({file_size_kb:.1f} KB)")
    return output_path


def main() -> None:
    """Run one ingestion cycle."""

    logger.info("=" * 60)
    logger.info("Starting NYC 311 ingestion run")
    logger.info("=" * 60)

    records = fetch_records(limit=BATCH_SIZE)
    output_path = save_to_disk(records)

    logger.info(f"Ingestion complete. Output: {output_path}")


if __name__ == "__main__":
    main()