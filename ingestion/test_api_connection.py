"""
test_api_connection.py
----------------------
Verify connectivity to the NYC 311 Service Requests API.

Run from the project root:
    python ingestion/test_api_connection.py
"""

import json
import sys
import requests

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
TIMEOUT_SECONDS = 30


def test_nyc_311_api():
    """Fetch 5 records from the NYC 311 API and print a sample."""

    print("Calling NYC 311 API...")
    print(f"URL: {API_URL}")
    print("-" * 60)

    try:
        response = requests.get(
            API_URL,
            params={"$limit": 5},
            timeout=TIMEOUT_SECONDS,
        )
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {TIMEOUT_SECONDS} seconds.")
        sys.exit(1)
    except requests.exceptions.ConnectionError as e:
        print(f"ERROR: Connection failed. Check your internet.\nDetails: {e}")
        sys.exit(1)

    if response.status_code == 200:
        records = response.json()
        print(f"SUCCESS - Status code: {response.status_code}")
        print(f"Records received: {len(records)}")
        print("-" * 60)
        print("First record (sample):")
        print(json.dumps(records[0], indent=2))
    else:
        print(f"FAILED - Status code: {response.status_code}")
        print(f"Response body: {response.text}")
        sys.exit(1)


if __name__ == "__main__":
    test_nyc_311_api()