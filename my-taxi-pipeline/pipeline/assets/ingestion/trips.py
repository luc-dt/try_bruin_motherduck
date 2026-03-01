"""@bruin

name: ingestion.trips
type: python
image: python:3.11
# connection: duckdb-default
connection: motherduck-prod

materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow or green)"
  - name: extracted_at
    type: timestamp
    description: "Timestamp when the record was extracted from the source"

@bruin"""

import os
import json
import io
from datetime import datetime, timezone

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def materialize():
    start_date_str = os.environ.get("BRUIN_START_DATE", "2022-01-01")
    end_date_str = os.environ.get("BRUIN_END_DATE", "2022-02-01")
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Build list of (taxi_type, year, month) tuples covering the date window
    months = []
    current = start_date.replace(day=1)
    while current < end_date:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    extracted_at = datetime.now(timezone.utc).isoformat()
    frames = []

    for taxi_type in taxi_types:
        for year, month in months:
            filename = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
            url = BASE_URL + filename
            print(f"Fetching: {url}")
            try:
                response = requests.get(url, timeout=120)
                response.raise_for_status()
                df = pd.read_parquet(io.BytesIO(response.content))
                df["taxi_type"] = taxi_type
                df["extracted_at"] = extracted_at
                frames.append(df)
                print(f"  -> {len(df):,} rows loaded")
            except requests.HTTPError as e:
                print(f"  -> Skipping {filename}: HTTP {e.response.status_code}")
            except Exception as e:
                print(f"  -> Skipping {filename}: {e}")

    if not frames:
        print("No data fetched for the given date range and taxi types.")
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    print(f"Total rows fetched: {len(result):,}")
    return result
