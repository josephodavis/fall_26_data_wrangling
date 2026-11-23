"""
transforms.py  --  the actual work of the Bison Bikes pipeline.

Notice that NONE of these functions import Airflow or Luigi. That is on
purpose, and it is one of the points your lesson should make: business logic
does not belong inside the orchestrator. Keep it in plain, testable functions
and let the framework do nothing but decide WHEN to call them and WHAT to do
if they fail.

Each function below has a docstring saying what it must do and a `TODO`.
Fill them in. The pipeline will not run until you do.
"""
import os
import shutil
import sqlite3

import numpy as np
import pandas as pd

RAW = "data/raw"
STAGE = "data/staging"
WAREHOUSE = "data/warehouse.db"

# Rows breaking more than this fraction of the rules => stop the pipeline.
QUALITY_THRESHOLD = 0.05


# =====================================================================
# 1. INGEST
# =====================================================================
def ingest_rides(ds: str) -> str:
    """Copy one day's raw ride file into staging, untouched.

    ds : the logical date as 'YYYY-MM-DD' (Airflow passes this as {{ ds }}).

    Ingest means MOVE, not FIX. Copy the bytes and stop. If you clean here,
    you can never answer "what did the source actually send us?"

    Returns the staged path. Must be idempotent: running it twice for the
    same ds leaves exactly the same result.

    TODO: read f"{RAW}/rides_{ds}.csv", write it to f"{STAGE}/rides_{ds}.csv",
          and raise FileNotFoundError with a useful message if it is missing.
    """

    input_path = f"{RAW}/rides_{ds}.csv"
    output_path = f"{STAGE}/rides_{ds}.csv"

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Raw ride file not found: {input_path}")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    shutil.copyfile(input_path, output_path)

    return output_path

# =====================================================================
# 2. VALIDATE  (the quality gate)
# =====================================================================
def validate_rides(ds: str) -> dict:
    """Check the staged file against the rules and decide whether to proceed.

    Rules to check (these are the defects we know the source has):
      - duration_min must be > 0 and < 1440     (9999 sentinel, negatives)
      - member_age must be between 13 and 100   (999 sentinel)
      - end_time must be >= start_time
      - ride_id must be unique
      - station_start / station_end must exist in stations.csv

    Return a dict of counts, e.g.
        {"rows": 1906, "bad_duration": 4, "bad_age": 4, ..., "bad_frac": 0.012}

    If bad_frac > QUALITY_THRESHOLD, raise ValueError. That failure is the
    POINT of this task: a pipeline that loads known-bad data silently is worse
    than one that stops loudly.

    TODO: implement the checks and the raise.
    """
    staged_path = f"{STAGE}/rides_{ds}.csv"
    df = pd.read_csv(staged_path)
    known_stations = set(pd.read_csv(f"{RAW}/stations.csv")["station_id"])

    # One boolean mask per rule: True => this row breaks the rule
    rules = {
        "bad_duration": ~df["duration_min"].between(0, 1440, inclusive="neither"),
        "bad_age": ~df["member_age"].between(13, 100),
        "bad_end_time": pd.to_datetime(df["end_time"]) < pd.to_datetime(df["start_time"]),
        "bad_ride_id": df["ride_id"].duplicated(),
        "bad_station": ~df["station_start"].isin(known_stations)
                       | ~df["station_end"].isin(known_stations),
    }

    counts = {"rows": len(df)}
    counts.update({name: int(mask.sum()) for name, mask in rules.items()})

    # Fraction of ROWS breaking at least one rule (a row that breaks two
    # rules is still one bad row, so don't just add the counts up)
    any_bad = pd.concat(rules, axis=1).any(axis=1)
    counts["bad_frac"] = float(any_bad.mean()) if len(df) > 0 else 0.0

    if counts["bad_frac"] > QUALITY_THRESHOLD:
        raise ValueError(
            f"Quality gate failed for {ds}: {counts['bad_frac']:.1%} of rows are bad "
            f"(threshold {QUALITY_THRESHOLD:.0%}). Counts: {counts}"
        )

    return counts


# =====================================================================
# 3. CLEAN
# =====================================================================
def clean_rides(ds: str) -> str:
    """Apply the fixes decided in your quality review. Write to staging.

    At minimum:
      - drop exact duplicate rows, and duplicate ride_ids
      - turn the 9999 durations and 999 ages into NaN (they are sentinels,
        not measurements)
      - drop rows where end_time < start_time
      - normalise user_type: 'Member', 'MEMBER', 'member ' -> 'member'
      - leave the unknown station ST99 in place but add a column
        `station_known` so the defect is visible downstream, not erased

    TODO: implement, write f"{STAGE}/rides_clean_{ds}.csv", return the path.
    """

    staged_path = f"{STAGE}/rides_{ds}.csv"
    output_path = f"{STAGE}/rides_clean_{ds}.csv"
    df = pd.read_csv(staged_path)
    known_stations = set(pd.read_csv(f"{RAW}/stations.csv")["station_id"])

    # Apply cleaning steps
    df = df.drop_duplicates(subset="ride_id")
    df = df[pd.to_datetime(df["end_time"]) >= pd.to_datetime(df["start_time"])].copy()

    # Sentinels (and negative durations) are not measurements -> NaN, keep the row
    df.loc[(df["duration_min"] == 9999) | (df["duration_min"] < 0), "duration_min"] = np.nan
    df.loc[df["member_age"] == 999, "member_age"] = np.nan

    df["user_type"] = df["user_type"].str.strip().str.lower()
    df["station_known"] = (df["station_start"].isin(known_stations)
                           & df["station_end"].isin(known_stations))


    df.to_csv(output_path, index=False)

    return output_path


def clean_weather() -> str:
    """Repair the weather file.

    Two defects:
      - two rows were recorded in Fahrenheit. Any temp_c above 45 is one of
        them. Convert, do not delete:  C = (F - 32) * 5/9
      - precip_mm uses -999 as a 'no reading' sentinel -> NaN
      - 2026-09-05 is missing entirely. Do NOT invent it. Decide what your
        downstream join does with a missing day, and say so in your slides.

    TODO: implement, write f"{STAGE}/weather_clean.csv", return the path.
    """
    raw_path = f"{RAW}/weather_2026-09.csv"
    output_path = f"{STAGE}/weather_clean.csv"
    df = pd.read_csv(raw_path)

    # Apply cleaning steps
    df['temp_c'] = df['temp_c'].apply(lambda x: round((x - 32) * 5/9, 1) if x > 45 else x)
    df['precip_mm'] = df['precip_mm'].replace(-999, pd.NA)

    # (this task can run before ingest_rides, so staging may not exist yet)
    os.makedirs(STAGE, exist_ok=True)
    df.to_csv(output_path, index=False)

    return output_path


def clean_stations() -> str:
    """Repair the station dimension.

    Defects: ST03 appears twice (second copy has messy whitespace and case),
    ST09 has capacity 0, ST10 is missing lat/lon.

    Deduplicate on station_id, keeping the first, cleanest row. Strip and
    title-case the names.

    TODO: implement, write f"{STAGE}/stations_clean.csv", return the path.
    """
    raw_path = f"{RAW}/stations.csv"
    output_path = f"{STAGE}/stations_clean.csv"
    df = pd.read_csv(raw_path)

    # Apply cleaning steps
    df = df.drop_duplicates(subset=['station_id'], keep='first').copy()
    df['station_name'] = df['station_name'].str.strip().str.title()

    # (this task can run before ingest_rides, so staging may not exist yet)
    os.makedirs(STAGE, exist_ok=True)
    df.to_csv(output_path, index=False)

    return output_path


# =====================================================================
# 4. TRANSFORM
# =====================================================================
def build_daily_metrics(ds: str) -> str:
    """Join the clean ride data to weather and stations, aggregate to one row.

    One row per day, with at least:
        date, n_rides, n_member, n_casual, median_duration_min,
        busiest_station, temp_c, precip_mm

    TODO: implement, write f"{STAGE}/daily_{ds}.csv", return the path.
    """
    output_path = f"{STAGE}/daily_{ds}.csv"
    rides = pd.read_csv(f"{STAGE}/rides_clean_{ds}.csv")
    weather = pd.read_csv(f"{STAGE}/weather_clean.csv")
    stations = pd.read_csv(f"{STAGE}/stations_clean.csv")

    # Busiest station = most ride starts, reported by name via the stations table
    busiest_id = rides["station_start"].value_counts().idxmax()
    busiest_name = stations.set_index("station_id").at[busiest_id, "station_name"]

    daily = pd.DataFrame([{
        "date": ds,
        "n_rides": len(rides),
        "n_member": int((rides["user_type"] == "member").sum()),
        "n_casual": int((rides["user_type"] == "casual").sum()),
        "median_duration_min": rides["duration_min"].median(),  # skips NaN
        "busiest_station": busiest_name,
    }])

    # LEFT join: a day with no weather row (2026-09-05) keeps its ride
    # metrics and gets NaN temp/precip -- we do not invent the reading.
    daily = daily.merge(weather[["date", "temp_c", "precip_mm"]], on="date", how="left")

    daily.to_csv(output_path, index=False)

    return output_path


# =====================================================================
# 5. LOAD
# =====================================================================
def load_warehouse(ds: str) -> int:
    """Write the day's metrics into SQLite.

    THE IDEMPOTENCY REQUIREMENT: running this twice for the same ds must NOT
    produce two rows. Delete the existing row for that date first, then
    insert. (This pattern is called delete-insert, or 'upsert by partition'.)

    Your demo will re-run one day on purpose to prove this works.

    TODO: implement against WAREHOUSE, return the number of rows written.
    """
    df = pd.read_csv(f"{STAGE}/daily_{ds}.csv")

    con = sqlite3.connect(WAREHOUSE)
    try:
        # PRIMARY KEY on date so we do not double count a day if the delete fails to run for some reason.
        con.execute("""
            CREATE TABLE IF NOT EXISTS daily_metrics (
                date                TEXT PRIMARY KEY,
                n_rides             INTEGER,
                n_member            INTEGER,
                n_casual            INTEGER,
                median_duration_min REAL,
                busiest_station     TEXT,
                temp_c              REAL,
                precip_mm           REAL
            )
        """)

        con.execute("DELETE FROM daily_metrics WHERE date = ?", (ds,))
        df.to_sql("daily_metrics", con, if_exists="append", index=False)
        con.commit()
    finally:
        con.close()

    return len(df)
