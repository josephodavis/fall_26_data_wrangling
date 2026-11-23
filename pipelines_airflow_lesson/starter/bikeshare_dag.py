"""
bikeshare_dag.py  --  Group 1 starter

Put this file (and transforms.py) in ~/airflow/dags/ and run:

    airflow standalone

then open http://localhost:8080 and find the DAG `bison_bikes`.

Your job is to finish the TODOs. The DAG will appear in the UI even while it
is broken -- Airflow shows import errors at the top of the page, which is
itself worth demoing to the class.
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context

import transforms as T


# ---------------------------------------------------------------------
# TODO 1 -- DAG-level settings.
#
#   schedule     : how often? The source drops one file per day.
#   start_date   : the first day of data we have.
#   end_date     : the last day of data we have.
#   catchup      : True makes Airflow run every missed interval between
#                  start_date and now. That is what gives you a backfill
#                  to demo. Set it True and be ready to explain what
#                  would happen in production if you left it True by
#                  accident on a DAG with a 2019 start_date.
#   retries      : the ingest task can fail because a file has not landed
#                  yet. That is a transient failure. How many retries,
#                  and how far apart?
# ---------------------------------------------------------------------
@dag(
    dag_id="bison_bikes",
    description="Daily campus bike-share pipeline",
    schedule="@daily",             # one raw file lands per day
    start_date=datetime(2026, 9, 1),
    end_date=datetime(2026, 9, 7), # last day of data we have
    catchup=True,                  # backfill 09-01 .. 09-07 on unpause
    max_active_runs=1,
    default_args={
        "retries": 3,              # a late file is transient: try again
        "retry_delay": timedelta(seconds=30),  # short for the demo; minutes in prod
    },
    tags=["group1", "data-wrangling"],
)
def bison_bikes():

    # -----------------------------------------------------------------
    # TODO 2 -- wrap each transform in a @task.
    #
    # get_current_context()["ds"] gives you the logical date of THIS run
    # as 'YYYY-MM-DD'. That is what makes one DAG definition able to
    # process any day -- including days in the past, during a backfill.
    # -----------------------------------------------------------------
    @task
    def ingest_rides() -> str:
        ds = get_current_context()["ds"]
        return T.ingest_rides(ds)

    # Bad data is NOT transient -- retrying the same file gives the same
    # answer, so the quality gate fails immediately instead of 3 more times.
    @task(retries=0)
    def validate_rides(_staged: str) -> dict:
        ds = get_current_context()["ds"]
        return T.validate_rides(ds)

    @task
    def clean_rides(_stats: dict) -> str:
        ds = get_current_context()["ds"]
        return T.clean_rides(ds)

    @task
    def clean_weather() -> str:
        return T.clean_weather()

    @task
    def clean_stations() -> str:
        return T.clean_stations()

    @task
    def build_daily_metrics(_r: str, _w: str, _s: str) -> str:
        ds = get_current_context()["ds"]
        return T.build_daily_metrics(ds)

    @task
    def load_warehouse(_metrics: str) -> int:
        ds = get_current_context()["ds"]
        return T.load_warehouse(ds)

    # -----------------------------------------------------------------
    # TODO 3 -- wire the dependencies.
    #
    # In the TaskFlow API you do NOT write `a >> b`. You pass one task's
    # return value into the next, and Airflow infers the edge. Passing a
    # value you do not actually use (like _stats) is how you say
    # "wait for this, even though I don't need its output."
    #
    # Target shape -- three independent roots converging:
    #
    #    ingest_rides -> validate_rides -> clean_rides --+
    #    clean_weather ---------------------------------+--> build_daily
    #    clean_stations --------------------------------+        |
    #                                                            v
    #                                                     load_warehouse
    #
    # Question for your slides: why can clean_weather and clean_stations
    # start immediately, while clean_rides has to wait?
    # -----------------------------------------------------------------
    staged = ingest_rides()
    stats = validate_rides(staged)
    rides = clean_rides(stats)
    weather = clean_weather()
    stations = clean_stations()
    metrics = build_daily_metrics(rides, weather, stations)
    load_warehouse(metrics)


bison_bikes()
