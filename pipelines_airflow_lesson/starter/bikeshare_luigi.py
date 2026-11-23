"""
bikeshare_luigi.py  --  Group 1 FALLBACK path

Use this if Airflow will not install on your machine (most often: Windows
without WSL). Luigi is a single pip install with no database, no scheduler
daemon and no web server to start.

    pip install luigi
    python bikeshare_luigi.py --local-scheduler LoadWarehouse --ds 2026-09-01

Everything you must say in your lesson still applies here -- Luigi has tasks,
dependencies, and a DAG. What it does NOT have is scheduling, backfill, retries
or a UI, and explaining THAT difference is worth more to the class than having
used the trendier tool.

The core difference to point out:

    Airflow : tasks declare what they depend on, a scheduler decides when
              to run them, and state lives in a database.
    Luigi   : tasks declare what they PRODUCE (an output target). A task is
              "done" if its output file exists. There is no scheduler --
              you ask for a target and Luigi works backwards.

That second model is called `target-based` and it gives you idempotency almost
for free. Ask the class why.
"""
import luigi

import transforms as T


class IngestRides(luigi.Task):
    ds = luigi.Parameter()

    def output(self):
        # TODO: return a luigi.LocalTarget for data/staging/rides_{ds}.csv
        ...

    def run(self):
        T.ingest_rides(self.ds)


class ValidateRides(luigi.Task):
    ds = luigi.Parameter()

    def requires(self):
        return IngestRides(ds=self.ds)

    def output(self):
        # TODO: validation produces a report file -- make that the target
        ...

    def run(self):
        stats = T.validate_rides(self.ds)
        # TODO: write stats to self.output() so the task has a real artifact
        ...


class CleanRides(luigi.Task):
    ds = luigi.Parameter()

    def requires(self):
        return ValidateRides(ds=self.ds)

    def output(self):
        ...  # TODO

    def run(self):
        T.clean_rides(self.ds)


class CleanWeather(luigi.Task):
    def output(self):
        ...  # TODO

    def run(self):
        T.clean_weather()


class CleanStations(luigi.Task):
    def output(self):
        ...  # TODO

    def run(self):
        T.clean_stations()


class BuildDailyMetrics(luigi.Task):
    ds = luigi.Parameter()

    def requires(self):
        # TODO: this one needs THREE upstreams. Return a list.
        ...

    def output(self):
        ...  # TODO

    def run(self):
        T.build_daily_metrics(self.ds)


class LoadWarehouse(luigi.Task):
    ds = luigi.Parameter()

    def requires(self):
        return BuildDailyMetrics(ds=self.ds)

    def output(self):
        # Careful: the warehouse is a database, not a file that appears once.
        # If you point output() at warehouse.db, the task will look "done"
        # forever after the first run. What should the target be instead?
        # (Hint: one marker file per ds.)
        ...  # TODO

    def run(self):
        T.load_warehouse(self.ds)
        # TODO: touch the marker


if __name__ == "__main__":
    luigi.run()
