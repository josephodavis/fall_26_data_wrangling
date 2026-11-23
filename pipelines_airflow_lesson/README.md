# Group 1 — Designing and Deploying Data Pipelines

**Data Wrangling, Fall 2026 · Student-led lesson · 50 minutes**

## Start here

Read **`Group1_Lesson_Skeleton.pdf`**. It is the assignment: what you must
teach, the questions you must answer on slides, your minute-by-minute class
plan, the live-demo script, and the grading rubric.

## Contents

```
Group1_Lesson_Skeleton.pdf     the assignment — read this first
Group1_Lesson_Skeleton.tex     LaTeX source, if you want to edit it

data/
  README_DATA.md               every defect in the data, documented
  raw/
    rides_2026-09-01.csv       seven daily partitions, so backfill is
    ...                        a real demo and not a hypothetical
    rides_2026-09-07.csv
    weather_2026-09.csv        two rows in Fahrenheit; one day missing
    stations.csv               a duplicate id and some messy names

starter/
  transforms.py                six functions to write — the actual work
  bikeshare_dag.py             Airflow DAG with the wiring left to you
  bikeshare_luigi.py           fallback if Airflow will not install
  requirements.txt
```

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r starter/requirements.txt

export AIRFLOW_HOME=$(pwd)/airflow_home
mkdir -p $AIRFLOW_HOME/dags
cp starter/transforms.py starter/bikeshare_dag.py $AIRFLOW_HOME/dags/
airflow standalone          # then open http://localhost:8080
```

**Windows:** Airflow has no native Windows support. Use WSL2, or take the
Luigi path — see §2 of the PDF. Decide this in your first 48 hours.

## What you hand in

Slides, a working live demo, and your code. The rubric is on the last page of
the PDF.
