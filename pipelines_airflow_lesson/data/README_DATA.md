# Bison Bikes raw data

A campus bike-share system. Nine files, all deliberately imperfect.

| File | Rows | Notes |
|---|---|---|
| `raw/rides_2026-09-01.csv` … `rides_2026-09-07.csv` | ~1,050–1,906 each | One file per day. Seven daily partitions is what makes a backfill demo possible. |
| `raw/weather_2026-09.csv` | 6 | **Only six rows for seven days.** 2026-09-05 is missing. |
| `raw/stations.csv` | 11 | 10 stations, but 11 rows. |

## Known defects (your pipeline must handle these)

**rides_*.csv**
- `duration_min = 9999` — a sentinel, not a 7-day bike rental
- negative `duration_min`
- `member_age = 999` — a sentinel
- `end_time` earlier than `start_time`
- duplicate `ride_id` values (a re-delivered batch)
- `station_end = ST99`, which exists in no station file

**weather_2026-09.csv**
- Two rows recorded in Fahrenheit. Any `temp_c` above 45 is one of them.
- `precip_mm = -999` — sentinel for "no reading"
- 2026-09-05 missing entirely. **Do not invent it.** Decide what your join does with a gap and say so in your slides.

**stations.csv**
- `ST03` appears twice; the second copy has messy whitespace and casing
- `ST09` has `capacity = 0`
- `ST10` has no coordinates

## One thing worth noticing before you start

2026-09-04 has roughly half the rides of a normal weekday. Check the weather
file before you decide that is a data problem.
