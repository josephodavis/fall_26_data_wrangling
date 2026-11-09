"""Small, mildly messy datasets for the ETL lesson and the in-class activity.

Deliberately kept SIMPLE. There are exactly two kinds of mess in each dataset:
  - one column that arrived as text and needs cleaning
  - one column with inconsistent capitalisation / stray spaces
Everything else is clean, so the lesson stays about the PIPELINE, not about puzzles.
"""
import numpy as np
import pandas as pd
from pathlib import Path

RNG = np.random.default_rng(153)

# =====================================================================
# 1. LECTURE DATA -- Bison Cafe daily sales
# =====================================================================
MENU = [
    ("Latte",         "Drink", 4.25),
    ("Drip Coffee",   "Drink", 2.50),
    ("Hot Chocolate", "Drink", 3.75),
    ("Iced Tea",      "Drink", 2.95),
    ("Bagel",         "Food",  2.25),
    ("Muffin",        "Food",  3.10),
    ("Turkey Wrap",   "Food",  7.50),
    ("Fruit Cup",     "Food",  4.00),
]
CAFE_DAYS = ["2027-03-02", "2027-03-03", "2027-03-04"]

# How the point-of-sale system mangles an item name.
def messy_name(name):
    roll = RNG.random()
    if roll < 0.20:
        return name.upper()
    if roll < 0.40:
        return name.lower()
    if roll < 0.55:
        return f"  {name} "
    return name

out = Path("data")
out.mkdir(exist_ok=True)

sale_id = 5000
for day in CAFE_DAYS:
    n = int(RNG.integers(32, 39))
    rows = []
    for _ in range(n):
        item, _cat, price = MENU[RNG.integers(len(MENU))]
        sale_id += 1
        hour = int(RNG.integers(7, 19))
        rows.append({
            "sale_id":    f"S-{sale_id}",
            "sale_date":  day,
            "sale_time":  f"{hour:02d}:{int(RNG.integers(0,60)):02d}",
            "item":       messy_name(item),
            "qty":        int(RNG.integers(1, 4)),
            "unit_price": f"${price:.2f}",        # <-- arrives as TEXT with a dollar sign
        })
    pd.DataFrame(rows).to_csv(out / f"sales_{day}.csv", index=False)
    print("wrote", out / f"sales_{day}.csv", f"({n} rows)")

pd.DataFrame(MENU, columns=["item", "category", "menu_price"]).to_csv(
    out / "menu.csv", index=False)
print("wrote", out / "menu.csv")

# =====================================================================
# 2. ACTIVITY DATA -- Founders Library checkouts
# =====================================================================
BOOKS = [
    ("BK-101", "Introductory Statistics",      "Mathematics", 89.00),
    ("BK-102", "A History of Washington, DC",  "History",     42.50),
    ("BK-103", "Organic Chemistry",            "Science",    134.00),
    ("BK-104", "Poems of the Harlem Era",      "Literature",  28.75),
    ("BK-105", "Linear Algebra Done Right",    "Mathematics", 64.00),
    ("BK-106", "Public Health Policy",         "Health",      71.25),
    ("BK-107", "Jazz and the American City",   "Music",       38.00),
    ("BK-108", "Databases: A First Course",    "Computing",   95.50),
]
PATRON_TYPES = ["Undergrad", "Graduate", "Faculty", "Staff"]
LIB_DAYS = ["2027-03-02", "2027-03-03", "2027-03-04"]

aout = Path("activity_data")
aout.mkdir(exist_ok=True)

co_id = 7000
for day in LIB_DAYS:
    n = int(RNG.integers(26, 33))
    rows = []
    for _ in range(n):
        book = BOOKS[RNG.integers(len(BOOKS))]
        patron = PATRON_TYPES[RNG.integers(len(PATRON_TYPES))]
        roll = RNG.random()
        if roll < 0.25:
            patron = patron.upper()
        elif roll < 0.45:
            patron = patron.lower()
        elif roll < 0.60:
            patron = f" {patron}  "
        co_id += 1
        rows.append({
            "checkout_id":   f"C-{co_id}",
            "checkout_date": day,
            "book_id":       book[0],
            "patron_type":   patron,
            "loan_days":     f"{int(RNG.choice([7, 14, 21, 28]))} days",   # <-- TEXT
        })
    pd.DataFrame(rows).to_csv(aout / f"checkouts_{day}.csv", index=False)
    print("wrote", aout / f"checkouts_{day}.csv", f"({n} rows)")

pd.DataFrame(BOOKS, columns=["book_id", "title", "genre", "replacement_cost"]).to_csv(
    aout / "books.csv", index=False)
print("wrote", aout / "books.csv")
