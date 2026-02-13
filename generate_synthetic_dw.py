#!/usr/bin/env python3
"""
generate_synthetic_dw.py

Generate and load synthetic data for the Al Sinama DW (PostgreSQL).

What it does:
- Creates schema dw (star schema)
- Populates dimensions:
  - dw.dim_date: 2014-01-01 .. 2026-12-31 (includes week_start_date, iso_year, iso_week)
  - dw.dim_time: every 5 minutes
  - dw.dim_customer
  - dw.dim_promotion
  - dw.dim_channel (ONLINE/OFFLINE + browsers)
  - dw.dim_showing (denormalized; includes cinema_city)
- Generates ticket-level fact rows (>= 1,000,000)
- Loads facts using COPY for speed

Usage:
  python3 generate_synthetic_dw.py --dsn "host=localhost port=5432 dbname=as_dw2 user=postgres password=changeme" --rows 1000000

Notes:
- This script DROPS and RECREATES schema dw.
- If your machine is slow, reduce --customers and --showings, keep --rows >= 1000000.
"""

from __future__ import annotations

import argparse
import random
import string
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time
from pathlib import Path
from typing import Iterable, List

import psycopg2
import psycopg2.extras


# ----------------------------
# Helpers
# ----------------------------
MONTH_NAMES = [
    "January","February","March","April","May","June",
    "July","August","September","October","November","December"
]

def rand_name(rng: random.Random) -> str:
    first = rng.choice(["Ali","Omar","Sara","Mona","Yousef","Khalid","Noor","Huda","Adam","Lina","Zain","Mariam","Hassan","Rami"])
    last  = rng.choice(["Khan","Haddad","Saleh","Nasser","Fahad","Farouk","Gamal","Said","Aziz","Rahman","Hussein","Mahmoud"])
    return f"{first} {last}"

def rand_str(rng: random.Random, n: int) -> str:
    return "".join(rng.choice(string.ascii_uppercase + string.digits) for _ in range(n))

def date_range(start: date, end: date) -> Iterable[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def is_weekend_qatar_style(d: date) -> bool:
    # Fri/Sat = weekend
    return d.weekday() in (4, 5)

def time_slot(t: time) -> str:
    if time(6,0) <= t <= time(11,59,59):
        return "MORNING"
    if time(12,0) <= t <= time(17,59,59):
        return "AFTERNOON"
    return "NIGHT"

def hall_size_category(size: int) -> str:
    if size <= 80:
        return "SMALL"
    if size <= 180:
        return "MID"
    return "LARGE"


# ----------------------------
# DDL (executed statement-by-statement)
# ----------------------------
DDL_STATEMENTS = [
    "DROP SCHEMA IF EXISTS dw CASCADE",
    "CREATE SCHEMA IF NOT EXISTS dw",

    # Date dimension (includes week fields for moving averages)
    """
    CREATE TABLE dw.dim_date (
      date_key        INTEGER PRIMARY KEY,             -- yyyymmdd
      full_date       DATE NOT NULL UNIQUE,
      day_of_month    SMALLINT NOT NULL,
      month_num       SMALLINT NOT NULL,
      month_name      TEXT NOT NULL,
      quarter_num     SMALLINT NOT NULL,
      year_num        SMALLINT NOT NULL,
      is_weekend      BOOLEAN NOT NULL,
      week_start_date DATE NOT NULL,                   -- Monday-based week start
      iso_year        SMALLINT NOT NULL,
      iso_week        SMALLINT NOT NULL
    )
    """,

    """
    CREATE TABLE dw.dim_time (
      time_key     INTEGER PRIMARY KEY,                -- hhmmss
      full_time    TIME NOT NULL UNIQUE,
      hour_num     SMALLINT NOT NULL,
      minute_num   SMALLINT NOT NULL,
      time_slot    TEXT NOT NULL
    )
    """,

    """
    CREATE TABLE dw.dim_customer (
      customer_key BIGSERIAL PRIMARY KEY,
      customer_id  BIGINT NOT NULL UNIQUE,
      name         TEXT NOT NULL,
      dob          DATE,
      gender       TEXT,
      address      TEXT
    )
    """,

    """
    CREATE TABLE dw.dim_promotion (
      promotion_key BIGSERIAL PRIMARY KEY,
      promotion_id  BIGINT UNIQUE,
      description   TEXT NOT NULL,
      discount      NUMERIC(6,2),
      start_date    DATE,
      end_date      DATE
    )
    """,

    """
    CREATE TABLE dw.dim_channel (
      channel_key   BIGSERIAL PRIMARY KEY,
      channel_type  TEXT NOT NULL,                     -- ONLINE/OFFLINE
      system_name   TEXT,
      browser_name  TEXT,
      pay_method    TEXT NOT NULL
    )
    """,

    # Showing dimension (denormalized; includes cinema_city for city-level queries)
    """
    CREATE TABLE dw.dim_showing (
      showing_key        BIGSERIAL PRIMARY KEY,
      showing_id         BIGINT NOT NULL UNIQUE,
      showing_date       DATE NOT NULL,
      showing_time       TIME NOT NULL,
      showing_is_weekend BOOLEAN NOT NULL,
      showing_time_slot  TEXT NOT NULL,

      hall_id            BIGINT,
      hall_size          INTEGER,
      hall_size_category TEXT NOT NULL,

      cinema_id          BIGINT,
      cinema_name        TEXT,
      cinema_address     TEXT,
      cinema_city        TEXT,
      cinema_state       TEXT,

      movie_id           BIGINT,
      movie_title        TEXT,
      movie_language     TEXT,
      movie_release_date DATE,
      movie_cost         NUMERIC(12,2),
      movie_country      TEXT,

      director_id        BIGINT,
      director_name      TEXT,
      director_dob       DATE,
      director_gender    TEXT,

      genre_name         TEXT,
      cast_list          TEXT,
      has_omar_sharif    BOOLEAN NOT NULL DEFAULT FALSE
    )
    """,

    # Fact: 1 row per ticket sold
    """
    CREATE TABLE dw.fact_ticket_sales (
      fact_id        BIGSERIAL PRIMARY KEY,
      ticket_id      BIGINT NOT NULL,
      transaction_id BIGINT NOT NULL,
      date_key       INTEGER NOT NULL REFERENCES dw.dim_date(date_key),
      time_key       INTEGER NOT NULL REFERENCES dw.dim_time(time_key),
      customer_key   BIGINT NOT NULL REFERENCES dw.dim_customer(customer_key),
      promotion_key  BIGINT NOT NULL REFERENCES dw.dim_promotion(promotion_key),
      channel_key    BIGINT NOT NULL REFERENCES dw.dim_channel(channel_key),
      showing_key    BIGINT NOT NULL REFERENCES dw.dim_showing(showing_key),
      ticket_price   NUMERIC(8,2) NOT NULL,
      ticket_count   SMALLINT NOT NULL DEFAULT 1
    )
    """,

    "CREATE INDEX ix_fact_date     ON dw.fact_ticket_sales(date_key)",
    "CREATE INDEX ix_fact_showing  ON dw.fact_ticket_sales(showing_key)",
    "CREATE INDEX ix_fact_channel  ON dw.fact_ticket_sales(channel_key)",
    "CREATE INDEX ix_fact_customer ON dw.fact_ticket_sales(customer_key)",
    "CREATE INDEX ix_fact_tx       ON dw.fact_ticket_sales(transaction_id)",
]


# ----------------------------
# Dimension builders
# ----------------------------
def build_dim_date(cur, start: date, end: date):
    rows = []
    for d in date_range(start, end):
        date_key = int(d.strftime("%Y%m%d"))
        iso_year, iso_week, _ = d.isocalendar()
        week_start = d - timedelta(days=d.weekday())  # Monday
        rows.append((
            date_key, d, d.day, d.month, MONTH_NAMES[d.month-1],
            (d.month-1)//3 + 1, d.year, is_weekend_qatar_style(d),
            week_start, iso_year, iso_week
        ))

    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO dw.dim_date
           (date_key, full_date, day_of_month, month_num, month_name, quarter_num, year_num, is_weekend,
            week_start_date, iso_year, iso_week)
           VALUES %s""",
        rows,
        page_size=5000
    )

def build_dim_time(cur):
    rows = []
    for hour in range(24):
        for minute in range(0, 60, 5):
            t = time(hour, minute, 0)
            time_key = hour*10000 + minute*100
            rows.append((time_key, t, hour, minute, time_slot(t)))

    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO dw.dim_time (time_key, full_time, hour_num, minute_num, time_slot) VALUES %s",
        rows
    )

def build_dim_promotion(cur):
    rows = [
        (None, "No Promotion", 0.00, None, None),
        (1, "Discount", 10.00, date(2014,1,1), date(2026,12,31)),
        (2, "Student", 15.00, date(2014,1,1), date(2026,12,31)),
        (3, "Weekend Deal", 12.50, date(2014,1,1), date(2026,12,31)),
    ]
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO dw.dim_promotion (promotion_id, description, discount, start_date, end_date) VALUES %s",
        rows
    )

def build_dim_channel(cur):
    pay_methods = ["Cash","Card","BenefitPay","ApplePay","GooglePay"]
    systems = ["iOS","Android","Web"]
    browsers = ["Chrome","Safari","Firefox","Edge"]

    rows = []
    # OFFLINE
    for pm in pay_methods:
        rows.append(("OFFLINE", None, None, pm))
    # ONLINE
    for pm in pay_methods:
        for sys in systems:
            for br in browsers:
                rows.append(("ONLINE", sys, br, pm))

    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO dw.dim_channel (channel_type, system_name, browser_name, pay_method) VALUES %s",
        rows
    )

def build_dim_customer(cur, rng: random.Random, n_customers: int):
    rows = []
    for i in range(1, n_customers+1):
        dob = date(rng.randint(1940, 2012), rng.randint(1, 12), rng.randint(1, 28))
        gender = rng.choice(["M","F","Other"])
        addr = rng.choice(["Doha","Al Wakrah","Al Rayyan","Al Khor","Lusail","Umm Salal"]) + ", Qatar"
        rows.append((i, rand_name(rng), dob, gender, addr))

    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO dw.dim_customer (customer_id, name, dob, gender, address) VALUES %s",
        rows,
        page_size=5000
    )

@dataclass
class Cinema:
    cinema_id: int
    cinema_name: str
    city: str
    state: str
    address: str

def build_dim_showing(cur, rng: random.Random, start: date, end: date, n_showings: int):
    genres = ["Action","Drama","Comedy","Sci-Fi","Romance","Thriller","Animation","Horror"]
    directors = [
        ("Mohamed Khan", date(1942,10,26), "M"),
        ("Nadine Labaki", date(1974,2,18), "F"),
        ("Christopher Nolan", date(1970,7,30), "M"),
        ("Hayao Miyazaki", date(1941,1,5), "M"),
        ("Greta Gerwig", date(1983,8,4), "F"),
    ]
    cities_states = [
        ("Doha","Qatar"),("Al Rayyan","Qatar"),("Al Wakrah","Qatar"),
        ("Dubai","UAE"),("Abu Dhabi","UAE"),("Riyadh","KSA"),("Jeddah","KSA"),
        ("Manama","Bahrain"),("Kuwait City","Kuwait")
    ]

    cinemas: List[Cinema] = []
    for cid in range(1, 61):
        city, state = rng.choice(cities_states)
        cinemas.append(Cinema(
            cinema_id=cid,
            cinema_name=f"Cinema {cid:02d}",
            city=city,
            state=state,
            address=f"{rng.randint(1,199)} Main St, {city}"
        ))

    movies = []
    for mid in range(1, 801):
        dname, ddob, dgen = rng.choice(directors)
        cast = (
            "Omar Sharif, " + rand_name(rng)
            if rng.random() < 0.03
            else rand_name(rng) + ", " + rand_name(rng)
        )
        movies.append((
            mid,
            f"Movie {mid:04d}",
            rng.choice(["English","Arabic","Hindi","French","Japanese"]),
            date(rng.randint(2000, 2026), rng.randint(1,12), rng.randint(1,28)),
            round(rng.uniform(1_000_000, 250_000_000), 2),
            rng.choice(["USA","UK","Egypt","Qatar","India","France","Japan"]),
            dname, ddob, dgen,
            rng.choice(genres),
            cast
        ))

    span_days = (end - start).days
    rows = []
    for sid in range(1, n_showings+1):
        d = start + timedelta(days=rng.randint(0, span_days))
        t = time(rng.randint(10, 23), rng.choice([0,5,10,15,20,25,30,35,40,45,50,55]), 0)
        cin = rng.choice(cinemas)
        hall_id = rng.randint(1, 500)
        hall_size = rng.randint(40, 260)
        hall_cat = hall_size_category(hall_size)
        movie = rng.choice(movies)

        (movie_id, movie_title, movie_lang, rel_date, movie_cost, movie_country,
         dname, ddob, dgen, genre, cast) = movie

        has_omar = "Omar Sharif" in cast

        rows.append((
            sid, d, t, is_weekend_qatar_style(d), time_slot(t),
            hall_id, hall_size, hall_cat,
            cin.cinema_id, cin.cinema_name, cin.address, cin.city, cin.state,
            movie_id, movie_title, movie_lang, rel_date, movie_cost, movie_country,
            None, dname, ddob, dgen,
            genre, cast, has_omar
        ))

    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO dw.dim_showing (
            showing_id, showing_date, showing_time, showing_is_weekend, showing_time_slot,
            hall_id, hall_size, hall_size_category,
            cinema_id, cinema_name, cinema_address, cinema_city, cinema_state,
            movie_id, movie_title, movie_language, movie_release_date, movie_cost, movie_country,
            director_id, director_name, director_dob, director_gender,
            genre_name, cast_list, has_omar_sharif
        ) VALUES %s""",
        rows,
        page_size=5000
    )


# ----------------------------
# Fact generator + loader
# ----------------------------
def load_fact(cur, conn, rng: random.Random, n_rows: int):
    """
    Generates ticket-level rows grouped into transactions.
    Writes temp TSV then COPY into dw.fact_ticket_sales.
    """
    # Ensure schema is resolvable during COPY
    cur.execute("SET search_path TO dw, public;")

    # Pre-fetch dimension keys for fast sampling
    cur.execute("SELECT date_key FROM dw.dim_date")
    date_keys = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT time_key FROM dw.dim_time")
    time_keys = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT customer_key FROM dw.dim_customer")
    customer_keys = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT promotion_key FROM dw.dim_promotion")
    promotion_keys = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT channel_key, channel_type FROM dw.dim_channel")
    channels = cur.fetchall()

    cur.execute("SELECT showing_key FROM dw.dim_showing")
    showing_keys = [r[0] for r in cur.fetchall()]

    # Temp file (TSV)
    tmp_path = Path("fact_ticket_sales.tsv")
    ticket_id = 1
    tx_id = 1

    with tmp_path.open("w", encoding="utf-8") as f:
        remaining = n_rows
        while remaining > 0:
            tickets_in_tx = rng.randint(1, 6)
            tickets_in_tx = min(tickets_in_tx, remaining)

            date_key = rng.choice(date_keys)
            time_key = rng.choice(time_keys)
            cust_key = rng.choice(customer_keys)
            promo_key = rng.choice(promotion_keys)
            ch_key, ch_type = rng.choice(channels)
            showing_key = rng.choice(showing_keys)

            base_price = rng.uniform(20, 80)
            if ch_type == "ONLINE":
                base_price += rng.uniform(-2, 3)
            if promo_key is not None and promo_key != 1 and rng.random() < 0.35:
                base_price *= rng.uniform(0.80, 0.95)

            for _ in range(tickets_in_tx):
                price = round(max(10.0, base_price + rng.uniform(-5, 5)), 2)
                f.write(
                    f"{ticket_id}\t{tx_id}\t{date_key}\t{time_key}\t{cust_key}\t{promo_key}\t"
                    f"{ch_key}\t{showing_key}\t{price}\t1\n"
                )
                ticket_id += 1

            tx_id += 1
            remaining -= tickets_in_tx

    # COPY (use copy_expert so schema + columns are explicit)
    with tmp_path.open("r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY dw.fact_ticket_sales
              (ticket_id, transaction_id, date_key, time_key, customer_key, promotion_key, channel_key, showing_key, ticket_price, ticket_count)
            FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')
            """,
            f
        )

    conn.commit()
    tmp_path.unlink(missing_ok=True)


# ----------------------------
# Main
# ----------------------------
def build_schema(cur, conn):
    for stmt in DDL_STATEMENTS:
        s = stmt.strip().rstrip(";")
        if not s:
            continue
        cur.execute(s + ";")
    conn.commit()

    # Hard sanity check
    cur.execute("SELECT to_regclass('dw.fact_ticket_sales')")
    if cur.fetchone()[0] is None:
        raise RuntimeError("Schema build failed: dw.fact_ticket_sales not created")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True, help="psycopg2 DSN string")
    ap.add_argument("--rows", type=int, default=1_000_000, help="Number of fact rows (>= 1,000,000 recommended)")
    ap.add_argument("--customers", type=int, default=80_000, help="Number of customers")
    ap.add_argument("--showings", type=int, default=40_000, help="Number of showings")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--start", default="2014-01-01")
    ap.add_argument("--end", default="2026-12-31")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = False
    cur = conn.cursor()

    # Build schema
    build_schema(cur, conn)

    # Populate dimensions
    build_dim_date(cur, start, end); conn.commit()
    build_dim_time(cur); conn.commit()
    build_dim_promotion(cur); conn.commit()
    build_dim_channel(cur); conn.commit()
    build_dim_customer(cur, rng, args.customers); conn.commit()
    build_dim_showing(cur, rng, start, end, args.showings); conn.commit()

    # Populate fact
    load_fact(cur, conn, rng, args.rows)

    # Quick counts (for your screenshot workflow)
    cur.execute("SELECT COUNT(*) FROM dw.fact_ticket_sales")
    fact_cnt = cur.fetchone()[0]
    print(f"Loaded fact rows: {fact_cnt:,}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
