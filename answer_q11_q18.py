#!/usr/bin/env python3
"""
Answer Q11-Q18 using SQL (PostgreSQL). Most questions are handled entirely in SQL
(window functions for ranks/moving averages). This script runs each query and prints
top rows, and can export CSV outputs.

Usage:
  python answer_q11_q18.py --dsn "host=localhost dbname=postgres user=postgres password=postgres" --outdir outputs
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import psycopg2


QUERIES = {
"Q11_city_rank_cinemas_sales_2018": r"""
WITH city_cinema_sales AS (
  SELECT
    s.cinema_city,
    s.cinema_name,
    SUM(f.ticket_price) AS total_sales_2018
  FROM dw.fact_ticket_sales f
  JOIN dw.dim_date d    ON d.date_key = f.date_key
  JOIN dw.dim_showing s ON s.showing_key = f.showing_key
  WHERE d.year_num = 2018
  GROUP BY s.cinema_city, s.cinema_name
)
SELECT
  cinema_city,
  cinema_name,
  total_sales_2018,
  RANK() OVER (PARTITION BY cinema_city ORDER BY total_sales_2018 DESC) AS sales_rank
FROM city_cinema_sales
ORDER BY cinema_city, sales_rank, cinema_name;
""",

"Q12_director_rank_movies_sales_under40": r"""
WITH base AS (
  SELECT
    s.director_name,
    s.movie_title,
    SUM(f.ticket_price) AS total_sales,
    COUNT(*) AS tickets_sold
  FROM dw.fact_ticket_sales f
  JOIN dw.dim_date d      ON d.date_key = f.date_key
  JOIN dw.dim_customer c  ON c.customer_key = f.customer_key
  JOIN dw.dim_showing s   ON s.showing_key = f.showing_key
  WHERE c.dob IS NOT NULL
    AND EXTRACT(YEAR FROM age(d.full_date, c.dob))::INT < 40
  GROUP BY s.director_name, s.movie_title
)
SELECT
  director_name,
  movie_title,
  total_sales,
  tickets_sold,
  RANK() OVER (PARTITION BY director_name ORDER BY total_sales DESC) AS movie_rank
FROM base
ORDER BY director_name, movie_rank, movie_title;
""",

"Q13_city_rank_browsers_online_tx": r"""
WITH base AS (
  SELECT
    s.cinema_city,
    ch.browser_name,
    COUNT(DISTINCT f.transaction_id) AS tx_count
  FROM dw.fact_ticket_sales f
  JOIN dw.dim_channel ch  ON ch.channel_key = f.channel_key
  JOIN dw.dim_showing s   ON s.showing_key = f.showing_key
  WHERE ch.channel_type = 'ONLINE'
  GROUP BY s.cinema_city, ch.browser_name
)
SELECT
  cinema_city,
  browser_name,
  tx_count,
  RANK() OVER (PARTITION BY cinema_city ORDER BY tx_count DESC) AS browser_rank
FROM base
ORDER BY cinema_city, browser_rank, browser_name;
""",

"Q14_top10_movies_2018_by_gender": r"""
WITH base AS (
  SELECT
    c.gender,
    s.movie_title,
    COUNT(*) AS tickets_sold
  FROM dw.fact_ticket_sales f
  JOIN dw.dim_date d     ON d.date_key = f.date_key
  JOIN dw.dim_customer c ON c.customer_key = f.customer_key
  JOIN dw.dim_showing s  ON s.showing_key = f.showing_key
  WHERE d.year_num = 2018
    AND c.gender IN ('M','F')
  GROUP BY c.gender, s.movie_title
),
ranked AS (
  SELECT
    gender,
    movie_title,
    tickets_sold,
    DENSE_RANK() OVER (PARTITION BY gender ORDER BY tickets_sold DESC) AS rnk
  FROM base
)
SELECT gender, movie_title, tickets_sold
FROM ranked
WHERE rnk <= 10
ORDER BY gender, tickets_sold DESC, movie_title;
""",

"Q15_city_top5_cinemas_tickets_2014_2018": r"""
WITH base AS (
  SELECT
    s.cinema_city,
    s.cinema_name,
    COUNT(*) AS tickets_sold
  FROM dw.fact_ticket_sales f
  JOIN dw.dim_date d    ON d.date_key = f.date_key
  JOIN dw.dim_showing s ON s.showing_key = f.showing_key
  WHERE d.year_num BETWEEN 2014 AND 2018
  GROUP BY s.cinema_city, s.cinema_name
),
ranked AS (
  SELECT
    cinema_city,
    cinema_name,
    tickets_sold,
    DENSE_RANK() OVER (PARTITION BY cinema_city ORDER BY tickets_sold DESC) AS rnk
  FROM base
)
SELECT cinema_city, cinema_name, tickets_sold
FROM ranked
WHERE rnk <= 5
ORDER BY cinema_city, tickets_sold DESC, cinema_name;
""",

"Q16_weekly_8wk_moving_avg_sales_2018": r"""
WITH weekly AS (
  SELECT
    d.week_start_date AS week_start,
    SUM(f.ticket_price) AS week_sales
  FROM dw.fact_ticket_sales f
  JOIN dw.dim_date d ON d.date_key = f.date_key
  WHERE d.year_num = 2018
  GROUP BY d.week_start_date
),
ma AS (
  SELECT
    week_start,
    week_sales,
    AVG(week_sales) OVER (ORDER BY week_start ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS ma_8wk
  FROM weekly
)
SELECT week_start, week_sales, ma_8wk
FROM ma
ORDER BY week_start;
""",

"Q17_top3_4wk_moving_avg_sales_2018": r"""
WITH weekly AS (
  SELECT
    d.week_start_date AS week_start,
    SUM(f.ticket_price) AS week_sales
  FROM dw.fact_ticket_sales f
  JOIN dw.dim_date d ON d.date_key = f.date_key
  WHERE d.year_num = 2018
  GROUP BY d.week_start_date
),
ma AS (
  SELECT
    week_start,
    AVG(week_sales) OVER (ORDER BY week_start ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ma_4wk
  FROM weekly
)
SELECT week_start, ma_4wk
FROM ma
ORDER BY ma_4wk DESC
LIMIT 3;
""",

"Q18_city_max_4wk_moving_avg_sales_2010_2018": r"""
WITH weekly_city AS (
  SELECT
    s.cinema_city,
    d.week_start_date AS week_start,
    SUM(f.ticket_price) AS week_sales
  FROM dw.fact_ticket_sales f
  JOIN dw.dim_date d    ON d.date_key = f.date_key
  JOIN dw.dim_showing s ON s.showing_key = f.showing_key
  WHERE d.year_num BETWEEN 2010 AND 2018
  GROUP BY s.cinema_city, d.week_start_date
),
ma AS (
  SELECT
    cinema_city,
    week_start,
    AVG(week_sales) OVER (PARTITION BY cinema_city ORDER BY week_start ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ma_4wk
  FROM weekly_city
),
mx AS (
  SELECT cinema_city, MAX(ma_4wk) AS max_ma_4wk
  FROM ma
  GROUP BY cinema_city
)
SELECT cinema_city, max_ma_4wk
FROM mx
ORDER BY max_ma_4wk DESC, cinema_city;
"""
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--outdir", default="outputs")
    ap.add_argument("--head", type=int, default=25)
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    conn = psycopg2.connect(args.dsn)

    for name, sql in QUERIES.items():
        df = pd.read_sql(sql, conn)
        csv_path = outdir / f"{name}.csv"
        df.to_csv(csv_path, index=False)
        print(f"\n=== {name} ===")
        print(df.head(args.head).to_string(index=False))
        print(f"Saved: {csv_path}")

    conn.close()

if __name__ == "__main__":
    main()
