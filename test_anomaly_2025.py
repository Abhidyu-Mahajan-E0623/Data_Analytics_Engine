"""
Standalone 2025 Anomaly Test Script
------------------------------------
Runs anomaly detection for 2025 only, showing ALL months and weeks
with a Y/N Outlier column. Outputs to Anomaly_2025.txt.

Uses fixed full-year P1/P99 percentiles computed from all 2025 data.

This is a standalone test script, separate from the main project pipeline.
"""

import os
from dotenv import load_dotenv
from databricks import sql as dbsql

load_dotenv()

HOST = os.getenv("DATABRICKS_HOST", "").replace("https://", "")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
WAREHOUSE_ID = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "")
CATALOG = os.getenv("DATABRICKS_CATALOG", "new_claim_catalog")
SCHEMA = "bronze"
HTTP_PATH = f"/sql/1.0/warehouses/{WAREHOUSE_ID}"

# -------------------------------------------------------------------
# Table and column discovery
# -------------------------------------------------------------------
TABLE_CANDIDATES = ["raw_ics_867_csl", "raw_ics_867_csl_sales"]
DATE_CANDIDATES = ["report_date_v", "report_date", "sale_date", "sales_date", "invoice_date"]
QTY_CANDIDATES = ["sales_qty_v", "sales_qty", "quantity", "qty", "sales_quantity"]

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "Anomaly_2025.txt")


def connect():
    return dbsql.connect(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        access_token=TOKEN,
    )


def fetch_all(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        columns = [col[0].lower() for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def find_table(conn):
    rows = fetch_all(conn, f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`")
    available = set()
    for row in rows:
        name = row.get("tablename") or row.get("table_name") or row.get("table")
        if name:
            available.add(str(name).strip().lower())
    for candidate in TABLE_CANDIDATES:
        if candidate.lower() in available:
            return candidate.lower()
    return None


def find_columns(conn, table_name):
    rows = fetch_all(conn, f"""
        SELECT LOWER(column_name) AS column_name
        FROM `{CATALOG}`.information_schema.columns
        WHERE table_schema = '{SCHEMA}' AND table_name = '{table_name}'
    """)
    return {str(r["column_name"]).strip().lower() for r in rows}


def pick(columns, candidates):
    for c in candidates:
        if c.lower() in columns:
            return c.lower()
    return None


def fmt(value):
    if value is None:
        return "N/A"
    return f"{float(value):,.2f}"


def run():
    print("Connecting to Databricks...")
    conn = connect()

    table_name = find_table(conn)
    if not table_name:
        print(f"ERROR: No sales table found. Checked: {TABLE_CANDIDATES}")
        conn.close()
        return

    table_fqn = f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"
    columns = find_columns(conn, table_name)
    date_col = pick(columns, DATE_CANDIDATES)
    qty_col = pick(columns, QTY_CANDIDATES)

    if not date_col or not qty_col:
        print(f"ERROR: Missing columns. date={date_col}, qty={qty_col}")
        conn.close()
        return

    print(f"Table: {table_fqn}")
    print(f"Date column: {date_col}, Qty column: {qty_col}")

    lines = []
    lines.append("=" * 80)
    lines.append("        ANOMALY DETECTION TEST REPORT — 2025")
    lines.append("=" * 80)
    lines.append(f"  Table : {CATALOG}.{SCHEMA}.{table_name}")
    lines.append(f"  Method: 1st / 99th Percentile (full-year fixed bounds)")
    lines.append(f"  Year  : 2025 only")
    lines.append("")

    # ---------------------------------------------------------------
    # MONTHLY — fixed full-year P1/P99 bounds
    # ---------------------------------------------------------------
    print("Running monthly analysis...")
    monthly_query = f"""
    WITH monthly AS (
        SELECT
            DATE_TRUNC('month', TO_DATE(`{date_col}`)) AS period,
            SUM(COALESCE(TRY_CAST(`{qty_col}` AS DOUBLE), 0.0)) AS total_qty
        FROM {table_fqn}
        WHERE `{date_col}` IS NOT NULL
          AND YEAR(TO_DATE(`{date_col}`)) = 2025
        GROUP BY DATE_TRUNC('month', TO_DATE(`{date_col}`))
    ),
    yearly_bounds AS (
        SELECT
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY total_qty) AS p01,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_qty) AS p99,
            COUNT(*) AS window_size
        FROM monthly
    )
    SELECT
        CAST(m.period AS STRING) AS period,
        m.total_qty,
        b.p01,
        b.p99,
        b.window_size
    FROM monthly m
    CROSS JOIN yearly_bounds b
    ORDER BY m.period
    """
    monthly_rows = fetch_all(conn, monthly_query)

    lines.append("-" * 80)
    lines.append("  MONTHLY TRENDS (full-year P1/P99 bounds)")
    lines.append("-" * 80)
    hdr = f"  {'Month':<12} {'Sales Volume':>16} {'Lower Bound (P1)':>18} {'Upper Bound (P99)':>19} {'Outlier':>9}"
    lines.append(hdr)
    lines.append("  " + "-" * 76)

    for row in monthly_rows:
        period = str(row.get("period", ""))[:7]
        qty = row.get("total_qty")
        p01 = row.get("p01")
        p99 = row.get("p99")
        ws = row.get("window_size") or 0

        if p01 is not None and p99 is not None and ws >= 3:
            is_outlier = "Y" if (float(qty) < float(p01) or float(qty) > float(p99)) else "N"
        else:
            is_outlier = "-"  # not enough data

        lines.append(f"  {period:<12} {fmt(qty):>16} {fmt(p01):>18} {fmt(p99):>19} {is_outlier:>9}")

    lines.append("  " + "-" * 76)
    lines.append("")

    # ---------------------------------------------------------------
    # WEEKLY — fixed full-year P1/P99 bounds
    # ---------------------------------------------------------------
    print("Running weekly analysis...")
    weekly_query = f"""
    WITH weekly AS (
        SELECT
            DATE_TRUNC('week', TO_DATE(`{date_col}`)) AS period,
            SUM(COALESCE(TRY_CAST(`{qty_col}` AS DOUBLE), 0.0)) AS total_qty
        FROM {table_fqn}
        WHERE `{date_col}` IS NOT NULL
          AND YEAR(TO_DATE(`{date_col}`)) = 2025
        GROUP BY DATE_TRUNC('week', TO_DATE(`{date_col}`))
    ),
    yearly_bounds AS (
        SELECT
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY total_qty) AS p01,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_qty) AS p99,
            COUNT(*) AS window_size
        FROM weekly
    )
    SELECT
        CAST(w.period AS STRING) AS period,
        w.total_qty,
        b.p01,
        b.p99,
        b.window_size
    FROM weekly w
    CROSS JOIN yearly_bounds b
    ORDER BY w.period
    """
    weekly_rows = fetch_all(conn, weekly_query)

    lines.append("-" * 80)
    lines.append("  WEEKLY TRENDS (full-year P1/P99 bounds)")
    lines.append("-" * 80)
    hdr = f"  {'Week Starting':<14} {'Sales Volume':>16} {'Lower Bound (P1)':>18} {'Upper Bound (P99)':>19} {'Outlier':>9}"
    lines.append(hdr)
    lines.append("  " + "-" * 76)

    for row in weekly_rows:
        period = str(row.get("period", ""))[:10]
        qty = row.get("total_qty")
        p01 = row.get("p01")
        p99 = row.get("p99")
        ws = row.get("window_size") or 0

        if p01 is not None and p99 is not None and ws >= 5:
            is_outlier = "Y" if (float(qty) < float(p01) or float(qty) > float(p99)) else "N"
        else:
            is_outlier = "-"

        lines.append(f"  {period:<14} {fmt(qty):>16} {fmt(p01):>18} {fmt(p99):>19} {is_outlier:>9}")

    lines.append("  " + "-" * 76)
    lines.append("")

    # ---------------------------------------------------------------
    # DAILY — fixed full-year P1/P99 bounds
    # ---------------------------------------------------------------
    print("Running daily analysis...")
    daily_query = f"""
    WITH daily AS (
        SELECT
            TO_DATE(`{date_col}`) AS period,
            SUM(COALESCE(TRY_CAST(`{qty_col}` AS DOUBLE), 0.0)) AS total_qty
        FROM {table_fqn}
        WHERE `{date_col}` IS NOT NULL
          AND YEAR(TO_DATE(`{date_col}`)) = 2025
        GROUP BY TO_DATE(`{date_col}`)
    ),
    yearly_bounds AS (
        SELECT
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY total_qty) AS p01,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_qty) AS p99,
            COUNT(*) AS window_size
        FROM daily
    )
    SELECT
        CAST(d.period AS STRING) AS period,
        d.total_qty,
        b.p01,
        b.p99,
        b.window_size
    FROM daily d
    CROSS JOIN yearly_bounds b
    ORDER BY d.period
    """
    daily_rows = fetch_all(conn, daily_query)

    lines.append("-" * 80)
    lines.append("  DAILY TRENDS (full-year P1/P99 bounds)")
    lines.append("-" * 80)
    hdr = f"  {'Date':<12} {'Sales Volume':>16} {'Lower Bound (P1)':>18} {'Upper Bound (P99)':>19} {'Outlier':>9}"
    lines.append(hdr)
    lines.append("  " + "-" * 76)

    for row in daily_rows:
        period = str(row.get("period", ""))[:10]
        qty = row.get("total_qty")
        p01 = row.get("p01")
        p99 = row.get("p99")
        ws = row.get("window_size") or 0

        if p01 is not None and p99 is not None and ws >= 10:
            is_outlier = "Y" if (float(qty) < float(p01) or float(qty) > float(p99)) else "N"
        else:
            is_outlier = "-"

        lines.append(f"  {period:<12} {fmt(qty):>16} {fmt(p01):>18} {fmt(p99):>19} {is_outlier:>9}")

    lines.append("  " + "-" * 76)
    lines.append("")

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    monthly_outliers = sum(1 for row in monthly_rows if _is_outlier(row, 3))
    weekly_outliers = sum(1 for row in weekly_rows if _is_outlier(row, 5))
    daily_outliers = sum(1 for row in daily_rows if _is_outlier(row, 10))

    lines.append("=" * 80)
    lines.append("  SUMMARY")
    lines.append("=" * 80)
    lines.append(f"  Monthly : {len(monthly_rows)} periods, {monthly_outliers} outliers")
    lines.append(f"  Weekly  : {len(weekly_rows)} periods, {weekly_outliers} outliers")
    lines.append(f"  Daily   : {len(daily_rows)} periods, {daily_outliers} outliers")
    lines.append("=" * 80)

    conn.close()

    report = "\n".join(lines)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(report + "\n")

    print(f"\nReport written to: {OUTPUT_FILE}")
    print(f"Monthly: {len(monthly_rows)} periods, {monthly_outliers} outliers")
    print(f"Weekly:  {len(weekly_rows)} periods, {weekly_outliers} outliers")
    print(f"Daily:   {len(daily_rows)} periods, {daily_outliers} outliers")


def _is_outlier(row, min_window):
    qty = row.get("total_qty")
    p01 = row.get("p01")
    p99 = row.get("p99")
    ws = row.get("window_size") or 0
    if p01 is None or p99 is None or ws < min_window:
        return False
    return float(qty) < float(p01) or float(qty) > float(p99)


if __name__ == "__main__":
    run()
