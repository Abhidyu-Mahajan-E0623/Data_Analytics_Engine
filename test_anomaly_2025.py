"""
Standalone 2025 Anomaly Test Script — SNR Tables
-------------------------------------------------
Runs anomaly detection for 5 SNR tables.
Training: Jan–Oct 2025 (P1/P99 percentiles).
Test: November 2025 data checked against those bounds.
Outputs to Anomaly_2025.txt.
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

TRAINING_START = "2025-01-01"
TRAINING_END = "2025-10-31"
TEST_START = "2025-11-01"
TEST_END = "2025-11-30"

# Table configs: (table_name, date_col, metric_expr, metric_label, group_col or None)
TABLE_CONFIGS = [
    ("snr_dim_snr_change_log", "staged_file_date", "COUNT(*)", "Record Count", None),
    ("snr_dim_snr_demographics", "staged_file_date", "COUNT(*)", "Record Count", "outlet_class_of_trade"),
    ("snr_dim_snr_product", "staged_file_date", "COUNT(*)", "Record Count", None),
    ("snr_fact_snr_control", "week_ending_date", "SUM(COALESCE(TRY_CAST(`volume_units` AS DOUBLE), 0.0))", "Volume Units", None),
    ("snr_fact_snr_sales", "week_ending_date", "SUM(COALESCE(TRY_CAST(`volume_units` AS DOUBLE), 0.0))", "Volume Units", None),
]

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


def fmt(value):
    if value is None:
        return "N/A"
    return f"{float(value):,.2f}"


def run_quartile_check(conn, table_fqn, date_col, metric_expr, trunc_expr, extra_filter, min_window):
    """Run P1/P99 quartile check: train on Jan-Oct, test on Nov."""
    query = f"""
    WITH training AS (
        SELECT
            {trunc_expr} AS period,
            {metric_expr} AS metric_value
        FROM {table_fqn}
        WHERE `{date_col}` IS NOT NULL
          AND TO_DATE(`{date_col}`) >= '{TRAINING_START}'
          AND TO_DATE(`{date_col}`) <= '{TRAINING_END}'
          {extra_filter}
        GROUP BY {trunc_expr}
    ),
    bounds AS (
        SELECT
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY metric_value) AS p01,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY metric_value) AS p99,
            COUNT(*) AS window_size
        FROM training
    ),
    test_data AS (
        SELECT
            {trunc_expr} AS period,
            {metric_expr} AS metric_value
        FROM {table_fqn}
        WHERE `{date_col}` IS NOT NULL
          AND TO_DATE(`{date_col}`) >= '{TEST_START}'
          AND TO_DATE(`{date_col}`) <= '{TEST_END}'
          {extra_filter}
        GROUP BY {trunc_expr}
    )
    SELECT
        CAST(t.period AS STRING) AS period,
        t.metric_value,
        b.p01,
        b.p99,
        b.window_size
    FROM test_data t
    CROSS JOIN bounds b
    ORDER BY t.period
    """
    return fetch_all(conn, query)


def check_anomaly(row, min_window):
    """Check if a row is an anomaly."""
    val = row.get("metric_value")
    p01 = row.get("p01")
    p99 = row.get("p99")
    ws = row.get("window_size") or 0
    if val is None or p01 is None or p99 is None or ws < min_window:
        return "-"
    if float(val) < float(p01) or float(val) > float(p99):
        return "Y"
    return "N"


def run():
    print("Connecting to Databricks...")
    conn = connect()

    lines = []
    lines.append("=" * 80)
    lines.append("        SNR TABLE ANOMALY TEST REPORT — 2025")
    lines.append("=" * 80)
    lines.append(f"  Training : {TRAINING_START} to {TRAINING_END}")
    lines.append(f"  Test     : {TEST_START} to {TEST_END}")
    lines.append(f"  Method   : P1/P99 Percentile (quartile bounds from training data)")
    lines.append("")

    total_anomalies = 0

    for table_name, date_col, metric_expr, metric_label, group_col in TABLE_CONFIGS:
        table_fqn = f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"
        print(f"\n--- Processing: {table_name} ---")

        lines.append("=" * 80)
        lines.append(f"  TABLE: {CATALOG}.{SCHEMA}.{table_name}")
        lines.append(f"  Metric: {metric_label}")
        if group_col:
            lines.append(f"  Grouped by: {group_col}")
        lines.append("=" * 80)

        if group_col:
            # Get distinct groups
            groups_query = f"""
            SELECT DISTINCT CAST(`{group_col}` AS STRING) AS grp
            FROM {table_fqn}
            WHERE `{group_col}` IS NOT NULL
            ORDER BY grp
            """
            groups = [str(r.get("grp", "")) for r in fetch_all(conn, groups_query) if r.get("grp")]
            print(f"  Found {len(groups)} groups: {groups[:5]}{'...' if len(groups) > 5 else ''}")

            for grp in groups:
                grp_filter = f"AND CAST(`{group_col}` AS STRING) = '{grp}'"
                table_anomalies = _run_table_analysis(
                    conn, lines, table_fqn, date_col, metric_expr, metric_label,
                    grp_filter, f"[{grp}]",
                )
                total_anomalies += table_anomalies
        else:
            table_anomalies = _run_table_analysis(
                conn, lines, table_fqn, date_col, metric_expr, metric_label,
                "", "",
            )
            total_anomalies += table_anomalies

        lines.append("")

    # Summary
    lines.append("=" * 80)
    lines.append("  SUMMARY")
    lines.append("=" * 80)
    lines.append(f"  Total anomalies found in November test data: {total_anomalies}")
    lines.append("=" * 80)

    conn.close()

    report = "\n".join(lines)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(report + "\n")

    print(f"\nReport written to: {OUTPUT_FILE}")
    print(f"Total anomalies: {total_anomalies}")


def _run_table_analysis(conn, lines, table_fqn, date_col, metric_expr, metric_label, extra_filter, label_prefix):
    """Run monthly/weekly/daily analysis for a table (or table+group)."""
    anomaly_count = 0

    # --- Monthly ---
    trunc_monthly = f"DATE_TRUNC('month', TO_DATE(`{date_col}`))"
    monthly_rows = run_quartile_check(conn, table_fqn, date_col, metric_expr, trunc_monthly, extra_filter, 3)

    if label_prefix:
        lines.append(f"\n  {label_prefix}")
    lines.append(f"  --- Monthly {metric_label} ---")
    hdr = f"    {'Month':<12} {metric_label:>16} {'Lower (P1)':>14} {'Upper (P99)':>14} {'Outlier':>9}"
    lines.append(hdr)
    lines.append("    " + "-" * 67)

    for row in monthly_rows:
        period = str(row.get("period", ""))[:7]
        val = row.get("metric_value")
        p01 = row.get("p01")
        p99 = row.get("p99")
        outlier = check_anomaly(row, 3)
        if outlier == "Y":
            anomaly_count += 1
        lines.append(f"    {period:<12} {fmt(val):>16} {fmt(p01):>14} {fmt(p99):>14} {outlier:>9}")

    lines.append("    " + "-" * 67)

    # --- Weekly ---
    trunc_weekly = f"DATE_TRUNC('week', TO_DATE(`{date_col}`))"
    weekly_rows = run_quartile_check(conn, table_fqn, date_col, metric_expr, trunc_weekly, extra_filter, 5)

    lines.append(f"  --- Weekly {metric_label} ---")
    hdr = f"    {'Week Starting':<14} {metric_label:>14} {'Lower (P1)':>14} {'Upper (P99)':>14} {'Outlier':>9}"
    lines.append(hdr)
    lines.append("    " + "-" * 67)

    for row in weekly_rows:
        period = str(row.get("period", ""))[:10]
        val = row.get("metric_value")
        p01 = row.get("p01")
        p99 = row.get("p99")
        outlier = check_anomaly(row, 5)
        if outlier == "Y":
            anomaly_count += 1
        lines.append(f"    {period:<14} {fmt(val):>14} {fmt(p01):>14} {fmt(p99):>14} {outlier:>9}")

    lines.append("    " + "-" * 67)

    # --- Daily ---
    trunc_daily = f"TO_DATE(`{date_col}`)"
    daily_rows = run_quartile_check(conn, table_fqn, date_col, metric_expr, trunc_daily, extra_filter, 10)

    lines.append(f"  --- Daily {metric_label} ---")
    hdr = f"    {'Date':<12} {metric_label:>16} {'Lower (P1)':>14} {'Upper (P99)':>14} {'Outlier':>9}"
    lines.append(hdr)
    lines.append("    " + "-" * 67)

    for row in daily_rows:
        period = str(row.get("period", ""))[:10]
        val = row.get("metric_value")
        p01 = row.get("p01")
        p99 = row.get("p99")
        outlier = check_anomaly(row, 10)
        if outlier == "Y":
            anomaly_count += 1
        lines.append(f"    {period:<12} {fmt(val):>16} {fmt(p01):>14} {fmt(p99):>14} {outlier:>9}")

    lines.append("    " + "-" * 67)

    return anomaly_count


if __name__ == "__main__":
    run()
