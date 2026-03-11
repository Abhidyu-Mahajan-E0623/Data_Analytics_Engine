"""
Validation script — Rolling Month-by-Month Anomaly Detection
-------------------------------------------------------------
Runs the rolling-month logic against Databricks to verify anomalies are found.
For each month Feb-Dec, uses all prior months for P1/P99 bounds.
Prints detailed output showing bounds vs actual values per month.
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
YEAR = 2025

# (table, date_col, metric_expr, metric_label, group_col)
TABLE_CONFIGS = [
    ("snr_dim_snr_change_log", "staged_file_date", "COUNT(*)", "Record Count", None),
    ("snr_dim_snr_demographics", "staged_file_date", "COUNT(*)", "Record Count", "outlet_class_of_trade"),
    ("snr_dim_snr_product", "staged_file_date", "COUNT(*)", "Record Count", None),
    ("snr_fact_snr_control", "week_ending_date", "SUM(COALESCE(TRY_CAST(`volume_units` AS DOUBLE), 0.0))", "Volume Units", None),
    ("snr_fact_snr_sales", "week_ending_date", "SUM(COALESCE(TRY_CAST(`volume_units` AS DOUBLE), 0.0))", "Volume Units", None),
]

MONTH_NAMES = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "Anomaly_Validation.txt")


def connect():
    return dbsql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=TOKEN)


def fetch_all(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        columns = [col[0].lower() for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def fmt(v):
    if v is None: return "N/A"
    return f"{float(v):,.2f}"


def last_day(month):
    if month == 2: return 28
    if month in (4, 6, 9, 11): return 30
    return 31


def get_monthly_values(conn, table_fqn, date_col, metric_expr, extra_filter=""):
    """Get the aggregated monthly values for the entire year."""
    query = f"""
    SELECT
        MONTH(TO_DATE(`{date_col}`)) AS month_num,
        {metric_expr} AS metric_value
    FROM {table_fqn}
    WHERE `{date_col}` IS NOT NULL
      AND YEAR(TO_DATE(`{date_col}`)) = {YEAR}
      {extra_filter}
    GROUP BY MONTH(TO_DATE(`{date_col}`))
    ORDER BY month_num
    """
    return fetch_all(conn, query)


def run_rolling_check(conn, table_fqn, date_col, metric_expr, extra_filter=""):
    """Run rolling P1/P99 check for a table, return list of (month, value, p01, p99, is_anomaly)."""
    # First get all monthly values
    monthly = get_monthly_values(conn, table_fqn, date_col, metric_expr, extra_filter)
    monthly_map = {int(r["month_num"]): float(r["metric_value"]) for r in monthly if r.get("metric_value") is not None}

    results = []
    # January: no prior data, skip
    jan_val = monthly_map.get(1)
    results.append((1, jan_val, None, None, None, "-"))

    for test_month in range(2, 13):
        train_start = f"{YEAR}-01-01"
        prev_month = test_month - 1
        train_end = f"{YEAR}-{prev_month:02d}-{last_day(prev_month)}"
        test_start = f"{YEAR}-{test_month:02d}-01"
        test_end = f"{YEAR}-{test_month:02d}-{last_day(test_month)}"

        # Get bounds from training period
        bounds_query = f"""
        WITH training AS (
            SELECT
                DATE_TRUNC('month', TO_DATE(`{date_col}`)) AS period,
                {metric_expr} AS metric_value
            FROM {table_fqn}
            WHERE `{date_col}` IS NOT NULL
              AND TO_DATE(`{date_col}`) >= '{train_start}'
              AND TO_DATE(`{date_col}`) <= '{train_end}'
              {extra_filter}
            GROUP BY DATE_TRUNC('month', TO_DATE(`{date_col}`))
        )
        SELECT
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY metric_value) AS p01,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY metric_value) AS p99,
            COUNT(*) AS window_size,
            MIN(metric_value) AS min_val,
            MAX(metric_value) AS max_val,
            AVG(metric_value) AS avg_val
        FROM training
        """
        bounds = fetch_all(conn, bounds_query)
        if not bounds or bounds[0].get("window_size", 0) < 2:
            test_val = monthly_map.get(test_month)
            results.append((test_month, test_val, None, None, None, "skip (< 2 training months)"))
            continue

        b = bounds[0]
        p01 = float(b["p01"]) if b["p01"] is not None else None
        p99 = float(b["p99"]) if b["p99"] is not None else None
        ws = int(b.get("window_size", 0))

        test_val = monthly_map.get(test_month)

        if test_val is not None and p01 is not None and p99 is not None:
            if test_val < p01:
                status = "ANOMALY (Low)"
            elif test_val > p99:
                status = "ANOMALY (High)"
            else:
                status = "OK"
        else:
            status = "no data"

        results.append((test_month, test_val, p01, p99, ws, status))

    return results


def main():
    print("Connecting to Databricks...")
    conn = connect()
    lines = []

    lines.append("=" * 90)
    lines.append("   ANOMALY DETECTION VALIDATION — Rolling Month-by-Month Logic")
    lines.append("=" * 90)
    lines.append(f"   Year: {YEAR}")
    lines.append(f"   Method: For each month M (Feb-Dec), P1/P99 bounds from Jan to M-1")
    lines.append(f"   January always = no anomalies (no prior data)")
    lines.append("")

    total_anomalies = 0

    for table_name, date_col, metric_expr, metric_label, group_col in TABLE_CONFIGS:
        table_fqn = f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"
        lines.append("=" * 90)
        lines.append(f"  TABLE: {table_name}")
        lines.append(f"  Metric: {metric_label} | Date: {date_col}")
        if group_col:
            lines.append(f"  Grouped by: {group_col}")
        lines.append("=" * 90)

        if group_col:
            print(f"\n--- {table_name} (grouped by {group_col}) ---")
            groups_q = f"SELECT DISTINCT CAST(`{group_col}` AS STRING) AS grp FROM {table_fqn} WHERE `{group_col}` IS NOT NULL ORDER BY grp"
            groups = [str(r["grp"]) for r in fetch_all(conn, groups_q) if r.get("grp")]
            for grp in groups:
                grp_filter = f"AND CAST(`{group_col}` AS STRING) = '{grp}'"
                lines.append(f"\n  --- Group: {grp} ---")
                results = run_rolling_check(conn, table_fqn, date_col, metric_expr, grp_filter)
                total_anomalies += _print_results(lines, results, metric_label)
        else:
            print(f"\n--- {table_name} ---")
            results = run_rolling_check(conn, table_fqn, date_col, metric_expr)
            total_anomalies += _print_results(lines, results, metric_label)

        lines.append("")

    lines.append("=" * 90)
    lines.append(f"  TOTAL ANOMALIES FOUND: {total_anomalies}")
    lines.append("=" * 90)

    conn.close()
    report = "\n".join(lines)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(report + "\n")

    print(f"\nValidation report written to: {OUTPUT_FILE}")
    print(f"Total anomalies found: {total_anomalies}")
    if total_anomalies == 0:
        print("\n  ** WARNING: No anomalies found! The data may be very uniform.")
        print("     Check the validation report for monthly values and bounds.")


def _print_results(lines, results, metric_label):
    """Print and return count of anomalies."""
    anomaly_count = 0
    hdr = f"    {'Month':<8} {metric_label:>16} {'P01 (Lower)':>14} {'P99 (Upper)':>14} {'Window':>8} {'Status':>20}"
    lines.append(hdr)
    lines.append("    " + "-" * 82)
    for month_num, val, p01, p99, ws, status in results:
        mn = MONTH_NAMES[month_num]
        if "ANOMALY" in status:
            anomaly_count += 1
            marker = " *** "
        else:
            marker = "     "
        lines.append(
            f"    {mn:<8} {fmt(val):>16} {fmt(p01):>14} {fmt(p99):>14} {(str(ws) if ws else '-'):>8} {status:>20}{marker}"
        )
    lines.append("    " + "-" * 82)
    if anomaly_count:
        lines.append(f"    ==> {anomaly_count} anomalies found in this section")
    else:
        lines.append(f"    ==> No anomalies found")
    return anomaly_count


if __name__ == "__main__":
    main()
