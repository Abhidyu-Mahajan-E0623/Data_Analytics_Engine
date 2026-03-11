"""SNR-table anomaly detection pipeline.

Targets 5 specific SNR tables. Uses a rolling month-by-month approach:
- January: always 0 anomalies (no prior data)
- February: bounds from Jan
- March: bounds from Jan-Feb
- ...
- December: bounds from Jan-Nov

Each month's data is tested against P1/P99 bounds computed from ALL prior months.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.config.settings import Settings
from src.connectors.databricks_sql import DatabricksSQLClient
from src.utils.io import anomaly_output_dir, atomic_write_text, ensure_project_dirs
from src.utils.logging import configure_logging
from src.utils.time import new_run_id, utc_iso

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SNR_TARGET_TABLES = [
    "snr_dim_snr_change_log",
    "snr_dim_snr_demographics",
    "snr_dim_snr_product",
    "snr_fact_snr_control",
    "snr_fact_snr_sales",
]

# Months to process (Feb-Dec; Jan is always clean — no prior data)
YEAR = 2025
MONTHS = list(range(2, 13))  # Feb(2) through Dec(12)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class AnomalyFinding:
    """Single anomaly finding for the report."""
    period: str
    actual_value: float
    lower_bound: float
    upper_bound: float
    direction: str  # "Unusually High" or "Unusually Low"
    group_label: str | None = None


@dataclass
class DetectorResult:
    """Result for a single table-level detector."""
    detector: str
    table_fqn: str
    threshold: str
    status: str
    anomaly_count: int
    findings: list[str]
    notes: list[str]
    monthly_anomalies: list[AnomalyFinding] = field(default_factory=list)
    weekly_anomalies: list[AnomalyFinding] = field(default_factory=list)
    daily_anomalies: list[AnomalyFinding] = field(default_factory=list)


@dataclass
class AnomalyDetectionOutcome:
    """Summary of an anomaly detection run."""
    run_id: str
    total_anomalies: int
    output_dir: str
    report_path: str
    detector_results: list[DetectorResult]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_bronze_anomaly_detection(
    settings: Settings,
    run_id: str | None = None,
    catalog: str | None = None,
    schema: str = "bronze",
    logger: Any | None = None,
) -> AnomalyDetectionOutcome:
    """Detect anomalies for 5 SNR tables and write text report."""
    ensure_project_dirs()
    resolved_run_id = run_id or new_run_id()
    logger = logger or configure_logging(run_id=resolved_run_id)
    resolved_catalog = (catalog or settings.DATABRICKS_CATALOG).strip()

    logger.info(
        "[anomaly] Step 1: Listing available tables in %s.%s",
        resolved_catalog, schema,
        extra={"run_id": resolved_run_id, "log_module": "anomaly", "step": "list_tables"},
    )
    sql_client = DatabricksSQLClient(settings=settings, logger=logger)
    available_tables = _list_tables(sql_client, resolved_catalog, schema)
    logger.info(
        "[anomaly] Found %d tables: %s",
        len(available_tables), sorted(available_tables),
        extra={"run_id": resolved_run_id, "log_module": "anomaly", "step": "list_tables"},
    )

    detector_results: list[DetectorResult] = []
    detectors = (
        ("snr_change_log_volume", _detect_snr_change_log),
        ("snr_demographics_class_dist", _detect_snr_demographics),
        ("snr_product_catalog_growth", _detect_snr_product),
        ("snr_control_volume_units", _detect_snr_control),
        ("snr_sales_volume_units", _detect_snr_sales),
    )
    for detector_idx, (detector_name, detector_fn) in enumerate(detectors, start=2):
        try:
            logger.info(
                "[anomaly] Step %d: Running detector — %s",
                detector_idx, detector_name,
                extra={"run_id": resolved_run_id, "log_module": "anomaly", "step": f"detector_{detector_name}"},
            )
            result = detector_fn(
                sql_client=sql_client,
                catalog=resolved_catalog,
                schema=schema,
                available_tables=available_tables,
            )
            detector_results.append(result)
            logger.info(
                "[anomaly] Step %d: Detector %s completed — %d anomalies, status=%s",
                detector_idx, detector_name, result.anomaly_count, result.status,
                extra={"run_id": resolved_run_id, "log_module": "anomaly", "step": f"detector_{detector_name}_done"},
            )
        except Exception as exc:
            logger.exception("Anomaly detector failed: %s", detector_name)
            detector_results.append(
                DetectorResult(
                    detector=detector_name,
                    table_fqn=f"{resolved_catalog}.{schema}.unknown_table",
                    threshold="-",
                    status="error",
                    anomaly_count=0,
                    findings=[],
                    notes=[_first_line(exc)],
                )
            )

    total_anomalies = sum(item.anomaly_count for item in detector_results)
    output_dir = anomaly_output_dir(resolved_run_id)
    report_path = output_dir / "anomalies.txt"

    logger.info(
        "[anomaly] Step 7: Rendering anomaly report (total_anomalies=%d)",
        total_anomalies,
        extra={"run_id": resolved_run_id, "log_module": "anomaly", "step": "render_report"},
    )
    content = _render_report(
        run_id=resolved_run_id,
        catalog=resolved_catalog,
        schema=schema,
        detector_results=detector_results,
    )
    atomic_write_text(report_path, content + "\n")
    logger.info(
        "[anomaly] Step 8: Report saved to %s — anomalies=%s",
        report_path, total_anomalies,
        extra={"run_id": resolved_run_id, "log_module": "anomaly", "step": "save_report"},
    )

    return AnomalyDetectionOutcome(
        run_id=resolved_run_id,
        total_anomalies=total_anomalies,
        output_dir=str(output_dir),
        report_path=str(report_path),
        detector_results=detector_results,
    )


# ---------------------------------------------------------------------------
# Generic rolling-month anomaly detector
# ---------------------------------------------------------------------------

def _detect_snr_table_anomalies(
    sql_client: DatabricksSQLClient,
    catalog: str,
    schema: str,
    table_name: str,
    date_col: str,
    metric_expr: str,
    metric_label: str,
    detector_name: str,
    group_col: str | None = None,
) -> DetectorResult:
    """Rolling month-by-month anomaly detector for SNR tables.

    For each month M (Feb-Dec):
      - Training: all months from Jan up to M-1
      - Test: month M
      - Flag if month M's value falls outside P1/P99 of training months
    """
    table_fqn = _fqn(catalog, schema, table_name)
    threshold_text = "Values below P1 or above P99 (rolling prior-month bounds)"
    dc = _qid(date_col)

    all_findings: list[str] = []
    monthly_anomalies: list[AnomalyFinding] = []
    weekly_anomalies: list[AnomalyFinding] = []
    daily_anomalies: list[AnomalyFinding] = []

    if group_col:
        # --- Grouped detection (demographics per class of trade) ---
        _run_rolling_checks(
            sql_client, table_fqn, dc, metric_expr,
            group_col=group_col,
            all_findings=all_findings,
            monthly_anomalies=monthly_anomalies,
            weekly_anomalies=weekly_anomalies,
            daily_anomalies=daily_anomalies,
        )
    else:
        # --- Simple detection (no grouping) ---
        _run_rolling_checks(
            sql_client, table_fqn, dc, metric_expr,
            group_col=None,
            all_findings=all_findings,
            monthly_anomalies=monthly_anomalies,
            weekly_anomalies=weekly_anomalies,
            daily_anomalies=daily_anomalies,
        )

    status = "anomaly" if all_findings else "ok"
    return DetectorResult(
        detector=detector_name,
        table_fqn=table_fqn,
        threshold=threshold_text,
        status=status,
        anomaly_count=len(all_findings),
        findings=all_findings,
        notes=[f"Date column: {date_col}, Metric: {metric_label}"],
        monthly_anomalies=monthly_anomalies,
        weekly_anomalies=weekly_anomalies,
        daily_anomalies=daily_anomalies,
    )


def _run_rolling_checks(
    sql_client: DatabricksSQLClient,
    table_fqn: str,
    dc: str,
    metric_expr: str,
    group_col: str | None,
    all_findings: list[str],
    monthly_anomalies: list[AnomalyFinding],
    weekly_anomalies: list[AnomalyFinding],
    daily_anomalies: list[AnomalyFinding],
):
    """Run rolling month-by-month P1/P99 checks at monthly, weekly, daily granularity."""
    for test_month in MONTHS:
        # Training: Jan (month 1) through test_month-1
        train_start = f"{YEAR}-01-01"
        # Last day of month before test_month
        train_end_month = test_month - 1
        if train_end_month == 1:
            train_end = f"{YEAR}-01-31"
        elif train_end_month == 2:
            train_end = f"{YEAR}-02-28"
        elif train_end_month in (4, 6, 9, 11):
            train_end = f"{YEAR}-{train_end_month:02d}-30"
        else:
            train_end = f"{YEAR}-{train_end_month:02d}-31"

        # Test month start/end
        test_start = f"{YEAR}-{test_month:02d}-01"
        if test_month == 2:
            test_end = f"{YEAR}-02-28"
        elif test_month in (4, 6, 9, 11):
            test_end = f"{YEAR}-{test_month:02d}-30"
        else:
            test_end = f"{YEAR}-{test_month:02d}-31"

        # --- Monthly check ---
        m_results = _run_single_quartile(
            sql_client, table_fqn, dc, metric_expr,
            f"DATE_TRUNC('month', TO_DATE({dc}))",
            train_start, train_end, test_start, test_end,
            group_col=group_col, min_window=2,
        )
        for a in m_results:
            period_str = a.period[:7]
            monthly_anomalies.append(a)
            grp_tag = f" [{a.group_label}]" if a.group_label and a.group_label != 'ALL' else ""
            all_findings.append(
                f"Monthly {period_str}{grp_tag}: {a.direction} "
                f"(value={_fmt_num(a.actual_value)}, range={_fmt_num(a.lower_bound)}-{_fmt_num(a.upper_bound)})"
            )

        # --- Weekly check (need >=4 prior weeks) ---
        w_results = _run_single_quartile(
            sql_client, table_fqn, dc, metric_expr,
            f"DATE_TRUNC('week', TO_DATE({dc}))",
            train_start, train_end, test_start, test_end,
            group_col=group_col, min_window=4,
        )
        for a in w_results:
            weekly_anomalies.append(a)
            grp_tag = f" [{a.group_label}]" if a.group_label and a.group_label != 'ALL' else ""
            all_findings.append(
                f"Weekly {a.period[:10]}{grp_tag}: {a.direction} "
                f"(value={_fmt_num(a.actual_value)}, range={_fmt_num(a.lower_bound)}-{_fmt_num(a.upper_bound)})"
            )

        # --- Daily check (need >=10 prior days) ---
        d_results = _run_single_quartile(
            sql_client, table_fqn, dc, metric_expr,
            f"TO_DATE({dc})",
            train_start, train_end, test_start, test_end,
            group_col=group_col, min_window=10,
        )
        for a in d_results:
            daily_anomalies.append(a)
            grp_tag = f" [{a.group_label}]" if a.group_label and a.group_label != 'ALL' else ""
            all_findings.append(
                f"Daily {a.period[:10]}{grp_tag}: {a.direction} "
                f"(value={_fmt_num(a.actual_value)}, range={_fmt_num(a.lower_bound)}-{_fmt_num(a.upper_bound)})"
            )


def _run_single_quartile(
    sql_client: DatabricksSQLClient,
    table_fqn: str,
    dc: str,
    metric_expr: str,
    trunc_expr: str,
    train_start: str,
    train_end: str,
    test_start: str,
    test_end: str,
    group_col: str | None,
    min_window: int,
) -> list[AnomalyFinding]:
    """Run P1/P99 quartile check with specific training and test date ranges."""
    group_select = f"CAST({_qid(group_col)} AS STRING) AS grp," if group_col else "'ALL' AS grp,"
    group_by = f"GROUP BY {_qid(group_col)}, {trunc_expr}" if group_col else f"GROUP BY {trunc_expr}"
    
    query = f"""
    WITH training AS (
        SELECT
            {group_select}
            {trunc_expr} AS period,
            {metric_expr} AS metric_value
        FROM {table_fqn}
        WHERE {dc} IS NOT NULL
          AND TO_DATE({dc}) >= '{train_start}'
          AND TO_DATE({dc}) <= '{train_end}'
          {f"AND {_qid(group_col)} IS NOT NULL" if group_col else ""}
        {group_by}
    ),
    bounds AS (
        SELECT
            grp,
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY metric_value) AS p01,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY metric_value) AS p99,
            COUNT(*) AS window_size
        FROM training
        GROUP BY grp
    ),
    test_data AS (
        SELECT
            {group_select}
            {trunc_expr} AS period,
            {metric_expr} AS metric_value
        FROM {table_fqn}
        WHERE {dc} IS NOT NULL
          AND TO_DATE({dc}) >= '{test_start}'
          AND TO_DATE({dc}) <= '{test_end}'
          {f"AND {_qid(group_col)} IS NOT NULL" if group_col else ""}
        {group_by}
    )
    SELECT
        CAST(t.period AS STRING) AS period,
        t.grp AS group_label,
        t.metric_value,
        b.p01,
        b.p99,
        b.window_size
    FROM test_data t
    JOIN bounds b ON t.grp = b.grp
    WHERE b.window_size >= {min_window}
      AND (t.metric_value < b.p01 OR t.metric_value > b.p99)
    ORDER BY t.grp, t.period
    """
    results: list[AnomalyFinding] = []
    for row in sql_client.fetch_all(query):
        val = _to_float(row.get("metric_value"))
        p01 = _to_float(row.get("p01"))
        p99 = _to_float(row.get("p99"))
        if val is None or p01 is None or p99 is None:
            continue
        direction = "Unusually High" if val > p99 else "Unusually Low"
        finding = AnomalyFinding(
            period=str(row.get("period", "")),
            actual_value=val,
            lower_bound=p01,
            upper_bound=p99,
            direction=direction,
        )
        finding.group_label = str(row.get("group_label", ""))
        results.append(finding)
    return results


# ---------------------------------------------------------------------------
# Per-table detector wrappers
# ---------------------------------------------------------------------------

def _detect_snr_change_log(
    sql_client: DatabricksSQLClient, catalog: str, schema: str, available_tables: set[str],
) -> DetectorResult:
    table_name = _resolve_table_name(available_tables, candidates=["snr_dim_snr_change_log"])
    if not table_name:
        return _skipped_result("snr_change_log_volume", catalog, schema, "snr_dim_snr_change_log")
    return _detect_snr_table_anomalies(
        sql_client, catalog, schema, table_name,
        date_col="staged_file_date", metric_expr="COUNT(*)",
        metric_label="Record Count", detector_name="snr_change_log_volume",
    )


def _detect_snr_demographics(
    sql_client: DatabricksSQLClient, catalog: str, schema: str, available_tables: set[str],
) -> DetectorResult:
    table_name = _resolve_table_name(available_tables, candidates=["snr_dim_snr_demographics"])
    if not table_name:
        return _skipped_result("snr_demographics_class_dist", catalog, schema, "snr_dim_snr_demographics")
    return _detect_snr_table_anomalies(
        sql_client, catalog, schema, table_name,
        date_col="staged_file_date", metric_expr="COUNT(*)",
        metric_label="Record Count", detector_name="snr_demographics_class_dist",
        group_col="outlet_class_of_trade",
    )


def _detect_snr_product(
    sql_client: DatabricksSQLClient, catalog: str, schema: str, available_tables: set[str],
) -> DetectorResult:
    table_name = _resolve_table_name(available_tables, candidates=["snr_dim_snr_product"])
    if not table_name:
        return _skipped_result("snr_product_catalog_growth", catalog, schema, "snr_dim_snr_product")
    return _detect_snr_table_anomalies(
        sql_client, catalog, schema, table_name,
        date_col="staged_file_date", metric_expr="COUNT(*)",
        metric_label="Record Count", detector_name="snr_product_catalog_growth",
    )


def _detect_snr_control(
    sql_client: DatabricksSQLClient, catalog: str, schema: str, available_tables: set[str],
) -> DetectorResult:
    table_name = _resolve_table_name(available_tables, candidates=["snr_fact_snr_control"])
    if not table_name:
        return _skipped_result("snr_control_volume_units", catalog, schema, "snr_fact_snr_control")
    return _detect_snr_table_anomalies(
        sql_client, catalog, schema, table_name,
        date_col="week_ending_date",
        metric_expr="SUM(COALESCE(TRY_CAST(`volume_units` AS DOUBLE), 0.0))",
        metric_label="Volume Units", detector_name="snr_control_volume_units",
    )


def _detect_snr_sales(
    sql_client: DatabricksSQLClient, catalog: str, schema: str, available_tables: set[str],
) -> DetectorResult:
    table_name = _resolve_table_name(available_tables, candidates=["snr_fact_snr_sales"])
    if not table_name:
        return _skipped_result("snr_sales_volume_units", catalog, schema, "snr_fact_snr_sales")
    return _detect_snr_table_anomalies(
        sql_client, catalog, schema, table_name,
        date_col="week_ending_date",
        metric_expr="SUM(COALESCE(TRY_CAST(`volume_units` AS DOUBLE), 0.0))",
        metric_label="Volume Units", detector_name="snr_sales_volume_units",
    )


def _skipped_result(detector: str, catalog: str, schema: str, table: str) -> DetectorResult:
    return DetectorResult(
        detector=detector, table_fqn=f"{catalog}.{schema}.{table}",
        threshold="-", status="skipped", anomaly_count=0, findings=[], notes=[f"Table {table} not found."],
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _list_tables(sql_client: DatabricksSQLClient, catalog: str, schema: str) -> set[str]:
    rows = sql_client.fetch_all(f"SHOW TABLES IN {_qid(catalog)}.{_qid(schema)}")
    return {str(r.get("tablename") or r.get("table_name") or r.get("table") or "").strip().lower()
            for r in rows if (r.get("tablename") or r.get("table_name") or r.get("table"))}


def _resolve_table_name(available_tables: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c.lower() in available_tables:
            return c.lower()
    return None


# ---------------------------------------------------------------------------
# Report Rendering
# ---------------------------------------------------------------------------

def _render_report(
    run_id: str, catalog: str, schema: str, detector_results: list[DetectorResult],
) -> str:
    total_anomalies = sum(item.anomaly_count for item in detector_results)
    anomaly_detectors = sum(1 for item in detector_results if item.status == "anomaly")

    lines: list[str] = []
    lines.append("=" * 70)
    lines.append("          SNR TABLE ANOMALY REPORT")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"  Report ID    : {run_id}")
    lines.append(f"  Generated    : {utc_iso()}")
    lines.append(f"  Data Source  : {catalog}.{schema}")
    lines.append(f"  Method       : Rolling month-by-month P1/P99 (each month tested against all prior months)")
    lines.append(f"  Year         : {YEAR}")
    lines.append("")
    lines.append(f"  Checks Run   : {len(detector_results)}")
    lines.append(f"  Issues Found  : {total_anomalies}")
    lines.append(f"  Checks with Issues : {anomaly_detectors}")
    lines.append("")
    lines.append("-" * 70)

    for result in detector_results:
        lines.append("")
        friendly_name = _friendly_detector_name(result.detector)
        status_label = "Issues Detected" if result.status == "anomaly" else (
            "Skipped" if result.status == "skipped" else (
                "Error" if result.status == "error" else "No Issues"
            )
        )
        lines.append(f"  CHECK: {friendly_name}")
        lines.append(f"  Table: {result.table_fqn.replace('`', '')}")
        lines.append(f"  Result: {status_label} ({result.anomaly_count} finding{'s' if result.anomaly_count != 1 else ''})")
        lines.append("")

        if result.status in ("skipped", "error"):
            for note in result.notes:
                lines.append(f"    {'Note' if result.status == 'skipped' else 'Error'}: {note}")
            lines.append("")
            lines.append("-" * 70)
            continue

        if result.monthly_anomalies or result.weekly_anomalies or result.daily_anomalies:
            if result.monthly_anomalies:
                _render_anomaly_table(lines, "MONTHLY ANOMALIES", result.monthly_anomalies, period_len=7)
            if result.weekly_anomalies:
                _render_anomaly_table(lines, "WEEKLY ANOMALIES", result.weekly_anomalies, period_len=10)
            if result.daily_anomalies:
                _render_anomaly_table(lines, "DAILY ANOMALIES", result.daily_anomalies, period_len=10)
        elif result.findings:
            lines.append("    Findings:")
            lines.append("")
            for idx, finding in enumerate(result.findings, 1):
                lines.append(f"    {idx}. {finding}")
            lines.append("")

        if not result.findings and result.status == "ok":
            lines.append("    All values are within the expected range. No issues found.")
            lines.append("")

        lines.append("-" * 70)

    lines.append("")
    lines.append("=" * 70)
    lines.append("  END OF REPORT")
    lines.append("=" * 70)
    return "\n".join(lines)


def _render_anomaly_table(lines: list[str], title: str, anomalies: list[AnomalyFinding], period_len: int):
    """Render a formatted anomaly sub-table."""
    lines.append(f"    {title} (rolling prior-month bounds)")
    lines.append("    " + "-" * 62)
    has_group = any(a.group_label for a in anomalies)
    if has_group:
        lines.append(f"    {'Period':<12} {'Group':<20} {'Value':>12} {'Expected Range':>22} {'Status':<16}")
    else:
        lines.append(f"    {'Period':<12} {'Value':>14} {'Expected Range':>22} {'Status':<16}")
    lines.append("    " + "-" * 62)
    for a in anomalies:
        p = a.period[:period_len]
        expected = f"{_fmt_num(a.lower_bound)} - {_fmt_num(a.upper_bound)}"
        if has_group:
            lines.append(f"    {p:<12} {(a.group_label or ''):<20} {_fmt_num(a.actual_value):>12} {expected:>22} {a.direction:<16}")
        else:
            lines.append(f"    {p:<12} {_fmt_num(a.actual_value):>14} {expected:>22} {a.direction:<16}")
    lines.append("    " + "-" * 62)
    lines.append("")


def _friendly_detector_name(detector: str) -> str:
    name_map = {
        "snr_change_log_volume": "Change Log Volume (snr_dim_snr_change_log)",
        "snr_demographics_class_dist": "Outlet Class Distribution (snr_dim_snr_demographics)",
        "snr_product_catalog_growth": "Product Catalog Growth (snr_dim_snr_product)",
        "snr_control_volume_units": "Control Volume Units (snr_fact_snr_control)",
        "snr_sales_volume_units": "Sales Volume Units (snr_fact_snr_sales)",
    }
    return name_map.get(detector, detector.replace("_", " ").title())


def _fqn(catalog: str, schema: str, table: str) -> str:
    return f"{_qid(catalog)}.{_qid(schema)}.{_qid(table)}"

def _qid(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"

def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"

def _first_line(exc: Exception) -> str:
    message = str(exc).strip()
    return message.splitlines()[0] if message else exc.__class__.__name__

def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _fmt_num(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:,.2f}"
