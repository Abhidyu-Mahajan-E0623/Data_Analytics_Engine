"""Bronze-layer anomaly detection pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.config.settings import Settings
from src.connectors.databricks_sql import DatabricksSQLClient
from src.utils.io import anomaly_output_dir, atomic_write_text, ensure_project_dirs
from src.utils.logging import configure_logging
from src.utils.time import new_run_id, utc_iso


@dataclass
class AnomalyFinding:
    """Single anomaly finding for the report."""

    period: str
    actual_value: float
    lower_bound: float
    upper_bound: float
    direction: str  # "Unusually High" or "Unusually Low"


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
    # Structured findings grouped by granularity for the formatted report
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


def run_bronze_anomaly_detection(
    settings: Settings,
    run_id: str | None = None,
    catalog: str | None = None,
    schema: str = "bronze",
    logger: Any | None = None,
) -> AnomalyDetectionOutcome:
    """Detect predefined anomalies for bronze-layer tables and write text report."""
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
        ("bronze_sales_volume_spike_drop", _detect_sales_volume_spike_drop),
        ("bronze_clinical_completed_enrollment", _detect_clinical_enrollment),
        ("bronze_drug_products_ndc_integrity", _detect_drug_ndc_integrity),
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
        except Exception as exc:  # pragma: no cover - integration path
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
        "[anomaly] Step 5: Rendering anomaly report (total_anomalies=%d)",
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
        "[anomaly] Step 6: Report saved to %s — anomalies=%s",
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
# Sales Volume Anomaly Detector — Quartile Method (P5 / P95)
# ---------------------------------------------------------------------------

def _detect_sales_volume_spike_drop(
    sql_client: DatabricksSQLClient,
    catalog: str,
    schema: str,
    available_tables: set[str],
) -> DetectorResult:
    table_name = _resolve_table_name(
        available_tables,
        candidates=["raw_ics_867_csl", "raw_ics_867_csl_sales"],
    )
    threshold_text = "Values below 1st percentile or above 99th percentile of rolling window"
    if not table_name:
        return DetectorResult(
            detector="bronze_sales_volume_spike_drop",
            table_fqn=f"{catalog}.{schema}.raw_ics_867_csl",
            threshold=threshold_text,
            status="skipped",
            anomaly_count=0,
            findings=[],
            notes=["No sales table found (checked raw_ics_867_csl, raw_ics_867_csl_sales)."],
        )

    table_fqn = _fqn(catalog, schema, table_name)
    columns = _fetch_table_columns(sql_client, catalog, schema, table_name)
    date_col = _pick_column(
        columns,
        candidates=["report_date_v", "report_date", "sale_date", "sales_date", "invoice_date"],
    )
    qty_col = _pick_column(
        columns,
        candidates=["sales_qty_v", "sales_qty", "quantity", "qty", "sales_quantity"],
    )
    if not date_col or not qty_col:
        return DetectorResult(
            detector="bronze_sales_volume_spike_drop",
            table_fqn=table_fqn,
            threshold=threshold_text,
            status="skipped",
            anomaly_count=0,
            findings=[],
            notes=[
                f"Missing required columns: date_col={date_col or '-'}, qty_col={qty_col or '-'}",
            ],
        )

    all_findings: list[str] = []
    monthly_anomalies: list[AnomalyFinding] = []
    weekly_anomalies: list[AnomalyFinding] = []
    daily_anomalies: list[AnomalyFinding] = []

    # --- Monthly anomalies (6-month rolling window) ---
    monthly_query = f"""
    WITH monthly AS (
        SELECT
            DATE_TRUNC('month', TO_DATE({_qid(date_col)})) AS period,
            SUM(COALESCE(TRY_CAST({_qid(qty_col)} AS DOUBLE), 0.0)) AS total_qty
        FROM {table_fqn}
        WHERE {_qid(date_col)} IS NOT NULL
        GROUP BY DATE_TRUNC('month', TO_DATE({_qid(date_col)}))
    ),
    bounds AS (
        SELECT
            m.period,
            m.total_qty,
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY h.total_qty) AS p05,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY h.total_qty) AS p95,
            COUNT(h.total_qty) AS window_size
        FROM monthly m
        JOIN monthly h
          ON h.period >= ADD_MONTHS(m.period, -6)
         AND h.period < m.period
        GROUP BY m.period, m.total_qty
        HAVING COUNT(h.total_qty) >= 3
    )
    SELECT
        CAST(period AS STRING) AS period,
        total_qty,
        p05,
        p95
    FROM bounds
    WHERE total_qty < p05 OR total_qty > p95
    ORDER BY period DESC
    LIMIT 100
    """
    for row in sql_client.fetch_all(monthly_query):
        period = str(row.get("period", ""))[:7]  # YYYY-MM
        qty = _to_float(row.get("total_qty"))
        p05 = _to_float(row.get("p05"))
        p95 = _to_float(row.get("p95"))
        if qty is None or p05 is None or p95 is None:
            continue
        direction = "Unusually High" if qty > p95 else "Unusually Low"
        monthly_anomalies.append(AnomalyFinding(
            period=period, actual_value=qty, lower_bound=p05,
            upper_bound=p95, direction=direction,
        ))
        all_findings.append(f"Monthly {period}: {direction} (value={_fmt_num(qty)}, range={_fmt_num(p05)}-{_fmt_num(p95)})")

    # --- Weekly anomalies (10-week rolling window) ---
    weekly_query = f"""
    WITH weekly AS (
        SELECT
            DATE_TRUNC('week', TO_DATE({_qid(date_col)})) AS period,
            SUM(COALESCE(TRY_CAST({_qid(qty_col)} AS DOUBLE), 0.0)) AS total_qty
        FROM {table_fqn}
        WHERE {_qid(date_col)} IS NOT NULL
        GROUP BY DATE_TRUNC('week', TO_DATE({_qid(date_col)}))
    ),
    bounds AS (
        SELECT
            w.period,
            w.total_qty,
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY h.total_qty) AS p05,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY h.total_qty) AS p95,
            COUNT(h.total_qty) AS window_size
        FROM weekly w
        JOIN weekly h
          ON h.period >= DATE_ADD(w.period, -70)
         AND h.period < w.period
        GROUP BY w.period, w.total_qty
        HAVING COUNT(h.total_qty) >= 5
    )
    SELECT
        CAST(period AS STRING) AS period,
        total_qty,
        p05,
        p95
    FROM bounds
    WHERE total_qty < p05 OR total_qty > p95
    ORDER BY period DESC
    LIMIT 100
    """
    for row in sql_client.fetch_all(weekly_query):
        period = str(row.get("period", ""))[:10]
        qty = _to_float(row.get("total_qty"))
        p05 = _to_float(row.get("p05"))
        p95 = _to_float(row.get("p95"))
        if qty is None or p05 is None or p95 is None:
            continue
        direction = "Unusually High" if qty > p95 else "Unusually Low"
        weekly_anomalies.append(AnomalyFinding(
            period=f"Week of {period}", actual_value=qty, lower_bound=p05,
            upper_bound=p95, direction=direction,
        ))
        all_findings.append(f"Weekly {period}: {direction} (value={_fmt_num(qty)}, range={_fmt_num(p05)}-{_fmt_num(p95)})")

    # --- Daily anomalies (30-day rolling window) ---
    daily_query = f"""
    WITH daily AS (
        SELECT
            TO_DATE({_qid(date_col)}) AS period,
            SUM(COALESCE(TRY_CAST({_qid(qty_col)} AS DOUBLE), 0.0)) AS total_qty
        FROM {table_fqn}
        WHERE {_qid(date_col)} IS NOT NULL
        GROUP BY TO_DATE({_qid(date_col)})
    ),
    bounds AS (
        SELECT
            d.period,
            d.total_qty,
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY h.total_qty) AS p05,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY h.total_qty) AS p95,
            COUNT(h.total_qty) AS window_size
        FROM daily d
        JOIN daily h
          ON h.period >= DATE_ADD(d.period, -30)
         AND h.period < d.period
        GROUP BY d.period, d.total_qty
        HAVING COUNT(h.total_qty) >= 10
    )
    SELECT
        CAST(period AS STRING) AS period,
        total_qty,
        p05,
        p95
    FROM bounds
    WHERE total_qty < p05 OR total_qty > p95
    ORDER BY period DESC
    LIMIT 200
    """
    for row in sql_client.fetch_all(daily_query):
        period = str(row.get("period", ""))[:10]
        qty = _to_float(row.get("total_qty"))
        p05 = _to_float(row.get("p05"))
        p95 = _to_float(row.get("p95"))
        if qty is None or p05 is None or p95 is None:
            continue
        direction = "Unusually High" if qty > p95 else "Unusually Low"
        daily_anomalies.append(AnomalyFinding(
            period=period, actual_value=qty, lower_bound=p05,
            upper_bound=p95, direction=direction,
        ))
        all_findings.append(f"Daily {period}: {direction} (value={_fmt_num(qty)}, range={_fmt_num(p05)}-{_fmt_num(p95)})")

    status = "anomaly" if all_findings else "ok"
    return DetectorResult(
        detector="bronze_sales_volume_spike_drop",
        table_fqn=table_fqn,
        threshold=threshold_text,
        status=status,
        anomaly_count=len(all_findings),
        findings=all_findings,
        notes=[f"Columns used: date={date_col}, quantity={qty_col}"],
        monthly_anomalies=monthly_anomalies,
        weekly_anomalies=weekly_anomalies,
        daily_anomalies=daily_anomalies,
    )


# ---------------------------------------------------------------------------
# Clinical Enrollment Detector (unchanged logic)
# ---------------------------------------------------------------------------

def _detect_clinical_enrollment(
    sql_client: DatabricksSQLClient,
    catalog: str,
    schema: str,
    available_tables: set[str],
) -> DetectorResult:
    table_name = _resolve_table_name(available_tables, candidates=["clinical_trials"])
    if not table_name:
        return DetectorResult(
            detector="bronze_clinical_completed_enrollment",
            table_fqn=f"{catalog}.{schema}.clinical_trials",
            threshold="Completed trials: non-numeric enrollment or enrollment ratio outside expected range",
            status="skipped",
            anomaly_count=0,
            findings=[],
            notes=["Table clinical_trials not found."],
        )

    table_fqn = _fqn(catalog, schema, table_name)
    columns = _fetch_table_columns(sql_client, catalog, schema, table_name)
    status_col = _pick_column(columns, candidates=["study_status", "status", "trial_status"])
    actual_col = _pick_column(columns, candidates=["actual_enrollment"])
    target_col = _pick_column(columns, candidates=["target_enrollment", "planned_enrollment"])
    trial_id_col = _pick_column(columns, candidates=["study_id", "trial_id", "nct_id", "id"])

    if not status_col or not actual_col or not target_col:
        return DetectorResult(
            detector="bronze_clinical_completed_enrollment",
            table_fqn=table_fqn,
            threshold="Completed trials: non-numeric enrollment or enrollment ratio outside expected range",
            status="skipped",
            anomaly_count=0,
            findings=[],
            notes=[
                (
                    "Missing required columns: "
                    f"status={status_col or '-'}, actual={actual_col or '-'}, target={target_col or '-'}"
                )
            ],
        )

    trial_id_select = f"CAST({_qid(trial_id_col)} AS STRING)" if trial_id_col else "'unknown_trial'"
    query = f"""
    SELECT
        {trial_id_select} AS trial_id,
        LOWER(TRIM(COALESCE(CAST({_qid(status_col)} AS STRING), ''))) AS trial_status,
        CAST({_qid(actual_col)} AS STRING) AS actual_raw,
        CAST({_qid(target_col)} AS STRING) AS target_raw,
        TRY_CAST({_qid(actual_col)} AS DOUBLE) AS actual_enrollment_num,
        TRY_CAST({_qid(target_col)} AS DOUBLE) AS target_enrollment_num
    FROM {table_fqn}
    WHERE LOWER(TRIM(COALESCE(CAST({_qid(status_col)} AS STRING), ''))) = 'completed'
    """
    rows = sql_client.fetch_all(query)
    findings: list[str] = []
    for index, row in enumerate(rows, start=1):
        trial_id = str(row.get("trial_id") or f"row_{index}")
        actual_raw = str(row.get("actual_raw") or "").strip()
        actual_num = _to_float(row.get("actual_enrollment_num"))
        target_num = _to_float(row.get("target_enrollment_num"))

        if actual_num is None and actual_raw:
            findings.append(
                f"Trial {trial_id}: Enrollment value '{actual_raw}' is not a valid number."
            )
            continue
        if actual_num is None or target_num is None or target_num <= 0:
            continue

        ratio = actual_num / target_num
        if ratio < 0.90 or ratio > 1.05:
            direction = "above" if ratio > 1.05 else "below"
            findings.append(
                f"Trial {trial_id}: Actual enrollment ({actual_num:.0f}) is {direction} target ({target_num:.0f}), ratio = {ratio:.2f}."
            )

    status = "anomaly" if findings else "ok"
    notes = [
        f"Columns used: trial_id={trial_id_col or '(auto-generated)'}, status={status_col}, actual={actual_col}, target={target_col}",
        f"Completed trials scanned: {len(rows)}",
    ]
    return DetectorResult(
        detector="bronze_clinical_completed_enrollment",
        table_fqn=table_fqn,
        threshold="Completed trials: enrollment ratio outside 90%-105% of target",
        status=status,
        anomaly_count=len(findings),
        findings=findings,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Drug NDC Integrity Detector (unchanged logic)
# ---------------------------------------------------------------------------

def _detect_drug_ndc_integrity(
    sql_client: DatabricksSQLClient,
    catalog: str,
    schema: str,
    available_tables: set[str],
) -> DetectorResult:
    table_name = _resolve_table_name(available_tables, candidates=["drug_products"])
    if not table_name:
        return DetectorResult(
            detector="bronze_drug_products_ndc_integrity",
            table_fqn=f"{catalog}.{schema}.drug_products",
            threshold="Missing NDC codes or NDC codes mapping to multiple products",
            status="skipped",
            anomaly_count=0,
            findings=[],
            notes=["Table drug_products not found."],
        )

    table_fqn = _fqn(catalog, schema, table_name)
    columns = _fetch_table_columns(sql_client, catalog, schema, table_name)
    ndc_col = _pick_column(columns, candidates=["ndc_code", "ndc", "ndc11"])
    drug_id_col = _pick_column(columns, candidates=["drug_id", "product_id", "item_id"])
    generic_col = _pick_column(columns, candidates=["generic_name", "drug_generic_name", "generic"])

    if not ndc_col:
        return DetectorResult(
            detector="bronze_drug_products_ndc_integrity",
            table_fqn=table_fqn,
            threshold="Missing NDC codes or NDC codes mapping to multiple products",
            status="skipped",
            anomaly_count=0,
            findings=[],
            notes=["Missing required NDC column (checked ndc_code, ndc, ndc11)."],
        )

    ndc_expr = f"TRIM(COALESCE(CAST({_qid(ndc_col)} AS STRING), ''))"
    stats_query = f"""
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN {ndc_expr} = '' THEN 1 ELSE 0 END) AS missing_ndc_rows
    FROM {table_fqn}
    """
    stats = sql_client.fetch_one(stats_query) or {}
    total_rows = int(stats.get("total_rows") or 0)
    missing_ndc_rows = int(stats.get("missing_ndc_rows") or 0)

    findings: list[str] = []
    if missing_ndc_rows > 0:
        missing_rate = (missing_ndc_rows / total_rows * 100.0) if total_rows else 0.0
        findings.append(
            f"{missing_ndc_rows} out of {total_rows} rows ({missing_rate:.1f}%) have missing NDC codes."
        )

    if drug_id_col:
        drug_conflict_query = f"""
        SELECT
            CAST({_qid(ndc_col)} AS STRING) AS ndc_code,
            COUNT(DISTINCT CAST({_qid(drug_id_col)} AS STRING)) AS distinct_drug_ids
        FROM {table_fqn}
        WHERE {ndc_expr} <> ''
        GROUP BY CAST({_qid(ndc_col)} AS STRING)
        HAVING COUNT(DISTINCT CAST({_qid(drug_id_col)} AS STRING)) > 1
        ORDER BY distinct_drug_ids DESC, ndc_code
        LIMIT 50
        """
        for row in sql_client.fetch_all(drug_conflict_query):
            ndc_code = str(row.get("ndc_code") or "")
            distinct_count = int(row.get("distinct_drug_ids") or 0)
            findings.append(
                f"NDC {ndc_code} is linked to {distinct_count} different drug IDs."
            )

    if generic_col:
        generic_conflict_query = f"""
        SELECT
            CAST({_qid(ndc_col)} AS STRING) AS ndc_code,
            COUNT(DISTINCT CAST({_qid(generic_col)} AS STRING)) AS distinct_generic_names
        FROM {table_fqn}
        WHERE {ndc_expr} <> ''
        GROUP BY CAST({_qid(ndc_col)} AS STRING)
        HAVING COUNT(DISTINCT CAST({_qid(generic_col)} AS STRING)) > 1
        ORDER BY distinct_generic_names DESC, ndc_code
        LIMIT 50
        """
        for row in sql_client.fetch_all(generic_conflict_query):
            ndc_code = str(row.get("ndc_code") or "")
            distinct_count = int(row.get("distinct_generic_names") or 0)
            findings.append(
                f"NDC {ndc_code} is linked to {distinct_count} different generic names."
            )

    status = "anomaly" if findings else "ok"
    notes = [
        f"Columns used: ndc={ndc_col}, drug_id={drug_id_col or '-'}, generic={generic_col or '-'}",
    ]
    return DetectorResult(
        detector="bronze_drug_products_ndc_integrity",
        table_fqn=table_fqn,
        threshold="Missing NDC codes or NDC codes mapping to multiple products",
        status=status,
        anomaly_count=len(findings),
        findings=findings,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _list_tables(sql_client: DatabricksSQLClient, catalog: str, schema: str) -> set[str]:
    query = f"SHOW TABLES IN {_qid(catalog)}.{_qid(schema)}"
    rows = sql_client.fetch_all(query)
    table_names: set[str] = set()
    for row in rows:
        raw_name = row.get("tablename") or row.get("table_name") or row.get("table")
        if raw_name:
            table_names.add(str(raw_name).strip().lower())
    return table_names


def _fetch_table_columns(
    sql_client: DatabricksSQLClient,
    catalog: str,
    schema: str,
    table_name: str,
) -> set[str]:
    query = f"""
    SELECT LOWER(column_name) AS column_name
    FROM {_qid(catalog)}.information_schema.columns
    WHERE table_schema = {_sql_literal(schema)}
      AND table_name = {_sql_literal(table_name)}
    """
    rows = sql_client.fetch_all(query)
    return {str(row.get("column_name", "")).strip().lower() for row in rows if row.get("column_name")}


def _resolve_table_name(available_tables: set[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate.lower() in available_tables:
            return candidate.lower()
    return None


def _pick_column(columns: set[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        normalized = candidate.strip().lower()
        if normalized in columns:
            return normalized
    return None


# ---------------------------------------------------------------------------
# Report Rendering — Clean, Non-Technical Format
# ---------------------------------------------------------------------------

def _render_report(
    run_id: str,
    catalog: str,
    schema: str,
    detector_results: list[DetectorResult],
) -> str:
    total_anomalies = sum(item.anomaly_count for item in detector_results)
    anomaly_detectors = sum(1 for item in detector_results if item.status == "anomaly")

    lines: list[str] = []
    lines.append("=" * 70)
    lines.append("          DATA QUALITY ANOMALY REPORT")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"  Report ID    : {run_id}")
    lines.append(f"  Generated    : {utc_iso()}")
    lines.append(f"  Data Source  : {catalog}.{schema}")
    lines.append("")
    lines.append(f"  Checks Run   : {len(detector_results)}")
    lines.append(f"  Issues Found  : {total_anomalies}")
    lines.append(f"  Checks with Issues : {anomaly_detectors}")
    lines.append("")
    lines.append("-" * 70)

    for result in detector_results:
        lines.append("")
        # Friendly detector names
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

        if result.status == "skipped":
            for note in result.notes:
                lines.append(f"    Note: {note}")
            lines.append("")
            lines.append("-" * 70)
            continue

        if result.status == "error":
            for note in result.notes:
                lines.append(f"    Error: {note}")
            lines.append("")
            lines.append("-" * 70)
            continue

        # Render structured anomaly tables for sales volume detector
        if result.monthly_anomalies or result.weekly_anomalies or result.daily_anomalies:
            if result.monthly_anomalies:
                lines.append("    MONTHLY TRENDS (6-month rolling window)")
                lines.append("    " + "-" * 62)
                lines.append(f"    {'Month':<12} {'Sales Volume':>14} {'Expected Range':>22} {'Status':<16}")
                lines.append("    " + "-" * 62)
                for a in result.monthly_anomalies:
                    expected = f"{_fmt_num(a.lower_bound)} - {_fmt_num(a.upper_bound)}"
                    lines.append(f"    {a.period:<12} {_fmt_num(a.actual_value):>14} {expected:>22} {a.direction:<16}")
                lines.append("    " + "-" * 62)
                lines.append("")

            if result.weekly_anomalies:
                lines.append("    WEEKLY TRENDS (10-week rolling window)")
                lines.append("    " + "-" * 62)
                lines.append(f"    {'Week Starting':<16} {'Sales Volume':>14} {'Expected Range':>18} {'Status':<16}")
                lines.append("    " + "-" * 62)
                for a in result.weekly_anomalies:
                    period_short = a.period.replace("Week of ", "")
                    expected = f"{_fmt_num(a.lower_bound)} - {_fmt_num(a.upper_bound)}"
                    lines.append(f"    {period_short:<16} {_fmt_num(a.actual_value):>14} {expected:>18} {a.direction:<16}")
                lines.append("    " + "-" * 62)
                lines.append("")

            if result.daily_anomalies:
                lines.append("    DAILY TRENDS (30-day rolling window)")
                lines.append("    " + "-" * 62)
                lines.append(f"    {'Date':<12} {'Sales Volume':>14} {'Expected Range':>22} {'Status':<16}")
                lines.append("    " + "-" * 62)
                for a in result.daily_anomalies:
                    expected = f"{_fmt_num(a.lower_bound)} - {_fmt_num(a.upper_bound)}"
                    lines.append(f"    {a.period:<12} {_fmt_num(a.actual_value):>14} {expected:>22} {a.direction:<16}")
                lines.append("    " + "-" * 62)
                lines.append("")
        elif result.findings:
            # Generic findings for other detectors (clinical, NDC)
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


def _friendly_detector_name(detector: str) -> str:
    """Convert internal detector name to a readable label."""
    name_map = {
        "bronze_sales_volume_spike_drop": "Sales Volume Anomalies",
        "bronze_clinical_completed_enrollment": "Clinical Trial Enrollment",
        "bronze_drug_products_ndc_integrity": "Drug Product NDC Integrity",
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
