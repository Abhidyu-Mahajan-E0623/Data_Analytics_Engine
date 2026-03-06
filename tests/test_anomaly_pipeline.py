"""Unit tests for anomaly pipeline helpers."""

from __future__ import annotations

from src_anomaly.pipeline import AnomalyFinding, DetectorResult, _pick_column, _render_report


def test_pick_column_returns_first_matching_candidate() -> None:
    columns = {"report_date_v", "sales_qty_v", "customer_id"}
    selected = _pick_column(columns, candidates=["report_date", "report_date_v", "sales_date"])
    assert selected == "report_date_v"


def test_render_report_summarizes_detector_counts() -> None:
    report = _render_report(
        run_id="run_20260227T000000Z_abc123",
        catalog="new_claim_catalog",
        schema="bronze",
        detector_results=[
            DetectorResult(
                detector="bronze_sales_volume_spike_drop",
                table_fqn="new_claim_catalog.bronze.raw_ics_867_csl",
                threshold="Values below 5th percentile or above 95th percentile",
                status="anomaly",
                anomaly_count=2,
                findings=["f1", "f2"],
                notes=["n1"],
                monthly_anomalies=[
                    AnomalyFinding(
                        period="2026-01",
                        actual_value=50000.0,
                        lower_bound=10000.0,
                        upper_bound=40000.0,
                        direction="Unusually High",
                    ),
                ],
                weekly_anomalies=[],
                daily_anomalies=[],
            ),
            DetectorResult(
                detector="bronze_clinical_completed_enrollment",
                table_fqn="new_claim_catalog.bronze.clinical_trials",
                threshold="Enrollment ratio outside expected range",
                status="ok",
                anomaly_count=0,
                findings=[],
                notes=[],
            ),
        ],
    )
    assert "Issues Found  : 2" in report
    assert "Checks with Issues : 1" in report
    assert "Sales Volume Anomalies" in report
    assert "MONTHLY TRENDS" in report
    assert "Unusually High" in report
    assert "No Issues" in report or "No issues found" in report
