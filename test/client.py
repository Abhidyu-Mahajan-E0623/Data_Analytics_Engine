"""
Schema Maker API Client
========================
Interactive client to call Anomaly Detection, Hypothesis Generation,
and Insight Generation APIs from any machine.

Usage:
    1. pip install -r requirements.txt
    2. python client.py
    3. Follow the prompts

Before running, set the API_BASE_URL below to point to the server.
"""

import os
import sys
from pathlib import Path

import pip_system_certs.wrapt_requests  # noqa: F401  — patches SSL to use Windows certs
import requests

# ── Output folders (created next to this script) ──────────────────────
SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
ANOMALY_DIR = SCRIPT_DIR / "Anomaly"
HYPOTHESIS_DIR = SCRIPT_DIR / "Hypothesis"
INSIGHT_DIR = SCRIPT_DIR / "Insight"

# ── Configure this to point to the running API server ──────────────────
API_BASE_URL = "https://azureapi-instance-gzd2h9dzhafbbcgv.centralus-01.azurewebsites.net"
# ───────────────────────────────────────────────────────────────────────


def print_banner():
    print()
    print("=" * 60)
    print("        SCHEMA MAKER — API CLIENT")
    print("=" * 60)
    print()
    print(f"  Server: {API_BASE_URL}")
    print()


def ensure_output_dirs():
    """Create Anomaly/, Hypothesis/, Insight/ folders if they don't exist."""
    for folder in (ANOMALY_DIR, HYPOTHESIS_DIR, INSIGHT_DIR):
        folder.mkdir(parents=True, exist_ok=True)


def check_server():
    """Verify the API server is reachable."""
    print("  Checking server (Azure cold-start may take up to 60 s)...")
    try:
        resp = requests.get(f"{API_BASE_URL}/health", timeout=60)
        if resp.status_code == 200:
            print("  ✓ Server is online\n")
            return True
        else:
            print(f"  ✗ Server returned status {resp.status_code}")
            return False
    except requests.ConnectionError as e:
        print(f"  ✗ Cannot reach server at {API_BASE_URL}")
        print(f"    Error: {e}\n")
    except requests.Timeout:
        print(f"  ✗ Server did not respond within 60 seconds.")
        print(f"    The server may be starting up. Try again in a minute.\n")
    except Exception as e:
        print(f"  ✗ Unexpected error: {e}\n")
    return False


def show_menu():
    print("-" * 60)
    print("  Select an option:\n")
    print("    1. Anomaly Detection")
    print("    2. Hypothesis Generation")
    print("    3. Insight Generation")
    print("    0. Exit")
    print()


def run_anomaly():
    """Call the anomaly detection API."""
    print("\n─── Anomaly Detection ───\n")
    schema = input("  Enter schema to scan [default: bronze]: ").strip() or "bronze"

    print(f"\n  Running anomaly detection on schema '{schema}'...")
    print("  (this may take a few minutes)\n")

    try:
        resp = requests.post(
            f"{API_BASE_URL}/api/anomaly",
            json={"schema": schema},
            timeout=300,
        )
        if resp.status_code != 200:
            print(f"  ✗ Error {resp.status_code}: {resp.text}\n")
            return

        data = resp.json()
        print(f"  ✓ Completed! Run ID: {data['run_id']}")
        print(f"  ✓ Total anomalies found: {data['total_anomalies']}")

        # Save to file
        out_file = ANOMALY_DIR / "anomalies.txt"
        out_file.write_text(data["report_text"], encoding="utf-8")
        print(f"  ✓ Saved to: {out_file}")
        print()
        print("=" * 60)
        print("  ANOMALY REPORT")
        print("=" * 60)
        print()
        print(data["report_text"])
        print()

    except requests.ConnectionError:
        print("  ✗ Connection lost to server.\n")
    except requests.Timeout:
        print("  ✗ Request timed out (>5 min).\n")


def run_hypothesis():
    """Call the hypothesis generation API."""
    print("\n─── Hypothesis Generation ───\n")
    print("  This requires 2 inputs:\n")

    schema = input("  1. Schema (metadata level, e.g. bronze / silver): ").strip()
    if not schema:
        print("  ✗ Schema is required.\n")
        return

    domain = input("  2. Domain (focus areas, e.g. sales / marketing / sales,administration): ").strip()
    if not domain:
        print("  ✗ Domain is required.\n")
        return

    print(f"\n  Generating hypotheses...")
    print(f"    Schema: {schema}")
    print(f"    Domain: {domain}")
    print("  (this may take several minutes)\n")

    try:
        resp = requests.post(
            f"{API_BASE_URL}/api/hypothesis",
            json={"schema": schema, "domain": domain},
            timeout=600,
        )
        if resp.status_code != 200:
            print(f"  ✗ Error {resp.status_code}: {resp.text}\n")
            return

        data = resp.json()
        print(f"  ✓ Completed! Run ID: {data['run_id']}")
        print(f"  ✓ Valid hypotheses: {data['valid_count']}")
        print(f"  ✓ Invalid hypotheses: {data['invalid_count']}")
        print(f"  ✓ Metrics tables created: {data['metrics_tables_created']}")

        # Save to file
        out_file = HYPOTHESIS_DIR / "hypotheses.txt"
        out_file.write_text(data["hypotheses_text"], encoding="utf-8")
        print(f"  ✓ Saved to: {out_file}")
        print()
        print("=" * 60)
        print("  HYPOTHESES REPORT")
        print("=" * 60)
        print()
        print(data["hypotheses_text"])
        print()

    except requests.ConnectionError:
        print("  ✗ Connection lost to server.\n")
    except requests.Timeout:
        print("  ✗ Request timed out (>10 min).\n")


def run_insight():
    """Call the insight generation API."""
    print("\n─── Insight Generation ───\n")

    run_id = input("  Enter run_id [leave blank for latest]: ").strip()

    hypo_input = input(
        "  Enter hypothesis numbers to use, comma-separated\n"
        "    (e.g. 1,4,5,6 — leave blank for all): "
    ).strip()

    hypothesis_ids = []
    if hypo_input:
        try:
            hypothesis_ids = [int(x.strip()) for x in hypo_input.split(",") if x.strip()]
        except ValueError:
            print("  ✗ Invalid input. Enter numbers separated by commas.\n")
            return

    print(f"\n  Generating insights...")
    if run_id:
        print(f"    Run ID: {run_id}")
    else:
        print(f"    Run ID: (latest)")
    if hypothesis_ids:
        print(f"    Hypothesis IDs: {hypothesis_ids}")
    else:
        print(f"    Hypothesis IDs: (all)")
    print("  (this may take a few minutes)\n")

    try:
        resp = requests.post(
            f"{API_BASE_URL}/api/insight",
            json={"run_id": run_id, "hypothesis_ids": hypothesis_ids},
            timeout=300,
        )
        if resp.status_code != 200:
            print(f"  ✗ Error {resp.status_code}: {resp.text}\n")
            return

        data = resp.json()
        print(f"  ✓ Completed! Run ID: {data['run_id']}")
        print(f"  ✓ Insights generated: {data['insight_count']}")

        # Save to file
        out_file = INSIGHT_DIR / "insight.txt"
        out_file.write_text(data["insight_text"], encoding="utf-8")
        print(f"  ✓ Saved to: {out_file}")
        print()
        print("=" * 60)
        print("  INSIGHT REPORT")
        print("=" * 60)
        print()
        print(data["insight_text"])
        print()

    except requests.ConnectionError:
        print("  ✗ Connection lost to server.\n")
    except requests.Timeout:
        print("  ✗ Request timed out (>5 min).\n")


def main():
    print_banner()
    ensure_output_dirs()
    print("  ✓ Output folders ready (Anomaly/, Hypothesis/, Insight/)\n")

    if not check_server():
        sys.exit(1)

    while True:
        show_menu()
        choice = input("  Your choice: ").strip()

        if choice == "1":
            run_anomaly()
        elif choice == "2":
            run_hypothesis()
        elif choice == "3":
            run_insight()
        elif choice == "0":
            print("\n  Goodbye!\n")
            break
        else:
            print("\n  ✗ Invalid choice. Enter 1, 2, 3, or 0.\n")


if __name__ == "__main__":
    main()
