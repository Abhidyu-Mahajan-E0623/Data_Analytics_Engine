"""Simple HTTP server to serve the anomaly dashboard locally.

Usage:
    python dashboard/serve.py

Then open http://localhost:8050 in your browser.
"""

import http.server
import os
from pathlib import Path

PORT = 8050
PROJECT_ROOT = Path(__file__).resolve().parent.parent  # Schema_Maker_Final
DASHBOARD_DIR = Path(__file__).resolve().parent         # dashboard/
ANOMALY_OUTPUT_DIR = PROJECT_ROOT / "Output" / "Anomaly"


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    """Serves dashboard files and the latest anomaly report."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(DASHBOARD_DIR), **kwargs)

    def do_GET(self):
        if self.path == "/data/anomalies.txt":
            self._serve_latest_anomaly_report()
            return
        super().do_GET()

    def _serve_latest_anomaly_report(self):
        """Find and serve the latest anomalies.txt from Output/Anomaly."""
        if not ANOMALY_OUTPUT_DIR.exists():
            self.send_error(404, "Anomaly output directory not found")
            return

        # Sort run directories by name (they contain timestamps) to find the latest
        run_dirs = sorted(
            [d for d in ANOMALY_OUTPUT_DIR.iterdir() if d.is_dir()],
            key=lambda d: d.name,
            reverse=True,
        )

        for run_dir in run_dirs:
            anomaly_file = run_dir / "anomalies.txt"
            if anomaly_file.exists():
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(anomaly_file.read_bytes())
                print(f"  Served: {anomaly_file}")
                return

        self.send_error(404, "No anomaly report found in Output/Anomaly/")

    def log_message(self, format, *args):
        """Custom log format — only log API calls."""
        try:
            msg = format % args
        except Exception:
            msg = str(args)
        if "/data/" in msg:
            print(f"  [API] {msg}")


if __name__ == "__main__":
    print(f"==============================================")
    print(f"   Anomaly Detection Dashboard")
    print(f"   http://localhost:{PORT}")
    print(f"----------------------------------------------")
    print(f"   Dashboard : {DASHBOARD_DIR}")
    print(f"   Data from : {ANOMALY_OUTPUT_DIR}")
    print(f"==============================================")
    print()

    server = http.server.HTTPServer(("", PORT), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()
