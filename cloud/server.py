"""
Cloud Ingestion & Online Processing Server
Zero-dependency, high-throughput HTTP server built on Python standard library.
Handles real-time streaming, online window processing, catch-up replication, and export queries.
"""

import argparse
import json
import logging
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from typing import Dict, List, Optional, Any

from cloud.storage import CloudStorage
from device.processor import WindowProcessor

logger = logging.getLogger("cloud.server")

class CloudServiceState:
    def __init__(self, storage: CloudStorage):
        self.storage = storage
        self.processors: Dict[str, WindowProcessor] = {}
        self.processed_windows: Dict[str, set] = {}

    def get_processor(self, session_id: str) -> WindowProcessor:
        if session_id not in self.processors:
            self.processors[session_id] = WindowProcessor()
            self.processed_windows[session_id] = set()
        return self.processors[session_id]

    def trigger_online_processing_if_ready(self, session_id: str, max_sample_index: int):
        """
        In online mode, each second's result must appear in the cloud within 5 seconds.
        A window w (samples 400w to 400w+399) is ready when max_sample_index >= 400w + 399.
        """
        proc = self.get_processor(session_id)
        processed_set = self.processed_windows[session_id]
        
        target_window = max_sample_index // 400
        # Process any window <= target_window that hasn't been processed yet
        for w in range(0, target_window + 1):
            if w in processed_set:
                continue
            # Check if samples exist
            w_start = 400 * w
            w_end = 400 * w + 399
            
            # Need previous samples up to 5 seconds for heart rate (800 before for peaks)
            fetch_start = max(0, 400 * (w - 4) - 801)
            s1 = self.storage.get_samples_for_window(session_id, sensor=1, start_idx=fetch_start, end_idx=w_end + 1)
            s2 = self.storage.get_samples_for_window(session_id, sensor=2, start_idx=w_start, end_idx=w_end)
            
            result = proc.process_window(
                session_id=session_id,
                window=w,
                mode="online",
                s1_samples=s1,
                s2_samples=s2
            )
            self.storage.save_result(result)
            processed_set.add(w)


GLOBAL_STATE: Optional[CloudServiceState] = None


class CloudRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Suppress verbose default access logs, log errors only
        pass

    def send_json_response(self, data: Any, status: int = HTTPStatus.OK):
        body = json.dumps(data).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path in ("/", ""):
            html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Never Lose a Heartbeat — Cloud Service</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #f8fafc; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; }
    .card { background: #1e293b; border-radius: 12px; padding: 32px 40px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); border: 1px solid #334155; max-width: 500px; text-align: center; }
    .status-badge { display: inline-flex; align-items: center; gap: 8px; background: rgba(34, 197, 94, 0.15); color: #4ade80; border: 1px solid rgba(34, 197, 94, 0.3); padding: 6px 14px; border-radius: 9999px; font-size: 14px; font-weight: 600; margin-bottom: 20px; }
    .dot { width: 8px; height: 8px; background: #22c55e; border-radius: 50%; box-shadow: 0 0 10px #22c55e; }
    h1 { margin: 0 0 8px 0; font-size: 24px; color: #ffffff; }
    p { color: #94a3b8; font-size: 15px; margin: 0 0 24px 0; }
    .endpoints { text-align: left; background: #0f172a; border-radius: 8px; padding: 14px 18px; font-family: monospace; font-size: 13px; color: #cbd5e1; }
    .endpoints a { color: #38bdf8; text-decoration: none; }
    .endpoints a:hover { text-decoration: underline; }
    .footer { margin-top: 20px; font-size: 12px; color: #64748b; }
  </style>
</head>
<body>
  <div class="card">
    <div class="status-badge"><span class="dot"></span> Cloud Backend Active</div>
    <h1>yash_data_pipeline</h1>
    <p>Never Lose a Heartbeat — Bedside Telemetry Service</p>
    <div class="endpoints">
      <div><b>API Endpoints:</b></div>
      <div>• <a href="/api/health">/api/health</a> (Liveness probe)</div>
      <div>• <a href="/api/status">/api/status</a> (Device & cloud stats)</div>
      <div>• <code>POST /api/ingest</code> (Realtime & catch-up)</div>
      <div>• <code>GET /api/export</code> (Session ground truth)</div>
      <div>• <code>GET /api/results</code> (Section 3 window results)</div>
    </div>
    <div class="footer">Author: Yash Saxena • Free Cloud Tier (Render)</div>
  </div>
</body>
</html>"""
            body = html.encode('utf-8')
            self.send_response(HTTPStatus.OK)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/api/health":
            self.send_json_response({"status": "ok", "time": time.time()})
            return

        if path == "/api/status":
            session_id = query.get("session_id", [None])[0]
            stats = GLOBAL_STATE.storage.get_stats(session_id)
            self.send_json_response(stats)
            return

        if path == "/api/export":
            session_id = query.get("session_id", [None])[0]
            if not session_id:
                self.send_error(HTTPStatus.BAD_REQUEST, "Missing session_id")
                return

            self.send_response(HTTPStatus.OK)
            self.send_header('Content-Type', 'application/x-ndjson')
            self.end_headers()

            for line in GLOBAL_STATE.storage.export_session_generator(session_id):
                self.wfile.write((line + "\n").encode('utf-8'))
            return

        if path == "/api/results":
            session_id = query.get("session_id", [None])[0]
            if not session_id:
                self.send_error(HTTPStatus.BAD_REQUEST, "Missing session_id")
                return

            self.send_response(HTTPStatus.OK)
            self.send_header('Content-Type', 'application/x-ndjson')
            self.end_headers()

            for line in GLOBAL_STATE.storage.export_results_generator(session_id):
                self.wfile.write((line + "\n").encode('utf-8'))
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        content_length = int(self.headers.get('Content-Length', 0))
        body_bytes = self.rfile.read(content_length)
        try:
            body = json.loads(body_bytes.decode('utf-8'))
        except Exception as e:
            self.send_error(HTTPStatus.BAD_REQUEST, f"Malformed JSON: {e}")
            return

        if path == "/api/ingest":
            # Batch of samples
            samples = body.get("samples", [])
            is_online_stream = body.get("is_online_stream", False)
            inserted = GLOBAL_STATE.storage.ingest_samples_batch(samples)
            
            # If real-time stream in online mode, trigger window calculation
            if is_online_stream and samples:
                session_id = samples[0]["session_id"]
                max_idx = max(s["index"] for s in samples)
                GLOBAL_STATE.trigger_online_processing_if_ready(session_id, max_idx)

            self.send_json_response({"status": "ok", "inserted": inserted})
            return

        if path == "/api/losses":
            losses = body.get("losses", [])
            GLOBAL_STATE.storage.ingest_losses_batch(losses)
            self.send_json_response({"status": "ok", "count": len(losses)})
            return

        if path == "/api/results":
            results = body.get("results", [])
            for r in results:
                GLOBAL_STATE.storage.save_result(r)
            self.send_json_response({"status": "ok", "count": len(results)})
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")


def run_cloud_server(host: str = "0.0.0.0", port: int = 8000, db_path: str = "cloud_data.db"):
    global GLOBAL_STATE
    storage = CloudStorage(db_path)
    GLOBAL_STATE = CloudServiceState(storage)
    
    server_address = (host, port)
    httpd = ThreadingHTTPServer(server_address, CloudRequestHandler)
    print(f"[Cloud] Server running at http://{host}:{port} (db: {db_path})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("[Cloud] Server stopped.")
    finally:
        httpd.server_close()


def main():
    parser = argparse.ArgumentParser(description="Cloud Ingest and Processing Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host address")
    parser.add_argument("--port", type=int, default=8000, help="Port")
    parser.add_argument("--db", default="cloud_data.db", help="Database file path")
    args = parser.parse_args()

    run_cloud_server(args.host, args.port, args.db)

if __name__ == "__main__":
    main()
