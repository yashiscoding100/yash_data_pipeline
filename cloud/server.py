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
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Never Lose a Heartbeat — Medical Data Pipeline</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 50%, #eff6ff 100%);
      color: #1e293b;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 24px;
    }
    .container {
      background: #ffffff;
      border-radius: 24px;
      box-shadow: 0 20px 40px -15px rgba(0, 0, 0, 0.05), 0 0 1px rgba(0, 0, 0, 0.1);
      border: 1px solid #e2e8f0;
      max-width: 640px;
      width: 100%;
      padding: 40px;
      text-align: center;
    }
    .badge-wrapper {
      display: flex;
      justify-content: center;
      margin-bottom: 20px;
    }
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      background: #ecfdf5;
      color: #059669;
      border: 1px solid #a7f3d0;
      padding: 6px 16px;
      border-radius: 9999px;
      font-size: 13px;
      font-weight: 600;
      letter-spacing: 0.01em;
    }
    .pulse-dot {
      width: 8px;
      height: 8px;
      background: #10b981;
      border-radius: 50%;
      box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
      animation: pulse 2s infinite;
    }
    @keyframes pulse {
      0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
      70% { transform: scale(1); box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
      100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }
    .heart-icon-wrapper {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 56px;
      height: 56px;
      background: #ffe4e6;
      border-radius: 16px;
      margin: 0 auto 16px auto;
      color: #e11d48;
    }
    h1 {
      font-size: 26px;
      font-weight: 700;
      color: #0f172a;
      letter-spacing: -0.02em;
      margin-bottom: 6px;
    }
    .subtitle {
      font-size: 15px;
      color: #64748b;
      margin-bottom: 28px;
    }
    .author-pill {
      display: inline-block;
      font-weight: 600;
      color: #334155;
      background: #f1f5f9;
      padding: 3px 10px;
      border-radius: 6px;
    }
    .features-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 12px;
      margin-bottom: 28px;
      text-align: left;
    }
    @media (max-width: 540px) {
      .features-grid { grid-template-columns: 1fr; }
    }
    .feature-card {
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 14px;
      padding: 16px;
      transition: transform 0.2s, box-shadow 0.2s;
    }
    .feature-card:hover {
      transform: translateY(-2px);
      box-shadow: 0 6px 15px -3px rgba(0, 0, 0, 0.05);
    }
    .feature-icon {
      font-size: 20px;
      margin-bottom: 8px;
    }
    .feature-title {
      font-size: 13px;
      font-weight: 700;
      color: #1e293b;
      margin-bottom: 4px;
    }
    .feature-desc {
      font-size: 12px;
      color: #64748b;
      line-height: 1.4;
    }
    .endpoints-section {
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 16px;
      padding: 20px;
      text-align: left;
      margin-bottom: 24px;
    }
    .endpoints-header {
      font-size: 13px;
      font-weight: 700;
      color: #475569;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 12px;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .endpoint-list {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .endpoint-item {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 10px 14px;
      background: #ffffff;
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      font-size: 13px;
      text-decoration: none;
      color: #1e293b;
      transition: all 0.2s;
    }
    .endpoint-item:hover {
      border-color: #cbd5e1;
      background: #f1f5f9;
      transform: translateX(2px);
    }
    .endpoint-route {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-weight: 600;
      color: #0284c7;
    }
    .endpoint-desc {
      font-size: 12px;
      color: #64748b;
    }
    .footer {
      font-size: 12px;
      color: #94a3b8;
      line-height: 1.5;
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="heart-icon-wrapper">
      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M19 14c1.49-1.46 3-3.21 3-5.5A5.5 5.5 0 0 0 16.5 3c-1.76 0-3 .5-4.5 2-1.5-1.5-2.74-2-4.5-2A5.5 5.5 0 0 0 2 8.5c0 2.3 1.5 4.05 3 5.5l7 7Z"/>
        <path d="M3.22 12H9.5l1.5-3 2 6 1.5-3h6.28"/>
      </svg>
    </div>
    
    <div class="badge-wrapper">
      <div class="status-badge">
        <span class="pulse-dot"></span>
        Cloud Service Active & Healthy
      </div>
    </div>

    <h1>Never Lose a Heartbeat</h1>
    <p class="subtitle">Resilient Bedside Telemetry Pipeline &bull; by <span class="author-pill">Yash Saxena</span></p>

    <div class="features-grid">
      <div class="feature-card">
        <div class="feature-icon">🛡️</div>
        <div class="feature-title">Zero Data Loss</div>
        <div class="feature-desc">Atomic SQLite WAL commits safely survive sudden power pulls.</div>
      </div>
      <div class="feature-card">
        <div class="feature-icon">⚡</div>
        <div class="feature-title">Live Analysis</div>
        <div class="feature-desc">1-second ECG windows processed in cloud within 2 seconds.</div>
      </div>
      <div class="feature-card">
        <div class="feature-icon">🔄</div>
        <div class="feature-title">Auto Catch-Up</div>
        <div class="feature-desc">Seamless switch offline and rapid recovery upon Wi-Fi return.</div>
      </div>
    </div>

    <div class="endpoints-section">
      <div class="endpoints-header">
        <span>🌐</span> Live Service Endpoints
      </div>
      <div class="endpoint-list">
        <a href="/api/health" class="endpoint-item" target="_blank">
          <span class="endpoint-route">GET /api/health</span>
          <span class="endpoint-desc">Live connection check &rarr;</span>
        </a>
        <a href="/api/status" class="endpoint-item" target="_blank">
          <span class="endpoint-route">GET /api/status</span>
          <span class="endpoint-desc">Device telemetry status &rarr;</span>
        </a>
        <div class="endpoint-item">
          <span class="endpoint-route">POST /api/ingest</span>
          <span class="endpoint-desc">Live stream & catch-up ingest</span>
        </div>
        <div class="endpoint-item">
          <span class="endpoint-route">GET /api/export</span>
          <span class="endpoint-desc">Complete session ground truth</span>
        </div>
      </div>
    </div>

    <div class="footer">
      Designed for critical hospital environments where Wi-Fi drops and reliability matters.<br>
      Host: Render Free Tier (Python 3) &bull; Author: Yash Saxena
    </div>
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
