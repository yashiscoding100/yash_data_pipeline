"""
Cloud Storage Engine
Persists ingested samples, loss events, and online/offline processed results.
Enforces idempotence and provides fast export matching judging criteria.
"""

import json
import sqlite3
import threading
import time
from typing import Dict, Generator, List, Optional, Tuple, Any

class CloudStorage:
    def __init__(self, db_path: str = "cloud_data.db"):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        return conn

    def _init_db(self):
        with self._lock:
            conn = self._get_connection()
            try:
                with conn:
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS samples (
                            session_id TEXT NOT NULL,
                            sensor INTEGER NOT NULL,
                            sample_index INTEGER NOT NULL,
                            values_json TEXT NOT NULL,
                            received_at REAL NOT NULL,
                            PRIMARY KEY (session_id, sensor, sample_index)
                        );
                    """)
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_cloud_sess_sensor_idx ON samples(session_id, sensor, sample_index);")

                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS losses (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            session_id TEXT NOT NULL,
                            sensor INTEGER NOT NULL,
                            from_index INTEGER NOT NULL,
                            to_index INTEGER,
                            reason TEXT NOT NULL,
                            UNIQUE (session_id, sensor, from_index, reason)
                        );
                    """)

                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS results (
                            session_id TEXT NOT NULL,
                            window_id INTEGER NOT NULL,
                            mode TEXT NOT NULL,
                            missing_1 INTEGER NOT NULL,
                            missing_2 INTEGER NOT NULL,
                            complete INTEGER NOT NULL,
                            mean_json TEXT,
                            min_json TEXT,
                            max_json TEXT,
                            magnitude REAL,
                            heart_rate REAL,
                            created_at REAL NOT NULL,
                            PRIMARY KEY (session_id, window_id)
                        );
                    """)
            finally:
                conn.close()

    def ingest_samples_batch(self, samples_data: List[Dict[str, Any]]) -> int:
        """
        Idempotent bulk ingestion of samples.
        samples_data: list of dicts with session_id, sensor, index, values
        """
        if not samples_data:
            return 0
        now = time.time()
        records = [
            (s["session_id"], s["sensor"], s["index"], json.dumps(s["values"], separators=(',', ':')), now)
            for s in samples_data
        ]
        with self._lock:
            conn = self._get_connection()
            try:
                with conn:
                    cur = conn.cursor()
                    cur.executemany(
                        "INSERT OR IGNORE INTO samples (session_id, sensor, sample_index, values_json, received_at) VALUES (?, ?, ?, ?, ?);",
                        records
                    )
                    return cur.rowcount
            finally:
                conn.close()

    def ingest_losses_batch(self, losses_data: List[Dict[str, Any]]):
        if not losses_data:
            return
        records = [
            (l["session_id"], l["sensor"], l["from_index"], l.get("to_index"), l.get("reason", "unknown"))
            for l in losses_data
        ]
        with self._lock:
            conn = self._get_connection()
            try:
                with conn:
                    conn.executemany(
                        "INSERT OR IGNORE INTO losses (session_id, sensor, from_index, to_index, reason) VALUES (?, ?, ?, ?, ?);",
                        records
                    )
            finally:
                conn.close()

    def save_result(self, result: Dict[str, Any]):
        with self._lock:
            conn = self._get_connection()
            try:
                with conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO results 
                        (session_id, window_id, mode, missing_1, missing_2, complete, mean_json, min_json, max_json, magnitude, heart_rate, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            result["session_id"],
                            result["window"],
                            result["mode"],
                            result["missing_1"],
                            result["missing_2"],
                            1 if result["complete"] else 0,
                            json.dumps(result["mean"]),
                            json.dumps(result["min"]),
                            json.dumps(result["max"]),
                            result["magnitude"],
                            result["heart_rate"],
                            time.time()
                        )
                    )
            finally:
                conn.close()

    def get_stats(self, session_id: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.cursor()
                if session_id:
                    cur.execute("SELECT COUNT(*) FROM samples WHERE session_id = ?;", (session_id,))
                else:
                    cur.execute("SELECT COUNT(*) FROM samples;")
                total_samples = cur.fetchone()[0]

                cur.execute("SELECT MAX(window_id) FROM results;")
                max_w = cur.fetchone()[0]
                return {
                    "total_samples": total_samples,
                    "latest_window": max_w
                }
            finally:
                conn.close()

    def get_samples_for_window(self, session_id: str, sensor: int, start_idx: int, end_idx: int) -> Dict[int, List[int]]:
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT sample_index, values_json FROM samples WHERE session_id = ? AND sensor = ? AND sample_index BETWEEN ? AND ?;",
                    (session_id, sensor, start_idx, end_idx)
                )
                rows = cur.fetchall()
                return {idx: json.loads(vals) for idx, vals in rows}
            finally:
                conn.close()

    def export_session_generator(self, session_id: str) -> Generator[str, None, None]:
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.cursor()
                # 1. Samples
                cur.execute(
                    "SELECT sensor, sample_index, values_json FROM samples WHERE session_id = ? ORDER BY sensor, sample_index ASC;",
                    (session_id,)
                )
                while True:
                    rows = cur.fetchmany(1000)
                    if not rows:
                        break
                    for sensor, idx, vals_json in rows:
                        yield json.dumps({
                            "sensor": sensor,
                            "index": idx,
                            "values": json.loads(vals_json)
                        })

                # 2. Losses
                cur.execute(
                    "SELECT sensor, from_index, to_index, reason FROM losses WHERE session_id = ? ORDER BY id ASC;",
                    (session_id,)
                )
                loss_rows = cur.fetchall()
                for sensor, f_idx, t_idx, reason in loss_rows:
                    yield json.dumps({
                        "sensor": sensor,
                        "from_index": f_idx,
                        "to_index": t_idx,
                        "reason": reason
                    })
            finally:
                conn.close()

    def export_results_generator(self, session_id: str) -> Generator[str, None, None]:
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT session_id, window_id, mode, missing_1, missing_2, complete, mean_json, min_json, max_json, magnitude, heart_rate
                    FROM results WHERE session_id = ? ORDER BY window_id ASC;
                    """,
                    (session_id,)
                )
                rows = cur.fetchall()
                for r in rows:
                    yield json.dumps({
                        "session_id": r[0],
                        "window": r[1],
                        "mode": r[2],
                        "missing_1": r[3],
                        "missing_2": r[4],
                        "complete": bool(r[5]),
                        "mean": json.loads(r[6]) if r[6] else None,
                        "min": json.loads(r[7]) if r[7] else None,
                        "max": json.loads(r[8]) if r[8] else None,
                        "magnitude": r[9],
                        "heart_rate": r[10]
                    })
            finally:
                conn.close()
