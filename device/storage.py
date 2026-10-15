"""
Device Storage Engine
Uses SQLite with Write-Ahead Logging (WAL) for crash resilience and zero corruption.
Guarantees atomic transactions, idempotence against duplicates, and honest loss tracking.
"""

import json
import os
import sqlite3
import threading
import time
from typing import Dict, Generator, List, Optional, Tuple, Any

class DeviceStorage:
    def __init__(self, db_path: str = "device_data.db"):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        # WAL mode provides concurrent read/write and crash safety
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        return conn

    def _init_db(self):
        with self._lock:
            conn = self._get_connection()
            try:
                with conn:
                    # Samples table
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS samples (
                            session_id TEXT NOT NULL,
                            sensor INTEGER NOT NULL,
                            sample_index INTEGER NOT NULL,
                            values_json TEXT NOT NULL,
                            synced_to_cloud INTEGER DEFAULT 0,
                            PRIMARY KEY (session_id, sensor, sample_index)
                        );
                    """)
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_unsynced ON samples(synced_to_cloud) WHERE synced_to_cloud = 0;")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_session_sensor_idx ON samples(session_id, sensor, sample_index);")

                    # Losses table
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS losses (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            session_id TEXT NOT NULL,
                            sensor INTEGER NOT NULL,
                            from_index INTEGER NOT NULL,
                            to_index INTEGER,
                            reason TEXT NOT NULL,
                            synced_to_cloud INTEGER DEFAULT 0
                        );
                    """)

                    # Results table
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

                    # Metadata table for state
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS metadata (
                            key TEXT PRIMARY KEY,
                            value TEXT NOT NULL
                        );
                    """)
                    
                    # Set default mode if not set
                    conn.execute("INSERT OR IGNORE INTO metadata (key, value) VALUES ('mode', 'online');")
                    conn.execute("INSERT OR IGNORE INTO metadata (key, value) VALUES ('active_session', '');")
            finally:
                conn.close()

    def set_mode(self, mode: str):
        with self._lock:
            conn = self._get_connection()
            try:
                with conn:
                    conn.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('mode', ?);", (mode,))
            finally:
                conn.close()

    def get_mode(self) -> str:
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.cursor()
                cur.execute("SELECT value FROM metadata WHERE key = 'mode';")
                row = cur.fetchone()
                return row[0] if row else "online"
            finally:
                conn.close()

    def set_active_session(self, session_id: str):
        with self._lock:
            conn = self._get_connection()
            try:
                with conn:
                    conn.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('active_session', ?);", (session_id,))
            finally:
                conn.close()

    def get_active_session(self) -> Optional[str]:
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.cursor()
                cur.execute("SELECT value FROM metadata WHERE key = 'active_session';")
                row = cur.fetchone()
                return row[0] if row and row[0] else None
            finally:
                conn.close()

    def save_samples(self, session_id: str, sensor: int, first_index: int, samples: List[List[int]]) -> int:
        """
        Saves a contiguous batch of samples atomically.
        Uses INSERT OR IGNORE to guarantee idempotence if duplicates arrive.
        Returns number of newly inserted samples.
        """
        if not samples:
            return 0
        records = [
            (session_id, sensor, first_index + offset, json.dumps(val, separators=(',', ':')))
            for offset, val in enumerate(samples)
        ]
        with self._lock:
            conn = self._get_connection()
            try:
                with conn:
                    cur = conn.cursor()
                    cur.executemany(
                        "INSERT OR IGNORE INTO samples (session_id, sensor, sample_index, values_json) VALUES (?, ?, ?, ?);",
                        records
                    )
                    return cur.rowcount
            finally:
                conn.close()

    def record_loss(self, session_id: str, sensor: int, from_index: int, to_index: Optional[int], reason: str):
        """
        Records an identified data loss.
        """
        with self._lock:
            conn = self._get_connection()
            try:
                with conn:
                    conn.execute(
                        "INSERT INTO losses (session_id, sensor, from_index, to_index, reason) VALUES (?, ?, ?, ?, ?);",
                        (session_id, sensor, from_index, to_index, reason)
                    )
            finally:
                conn.close()

    def save_result(self, result: Dict[str, Any]):
        """
        Saves a 1-second window processing result.
        """
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

    def get_status(self, session_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Returns status JSON line:
        {"mode": mode, "samples_saved": saved, "samples_in_cloud": in_cloud, "backlog": backlog}
        """
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.cursor()
                cur.execute("SELECT value FROM metadata WHERE key = 'mode';")
                row = cur.fetchone()
                mode = row[0] if row else "online"

                if session_id:
                    cur.execute("SELECT COUNT(*), SUM(synced_to_cloud) FROM samples WHERE session_id = ?;", (session_id,))
                else:
                    cur.execute("SELECT COUNT(*), SUM(synced_to_cloud) FROM samples;")
                
                count_row = cur.fetchone()
                total_saved = count_row[0] or 0
                in_cloud = count_row[1] or 0
                backlog = total_saved - in_cloud
                
                return {
                    "mode": mode,
                    "samples_saved": total_saved,
                    "samples_in_cloud": in_cloud,
                    "backlog": backlog
                }
            finally:
                conn.close()

    def get_unsynced_samples_batch(self, batch_size: int = 4000) -> List[Dict[str, Any]]:
        """
        Fetches a batch of samples that haven't been acknowledged by cloud yet.
        """
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT session_id, sensor, sample_index, values_json FROM samples WHERE synced_to_cloud = 0 ORDER BY session_id, sensor, sample_index ASC LIMIT ?;",
                    (batch_size,)
                )
                rows = cur.fetchall()
                results = []
                for s_id, sensor, idx, vals_json in rows:
                    results.append({
                        "session_id": s_id,
                        "sensor": sensor,
                        "index": idx,
                        "values": json.loads(vals_json)
                    })
                return results
            finally:
                conn.close()

    def mark_samples_synced(self, items: List[Tuple[str, int, int]]):
        """
        Marks a list of (session_id, sensor, sample_index) as synced.
        """
        if not items:
            return
        with self._lock:
            conn = self._get_connection()
            try:
                with conn:
                    conn.executemany(
                        "UPDATE samples SET synced_to_cloud = 1 WHERE session_id = ? AND sensor = ? AND sample_index = ?;",
                        items
                    )
            finally:
                conn.close()

    def get_unsynced_losses(self, batch_size: int = 500) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id, session_id, sensor, from_index, to_index, reason FROM losses WHERE synced_to_cloud = 0 LIMIT ?;",
                    (batch_size,)
                )
                rows = cur.fetchall()
                return [
                    {
                        "id": r[0],
                        "session_id": r[1],
                        "sensor": r[2],
                        "from_index": r[3],
                        "to_index": r[4],
                        "reason": r[5]
                    }
                    for r in rows
                ]
            finally:
                conn.close()

    def mark_losses_synced(self, loss_ids: List[int]):
        if not loss_ids:
            return
        with self._lock:
            conn = self._get_connection()
            try:
                with conn:
                    conn.executemany(
                        "UPDATE losses SET synced_to_cloud = 1 WHERE id = ?;",
                        [(lid,) for lid in loss_ids]
                    )
            finally:
                conn.close()

    def get_samples_for_window(self, session_id: str, sensor: int, start_idx: int, end_idx: int) -> Dict[int, List[int]]:
        """
        Fetches samples for processing.
        """
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
        """
        Yields JSON lines for session export:
        First: samples: {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
        Then: losses: {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}
        """
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.cursor()
                # 1. Samples ordered by sensor and index
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
        """
        Yields JSON lines for session results:
        one JSON line per window and mode with section 3 outputs.
        """
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
