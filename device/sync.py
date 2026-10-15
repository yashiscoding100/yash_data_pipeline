"""
Device Cloud Synchronizer and Network Watcher
Monitors cloud connectivity, transitions between online and offline modes within 10s,
streams live data to cloud in online mode (< 5s latency), runs offline window processing
when disconnected, and executes high-speed catch-up replication upon network restoration.
"""

import asyncio
import json
import logging
import time
import urllib.request
import urllib.error
from typing import Dict, List, Optional, Tuple, Any

from device.storage import DeviceStorage
from device.processor import WindowProcessor

logger = logging.getLogger("device.sync")

class CloudSyncManager:
    def __init__(
        self,
        storage: DeviceStorage,
        cloud_url: str = "http://127.0.0.1:8000",
        probe_interval: float = 1.5,
        probe_timeout: float = 1.5
    ):
        self.storage = storage
        self.cloud_url = cloud_url.rstrip('/')
        self.probe_interval = probe_interval
        self.probe_timeout = probe_timeout
        
        self.running = False
        self.current_mode = "online"
        self.offline_processor = WindowProcessor()
        self.device_processed_windows: set = set()
        
        # Real-time queue for online streaming
        self.realtime_queue: asyncio.Queue = asyncio.Queue()
        
        # Consecutively failed/successful probe tracking
        self.consecutive_failures = 0
        self.consecutive_successes = 0

    def check_cloud_health(self) -> bool:
        url = f"{self.cloud_url}/api/health"
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.probe_timeout) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode('utf-8'))
                    return data.get("status") == "ok"
        except Exception:
            return False
        return False

    def enqueue_new_samples(self, session_id: str, sensor: int, first_idx: int, count: int):
        """
        Called by reader callback whenever new samples are persisted to disk.
        """
        self.realtime_queue.put_nowait((session_id, sensor, first_idx, count))

    async def _post_json(self, endpoint: str, payload: dict) -> bool:
        url = f"{self.cloud_url}{endpoint}"
        body = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        try:
            # Run blocking HTTP request in executor to avoid blocking the asyncio loop
            loop = asyncio.get_running_loop()
            resp_code = await loop.run_in_executor(None, self._send_request_sync, req)
            return resp_code == 200
        except Exception as e:
            logger.debug(f"HTTP POST to {url} failed: {e}")
            return False

    def _send_request_sync(self, req: urllib.request.Request) -> int:
        try:
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                return resp.status
        except urllib.error.HTTPError as e:
            return e.code
        except Exception:
            return 500

    async def _network_watcher_loop(self):
        """
        Continuously checks network connection.
        Switches between online and offline mode within 10s.
        """
        while self.running:
            loop = asyncio.get_running_loop()
            is_healthy = await loop.run_in_executor(None, self.check_cloud_health)
            
            if is_healthy:
                self.consecutive_successes += 1
                self.consecutive_failures = 0
                if self.current_mode != "online" and self.consecutive_successes >= 1:
                    logger.info("Cloud connected! Switching mode to ONLINE within 10s.")
                    self.current_mode = "online"
                    self.storage.set_mode("online")
            else:
                self.consecutive_failures += 1
                self.consecutive_successes = 0
                # If 2 probes fail (~3 seconds), switch to offline
                if self.current_mode != "offline" and self.consecutive_failures >= 2:
                    logger.warning("Cloud unreachable! Switching mode to OFFLINE within 10s.")
                    self.current_mode = "offline"
                    self.storage.set_mode("offline")
                    
            await asyncio.sleep(self.probe_interval)

    async def _realtime_stream_loop(self):
        """
        In online mode, streams incoming samples to cloud immediately.
        In offline mode, triggers local window processor.
        """
        while self.running:
            try:
                session_id, sensor, first_idx, count = await self.realtime_queue.get()
                
                if self.current_mode == "online":
                    # Fetch samples from local DB to stream
                    samples_dict = self.storage.get_samples_for_window(
                        session_id, sensor, first_idx, first_idx + count - 1
                    )
                    payload_samples = [
                        {
                            "session_id": session_id,
                            "sensor": sensor,
                            "index": idx,
                            "values": vals
                        }
                        for idx, vals in samples_dict.items()
                    ]
                    
                    if payload_samples:
                        success = await self._post_json("/api/ingest", {
                            "samples": payload_samples,
                            "is_online_stream": True
                        })
                        if success:
                            # Mark as synced
                            synced_tuples = [(session_id, sensor, idx) for idx in samples_dict.keys()]
                            self.storage.mark_samples_synced(synced_tuples)
                        else:
                            # Ingest failed, mode will switch to offline in watcher
                            pass
                            
                else:
                    # OFFLINE MODE: process data on device
                    # Check if a 1-second window has completed
                    end_idx = first_idx + count - 1
                    target_window = end_idx // 400
                    for w in range(0, target_window + 1):
                        if w in self.device_processed_windows:
                            continue
                        w_start = 400 * w
                        w_end = 400 * w + 399
                        fetch_start = max(0, 400 * (w - 4) - 801)
                        s1 = self.storage.get_samples_for_window(session_id, sensor=1, start_idx=fetch_start, end_idx=w_end + 1)
                        s2 = self.storage.get_samples_for_window(session_id, sensor=2, start_idx=w_start, end_idx=w_end)
                        
                        result = self.offline_processor.process_window(
                            session_id=session_id,
                            window=w,
                            mode="offline",
                            s1_samples=s1,
                            s2_samples=s2
                        )
                        self.storage.save_result(result)
                        self.device_processed_windows.add(w)
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in realtime stream loop: {e}", exc_info=True)
                await asyncio.sleep(0.1)

    async def _catchup_replication_loop(self):
        """
        When online, syncs any backlog of unsynced samples, losses, and offline results.
        Runs at high speed to fulfill Criterion 6 (catch up 2h backlog in < 15 min).
        """
        while self.running:
            if self.current_mode != "online":
                await asyncio.sleep(1.0)
                continue
                
            # Check unsynced samples
            batch = self.storage.get_unsynced_samples_batch(batch_size=4000)
            if not batch:
                # Also check unsynced losses
                losses_batch = self.storage.get_unsynced_losses(batch_size=200)
                if losses_batch:
                    success = await self._post_json("/api/losses", {"losses": losses_batch})
                    if success:
                        self.storage.mark_losses_synced([l["id"] for l in losses_batch])
                await asyncio.sleep(0.5)
                continue
                
            # Send batch to cloud
            success = await self._post_json("/api/ingest", {
                "samples": batch,
                "is_online_stream": False
            })
            if success:
                tuples_to_mark = [(s["session_id"], s["sensor"], s["index"]) for s in batch]
                self.storage.mark_samples_synced(tuples_to_mark)
            else:
                await asyncio.sleep(1.0)

    async def run(self):
        self.running = True
        logger.info(f"Starting CloudSyncManager targeting {self.cloud_url}")
        
        watcher_task = asyncio.create_task(self._network_watcher_loop())
        stream_task = asyncio.create_task(self._realtime_stream_loop())
        catchup_task = asyncio.create_task(self._catchup_replication_loop())
        
        try:
            await asyncio.gather(watcher_task, stream_task, catchup_task)
        except asyncio.CancelledError:
            pass
        finally:
            watcher_task.cancel()
            stream_task.cancel()
            catchup_task.cancel()

    def stop(self):
        self.running = False
