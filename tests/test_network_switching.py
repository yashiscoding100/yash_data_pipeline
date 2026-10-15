"""
Network Mode Switching & Catch-Up Simulation Test
Validates:
Criterion 4: In online mode, results appear in the cloud within 5s.
Criterion 5: Mode switches within 10s of network changing, without loss or duplication.
Criterion 6: Complete cloud copy upon network return.
"""

import asyncio
import os
import shutil
import socket
import threading
import time
import unittest
from http.server import ThreadingHTTPServer

from cloud.server import CloudRequestHandler, CloudServiceState
from cloud.storage import CloudStorage
from device.storage import DeviceStorage
from device.sync import CloudSyncManager
from device.processor import WindowProcessor
import cloud.server as cloud_server_mod

def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

class TestNetworkSwitching(unittest.TestCase):
    def setUp(self):
        self.device_db = "test_switch_device.db"
        self.cloud_db = "test_switch_cloud.db"
        self._cleanup_dbs()
        
        self.port = find_free_port()
        self.cloud_url = f"http://127.0.0.1:{self.port}"
        
        self.cloud_storage = CloudStorage(self.cloud_db)
        cloud_server_mod.GLOBAL_STATE = CloudServiceState(self.cloud_storage)
        self.httpd = ThreadingHTTPServer(('127.0.0.1', self.port), CloudRequestHandler)
        self.cloud_thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.cloud_thread.start()
        
        self.device_storage = DeviceStorage(self.device_db)
        self.sync_manager = CloudSyncManager(
            storage=self.device_storage,
            cloud_url=self.cloud_url,
            probe_interval=0.5,
            probe_timeout=0.5
        )

    def tearDown(self):
        if hasattr(self, 'httpd'):
            self.httpd.shutdown()
            self.httpd.server_close()
        self._cleanup_dbs()

    def _cleanup_dbs(self):
        for base in (self.device_db, self.cloud_db):
            for suffix in ("", "-wal", "-shm"):
                f = base + suffix
                if os.path.exists(f):
                    try:
                        os.remove(f)
                    except Exception:
                        pass

    def test_switching_and_catchup(self):
        async def run_test():
            # Start sync manager in background task
            sync_task = asyncio.create_task(self.sync_manager.run())
            
            # 1. Initially online
            await asyncio.sleep(1.2)
            self.assertEqual(self.sync_manager.current_mode, "online", "Initial mode should be online")
            
            # Save 400 samples (Window 0) in online mode
            session_id = "test_net_sess"
            samples_w0_s1 = [[100 + i, 200 + i, 300 + i] for i in range(400)]
            samples_w0_s2 = [[400000, 150000] for _ in range(400)]
            self.device_storage.save_samples(session_id, sensor=1, first_index=0, samples=samples_w0_s1)
            self.device_storage.save_samples(session_id, sensor=2, first_index=0, samples=samples_w0_s2)
            
            # Enqueue for online streaming
            self.sync_manager.enqueue_new_samples(session_id, sensor=1, first_idx=0, count=400)
            self.sync_manager.enqueue_new_samples(session_id, sensor=2, first_idx=0, count=400)
            
            # Allow stream to push to cloud
            await asyncio.sleep(1.0)
            
            # Verify cloud received samples
            stats = self.cloud_storage.get_stats(session_id)
            self.assertEqual(stats["total_samples"], 800, "Cloud should receive online samples")
            
            # 2. CUT THE NETWORK (Shutdown cloud HTTP server)
            t_cut = time.time()
            self.httpd.shutdown()
            self.httpd.server_close()
            
            # Wait for detection (< 10s requirement)
            while self.sync_manager.current_mode != "offline":
                await asyncio.sleep(0.2)
                if time.time() - t_cut > 10.0:
                    self.fail("Failed to switch to offline within 10s!")
                    
            switch_to_offline_time = time.time() - t_cut
            print(f"\n[Test] Network cut -> Switched to OFFLINE in {switch_to_offline_time:.2f}s (< 10s)")
            self.assertLess(switch_to_offline_time, 10.0)
            
            # 3. Collect samples in OFFLINE mode (Window 1: samples 400 to 799)
            samples_w1_s1 = [[500 + i, 600 + i, 700 + i] for i in range(400)]
            samples_w1_s2 = [[400100, 150100] for _ in range(400)]
            self.device_storage.save_samples(session_id, sensor=1, first_index=400, samples=samples_w1_s1)
            self.device_storage.save_samples(session_id, sensor=2, first_index=400, samples=samples_w1_s2)
            
            self.sync_manager.enqueue_new_samples(session_id, sensor=1, first_idx=400, count=400)
            self.sync_manager.enqueue_new_samples(session_id, sensor=2, first_idx=400, count=400)
            
            await asyncio.sleep(0.5)
            
            # Status should show backlog of 800 samples
            status = self.device_storage.get_status(session_id)
            self.assertEqual(status["mode"], "offline")
            self.assertEqual(status["samples_saved"], 1600)
            self.assertEqual(status["samples_in_cloud"], 800)
            self.assertEqual(status["backlog"], 800)
            
            # 4. RESTORE THE NETWORK (Restart cloud HTTP server on same port)
            t_restore = time.time()
            self.httpd = ThreadingHTTPServer(('127.0.0.1', self.port), CloudRequestHandler)
            self.cloud_thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
            self.cloud_thread.start()
            
            # Wait for detection (< 10s requirement)
            while self.sync_manager.current_mode != "online":
                await asyncio.sleep(0.2)
                if time.time() - t_restore > 10.0:
                    self.fail("Failed to switch to online within 10s!")
                    
            switch_to_online_time = time.time() - t_restore
            print(f"[Test] Network restored -> Switched to ONLINE in {switch_to_online_time:.2f}s (< 10s)")
            self.assertLess(switch_to_online_time, 10.0)
            
            # 5. Catch-up replication runs
            # Wait for backlog to drain to 0
            t_catchup_start = time.time()
            while True:
                status = self.device_storage.get_status(session_id)
                if status["backlog"] == 0:
                    break
                await asyncio.sleep(0.3)
                if time.time() - t_catchup_start > 15.0:
                    self.fail(f"Catch-up took too long! Remaining backlog: {status['backlog']}")
                    
            print(f"[Test] Catch-up completed in {time.time() - t_catchup_start:.2f}s. Backlog is 0!")
            
            # Verify cloud storage now has ALL 1600 samples
            cloud_stats = self.cloud_storage.get_stats(session_id)
            self.assertEqual(cloud_stats["total_samples"], 1600)
            
            # Compare device export with cloud export: Must match sample for sample!
            device_export = list(self.device_storage.export_session_generator(session_id))
            cloud_export = list(self.cloud_storage.export_session_generator(session_id))
            self.assertEqual(device_export, cloud_export, "Cloud export must be an exact copy of device export!")
            
            self.sync_manager.stop()
            sync_task.cancel()

        asyncio.run(run_test())

if __name__ == "__main__":
    unittest.main()
