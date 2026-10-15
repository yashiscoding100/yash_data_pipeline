"""
End-to-End System Test
Tests complete execution pipeline:
Mock Server <--> Device Pipeline Service <--> Cloud Server
Verifies pipeline status, export, ground truth verification, and results output.
"""

import asyncio
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import unittest

from mock.check_ground_truth import verify_export
from device.storage import DeviceStorage

def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

class TestEndToEndPipeline(unittest.TestCase):
    def setUp(self):
        self.device_db = "test_e2e_device.db"
        self.cloud_db = "test_e2e_cloud.db"
        self.export_file_device = "test_e2e_export_device.jsonl"
        self.export_file_cloud = "test_e2e_export_cloud.jsonl"
        self._cleanup()
        
        self.mock_port = find_free_port()
        self.cloud_port = find_free_port()

    def tearDown(self):
        self._cleanup()

    def _cleanup(self):
        for base in (self.device_db, self.cloud_db):
            for suffix in ("", "-wal", "-shm"):
                f = base + suffix
                if os.path.exists(f):
                    try:
                        os.remove(f)
                    except Exception:
                        pass
        for f in (self.export_file_device, self.export_file_cloud):
            if os.path.exists(f):
                try:
                    os.remove(f)
                except Exception:
                    pass

    def test_full_pipeline_run(self):
        session_id = "e2e_sess_01"
        seed = 42
        
        # 1. Start Mock Server
        mock_proc = subprocess.Popen([
            sys.executable, "-m", "mock.mock_server",
            "--mode", "ramp",
            "--seed", str(seed),
            "--port", str(self.mock_port),
            "--session-id", session_id
        ])
        
        # 2. Start Cloud Server
        cloud_proc = subprocess.Popen([
            sys.executable, "-m", "cloud.server",
            "--port", str(self.cloud_port),
            "--db", self.cloud_db
        ])
        
        # Give servers a moment to bind ports
        time.sleep(1.0)
        
        # 3. Start Device Service
        device_proc = subprocess.Popen([
            sys.executable, "-m", "device.service",
            "--db", self.device_db,
            "--mock-port", str(self.mock_port),
            "--cloud-url", f"http://127.0.0.1:{self.cloud_port}"
        ])
        
        try:
            # Let the pipeline collect and stream for 3.5 seconds (~1400 samples per sensor = 2800 total)
            time.sleep(3.5)
            
            # Check pipeline status command
            status_out = subprocess.check_output([
                sys.executable, "pipeline.py", "status",
                "--db", self.device_db,
                "--session", session_id
            ]).decode('utf-8').strip()
            
            print(f"\n[E2E] pipeline status output: {status_out}")
            status_data = json.loads(status_out)
            self.assertEqual(status_data["mode"], "online")
            self.assertGreater(status_data["samples_saved"], 800)
            
            # Allow catchup to finish syncing
            time.sleep(1.0)
            
            # Export from device
            with open(self.export_file_device, "w", encoding="utf-8") as f:
                subprocess.run([
                    sys.executable, "pipeline.py", "export",
                    "--session", session_id,
                    "--from", "device",
                    "--db", self.device_db
                ], stdout=f, check=True)
                
            # Export from cloud
            with open(self.export_file_cloud, "w", encoding="utf-8") as f:
                subprocess.run([
                    sys.executable, "pipeline.py", "export",
                    "--session", session_id,
                    "--from", "cloud",
                    "--cloud-url", f"http://127.0.0.1:{self.cloud_port}"
                ], stdout=f, check=True)
                
            # Check results output command
            results_out = subprocess.check_output([
                sys.executable, "pipeline.py", "results",
                "--session", session_id,
                "--db", self.device_db,
                "--cloud-url", f"http://127.0.0.1:{self.cloud_port}"
            ]).decode('utf-8').strip()
            print(f"[E2E] First line of pipeline results:\n{results_out.splitlines()[0] if results_out else 'None'}")
            self.assertTrue(len(results_out.splitlines()) >= 1)
            
            # Verify Device Export against Ground Truth
            print("[E2E] Verifying device export against Ground Truth...")
            ok_device = verify_export(self.export_file_device, mode="ramp", seed=seed)
            self.assertTrue(ok_device, "Device export failed ground truth verification!")
            
            # Verify Cloud Export against Ground Truth
            print("[E2E] Verifying cloud export against Ground Truth...")
            ok_cloud = verify_export(self.export_file_cloud, mode="ramp", seed=seed)
            self.assertTrue(ok_cloud, "Cloud export failed ground truth verification!")
            
        finally:
            device_proc.terminate()
            cloud_proc.terminate()
            mock_proc.terminate()
            device_proc.wait()
            cloud_proc.wait()
            mock_proc.wait()

if __name__ == "__main__":
    unittest.main()
