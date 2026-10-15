"""
Unit Tests for Device Storage Engine
Tests crash safety, idempotence against duplicate samples, loss logging, and export formatting.
"""

import json
import os
import unittest
from device.storage import DeviceStorage

class TestDeviceStorage(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_device_data.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.storage = DeviceStorage(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            try:
                os.remove(self.db_path)
            except Exception:
                pass
        wal_file = f"{self.db_path}-wal"
        shm_file = f"{self.db_path}-shm"
        for f in (wal_file, shm_file):
            if os.path.exists(f):
                try:
                    os.remove(f)
                except Exception:
                    pass

    def test_save_and_idempotence(self):
        session_id = "sess_01"
        samples = [
            [720, 1200, 1560],
            [708, 1180, 1534],
            [726, 1210, 1573]
        ]
        # Insert 3 samples
        inserted = self.storage.save_samples(session_id, sensor=1, first_index=0, samples=samples)
        self.assertEqual(inserted, 3)

        status = self.storage.get_status(session_id)
        self.assertEqual(status["samples_saved"], 3)
        self.assertEqual(status["samples_in_cloud"], 0)
        self.assertEqual(status["backlog"], 3)

        # Duplicate arrival: mock sends same reply again
        inserted_dup = self.storage.save_samples(session_id, sensor=1, first_index=0, samples=samples)
        self.assertEqual(inserted_dup, 0)  # Zero duplicates inserted!

        status_after = self.storage.get_status(session_id)
        self.assertEqual(status_after["samples_saved"], 3)  # Still exactly 3

    def test_record_loss(self):
        session_id = "sess_02"
        self.storage.record_loss(session_id, sensor=2, from_index=4000, to_index=4399, reason="buffer_overflow")
        
        losses = self.storage.get_unsynced_losses(batch_size=10)
        self.assertEqual(len(losses), 1)
        self.assertEqual(losses[0]["session_id"], session_id)
        self.assertEqual(losses[0]["sensor"], 2)
        self.assertEqual(losses[0]["from_index"], 4000)
        self.assertEqual(losses[0]["to_index"], 4399)
        self.assertEqual(losses[0]["reason"], "buffer_overflow")

    def test_sync_marking(self):
        session_id = "sess_03"
        samples = [[10, 20, 30], [40, 50, 60]]
        self.storage.save_samples(session_id, sensor=1, first_index=100, samples=samples)
        
        batch = self.storage.get_unsynced_samples_batch(batch_size=10)
        self.assertEqual(len(batch), 2)
        
        # Mark synced
        self.storage.mark_samples_synced([(session_id, 1, 100), (session_id, 1, 101)])
        
        status = self.storage.get_status(session_id)
        self.assertEqual(status["samples_saved"], 2)
        self.assertEqual(status["samples_in_cloud"], 2)
        self.assertEqual(status["backlog"], 0)
        
        batch_after = self.storage.get_unsynced_samples_batch(batch_size=10)
        self.assertEqual(len(batch_after), 0)

    def test_export_format(self):
        session_id = "sess_04"
        self.storage.save_samples(session_id, sensor=1, first_index=120000, samples=[[720, 1200, 1560]])
        self.storage.record_loss(session_id, sensor=2, from_index=4000, to_index=4399, reason="buffer_overflow")

        lines = list(self.storage.export_session_generator(session_id))
        self.assertEqual(len(lines), 2)
        
        # Line 1: sample
        sample_rec = json.loads(lines[0])
        self.assertEqual(sample_rec, {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]})
        
        # Line 2: loss
        loss_rec = json.loads(lines[1])
        self.assertEqual(loss_rec, {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"})

if __name__ == "__main__":
    unittest.main()
