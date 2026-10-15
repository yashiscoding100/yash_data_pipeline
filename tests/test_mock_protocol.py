"""
Unit Tests for Mock Protocol and CRC32 Validation
"""

import json
import unittest
import zlib
from device.reader import extract_data_text_and_crc

class TestMockProtocol(unittest.TestCase):
    def test_crc32_extraction(self):
        # Example from section 6.2:
        # data: [[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]
        sample_line = '{"session_id": "5f0c2a9e", "sensor": 1, "first_index": 120000, "count": 3, "lost": [], "session_end": false, "crc32": "abc12345", "data": [[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]}'
        data_text, crc_hex = extract_data_text_and_crc(sample_line)
        self.assertEqual(data_text, "[[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]")
        
        expected_crc = zlib.crc32(data_text.encode('utf-8')) & 0xffffffff
        self.assertEqual(crc_hex, f"{expected_crc:08x}")

    def test_corrupt_crc_mismatch(self):
        line = '{"session_id": "test", "sensor": 1, "first_index": 0, "count": 1, "lost": [], "session_end": false, "crc32": "00000000", "data": [[1, 2, 3]]}'
        _, computed_crc = extract_data_text_and_crc(line)
        self.assertNotEqual(computed_crc, "00000000")

if __name__ == "__main__":
    unittest.main()
