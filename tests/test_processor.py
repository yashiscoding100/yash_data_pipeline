"""
Unit Tests for Section 3 Window Processor & Heart-Rate Algorithm
"""

import math
import unittest
from device.processor import WindowProcessor
from mock.mock_server import BiosignalGenerator

class TestWindowProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = WindowProcessor()
        self.gen_signal = BiosignalGenerator(mode="signal", seed=7)
        self.gen_ramp = BiosignalGenerator(mode="ramp", seed=7)

    def test_ramp_mean_min_max(self):
        # 400 samples for window 0
        s1 = {i: self.gen_ramp.generate_sensor1_sample(i) for i in range(400)}
        s2 = {i: self.gen_ramp.generate_sensor2_sample(i) for i in range(400)}
        
        res = self.processor.process_window(
            session_id="test",
            window=0,
            mode="offline",
            s1_samples=s1,
            s2_samples=s2
        )
        
        self.assertEqual(res["window"], 0)
        self.assertEqual(res["missing_1"], 0)
        self.assertEqual(res["missing_2"], 0)
        self.assertTrue(res["complete"])
        
        # Check channel values for ramp:
        # Sensor 1 ch0: 3*i mod 8388608
        # min for i=0 is 0, max for i=399 is 3*399 = 1197
        self.assertEqual(res["min"]["sensor_1"][0], 0)
        self.assertEqual(res["max"]["sensor_1"][0], 1197)
        expected_mean_0 = sum(3 * i for i in range(400)) / 400.0
        self.assertAlmostEqual(res["mean"]["sensor_1"][0], round(expected_mean_0, 1), places=1)
        
        # Magnitude for sensor 2: sqrt(ch1^2 + ch2^2)
        expected_mag = sum(math.sqrt((2*i)**2 + (2*i+1)**2) for i in range(400)) / 400.0
        self.assertAlmostEqual(res["magnitude"], round(expected_mag, 1), places=1)
        
        # Window 0 has w < 4, so heart rate MUST be None
        self.assertIsNone(res["heart_rate"])

    def test_missing_samples(self):
        # Only 350 samples available for sensor 1, 380 for sensor 2
        s1 = {i: [100, 200, 300] for i in range(350)}
        s2 = {i: [400000, 150000] for i in range(380)}
        
        res = self.processor.process_window(
            session_id="test",
            window=0,
            mode="online",
            s1_samples=s1,
            s2_samples=s2
        )
        self.assertEqual(res["missing_1"], 50)
        self.assertEqual(res["missing_2"], 20)
        self.assertFalse(res["complete"])
        self.assertEqual(res["mode"], "online")

    def test_heart_rate_signal_60bpm(self):
        """
        Mode 'signal' produces 60 bpm in minutes 0-2 (0-120s = 0-48000 samples).
        Let's generate 5 seconds (2000 samples) up to window 4 (samples 0 to 1999).
        Window 4 covers samples 1600 to 1999. Last 5 seconds is samples 0 to 1999.
        Window 4 has w = 4 >= 4, and all samples available!
        """
        proc = WindowProcessor()
        # Generate samples 0 to 2005
        s1 = {i: self.gen_signal.generate_sensor1_sample(i) for i in range(2005)}
        s2 = {i: self.gen_signal.generate_sensor2_sample(i) for i in range(2005)}
        
        # Process window 0 through 4
        for w in range(5):
            res = proc.process_window("test", w, "offline", s1, s2)
            if w < 4:
                self.assertIsNone(res["heart_rate"], f"Window {w} should have null heart_rate")
            else:
                self.assertIsNotNone(res["heart_rate"], "Window 4 should have a heart_rate")
                # At 60 bpm, heart rate should be approximately 60.0 bpm
                self.assertAlmostEqual(res["heart_rate"], 60.0, delta=1.5)

    def test_heart_rate_null_if_missing_sample_in_5s_window(self):
        proc = WindowProcessor()
        s1 = {i: self.gen_signal.generate_sensor1_sample(i) for i in range(2005)}
        s2 = {i: self.gen_signal.generate_sensor2_sample(i) for i in range(2005)}
        
        # Artificially remove one sample in the 5-second window
        del s1[1000]
        
        for w in range(5):
            res = proc.process_window("test", w, "online", s1, s2)
            if w == 4:
                self.assertIsNone(res["heart_rate"], "Heart rate must be null if any sample in 5s is missing")

if __name__ == "__main__":
    unittest.main()
