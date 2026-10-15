"""
Section 3 Data Processing Logic
Implements exact windowing, channel statistics, magnitude calculation, and heart-rate detection.
Can be executed in online mode (cloud) or offline mode (device).
"""

import math
from typing import Dict, List, Optional, Tuple, Any

SAMPLES_PER_SECOND = 400

class WindowProcessor:
    def __init__(self):
        # Peak tracking for Sensor 1 Channel 2 (0-indexed channel 1)
        # We maintain all identified peaks in the session: list of sample indices
        self.peaks: List[int] = []
        self.last_checked_sample: int = -1

    def detect_peaks_up_to(self, s1_samples: Dict[int, List[int]], up_to_index: int):
        """
        Scans for peaks in Sensor 1 Channel 2 up to up_to_index.
        A sample i is a peak when:
        1. Samples i-800 to i+1 are all available (802 samples total).
        2. val[i] > val[i-1] and val[i] > val[i+1].
        3. val[i] > 0.5 * max(val[i-800:i]).
        4. val[i] is at least 100 samples after the previous peak.
        """
        # We start checking from max(800, self.last_checked_sample + 1)
        start_idx = max(800, self.last_checked_sample + 1)
        # We need sample i+1 to check condition 2
        end_idx = up_to_index
        
        for i in range(start_idx, end_idx):
            # Check availability of samples i-800 to i+1
            # Fast check: sample i and neighbours
            if i not in s1_samples or (i - 1) not in s1_samples or (i + 1) not in s1_samples:
                continue
                
            val_i = s1_samples[i][1]  # Channel 2 is index 1
            val_prev = s1_samples[i - 1][1]
            val_next = s1_samples[i + 1][1]
            
            # Condition 2: larger than both neighbours
            if val_i <= val_prev or val_i <= val_next:
                continue
                
            # Condition 4: at least 100 samples after the previous peak
            if self.peaks and (i - self.peaks[-1] < 100):
                continue
                
            # Condition 1: 800 samples before it are available
            all_800_available = True
            max_800 = -float('inf')
            for j in range(i - 800, i):
                if j not in s1_samples:
                    all_800_available = False
                    break
                val_j = s1_samples[j][1]
                if val_j > max_800:
                    max_800 = val_j
                    
            if not all_800_available:
                continue
                
            # Condition 3: larger than half of the maximum of the 800 samples before it
            if val_i <= 0.5 * max_800:
                continue
                
            # All conditions met! Record peak
            self.peaks.append(i)

        self.last_checked_sample = max(self.last_checked_sample, end_idx - 1)

    def compute_heart_rate(self, window: int, s1_samples: Dict[int, List[int]]) -> Optional[float]:
        """
        Rule for window w:
        Take peaks in last 5 seconds: samples 400(w-4) to 400(w+1)-1.
        heart_rate = 60 / (mean gap between consecutive peaks, in seconds).
        No rate: null if w < 4, if any sample in those 5 seconds is missing, or fewer than 2 peaks.
        Round to 1 decimal place.
        """
        if window < 4:
            return None
            
        start_sample = 400 * (window - 4)
        end_sample = 400 * (window + 1) - 1
        
        # Check if ANY sample in those 5 seconds is missing
        for i in range(start_sample, end_sample + 1):
            if i not in s1_samples:
                return None
                
        # Find peaks in this range
        peaks_in_range = [p for p in self.peaks if start_sample <= p <= end_sample]
        if len(peaks_in_range) < 2:
            return None
            
        # Consecutive gaps
        k = len(peaks_in_range)
        # Mean gap in seconds = (p_last - p_first) / (400 * (k - 1))
        p_first = peaks_in_range[0]
        p_last = peaks_in_range[-1]
        mean_gap_sec = (p_last - p_first) / (400.0 * (k - 1))
        
        if mean_gap_sec <= 0:
            return None
            
        hr = 60.0 / mean_gap_sec
        return round(hr, 1)

    def process_window(
        self,
        session_id: str,
        window: int,
        mode: str,
        s1_samples: Dict[int, List[int]],
        s2_samples: Dict[int, List[int]]
    ) -> Dict[str, Any]:
        """
        Processes window w (samples 400w to 400w + 399) and produces Section 3 result.
        """
        w_start = 400 * window
        w_end = 400 * window + 399
        
        s1_window_samples = [s1_samples[i] for i in range(w_start, w_end + 1) if i in s1_samples]
        s2_window_samples = [s2_samples[i] for i in range(w_start, w_end + 1) if i in s2_samples]
        
        missing_1 = 400 - len(s1_window_samples)
        missing_2 = 400 - len(s2_window_samples)
        complete = (missing_1 == 0 and missing_2 == 0)
        
        # Mean, min, max per channel
        # Sensor 1: 3 channels
        if s1_window_samples:
            s1_mean = [
                round(sum(sample[c] for sample in s1_window_samples) / len(s1_window_samples), 1)
                for c in range(3)
            ]
            s1_min = [min(sample[c] for sample in s1_window_samples) for c in range(3)]
            s1_max = [max(sample[c] for sample in s1_window_samples) for c in range(3)]
        else:
            s1_mean = None
            s1_min = None
            s1_max = None
            
        # Sensor 2: 2 channels
        if s2_window_samples:
            s2_mean = [
                round(sum(sample[c] for sample in s2_window_samples) / len(s2_window_samples), 1)
                for c in range(2)
            ]
            s2_min = [min(sample[c] for sample in s2_window_samples) for c in range(2)]
            s2_max = [max(sample[c] for sample in s2_window_samples) for c in range(2)]
            
            # Magnitude: Mean of sqrt(ch1^2 + ch2^2) over the available sensor 2 samples
            magnitudes = [math.sqrt(sample[0]**2 + sample[1]**2) for sample in s2_window_samples]
            magnitude = round(sum(magnitudes) / len(magnitudes), 1)
        else:
            s2_mean = None
            s2_min = None
            s2_max = None
            magnitude = None
            
        # Update peak detector up to window end (sample w_end + 1 needed for neighbour check)
        self.detect_peaks_up_to(s1_samples, w_end + 1)
        
        # Calculate heart rate
        heart_rate = self.compute_heart_rate(window, s1_samples)
        
        result = {
            "session_id": session_id,
            "window": window,
            "mode": mode,
            "missing_1": missing_1,
            "missing_2": missing_2,
            "complete": complete,
            "mean": {
                "sensor_1": s1_mean,
                "sensor_2": s2_mean
            },
            "min": {
                "sensor_1": s1_min,
                "sensor_2": s2_min
            },
            "max": {
                "sensor_1": s1_max,
                "sensor_2": s2_max
            },
            "magnitude": magnitude,
            "heart_rate": heart_rate
        }
        return result
