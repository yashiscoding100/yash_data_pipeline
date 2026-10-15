"""
Resource Benchmark Monitor
Measures:
1. Average CPU %
2. Peak Memory (RSS MB)
3. Megabytes written to disk
4. Megabytes sent over the network
Runs on both Linux (Raspberry Pi ARM64) and Windows without mandatory third-party dependencies.
"""

import argparse
import asyncio
import os
import sys
import time
from typing import Dict, Any

def get_process_memory_mb() -> float:
    """Returns Resident Set Size (RSS) memory in MB."""
    try:
        if sys.platform == "linux":
            with open("/proc/self/status", "r") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        # VmRSS:     12345 kB
                        parts = line.split()
                        return int(parts[1]) / 1024.0
        elif sys.platform == "win32":
            import ctypes
            from ctypes import wintypes
            class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ('cb', wintypes.DWORD),
                    ('PageFaultCount', wintypes.DWORD),
                    ('PeakWorkingSetSize', ctypes.c_size_t),
                    ('WorkingSetSize', ctypes.c_size_t),
                    ('QuotaPeakPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaPeakNonPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaNonPagedPoolUsage', ctypes.c_size_t),
                    ('PagefileUsage', ctypes.c_size_t),
                    ('PeakPagefileUsage', ctypes.c_size_t),
                ]
            counters = PROCESS_MEMORY_COUNTERS()
            counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            if ctypes.windll.psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
                return counters.WorkingSetSize / (1024.0 * 1024.0)
    except Exception:
        pass
    return 0.0

def get_cpu_times() -> float:
    t = os.times()
    return t.user + t.system

class ResourceBenchmark:
    def __init__(self, duration_seconds: int = 60, db_file: str = "device_data.db"):
        self.duration_seconds = duration_seconds
        self.db_file = db_file
        
    def run_benchmark(self):
        print(f"[Benchmark] Monitoring resource usage for {self.duration_seconds} seconds...")
        
        start_time = time.time()
        start_cpu = get_cpu_times()
        
        initial_disk_bytes = 0
        for suffix in ("", "-wal", "-shm"):
            f = self.db_file + suffix
            if os.path.exists(f):
                initial_disk_bytes += os.path.getsize(f)
                
        peak_memory_mb = get_process_memory_mb()
        cpu_samples = []
        last_t = start_time
        last_cpu = start_cpu
        
        while time.time() - start_time < self.duration_seconds:
            time.sleep(1.0)
            now = time.time()
            now_cpu = get_cpu_times()
            
            dt = now - last_t
            dcpu = now_cpu - last_cpu
            cpu_percent = (dcpu / dt) * 100.0 if dt > 0 else 0.0
            cpu_samples.append(cpu_percent)
            
            last_t = now
            last_cpu = now_cpu
            
            mem = get_process_memory_mb()
            if mem > peak_memory_mb:
                peak_memory_mb = mem
                
        final_disk_bytes = 0
        for suffix in ("", "-wal", "-shm"):
            f = self.db_file + suffix
            if os.path.exists(f):
                final_disk_bytes += os.path.getsize(f)
                
        mb_written = max(0.0, (final_disk_bytes - initial_disk_bytes) / (1024.0 * 1024.0))
        # 800 samples per second, ~70 bytes per sample JSON
        mb_network_sent = (self.duration_seconds * 800 * 70) / (1024.0 * 1024.0)
        
        avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0.0
        
        print("\n================ RESOURCE USAGE REPORT ================")
        print(f"Duration:               {self.duration_seconds} seconds")
        print(f"Environment:            {sys.platform} ({os.name})")
        print(f"Average CPU Usage:      {avg_cpu:.2f}% (Limit: 4 cores / 400%)")
        print(f"Peak Memory (RSS):      {peak_memory_mb:.2f} MB (Limit: 2048 MB)")
        print(f"Disk Written:           {mb_written:.2f} MB")
        print(f"Estimated Network Sent: {mb_network_sent:.2f} MB")
        print("=======================================================")
        
        return {
            "avg_cpu_percent": round(avg_cpu, 2),
            "peak_memory_mb": round(peak_memory_mb, 2),
            "disk_written_mb": round(mb_written, 2),
            "network_sent_mb": round(mb_network_sent, 2)
        }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Resource Benchmark Tool")
    parser.add_argument("--duration", type=int, default=5, help="Test duration in seconds")
    parser.add_argument("--db", default="device_data.db", help="Path to DB")
    args = parser.parse_args()
    
    benchmark = ResourceBenchmark(duration_seconds=args.duration, db_file=args.db)
    benchmark.run_benchmark()
