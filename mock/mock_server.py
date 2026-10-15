"""
Biosignal Mock Service Replica
Implements Section 6 of the Case Assessment:
- Port 7100 (or configurable)
- Sensor 1 (3 channels, 400 sps, signed 24-bit)
- Sensor 2 (2 channels, 400 sps, signed 24-bit)
- Two modes: --mode signal and --mode ramp
- Seed reproducible values
- Newline-delimited JSON over TCP
- CRC32 verification of data text
- 10-second buffer limit (data not read in time is lost)
- Fault injection capabilities for test verification
"""

import argparse
import asyncio
import json
import math
import random
import sys
import time
import zlib
from typing import Dict, List, Optional, Tuple

MOD_24BIT = 8_388_608  # 2^23, signed 24-bit integer limit is -8388608 to 8388607

class BiosignalGenerator:
    def __init__(self, mode: str = "signal", seed: int = 7):
        self.mode = mode
        self.seed = seed
        self.rng = random.Random(seed)
        
    def generate_sensor1_sample(self, index: int) -> List[int]:
        """
        Sensor 1: 3 channels, 400 sps.
        Channel 2: ECG-like with sharp peak per heartbeat.
        HR: 60 bpm (min 0-2), 90 bpm (min 2-4), 72 bpm (min 4-6), repeating every 6 minutes (360s = 144,000 samples).
        Channel 1: Channel 2 * 0.6
        Channel 3: Channel 2 * 1.3
        """
        if self.mode == "ramp":
            return [
                (3 * index + 0) % MOD_24BIT,
                (3 * index + 1) % MOD_24BIT,
                (3 * index + 2) % MOD_24BIT
            ]
        
        # Mode: signal
        # Time in session
        t = index / 400.0
        cycle_t = t % 360.0  # 6-minute repeating cycle
        
        if cycle_t < 120.0:
            bpm = 60.0
        elif cycle_t < 240.0:
            bpm = 90.0
        else:
            bpm = 72.0
            
        period_s = 60.0 / bpm
        phase = (t % period_s) / period_s  # 0.0 to 1.0 within beat
        
        # Synthetic ECG wave for channel 2
        # Baseline noise/variation
        baseline = 1000 + int(50 * math.sin(2 * math.pi * 0.1 * t))
        
        # ECG morphology: P wave, QRS complex, T wave
        # R peak occurs at phase ~ 0.3
        ecg_val = baseline
        if 0.18 <= phase < 0.24:
            # P-wave
            p_phase = (phase - 0.21) / 0.03
            ecg_val += int(150 * math.exp(-p_phase * p_phase * 4))
        elif 0.27 <= phase < 0.29:
            # Q wave (slight dip)
            ecg_val -= 100
        elif 0.29 <= phase <= 0.33:
            # R peak (sharp spike)
            r_phase = (phase - 0.31) / 0.02
            # Sharp tall peak: max ~ 1200 + baseline
            ecg_val += int(2500 * math.exp(-r_phase * r_phase * 9))
        elif 0.33 < phase <= 0.36:
            # S wave (dip)
            ecg_val -= 200
        elif 0.45 <= phase < 0.60:
            # T wave
            t_phase = (phase - 0.525) / 0.075
            ecg_val += int(300 * math.exp(-t_phase * t_phase * 4))
            
        ch2 = int(ecg_val)
        ch1 = int(round(ch2 * 0.6))
        ch3 = int(round(ch2 * 1.3))
        
        return [ch1, ch2, ch3]

    def generate_sensor2_sample(self, index: int) -> List[int]:
        """
        Sensor 2: 2 channels, 400 sps.
        Channel 1 near 400,000, channel 2 near 150,000, varying slowly.
        """
        if self.mode == "ramp":
            return [
                (2 * index + 0) % MOD_24BIT,
                (2 * index + 1) % MOD_24BIT
            ]
        
        # Mode: signal
        t = index / 400.0
        # Slowly varying signal
        ch1 = 400000 + int(5000 * math.sin(2 * math.pi * 0.05 * t) + 1000 * math.cos(2 * math.pi * 0.01 * t))
        ch2 = 150000 + int(3000 * math.cos(2 * math.pi * 0.07 * t) + 800 * math.sin(2 * math.pi * 0.02 * t))
        return [ch1, ch2]


class BiosignalMockServer:
    def __init__(self, mode: str = "signal", seed: int = 7, duration: int = 3600, port: int = 7100, session_id: Optional[str] = None):
        self.mode = mode
        self.seed = seed
        self.duration = duration
        self.port = port
        self.session_id = session_id or f"{random.randint(0x10000000, 0xffffffff):08x}"
        self.generator = BiosignalGenerator(mode, seed)
        
        # Start time of current session
        self.start_time = time.time()
        self.total_samples = duration * 400
        
        # Last read indexes per sensor
        self.client_read_index: Dict[int, int] = {1: 0, 2: 0}
        
        # Buffer tracks available samples: mock only keeps the last 10 seconds (4000 samples)
        # Oldest available sample index = max(0, current_head - 4000)
        self.buffer_window_samples = 4000  # 10 seconds * 400 samples/s
        
        # Fault injection flags
        self.inject_crc_error_once = False
        self.inject_duplicate_once = False
        self.inject_drop_sensor_until: Optional[float] = None
        self.drop_sensor_num: Optional[int] = None
        
    def current_head_index(self) -> int:
        elapsed = time.time() - self.start_time
        head = int(elapsed * 400)
        return min(head, self.total_samples)

    def get_oldest_available_index(self) -> int:
        head = self.current_head_index()
        return max(0, head - self.buffer_window_samples)

    def read_sensor_data(self, sensor: int, from_index: Optional[int] = None) -> Tuple[dict, str]:
        """
        Produces reply dict and the exact data text from [ to ].
        """
        head = self.current_head_index()
        oldest_avail = self.get_oldest_available_index()
        
        lost_ranges = []
        if from_index is not None:
            req_index = from_index
        else:
            req_index = self.client_read_index[sensor]
            
        # Check if requested index has fallen out of the 10-second buffer
        if req_index < oldest_avail:
            lost_ranges.append({
                "from_index": req_index,
                "to_index": oldest_avail - 1,
                "reason": "buffer_overflow"
            })
            req_index = oldest_avail
            
        # Mock delivers at most 1 second of data per reply (400 samples)
        count = min(400, head - req_index)
        if count < 0:
            count = 0
            
        data_samples = []
        for i in range(req_index, req_index + count):
            if sensor == 1:
                data_samples.append(self.generator.generate_sensor1_sample(i))
            else:
                data_samples.append(self.generator.generate_sensor2_sample(i))
                
        # Advance read index
        self.client_read_index[sensor] = req_index + count
        session_end = (req_index + count >= self.total_samples)
        
        # Serialize data text exactly
        data_text = json.dumps(data_samples, separators=(',', ':'))
        crc = zlib.crc32(data_text.encode('utf-8')) & 0xffffffff
        crc_hex = f"{crc:08x}"
        
        if self.inject_crc_error_once:
            self.inject_crc_error_once = False
        reply = {
            "session_id": self.session_id,
            "sensor": sensor,
            "first_index": req_index,
            "count": count,
            "lost": lost_ranges,
            "session_end": session_end,
            "crc32": "00000000",
            "data": data_samples
        }
        raw_json = json.dumps(reply)
        idx_data = raw_json.find('"data"')
        start_bracket = raw_json.find('[', idx_data)
        end_bracket = raw_json.rfind(']')
        data_text = raw_json[start_bracket:end_bracket + 1]
        crc = zlib.crc32(data_text.encode('utf-8')) & 0xffffffff
        crc_hex = f"{crc:08x}"
        
        if self.inject_crc_error_once:
            self.inject_crc_error_once = False
            crc_hex = "ffffffff"  # corrupt CRC
            
        reply["crc32"] = crc_hex
        final_msg = json.dumps(reply) + "\n"
        return reply, final_msg

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client_addr = writer.get_extra_info('peername')
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                
                try:
                    req = json.loads(line.decode('utf-8').strip())
                except Exception:
                    continue
                    
                if req.get("cmd") != "read":
                    continue
                    
                sensor = req.get("sensor", 1)
                from_index = req.get("from_index")
                
                # Check drop sensor fault injection
                if self.inject_drop_sensor_until and time.time() < self.inject_drop_sensor_until:
                    if sensor == self.drop_sensor_num:
                        # Report lost due to sensor drop
                        head = self.current_head_index()
                        req_idx = from_index if from_index is not None else self.client_read_index[sensor]
                        reply = {
                            "session_id": self.session_id,
                            "sensor": sensor,
                            "first_index": req_idx,
                            "count": 0,
                            "lost": [{
                                "from_index": req_idx,
                                "to_index": head,
                                "reason": "sensor_disconnected"
                            }],
                            "session_end": False,
                            "crc32": "00000000",
                            "data": []
                        }
                        self.client_read_index[sensor] = head + 1
                        msg = json.dumps(reply) + "\n"
                        writer.write(msg.encode('utf-8'))
                        await writer.drain()
                        continue
                        
                _, msg = self.read_sensor_data(sensor, from_index)
                writer.write(msg.encode('utf-8'))
                await writer.drain()
                
                if self.inject_duplicate_once:
                    self.inject_duplicate_once = False
                    # Send duplicate reply
                    writer.write(msg.encode('utf-8'))
                    await writer.drain()
                    
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def run(self):
        server = await asyncio.start_server(self.handle_client, '0.0.0.0', self.port)
        async with server:
            await server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Biosignal Mock Service")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal", help="Mode: signal or ramp")
    parser.add_argument("--seed", type=int, default=7, help="Random seed")
    parser.add_argument("--duration", type=int, default=3600, help="Duration in seconds")
    parser.add_argument("--port", type=int, default=7100, help="TCP port")
    parser.add_argument("--session-id", type=str, default=None, help="Session ID")
    args = parser.parse_args()

    mock = BiosignalMockServer(
        mode=args.mode,
        seed=args.seed,
        duration=args.duration,
        port=args.port,
        session_id=args.session_id
    )
    print(f"[Mock] Running biosignal-mock on port {args.port} (mode={args.mode}, seed={args.seed}, session={mock.session_id})")
    try:
        asyncio.run(mock.run())
    except KeyboardInterrupt:
        print("[Mock] Stopped.")

if __name__ == "__main__":
    main()
