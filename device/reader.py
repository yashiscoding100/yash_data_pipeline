"""
Device Sensor Reader Daemon
Connects to biosignal-mock via TCP, continuously reads Sensor 1 and Sensor 2,
validates CRC32 checksums, detects and logs data losses honestly, handles mock restarts,
and persists all samples to crash-safe local storage.
"""

import asyncio
import json
import logging
import time
import zlib
from typing import Callable, Dict, List, Optional, Tuple, Any

from device.storage import DeviceStorage

logger = logging.getLogger("device.reader")

def extract_data_text_and_crc(line_str: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Finds the substring from '[' to matching ']' corresponding to "data": [...]
    and computes its zlib CRC32 hex string.
    """
    idx_data = line_str.find('"data"')
    if idx_data == -1:
        return None, None
        
    start_bracket = line_str.find('[', idx_data)
    if start_bracket == -1:
        return None, None
        
    end_bracket = line_str.rfind(']')
    if end_bracket == -1 or end_bracket <= start_bracket:
        return None, None
        
    data_text = line_str[start_bracket:end_bracket + 1]
    crc_val = zlib.crc32(data_text.encode('utf-8')) & 0xffffffff
    return data_text, f"{crc_val:08x}"


class MockReader:
    def __init__(
        self,
        storage: DeviceStorage,
        mock_host: str = "127.0.0.1",
        mock_port: int = 7100,
        on_samples_saved: Optional[Callable[[str, int, int, int], None]] = None
    ):
        self.storage = storage
        self.mock_host = mock_host
        self.mock_port = mock_port
        self.on_samples_saved = on_samples_saved  # Callback: (session_id, sensor, first_idx, count)
        
        self.running = False
        self.current_session_id: Optional[str] = None
        self.expected_next_index: Dict[int, int] = {1: 0, 2: 0}
        self.retry_from_index: Dict[int, Optional[int]] = {1: None, 2: None}

    def _handle_session_change(self, new_session_id: str):
        if self.current_session_id and self.current_session_id != new_session_id:
            logger.warning(f"Mock restarted! Session changed from {self.current_session_id} to {new_session_id}")
            # Section 6.3: Unread data of the old session is then lost and must be listed, with "to_index": null
            for sensor in (1, 2):
                last_idx = self.expected_next_index[sensor]
                self.storage.record_loss(
                    session_id=self.current_session_id,
                    sensor=sensor,
                    from_index=last_idx,
                    to_index=None,
                    reason="mock_restarted"
                )
        self.current_session_id = new_session_id
        self.storage.set_active_session(new_session_id)
        self.expected_next_index = {1: 0, 2: 0}
        self.retry_from_index = {1: None, 2: None}

    async def _read_sensor(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, sensor: int) -> bool:
        """
        Sends read command for sensor, parses response, validates CRC, and saves data.
        Returns True if samples were read or processed.
        """
        # Formulate request
        req = {"cmd": "read", "sensor": sensor}
        if self.retry_from_index[sensor] is not None:
            req["from_index"] = self.retry_from_index[sensor]
            
        req_line = json.dumps(req) + "\n"
        writer.write(req_line.encode('utf-8'))
        await writer.drain()
        
        resp_line_bytes = await reader.readline()
        if not resp_line_bytes:
            raise ConnectionResetError("Mock closed connection")
            
        resp_str = resp_line_bytes.decode('utf-8').strip()
        if not resp_str:
            return False
            
        try:
            reply = json.loads(resp_str)
        except Exception as e:
            logger.error(f"Failed to parse JSON reply from mock: {e}")
            return False
            
        session_id = reply.get("session_id")
        if not session_id:
            return False
            
        # Detect session restart
        if self.current_session_id != session_id:
            self._handle_session_change(session_id)
            
        # Verify CRC32
        _, computed_crc = extract_data_text_and_crc(resp_str)
        reported_crc = reply.get("crc32")
        
        if computed_crc != reported_crc:
            logger.warning(
                f"Corrupt reply for sensor {sensor} (expected CRC {reported_crc}, got {computed_crc})! Retrying..."
            )
            # Re-read data starting from expected index
            first_idx = reply.get("first_index", self.expected_next_index[sensor])
            self.retry_from_index[sensor] = first_idx
            return False
            
        # CRC valid! Clear any retry request
        self.retry_from_index[sensor] = None
        
        # Process any losses reported by mock
        lost_ranges = reply.get("lost", [])
        for lost in lost_ranges:
            f_idx = lost.get("from_index")
            t_idx = lost.get("to_index")
            reason = lost.get("reason", "mock_reported_loss")
            logger.info(f"Mock reported loss for sensor {sensor}: [{f_idx}, {t_idx}] ({reason})")
            self.storage.record_loss(session_id, sensor, f_idx, t_idx, reason)
            if t_idx is not None and t_idx >= self.expected_next_index[sensor]:
                self.expected_next_index[sensor] = t_idx + 1

        first_index = reply.get("first_index", 0)
        count = reply.get("count", 0)
        data = reply.get("data", [])
        
        # Detect gap between expected index and first_index that wasn't listed in lost
        if first_index > self.expected_next_index[sensor]:
            gap_from = self.expected_next_index[sensor]
            gap_to = first_index - 1
            logger.warning(f"Detected unlisted gap for sensor {sensor}: [{gap_from}, {gap_to}]")
            self.storage.record_loss(session_id, sensor, gap_from, gap_to, "unlisted_gap")
            
        if count > 0 and data:
            # Save to disk immediately
            inserted = self.storage.save_samples(session_id, sensor, first_index, data)
            self.expected_next_index[sensor] = max(self.expected_next_index[sensor], first_index + count)
            if self.on_samples_saved:
                self.on_samples_saved(session_id, sensor, first_index, count)
            return True
            
        return len(lost_ranges) > 0

    async def run(self):
        self.running = True
        logger.info(f"Starting MockReader connecting to {self.mock_host}:{self.mock_port}")
        
        while self.running:
            try:
                reader, writer = await asyncio.open_connection(self.mock_host, self.mock_port)
                logger.info(f"Connected to biosignal-mock at {self.mock_host}:{self.mock_port}")
                
                while self.running:
                    # Alternating read of Sensor 1 and Sensor 2
                    s1_active = await self._read_sensor(reader, writer, sensor=1)
                    s2_active = await self._read_sensor(reader, writer, sensor=2)
                    
                    # If neither sensor had new data, wait briefly to let mock generate samples
                    if not s1_active and not s2_active:
                        await asyncio.sleep(0.05)
                        
            except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError, OSError) as e:
                if self.running:
                    logger.warning(f"Connection to mock failed ({e}), retrying in 1s...")
                    await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Unexpected reader error: {e}", exc_info=True)
                await asyncio.sleep(1.0)

    def stop(self):
        self.running = False
