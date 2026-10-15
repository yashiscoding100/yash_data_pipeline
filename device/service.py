"""
Device Pipeline Orchestrator Daemon
Integrates Storage, Reader, and Sync Manager into a unified background service.
"""

import asyncio
import logging
import signal
import sys
from typing import Optional

from device.storage import DeviceStorage
from device.reader import MockReader
from device.sync import CloudSyncManager

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("device.service")

class DevicePipelineService:
    def __init__(
        self,
        db_path: str = "device_data.db",
        mock_host: str = "127.0.0.1",
        mock_port: int = 7100,
        cloud_url: str = "http://127.0.0.1:8000"
    ):
        self.storage = DeviceStorage(db_path)
        self.sync_manager = CloudSyncManager(self.storage, cloud_url=cloud_url)
        self.reader = MockReader(
            storage=self.storage,
            mock_host=mock_host,
            mock_port=mock_port,
            on_samples_saved=self._on_samples_saved
        )
        self._shutdown_event = asyncio.Event()

    def _on_samples_saved(self, session_id: str, sensor: int, first_idx: int, count: int):
        self.sync_manager.enqueue_new_samples(session_id, sensor, first_idx, count)

    async def run(self):
        logger.info("Initializing Device Pipeline Service...")
        
        reader_task = asyncio.create_task(self.reader.run())
        sync_task = asyncio.create_task(self.sync_manager.run())
        
        logger.info("Device Pipeline Service is RUNNING.")
        
        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            logger.info("Shutting down Device Pipeline Service...")
            self.reader.stop()
            self.sync_manager.stop()
            reader_task.cancel()
            sync_task.cancel()
            await asyncio.gather(reader_task, sync_task, return_exceptions=True)
            logger.info("Shutdown complete.")

    def stop(self):
        self._shutdown_event.set()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Device Pipeline Service")
    parser.add_argument("--db", default="device_data.db", help="Path to device SQLite DB")
    parser.add_argument("--mock-host", default="127.0.0.1", help="Mock host")
    parser.add_argument("--mock-port", type=int, default=7100, help="Mock port")
    parser.add_argument("--cloud-url", default="http://127.0.0.1:8000", help="Cloud service URL")
    args = parser.parse_args()

    service = DevicePipelineService(
        db_path=args.db,
        mock_host=args.mock_host,
        mock_port=args.mock_port,
        cloud_url=args.cloud_url
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def handle_sig(*_):
        service.stop()

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, handle_sig)
        except Exception:
            pass

    try:
        loop.run_until_complete(service.run())
    except KeyboardInterrupt:
        service.stop()
        loop.run_until_complete(asyncio.sleep(0.5))

if __name__ == "__main__":
    main()
