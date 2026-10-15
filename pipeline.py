#!/usr/bin/env python3
"""
Pipeline Command-Line Tool
Required Commands (Section 4):
1. `pipeline status`
   Prints one JSON line: current mode, samples saved, samples in the cloud, backlog.
2. `pipeline export --session ID --from device` (or `--from cloud`)
   Prints JSON lines: one per sample, then one per loss.
3. `pipeline results --session ID`
   Prints one JSON line per window and mode, with Section 3 outputs.

Additional Service Subcommands:
- `pipeline run`: Runs the device data collection and sync daemon.
- `pipeline cloud`: Runs the cloud ingestion and processing server.
- `pipeline mock`: Runs the biosignal mock sensor service.
- `pipeline verify`: Checks an exported session against ground truth.
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.error

from device.storage import DeviceStorage

DEFAULT_DEVICE_DB = "device_data.db"
DEFAULT_CLOUD_URL = "http://127.0.0.1:8000"


def cmd_status(args):
    db_path = getattr(args, 'db', DEFAULT_DEVICE_DB)
    storage = DeviceStorage(db_path)
    status_dict = storage.get_status(session_id=getattr(args, 'session', None))
    # Printed as exactly one JSON line
    print(json.dumps(status_dict))


def cmd_export(args):
    session_id = args.session
    source = args.source.lower()
    
    if source == "device":
        db_path = getattr(args, 'db', DEFAULT_DEVICE_DB)
        storage = DeviceStorage(db_path)
        for line in storage.export_session_generator(session_id):
            print(line)
    elif source == "cloud":
        cloud_url = getattr(args, 'cloud_url', DEFAULT_CLOUD_URL).rstrip('/')
        url = f"{cloud_url}/api/export?session_id={session_id}"
        try:
            with urllib.request.urlopen(url, timeout=60.0) as resp:
                for line in resp:
                    decoded = line.decode('utf-8').strip()
                    if decoded:
                        print(decoded)
        except Exception as e:
            print(f"[ERROR] Could not fetch export from cloud: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"[ERROR] Invalid --from source: {source}. Must be 'device' or 'cloud'.", file=sys.stderr)
        sys.exit(1)


def cmd_results(args):
    session_id = args.session
    db_path = getattr(args, 'db', DEFAULT_DEVICE_DB)
    storage = DeviceStorage(db_path)
    
    found_any = False
    for line in storage.export_results_generator(session_id):
        print(line)
        found_any = True
        
    if not found_any:
        # Check cloud fallback
        cloud_url = getattr(args, 'cloud_url', DEFAULT_CLOUD_URL).rstrip('/')
        url = f"{cloud_url}/api/results?session_id={session_id}"
        try:
            with urllib.request.urlopen(url, timeout=30.0) as resp:
                for line in resp:
                    decoded = line.decode('utf-8').strip()
                    if decoded:
                        print(decoded)
        except Exception:
            pass


def cmd_run(args):
    from device.service import DevicePipelineService
    import asyncio
    service = DevicePipelineService(
        db_path=getattr(args, 'db', DEFAULT_DEVICE_DB),
        mock_host=args.mock_host,
        mock_port=args.mock_port,
        cloud_url=args.cloud_url
    )
    asyncio.run(service.run())


def cmd_cloud(args):
    from cloud.server import run_cloud_server
    run_cloud_server(
        host=args.host,
        port=args.port,
        db_path=args.db
    )


def cmd_mock(args):
    from mock.mock_server import BiosignalMockServer
    import asyncio
    mock = BiosignalMockServer(
        mode=args.mode,
        seed=args.seed,
        duration=args.duration,
        port=args.port,
        session_id=args.session_id
    )
    print(f"[Mock] Running biosignal-mock on port {args.port} (mode={args.mode}, seed={args.seed})")
    asyncio.run(mock.run())


def cmd_verify(args):
    from mock.check_ground_truth import verify_export
    ok = verify_export(
        export_file=args.export_file,
        mode=args.mode,
        seed=args.seed,
        max_expected_samples=args.expected_samples
    )
    sys.exit(0 if ok else 1)


def main():
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="Never Lose a Heartbeat - Bedside Device & Cloud Data Pipeline"
    )
    subparsers = parser.add_subparsers(dest="subcommand", help="Available commands")

    # 1. status
    p_status = subparsers.add_parser("status", help="Print pipeline status as one JSON line")
    p_status.add_argument("--db", default=DEFAULT_DEVICE_DB, help="Device DB path")
    p_status.add_argument("--session", default=None, help="Optional session ID")
    p_status.set_defaults(func=cmd_status)

    # 2. export
    p_export = subparsers.add_parser("export", help="Export session data and losses as JSON lines")
    p_export.add_argument("--session", required=True, help="Session ID to export")
    p_export.add_argument("--from", dest="source", required=True, choices=["device", "cloud"], help="Export source")
    p_export.add_argument("--db", default=DEFAULT_DEVICE_DB, help="Device DB path")
    p_export.add_argument("--cloud-url", default=DEFAULT_CLOUD_URL, help="Cloud URL")
    p_export.set_defaults(func=cmd_export)

    # 3. results
    p_results = subparsers.add_parser("results", help="Print window processing results as JSON lines")
    p_results.add_argument("--session", required=True, help="Session ID")
    p_results.add_argument("--db", default=DEFAULT_DEVICE_DB, help="Device DB path")
    p_results.add_argument("--cloud-url", default=DEFAULT_CLOUD_URL, help="Cloud URL")
    p_results.set_defaults(func=cmd_results)

    # 4. run (service daemon)
    p_run = subparsers.add_parser("run", help="Run device pipeline service")
    p_run.add_argument("--db", default=DEFAULT_DEVICE_DB, help="Device DB path")
    p_run.add_argument("--mock-host", default="127.0.0.1", help="Mock host")
    p_run.add_argument("--mock-port", type=int, default=7100, help="Mock port")
    p_run.add_argument("--cloud-url", default=DEFAULT_CLOUD_URL, help="Cloud URL")
    p_run.set_defaults(func=cmd_run)

    # 5. cloud
    p_cloud = subparsers.add_parser("cloud", help="Run cloud server")
    p_cloud.add_argument("--host", default="0.0.0.0", help="Host")
    p_cloud.add_argument("--port", type=int, default=8000, help="Port")
    p_cloud.add_argument("--db", default="cloud_data.db", help="Cloud DB path")
    p_cloud.set_defaults(func=cmd_cloud)

    # 6. mock
    p_mock = subparsers.add_parser("mock", help="Run mock biosignal service")
    p_mock.add_argument("--mode", choices=["signal", "ramp"], default="signal", help="Mode")
    p_mock.add_argument("--seed", type=int, default=7, help="Seed")
    p_mock.add_argument("--duration", type=int, default=3600, help="Duration in seconds")
    p_mock.add_argument("--port", type=int, default=7100, help="Port")
    p_mock.add_argument("--session-id", default=None, help="Session ID")
    p_mock.set_defaults(func=cmd_mock)

    # 7. verify
    p_verify = subparsers.add_parser("verify", help="Check export against ground truth")
    p_verify.add_argument("export_file", help="Export file")
    p_verify.add_argument("--mode", choices=["signal", "ramp"], default="signal", help="Mode")
    p_verify.add_argument("--seed", type=int, default=7, help="Seed")
    p_verify.add_argument("--expected-samples", type=int, default=None, help="Expected count")
    p_verify.set_defaults(func=cmd_verify)

    args = parser.parse_args()
    if not args.subcommand:
        parser.print_help()
        sys.exit(1)
        
    args.func(args)


if __name__ == "__main__":
    main()
