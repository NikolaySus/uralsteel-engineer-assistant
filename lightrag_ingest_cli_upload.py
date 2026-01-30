#!/usr/bin/env python3
"""
LightRag Markdown Ingestion CLI

This script ingests markdown files into LightRag service with proper
concurrency control and processing status tracking.
"""

import asyncio
import json
import os
import sys
import time
import argparse
import multiprocessing
import subprocess
import psutil
from pathlib import Path
from lightrag.api import AsyncLightRagClient
from tqdm.asyncio import tqdm_asyncio

# --------------------------
# CONFIG
# --------------------------
LIGHTRAG_URL = "http://localhost:9621"
API_KEY = None
CONCURRENCY = 1  # Reduced concurrency to avoid rate limiting
STATUS_FILE = Path("ingest_status.json")
PROCESSING_STATUS_FILE = Path("processing_status.json")
POLL_INTERVAL = 5  # seconds between status checks

# --------------------------
# HELPERS
# --------------------------
def collect_markdown_files(root: str):
    """Collect all markdown files recursively from a directory"""
    return sorted(Path(root).rglob("*.md"))

async def wait_for_capacity(client, max_concurrent: int):
    """Wait until there's capacity in the processing pipeline"""
    while True:
        status_counts = await client.get_status_counts()
        # Count documents that are still processing (not processed or failed)
        processing_count = sum(
            count for status, count in status_counts.status_counts.items()
            if status in ["pending", "processing", "preprocessed"]
        )
        if processing_count < max_concurrent:
            break
        await asyncio.sleep(POLL_INTERVAL)

async def upload_one(semaphore, client, path: Path, status_file: Path, processing_status_file: Path):
    """Upload a single document to LightRag"""
    async with semaphore:
        text = path.read_text(encoding="utf-8", errors="ignore")

        # Use the file path as file_source
        file_source = str(path)

        # Wait for capacity before uploading
        await wait_for_capacity(client, CONCURRENCY)

        # Upload the document and get track_id
        response = await client.insert_text(
            text,
            file_source=file_source
        )

        # Store processing status
        processing_status = {}
        if processing_status_file.exists():
            processing_status = json.loads(processing_status_file.read_text(encoding="utf-8"))

        processing_status[str(path)] = {
            "track_id": response.track_id,
            "status": "pending",
            "file_source": file_source
        }
        processing_status_file.write_text(json.dumps(processing_status, ensure_ascii=False))

        # Update progress
        progress = json.loads(status_file.read_text(encoding="utf-8"))
        progress["processed"] += 1
        progress["last_modified"] = time.strftime("%Y-%m-%d %H:%M:%S")
        status_file.write_text(json.dumps(progress, ensure_ascii=False))

async def check_processing_status(client, processing_status_file: Path):
    """Check and update processing status of documents"""
    if not processing_status_file.exists():
        return True  # No documents to track

    processing_status = json.loads(processing_status_file.read_text(encoding="utf-8"))
    all_done = True

    for file_path, doc_info in processing_status.items():
        if doc_info["status"] in ["pending", "processing", "preprocessed"]:
            try:
                track_status = await client.get_track_status(doc_info["track_id"])
                # Update status based on the latest track status
                final_statuses = ["processed", "failed"]
                doc_status = track_status.documents[0].status if track_status.documents else "unknown"
                processing_status[file_path]["status"] = doc_status

                if doc_status not in final_statuses:
                    all_done = False
            except Exception:
                # If tracking fails, assume it's still processing
                all_done = False

    processing_status_file.write_text(json.dumps(processing_status, ensure_ascii=False))
    return all_done

async def wait_for_processing_completion(client, processing_status_file: Path):
    """Wait until all documents are processed"""
    while True:
        all_done = await check_processing_status(client, processing_status_file)
        if all_done:
            break
        await asyncio.sleep(POLL_INTERVAL)

async def ingest_async(root_dir: str, status_file: Path):
    """Main ingestion function"""
    files = collect_markdown_files(root_dir)

    # Initialize status files
    status_file.write_text(json.dumps({
        "processed": 0,
        "total": len(files),
        "done": False,
        "last_modified": time.strftime("%Y-%m-%d %H:%M:%S")
    }, ensure_ascii=False))

    PROCESSING_STATUS_FILE.write_text(json.dumps({}, ensure_ascii=False))

    client = AsyncLightRagClient(base_url=LIGHTRAG_URL, api_key=API_KEY)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    try:
        # Upload all files first
        tasks = [
            upload_one(semaphore, client, path, status_file, PROCESSING_STATUS_FILE)
            for path in files
        ]
        await tqdm_asyncio.gather(*tasks)

        # Wait for all documents to be processed
        await wait_for_processing_completion(client, PROCESSING_STATUS_FILE)

        # Mark as done
        progress = json.loads(status_file.read_text(encoding="utf-8"))
        progress["done"] = True
        progress["last_modified"] = time.strftime("%Y-%m-%d %H:%M:%S")
        status_file.write_text(json.dumps(progress, ensure_ascii=False))
    finally:
        await client.close()

def run_ingestion(root_dir: str):
    """Run ingestion in async context"""
    asyncio.run(ingest_async(root_dir, STATUS_FILE))

def find_ingestion_process():
    """Find the ingestion process using psutil"""
    current_script = os.path.abspath(__file__)
    current_dir = os.getcwd()
    current_pid = os.getpid()  # Get current process PID to exclude it

    for proc in psutil.process_iter(['pid', 'cmdline', 'name']):
        try:
            cmdline = proc.info['cmdline']
            pid = proc.info['pid']

            # Skip the current process (status command)
            if pid == current_pid:
                continue

            # Skip if no command line
            if not cmdline or len(cmdline) < 1:
                continue

            # More specific checks for ingestion process
            # Check for Python process running our script
            if (any('lightrag_ingest_cli_upload.py' in arg for arg in cmdline) and
                any('run_ingestion' in arg for arg in cmdline)):
                return proc

            # Check for process started with the specific command pattern
            if (len(cmdline) >= 2 and cmdline[0].endswith('python') and
                any('from lightrag_ingest_cli_upload import run_ingestion' in arg for arg in cmdline)):
                return proc

            # Check for process running the specific ingestion function
            if any('run_ingestion' in arg for arg in cmdline) and not any('status' in arg for arg in cmdline):
                return proc

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return None

def stop_ingestion():
    """Stop the background ingestion process"""
    # Check if status file exists to confirm ingestion is running
    if not STATUS_FILE.exists():
        print("❌ No active ingestion found")
        return 1

    # Try to find the ingestion process
    proc = find_ingestion_process()

    if proc:
        try:
            # Terminate the process and all its children
            for child in proc.children(recursive=True):
                try:
                    child.terminate()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

            proc.terminate()

            # Wait for process to terminate
            try:
                proc.wait(timeout=5)
            except psutil.TimeoutExpired:
                # Force kill if process doesn't terminate
                proc.kill()

            print(f"✅ Ingestion process (PID={proc.pid}) stopped successfully")

            # Clean up status files
            if STATUS_FILE.exists():
                STATUS_FILE.unlink()
            if PROCESSING_STATUS_FILE.exists():
                PROCESSING_STATUS_FILE.unlink()

            print("🧹 Status files cleaned up")
            return 0
        except Exception as e:
            print(f"❌ Failed to stop ingestion process: {e}")
            return 1
    else:
        print("❌ No running ingestion process found")
        print("Note: If ingestion was started in a different session, you may need to stop it manually")

        # Offer to clean up status files
        if input("Would you like to clean up status files? [y/N]: ").lower() == 'y':
            if STATUS_FILE.exists():
                STATUS_FILE.unlink()
            if PROCESSING_STATUS_FILE.exists():
                PROCESSING_STATUS_FILE.unlink()
            print("🧹 Status files cleaned up")
        return 1

def show_status():
    """Show ingestion status"""
    if not STATUS_FILE.exists():
        print("❌ No status file found")
        return 1

    s = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    pct = (s["processed"] / s["total"] * 100) if s["total"] else 0
    print(f"📊 Overall Progress: {s['processed']} / {s['total']} ({pct:.1f}%)")
    print(f"✅ Ingestion Done: {s['done']}")
    print(f"🕒 Last Updated: {s.get('last_modified', 'Unknown')}")

    # Check if the ingestion process still exists
    proc = find_ingestion_process()

    # Check for recent activity by examining file timestamps
    status_age = 0
    if 'last_modified' in s:
        try:
            last_modified_time = time.strptime(s['last_modified'], "%Y-%m-%d %H:%M:%S")
            status_age = (time.time() - time.mktime(last_modified_time)) / 60  # in minutes
        except:
            pass

    # Check if processing status file exists and has active documents
    active_processing = False
    if PROCESSING_STATUS_FILE.exists():
        try:
            processing_status = json.loads(PROCESSING_STATUS_FILE.read_text(encoding="utf-8"))
            for doc_info in processing_status.values():
                if doc_info.get("status") in ["pending", "processing", "preprocessed"]:
                    active_processing = True
                    break
        except:
            pass

    # Determine process status
    if proc:
        print(f"🔄 Process Status: Running (PID={proc.pid})")
        if status_age > 5 and not active_processing and s['processed'] < s['total']:
            print("⚠️  Warning: Process running but no recent activity detected")
    else:
        if s.get('done', False):
            print("🔄 Process Status: Completed")
        else:
            print("⚠️  Process Status: Not found (may have crashed or been stopped)")
            if status_age > 5 and not active_processing:
                print("ℹ️  No recent activity detected in status files")

    # Add troubleshooting suggestion if process is not found but status shows incomplete
    if not proc and not s.get('done', False) and s['processed'] < s['total']:
        print("\n💡 Troubleshooting:")
        print("   • Try starting ingestion again with 'start' command")
        print("   • Check if LightRag service is running")
        print("   • Verify network connectivity to LightRag")

    # Show detailed processing status if available
    if PROCESSING_STATUS_FILE.exists():
        processing_status = json.loads(PROCESSING_STATUS_FILE.read_text(encoding="utf-8"))
        if processing_status:
            print("\n📋 Document Processing Status:")

            status_counts = {}
            for file_path, doc_info in processing_status.items():
                status = doc_info.get("status", "unknown")
                status_counts[status] = status_counts.get(status, 0) + 1
                # Show only the filename for brevity
                filename = Path(file_path).name
                print(f"  {filename}: {status}")

            print(f"\n📊 Processing Summary:")
            for status, count in status_counts.items():
                print(f"  {status}: {count}")

    return 0

def start_background_ingestion(root_dir: str):
    """Start ingestion as a background process that persists after SSH disconnect"""
    # Check if we're on Windows
    is_windows = sys.platform.startswith('win')

    if is_windows:
        try:
            # Windows approach using start command
            script_path = os.path.abspath(__file__)
            current_dir = os.getcwd()

            # Use start command to launch in background
            command = f'start /B python -c "import sys, os; os.chdir(\'{current_dir}\'); sys.path.insert(0, \'.\'); from lightrag_ingest_cli_upload import run_ingestion; run_ingestion(\'{root_dir}\')""'
            result = os.system(command)

            if result == 0:
                print("🚀 Ingestion started in background on Windows")
                print("Use `status` command to check progress")
                return 0
            else:
                print("❌ Failed to start ingestion process on Windows")
                return 1
        except Exception as e:
            print(f"❌ Failed to start ingestion process on Windows: {e}")
            return 1
    else:
        # Unix/Linux approach using psutil for proper process detachment
        try:
            # Start a fully detached process using psutil
            current_dir = os.getcwd()

            # Create a command that will run in a detached process
            command = [
                sys.executable,
                "-c",
                f"import sys, os; os.chdir('{current_dir}'); sys.path.insert(0, '.'); from lightrag_ingest_cli_upload import run_ingestion; run_ingestion('{root_dir}')"
            ]

            # Start a fully detached process
            process = psutil.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True  # This detaches the process
            )

            print(f"🚀 Ingestion started in background (PID={process.pid})")
            print("Use `status` command to check progress")
            print("Process is fully detached and will persist after SSH disconnect")
            return 0
        except Exception as e:
            print(f"❌ Failed to start ingestion process: {e}")
            return 1

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="LightRag Markdown Ingestion CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Start command
    start_parser = subparsers.add_parser("start", help="Start background ingestion")
    start_parser.add_argument("root_dir", help="Root directory containing markdown files")

    # Status command
    subparsers.add_parser("status", help="Check ingestion status")

    # Stop command
    subparsers.add_parser("stop", help="Stop background ingestion")

    args = parser.parse_args()

    if args.command == "start":
        return start_background_ingestion(args.root_dir)
    elif args.command == "status":
        return show_status()
    elif args.command == "stop":
        return stop_ingestion()
    else:
        parser.print_help()
        return 1

if __name__ == "__main__":
    sys.exit(main())