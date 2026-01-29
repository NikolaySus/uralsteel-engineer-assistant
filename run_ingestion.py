#!/usr/bin/env python3
"""
Start/stop LightRag ingestion daemon.
"""
import os
import sys
import subprocess
import time
import signal
from pathlib import Path
from config import Config


def check_running(pid_file: Path) -> bool:
    """Check if process is running."""
    if not pid_file.exists():
        return False
    
    try:
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)  # Check if process exists
        return True
    except:
        return False


def start_daemon(force: bool = False, skip_check: bool = False):
    """Start the daemon."""
    config = Config
    pid_file = config.get_pid_file()
    
    if check_running(pid_file):
        print("⚠️  Ingestion is already running.")
        print("   Use 'uv run monitor.py' to check progress")
        return False
    
    # Create directories
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    config.get_log_file().parent.mkdir(parents=True, exist_ok=True)
    config.get_progress_file().parent.mkdir(parents=True, exist_ok=True)
    
    # Build command
    cmd = [sys.executable, "ingest.py"]
    if force:
        cmd.append("--force")
    if skip_check:
        cmd.append("--skip-check")
    
    # Start process
    print("🚀 Starting LightRag ingestion daemon...")
    
    # Redirect output to log file
    log_file = config.get_log_file()
    with open(log_file, 'a') as log:
        log.write(f"\n{'='*60}\n")
        log.write(f"Ingestion started at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log.write(f"Command: {' '.join(cmd)}\n")
        log.write(f"{'='*60}\n\n")
    
    # Start process
    if sys.platform == "win32":
        process = subprocess.Popen(
            cmd,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            stdout=open(log_file, 'a'),
            stderr=subprocess.STDOUT,
            start_new_session=True
        )
    else:
        process = subprocess.Popen(
            cmd,
            stdout=open(log_file, 'a'),
            stderr=subprocess.STDOUT,
            start_new_session=True
        )
    
    # Save PID
    with open(pid_file, 'w') as f:
        f.write(str(process.pid))
    
    print(f"✅ Daemon started with PID: {process.pid}")
    print(f"📝 Log file: {log_file}")
    print("📊 Monitor: uv run monitor.py")
    
    return True


def stop_daemon():
    """Stop the daemon."""
    config = Config
    pid_file = config.get_pid_file()
    
    if not pid_file.exists():
        print("❌ No ingestion process is running.")
        return False
    
    try:
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip())
        
        print(f"🛑 Stopping process (PID: {pid})...")
        
        try:
            if sys.platform == "win32":
                import signal
                os.kill(pid, signal.CTRL_BREAK_EVENT)
            else:
                os.kill(pid, signal.SIGTERM)
            
            # Wait for process to terminate
            for _ in range(10):
                try:
                    os.kill(pid, 0)
                    time.sleep(0.5)
                except OSError:
                    break
        except ProcessLookupError:
            pass
        
        # Remove PID file
        pid_file.unlink(missing_ok=True)
        print("✅ Process stopped.")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        pid_file.unlink(missing_ok=True)
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage ingestion daemon")
    parser.add_argument("action", choices=["start", "stop", "restart"],
                       help="Action to perform")
    parser.add_argument("--force", action="store_true", help="Force re-ingestion")
    parser.add_argument("--skip-check", action="store_true", help="Skip document check")
    
    args = parser.parse_args()
    
    if args.action == "start":
        start_daemon(force=args.force, skip_check=args.skip_check)
    elif args.action == "stop":
        stop_daemon()
    elif args.action == "restart":
        stop_daemon()
        time.sleep(1)
        start_daemon(force=args.force, skip_check=args.skip_check)