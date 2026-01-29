#!/usr/bin/env python3
"""
Start the LightRag ingestion process in the background.
"""
import os
import sys
import subprocess
import time
import signal
from pathlib import Path
from config import Config


def check_already_running(pid_file: Path) -> bool:
    """Check if ingestion is already running."""
    if not pid_file.exists():
        return False
    
    try:
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip())
        
        # Check if process is still running
        try:
            os.kill(pid, 0)  # Check if process exists
            return True
        except OSError:
            return False
    except (ValueError, OSError):
        return False


def start_daemon(force: bool = False, skip_check: bool = False, root_dir: str = None, language: str = None):
    """Start the ingestion daemon."""
    config = Config
    pid_file = config.get_pid_file()
    
    # Check if already running
    if check_already_running(pid_file):
        print("⚠️  Ingestion is already running.")
        print(f"   PID file: {pid_file}")
        print("   Use 'python monitor.py status' to check progress")
        return False
    
    # Create necessary directories
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    config.get_log_file().parent.mkdir(parents=True, exist_ok=True)
    config.get_progress_file().parent.mkdir(parents=True, exist_ok=True)
    
    # Build command
    cmd = [sys.executable, "ingest.py"]
    if force:
        cmd.append("--force")
    if skip_check:
        cmd.append("--skip-check")
    if root_dir:
        cmd.extend(["--root-dir", root_dir])
    if language:
        cmd.extend(["--language", language])
    
    # Start process
    print("🚀 Starting LightRag ingestion daemon...")
    
    # Redirect output to log file
    log_file = config.get_log_file()
    with open(log_file, 'a') as log:
        log.write(f"\n{'='*60}\n")
        log.write(f"Ingestion started at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log.write(f"Command: {' '.join(cmd)}\n")
        log.write(f"{'='*60}\n\n")
    
    # Start the process
    if sys.platform == "win32":
        # Windows
        process = subprocess.Popen(
            cmd,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            stdout=open(log_file, 'a'),
            stderr=subprocess.STDOUT,
            start_new_session=True
        )
    else:
        # Unix/Linux/Mac
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
    print("📊 Check progress with: python monitor.py")
    
    return True


def stop_daemon():
    """Stop the ingestion daemon."""
    config = Config
    pid_file = config.get_pid_file()
    
    if not pid_file.exists():
        print("❌ No ingestion process is running.")
        return False
    
    try:
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip())
        
        print(f"🛑 Stopping ingestion process (PID: {pid})...")
        
        try:
            if sys.platform == "win32":
                import signal
                os.kill(pid, signal.CTRL_BREAK_EVENT)
            else:
                os.kill(pid, signal.SIGTERM)
            
            # Wait for process to terminate
            for _ in range(10):  # Wait up to 5 seconds
                try:
                    os.kill(pid, 0)
                    time.sleep(0.5)
                except OSError:
                    break
            
        except ProcessLookupError:
            print(f"⚠️  Process {pid} not found")
        
        # Remove PID file
        if pid_file.exists():
            pid_file.unlink()
        
        print("✅ Process stopped.")
        return True
        
    except (ValueError, OSError) as e:
        print(f"❌ Failed to stop process: {e}")
        if pid_file.exists():
            pid_file.unlink()
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage LightRag ingestion daemon")
    parser.add_argument("action", choices=["start", "stop", "restart"],
                       help="Action to perform")
    parser.add_argument("--force", action="store_true",
                       help="Force re-ingestion when starting")
    parser.add_argument("--skip-check", action="store_true",
                       help="Skip document check when starting")
    parser.add_argument("--root-dir", type=str,
                       help="Override the MARKDOWN_ROOT_DIR from .env file")
    parser.add_argument("--language", type=str,
                       help="Override the LANGUAGE from .env file")
    
    args = parser.parse_args()
    
    if args.action == "start":
        start_daemon(
            force=args.force, 
            skip_check=args.skip_check,
            root_dir=args.root_dir,
            language=args.language
        )
    elif args.action == "stop":
        stop_daemon()
    elif args.action == "restart":
        stop_daemon()
        time.sleep(1)
        start_daemon(
            force=args.force, 
            skip_check=args.skip_check,
            root_dir=args.root_dir,
            language=args.language
        )