#!/usr/bin/env python3
"""
Monitor LightRag ingestion progress.
"""
import json
import time
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

from config import Config


class Monitor:
    """Monitor ingestion progress."""
    
    def __init__(self):
        self.config = Config
    
    def get_progress(self) -> Optional[Dict[str, Any]]:
        """Read progress data."""
        progress_file = self.config.get_progress_file()
        if not progress_file.exists():
            return None
        
        try:
            with open(progress_file, 'r') as f:
                return json.load(f)
        except:
            return None
    
    def is_running(self) -> bool:
        """Check if process is running."""
        pid_file = self.config.get_pid_file()
        if not pid_file.exists():
            return False
        
        try:
            with open(pid_file, 'r') as f:
                pid = int(f.read().strip())
            
            # Check if process exists
            try:
                os.kill(pid, 0)
                return True
            except OSError:
                return False
        except:
            return False
    
    def format_duration(self, seconds: float) -> str:
        """Format duration nicely."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            m = int(seconds // 60)
            s = int(seconds % 60)
            return f"{m}m {s}s"
        else:
            h = int(seconds // 3600)
            m = int((seconds % 3600) // 60)
            return f"{h}h {m}m"
    
    def show_status(self):
        """Show detailed status."""
        is_running = self.is_running()
        progress = self.get_progress()
        
        print("\n" + "="*60)
        print("📊 LIGHTRAG INGESTION STATUS")
        print("="*60)
        
        # Process status
        status = "🟢 RUNNING" if is_running else "🔴 STOPPED"
        print(f"\nProcess: {status}")
        
        if not progress:
            print("\nNo progress data available.")
            return
        
        # Timing
        started = datetime.fromisoformat(progress["started_at"])
        duration = (datetime.now() - started).total_seconds()
        
        print(f"\n📈 Progress:")
        print(f"   Started: {started.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Duration: {self.format_duration(duration)}")
        print(f"   Status: {progress['status'].upper()}")
        
        if progress.get("completed_at"):
            completed = datetime.fromisoformat(progress["completed_at"])
            total = (completed - started).total_seconds()
            print(f"   Completed: {completed.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Total time: {self.format_duration(total)}")
        
        # Statistics
        print(f"\n📁 Statistics:")
        print(f"   Total files: {progress.get('total_files', 0)}")
        print(f"   To process: {progress.get('to_process', 0)}")
        print(f"   Processed: {progress.get('processed_files', 0)}")
        print(f"   Successful: {progress.get('successful', 0)}")
        print(f"   Failed: {progress.get('failed', 0)}")
        print(f"   Skipped: {progress.get('skipped', 0)}")
        print(f"   Deleted: {progress.get('deleted', 0)}")
        
        # Progress bar
        total = progress.get('total_files', 0)
        processed = progress.get('processed_files', 0)
        if total > 0:
            percent = (processed / total * 100) if total > 0 else 0
            bar_width = 40
            filled = int(bar_width * percent / 100)
            bar = "█" * filled + "░" * (bar_width - filled)
            print(f"\n📊 Progress: [{bar}] {processed}/{total} ({percent:.1f}%)")
    
    def show_summary(self):
        """Show brief summary."""
        is_running = self.is_running()
        progress = self.get_progress()
        
        if is_running:
            print("🟢 Ingestion is running")
        else:
            print("🔴 Ingestion is stopped")
        
        if progress:
            total = progress.get('total_files', 0)
            processed = progress.get('processed_files', 0)
            successful = progress.get('successful', 0)
            
            if total > 0:
                percent = (processed / total * 100) if total > 0 else 0
                print(f"📊 {processed}/{total} files ({percent:.1f}%)")
                print(f"✅ {successful} successful, {progress.get('failed', 0)} failed")
                print(f"⏭️  {progress.get('skipped', 0)} skipped")
                print(f"🗑️  {progress.get('deleted', 0)} deleted")
                print(f"📈 Status: {progress['status']}")
    
    def show_log(self, lines: int = 20):
        """Show log file tail."""
        log_file = self.config.get_log_file()
        if log_file.exists():
            os.system(f"tail -{lines} {log_file}")
        else:
            print(f"Log file not found: {log_file}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor LightRag ingestion")
    parser.add_argument("command", nargs="?", default="status",
                       choices=["status", "summary", "log"],
                       help="Command to run")
    parser.add_argument("--lines", "-n", type=int, default=20,
                       help="Number of log lines to show")
    
    args = parser.parse_args()
    
    monitor = Monitor()
    
    if args.command == "status":
        monitor.show_status()
    elif args.command == "summary":
        monitor.show_summary()
    elif args.command == "log":
        monitor.show_log(args.lines)