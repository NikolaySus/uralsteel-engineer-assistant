#!/usr/bin/env python3
"""
Monitor LightRag ingestion progress.
"""
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

from config import Config


class Monitor:
    """Monitor ingestion progress."""
    
    def __init__(self):
        self.config = Config
    
    def get_progress_data(self) -> Optional[Dict[str, Any]]:
        """Read progress data from file."""
        progress_file = self.config.get_progress_file()
        
        if not progress_file.exists():
            return None
        
        try:
            with open(progress_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading progress file: {e}")
            return None
    
    def get_process_status(self) -> str:
        """Check if ingestion process is running."""
        pid_file = self.config.get_pid_file()
        
        if not pid_file.exists():
            return "stopped"
        
        try:
            with open(pid_file, 'r') as f:
                pid = int(f.read().strip())
            
            # Check if process exists
            if sys.platform == "win32":
                import ctypes
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(1, False, pid)
                if handle:
                    kernel32.CloseHandle(handle)
                    return "running"
                return "stopped"
            else:
                import os
                try:
                    os.kill(pid, 0)
                    return "running"
                except OSError:
                    return "stopped"
        except (ValueError, OSError):
            return "stopped"
    
    def format_duration(self, seconds: float) -> str:
        """Format duration in human-readable form."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
    
    def show_progress_bar(self, current: int, total: int, width: int = 40):
        """Display a progress bar."""
        if total == 0:
            return
        
        percent = current / total
        filled = int(width * percent)
        bar = "█" * filled + "░" * (width - filled)
        print(f"[{bar}] {current}/{total} ({percent:.1%})")
    
    def show_detailed_status(self):
        """Show detailed status information."""
        process_status = self.get_process_status()
        progress_data = self.get_progress_data()
        
        print("\n" + "="*60)
        print("📊 LIGHTRAG INGESTION MONITOR")
        print("="*60)
        
        # Process status
        status_icons = {
            "running": "🟢",
            "stopped": "🔴",
            "starting": "🟡"
        }
        icon = status_icons.get(process_status, "⚪")
        print(f"\nProcess Status: {icon} {process_status.upper()}")
        
        if not progress_data:
            print("\nNo progress data available.")
            return
        
        # Progress information
        started_at = datetime.fromisoformat(progress_data["started_at"])
        duration = (datetime.now() - started_at).total_seconds()
        
        print(f"\n📈 Progress:")
        print(f"   Started: {started_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Duration: {self.format_duration(duration)}")
        print(f"   Status: {progress_data['status'].upper()}")
        
        if progress_data.get("completed_at"):
            completed_at = datetime.fromisoformat(progress_data["completed_at"])
            total_duration = (completed_at - started_at).total_seconds()
            print(f"   Completed: {completed_at.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Total time: {self.format_duration(total_duration)}")
        
        # File statistics
        print(f"\n📁 Files:")
        print(f"   Total: {progress_data.get('total_files', 0)}")
        print(f"   To Process: {progress_data.get('to_process', 0)}")
        print(f"   Processed: {progress_data.get('processed_files', 0)}")
        print(f"   Successful: {progress_data.get('successful', 0)}")
        print(f"   Failed: {progress_data.get('failed', 0)}")
        print(f"   Skipped: {progress_data.get('skipped', 0)}")
        print(f"   Deleted: {progress_data.get('deleted', 0)}")
        
        # Progress bar
        total = progress_data.get('total_files', 0)
        processed = progress_data.get('processed_files', 0)
        if total > 0 and processed > 0:
            print(f"\n📊 Progress:")
            self.show_progress_bar(processed, total)
        
        # Current file
        current_file = progress_data.get('current_file')
        if current_file:
            print(f"\n📄 Current File:")
            print(f"   {current_file}")
        
        # Recent files
        files = progress_data.get('files', {})
        if files:
            recent_files = list(files.items())[-5:]  # Last 5 files
            print(f"\n⏱️  Recent Files:")
            for filepath, data in recent_files:
                status_icon = "✅" if data.get('status') == 'success' else "❌"
                print(f"   {status_icon} {Path(filepath).name}")
    
    def show_live_monitor(self, interval: int = 2):
        """Show live monitoring with auto-refresh."""
        import sys
        
        print("🔄 Starting live monitor (Press Ctrl+C to stop)...\n")
        
        try:
            while True:
                # Clear screen (cross-platform)
                os.system('cls' if os.name == 'nt' else 'clear')
                
                self.show_detailed_status()
                
                print(f"\n⏱️  Auto-refresh in {interval} seconds...")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n\n🛑 Monitoring stopped.")
    
    def show_summary(self):
        """Show a brief summary."""
        process_status = self.get_process_status()
        progress_data = self.get_progress_data()
        
        if process_status == "running":
            print("🟢 Ingestion is running")
        elif process_status == "stopped":
            print("🔴 Ingestion is stopped")
        
        if progress_data:
            total = progress_data.get('total_files', 0)
            processed = progress_data.get('processed_files', 0)
            status = progress_data.get('status', 'unknown')
            
            if total > 0:
                percent = (processed / total * 100) if total > 0 else 0
                print(f"📊 Progress: {processed}/{total} files ({percent:.1f}%)")
                print(f"📈 Status: {status}")
                
                if status == "completed":
                    print("✅ Ingestion completed")
                elif status == "failed":
                    print("❌ Ingestion failed")
            else:
                print("📊 No files processed yet")
        else:
            print("📊 No progress data available")


if __name__ == "__main__":
    import argparse
    import sys
    import os
    
    parser = argparse.ArgumentParser(description="Monitor LightRag ingestion progress")
    parser.add_argument("command", nargs="?", default="status",
                       choices=["status", "live", "summary", "log", "files"],
                       help="Monitor command")
    parser.add_argument("--interval", "-i", type=int, default=2,
                       help="Refresh interval for live monitor (seconds)")
    
    args = parser.parse_args()
    
    monitor = Monitor()
    
    if args.command == "status":
        monitor.show_detailed_status()
    elif args.command == "live":
        monitor.show_live_monitor(interval=args.interval)
    elif args.command == "summary":
        monitor.show_summary()
    elif args.command == "log":
        log_file = Config.get_log_file()
        if log_file.exists():
            # Show last 20 lines of log
            os.system(f"tail -20 {log_file}")
        else:
            print(f"Log file not found: {log_file}")
    elif args.command == "files":
        progress_data = monitor.get_progress_data()
        if progress_data:
            files = progress_data.get('files', {})
            print(f"\n📁 Processed Files ({len(files)}):")
            print("-" * 60)
            for filepath, data in files.items():
                status_icon = "✅" if data.get('status') == 'success' else "❌"
                filename = Path(filepath).name
                print(f"{status_icon} {filename}")
        else:
            print("No progress data available")