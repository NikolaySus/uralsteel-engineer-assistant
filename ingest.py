#!/usr/bin/env python3
"""
LightRag Ingestion Daemon
Run as a background process to ingest markdown documents.
"""
import asyncio
import json
import logging
import signal
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Any

from lightrag.api import AsyncLightRagClient
from tqdm.asyncio import tqdm_asyncio

from config import Config


class ProgressTracker:
    """Track ingestion progress and save to file."""
    
    def __init__(self, progress_file: Path):
        self.progress_file = progress_file
        self.progress: Dict[str, Any] = {
            "started_at": datetime.now().isoformat(),
            "total_files": 0,
            "processed_files": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "deleted": 0,
            "current_file": None,
            "status": "starting",  # starting, running, completed, failed
            "completed_at": None,
            "files": {}
        }
    
    def update_file_count(self, total: int, to_process: int, skipped: int = 0):
        self.progress["total_files"] = total
        self.progress["to_process"] = to_process
        self.progress["skipped"] = skipped
        self.save()
    
    def update_status(self, status: str):
        self.progress["status"] = status
        if status == "completed":
            self.progress["completed_at"] = datetime.now().isoformat()
        self.save()
    
    def start_file(self, filepath: str):
        self.progress["current_file"] = filepath
        self.progress["files"][filepath] = {
            "started_at": datetime.now().isoformat(),
            "status": "processing"
        }
        self.save()
    
    def finish_file(self, filepath: str, success: bool, error: str = None):
        self.progress["processed_files"] += 1
        if success:
            self.progress["successful"] += 1
        else:
            self.progress["failed"] += 1
        
        self.progress["files"][filepath].update({
            "completed_at": datetime.now().isoformat(),
            "status": "success" if success else "failed",
            "error": error
        })
        
        if self.progress["current_file"] == filepath:
            self.progress["current_file"] = None
        
        self.save()
    
    def save(self):
        """Save progress to file."""
        try:
            self.progress_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save progress: {e}")


class IngestionDaemon:
    """Main ingestion daemon class."""
    
    def __init__(self):
        self.config = Config
        self.progress_tracker = ProgressTracker(self.config.get_progress_file())
        self.logger = self._setup_logging()
        self.client = None
        self.shutdown_requested = False
    
    def _setup_logging(self):
        """Setup logging to both file and console."""
        log_file = self.config.get_log_file()
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger = logging.getLogger('ingestion_daemon')
        logger.setLevel(self.config.get_log_level())
        
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(levelname)s: %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    async def get_indexed_documents_with_ids(self) -> Dict[str, str]:
        """Query LightRag for already indexed documents."""
        indexed_docs = {}
        page_token = None
        
        self.logger.info("Checking for already indexed documents...")
        
        try:
            while True:
                response = await self.client.query_documents(
                    query="",
                    top_k=self.config.get_batch_size(),
                    metadata_filters={},
                    page_token=page_token
                )
                
                for doc in response.get("documents", []):
                    metadata = doc.get("metadata", {})
                    if "source_path" in metadata:
                        indexed_docs[metadata["source_path"]] = doc.get("id")
                
                page_token = response.get("next_page_token")
                if not page_token:
                    break
                    
        except Exception as e:
            self.logger.error(f"Could not query existing documents: {e}")
            return {}
        
        self.logger.info(f"Found {len(indexed_docs)} already indexed documents")
        return indexed_docs
    
    async def delete_document_by_id(self, doc_id: str) -> bool:
        """Delete a document by its ID."""
        try:
            await self.client.delete_document(doc_id=doc_id)
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete document {doc_id}: {e}")
            return False
    
    async def ingest_one(self, md_path: Path, language: str = "en") -> bool:
        """Ingest a single markdown file."""
        filepath = str(md_path)
        self.progress_tracker.start_file(filepath)
        
        try:
            text = md_path.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            error_msg = f"Failed to read file: {e}"
            self.logger.error(f"{filepath}: {error_msg}")
            self.progress_tracker.finish_file(filepath, False, error_msg)
            return False
        
        metadata = {
            "source_path": filepath,
            "filename": md_path.name,
            "folder": str(md_path.parent),
            "filetype": "markdown",
            "language": language
        }
        
        try:
            await self.client.insert_document(
                text=text,
                metadata=metadata
            )
            self.progress_tracker.finish_file(filepath, True)
            return True
        except Exception as e:
            error_msg = f"Failed to ingest: {e}"
            self.logger.error(f"{filepath}: {error_msg}")
            self.progress_tracker.finish_file(filepath, False, error_msg)
            return False
    
    async def bounded_ingest(self, semaphore, md_path, language):
        """Ingest with concurrency control."""
        async with semaphore:
            return await self.ingest_one(md_path, language)
    
    def collect_markdown_files(self, root: str) -> List[Path]:
        """Collect all markdown files recursively."""
        return sorted(Path(root).rglob("*.md"))
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_requested = True
    
    async def run(self, force: bool = False, skip_check: bool = False):
        """Main ingestion loop."""
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        root_dir = self.config.get_root_dir()
        language = self.config.get_language()
        
        self.logger.info(f"Starting ingestion from {root_dir}")
        self.progress_tracker.update_status("running")
        
        files = self.collect_markdown_files(root_dir)
        
        if not files:
            self.logger.error(f"No markdown files found in {root_dir}")
            self.progress_tracker.update_status("failed")
            return
        
        self.logger.info(f"Found {len(files)} markdown files")
        
        self.client = AsyncLightRagClient(
            base_url=self.config.get_lightrag_url(),
            api_key=self.config.get_api_key()
        )
        
        try:
            files_to_process = files
            deleted_count = 0
            
            if not skip_check:
                if force:
                    # Force mode: delete all existing documents
                    indexed_docs = await self.get_indexed_documents_with_ids()
                    
                    if indexed_docs:
                        self.logger.info(f"Deleting {len(indexed_docs)} existing documents...")
                        delete_tasks = [
                            self.delete_document_by_id(doc_id)
                            for doc_id in indexed_docs.values()
                        ]
                        
                        results = await asyncio.gather(*delete_tasks)
                        deleted_count = sum(1 for r in results if r)
                        self.logger.info(f"Deleted {deleted_count} documents")
                    
                    files_to_process = files
                else:
                    # Normal mode: skip already indexed files
                    indexed_docs = await self.get_indexed_documents_with_ids()
                    
                    files_to_process = [
                        f for f in files 
                        if str(f) not in indexed_docs
                    ]
                    
                    skipped_count = len(files) - len(files_to_process)
                    if skipped_count > 0:
                        self.logger.info(f"Skipping {skipped_count} already indexed files")
            else:
                self.logger.info("Skipping document check")
            
            if not files_to_process:
                self.logger.info("All documents are already indexed")
                self.progress_tracker.update_status("completed")
                return
            
            self.progress_tracker.update_file_count(
                total=len(files),
                to_process=len(files_to_process),
                skipped=len(files) - len(files_to_process)
            )
            
            self.logger.info(f"Processing {len(files_to_process)} files...")
            
            semaphore = asyncio.Semaphore(self.config.get_concurrency())
            
            # Process files in batches to allow graceful shutdown
            batch_size = 50
            for i in range(0, len(files_to_process), batch_size):
                if self.shutdown_requested:
                    self.logger.info("Shutdown requested, stopping ingestion...")
                    break
                
                batch = files_to_process[i:i + batch_size]
                self.logger.info(f"Processing batch {i//batch_size + 1}/{(len(files_to_process)-1)//batch_size + 1}")
                
                tasks = [
                    self.bounded_ingest(semaphore, md_path, language)
                    for md_path in batch
                ]
                
                results = await tqdm_asyncio.gather(*tasks)
                
                successful = sum(1 for r in results if r)
                failed = len(results) - successful
                
                self.logger.info(f"Batch completed: {successful} successful, {failed} failed")
            
            if self.shutdown_requested:
                self.progress_tracker.update_status("interrupted")
                self.logger.info("Ingestion interrupted by user")
            else:
                self.progress_tracker.update_status("completed")
                self.logger.info("Ingestion completed successfully")
                
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            self.progress_tracker.update_status("failed")
            raise
        finally:
            if self.client:
                await self.client.close()


async def main():
    """Entry point for the daemon."""
    import argparse
    
    parser = argparse.ArgumentParser(description="LightRag Ingestion Daemon")
    parser.add_argument("--force", action="store_true",
                       help="Force re-ingestion")
    parser.add_argument("--skip-check", action="store_true",
                       help="Skip document check")
    args = parser.parse_args()
    
    daemon = IngestionDaemon()
    await daemon.run(force=args.force, skip_check=args.skip_check)


if __name__ == "__main__":
    asyncio.run(main())