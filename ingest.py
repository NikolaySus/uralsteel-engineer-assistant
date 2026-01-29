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
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Any, Optional

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
            "status": "starting",  # starting, running, completed, failed, interrupted
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
        
        # Clear existing handlers
        logger.handlers.clear()
        
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
    
    async def get_indexed_documents(self) -> Dict[str, str]:
        """Query LightRag for already indexed documents and return mapping: source_path -> document_id."""
        indexed_docs = {}
        
        self.logger.info("Checking for already indexed documents...")
        
        try:
            # Use query instead of query_documents with empty query
            response = await self.client.query(
                query="",  # Empty query to get all documents
                top_k=self.config.get_batch_size(),
                space="default"
            )
            
            if "hits" in response:
                for hit in response["hits"]:
                    metadata = hit.get("metadata", {})
                    if "source_path" in metadata:
                        doc_id = hit.get("id")
                        if doc_id:
                            indexed_docs[metadata["source_path"]] = doc_id
            
        except Exception as e:
            self.logger.error(f"Could not query existing documents: {e}")
            return {}
        
        self.logger.info(f"Found {len(indexed_docs)} already indexed documents")
        return indexed_docs
    
    async def delete_document_by_id(self, doc_id: str) -> bool:
        """Delete a document by its ID."""
        try:
            await self.client.delete(doc_id=doc_id)
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete document {doc_id}: {e}")
            return False
    
    async def delete_documents_by_ids(self, doc_ids: List[str]) -> int:
        """Delete multiple documents by their IDs."""
        successful = 0
        for doc_id in doc_ids:
            if await self.delete_document_by_id(doc_id):
                successful += 1
        return successful
    
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
            # Use insert_documents (plural) with a list
            response = await self.client.insert_documents(
                documents=[{
                    "text": text,
                    "metadata": metadata
                }]
            )
            
            # Check if insertion was successful
            if response and "ids" in response and len(response["ids"]) > 0:
                self.progress_tracker.finish_file(filepath, True)
                return True
            else:
                error_msg = "No document ID returned"
                self.logger.error(f"{filepath}: {error_msg}")
                self.progress_tracker.finish_file(filepath, False, error_msg)
                return False
                
        except Exception as e:
            error_msg = f"Failed to ingest: {e}"
            self.logger.error(f"{filepath}: {error_msg}")
            self.progress_tracker.finish_file(filepath, False, error_msg)
            return False
    
    async def ingest_batch(self, batch: List[Path], language: str) -> tuple[int, int]:
        """Ingest a batch of files."""
        successful = 0
        failed = 0
        
        for md_path in batch:
            if self.shutdown_requested:
                break
                
            if await self.ingest_one(md_path, language):
                successful += 1
            else:
                failed += 1
        
        return successful, failed
    
    async def bounded_ingest(self, semaphore, md_path, language):
        """Ingest with concurrency control."""
        async with semaphore:
            return await self.ingest_one(md_path, language)
    
    def collect_markdown_files(self, root: str) -> List[Path]:
        """Collect all markdown files recursively."""
        try:
            files = sorted(Path(root).rglob("*.md"))
            return files
        except Exception as e:
            self.logger.error(f"Failed to collect files from {root}: {e}")
            return []
    
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
        
        try:
            self.client = AsyncLightRagClient(
                base_url=self.config.get_lightrag_url(),
                api_key=self.config.get_api_key()
            )
            
            # Test connection
            await self.client.health()
            self.logger.info("Connected to LightRag successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to LightRag: {e}")
            self.progress_tracker.update_status("failed")
            return
        
        try:
            files_to_process = files
            deleted_count = 0
            
            if not skip_check:
                if force:
                    # Force mode: delete all existing documents
                    indexed_docs = await self.get_indexed_documents()
                    
                    if indexed_docs:
                        self.logger.info(f"Deleting {len(indexed_docs)} existing documents...")
                        
                        # Delete in batches to avoid overwhelming the API
                        doc_ids = list(indexed_docs.values())
                        batch_size = 20
                        
                        for i in range(0, len(doc_ids), batch_size):
                            if self.shutdown_requested:
                                break
                                
                            batch = doc_ids[i:i + batch_size]
                            deleted_in_batch = await self.delete_documents_by_ids(batch)
                            deleted_count += deleted_in_batch
                            
                            if len(doc_ids) > batch_size:
                                self.logger.info(f"Deleted {i + deleted_in_batch}/{len(doc_ids)} documents...")
                        
                        self.logger.info(f"Deleted {deleted_count} documents")
                    
                    files_to_process = files
                else:
                    # Normal mode: skip already indexed files
                    indexed_docs = await self.get_indexed_documents()
                    
                    files_to_process = [
                        f for f in files 
                        if str(f) not in indexed_docs
                    ]
                    
                    skipped_count = len(files) - len(files_to_process)
                    if skipped_count > 0:
                        self.logger.info(f"Skipping {skipped_count} already indexed files")
            else:
                self.logger.info("Skipping document check")
                skipped_count = 0
            
            if not files_to_process:
                self.logger.info("All documents are already indexed")
                self.progress_tracker.update_status("completed")
                return
            
            self.progress_tracker.update_file_count(
                total=len(files),
                to_process=len(files_to_process),
                skipped=skipped_count
            )
            
            self.logger.info(f"Processing {len(files_to_process)} files...")
            
            # Process files
            semaphore = asyncio.Semaphore(self.config.get_concurrency())
            
            # Create tasks for all files
            tasks = [
                self.bounded_ingest(semaphore, md_path, language)
                for md_path in files_to_process
            ]
            
            # Process with progress bar
            results = []
            for i in range(0, len(tasks), self.config.get_concurrency()):
                if self.shutdown_requested:
                    break
                    
                batch_tasks = tasks[i:i + self.config.get_concurrency()]
                batch_results = await asyncio.gather(*batch_tasks)
                results.extend(batch_results)
                
                # Update progress
                processed_so_far = i + len(batch_tasks)
                self.logger.info(f"Progress: {processed_so_far}/{len(tasks)} files")
            
            successful = sum(1 for r in results if r)
            failed = len(results) - successful
            
            if self.shutdown_requested:
                self.progress_tracker.update_status("interrupted")
                self.logger.info("Ingestion interrupted by user")
            else:
                self.progress_tracker.update_status("completed")
                self.logger.info(f"Ingestion completed: {successful} successful, {failed} failed")
                
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
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
    parser.add_argument("--root-dir", type=str,
                       help="Override the MARKDOWN_ROOT_DIR from .env file")
    parser.add_argument("--language", type=str,
                       help="Override the LANGUAGE from .env file")
    args = parser.parse_args()
    
    daemon = IngestionDaemon()
    await daemon.run(force=args.force, skip_check=args.skip_check)


if __name__ == "__main__":
    asyncio.run(main())