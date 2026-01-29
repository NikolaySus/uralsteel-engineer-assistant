#!/usr/bin/env python3
"""
LightRag Ingestion Daemon - Simplified and Optimized
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
            "status": "starting",
            "completed_at": None,
            "files": {}
        }
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
    
    def save(self):
        """Save progress to file."""
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save progress: {e}")
    
    def update(self, **kwargs):
        """Update progress fields."""
        self.progress.update(kwargs)
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


class LightRagManager:
    """LightRag API manager with proper methods."""
    
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.client = AsyncLightRagClient(base_url=base_url, api_key=api_key)
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.close()
    
    async def check_health(self) -> bool:
        """Check if server is healthy."""
        try:
            health = await self.client.get_health()
            return health.get("status") == "healthy"
        except Exception:
            return False
    
    async def get_indexed_documents(self) -> Dict[str, str]:
        """Get all indexed documents using paginated method."""
        indexed_docs = {}
        page_token = None
        
        try:
            while True:
                response = await self.client.get_documents_paginated(
                    page_token=page_token,
                    page_size=Config.get_batch_size()
                )
                
                # Parse documents
                documents = response.get("documents", [])
                for doc in documents:
                    metadata = doc.get("metadata", {})
                    if "source_path" in metadata:
                        doc_id = doc.get("id")
                        if doc_id:
                            indexed_docs[metadata["source_path"]] = doc_id
                
                # Check for next page
                page_token = response.get("next_page_token")
                if not page_token:
                    break
                    
        except Exception as e:
            # If paginated method fails, fall back to regular get_documents
            try:
                documents = await self.client.get_documents()
                if isinstance(documents, list):
                    for doc in documents:
                        metadata = doc.get("metadata", {})
                        if "source_path" in metadata:
                            doc_id = doc.get("id")
                            if doc_id:
                                indexed_docs[metadata["source_path"]] = doc_id
            except Exception:
                pass
        
        return indexed_docs
    
    async def delete_document(self, doc_id: str) -> bool:
        """Delete a document."""
        try:
            await self.client.delete_document(doc_id=doc_id)
            return True
        except Exception:
            return False
    
    async def insert_document(self, text: str, metadata: Dict[str, Any]) -> bool:
        """Insert a single document with metadata."""
        try:
            # Use insert_texts (plural) which accepts documents with metadata
            response = await self.client.insert_texts(
                texts=[{
                    "content": text,  # Note: the key is 'content' in the document object
                    "metadata": metadata  # Include your metadata dictionary here
                }]
            )
            # Check response - expecting a list of document IDs
            return bool(response and isinstance(response, list) and len(response) > 0)
        except Exception as e:
            self.logger.error(f"Batch insert failed: {e}")
            return False


class IngestionDaemon:
    """Main ingestion daemon."""
    
    def __init__(self):
        self.config = Config
        self.progress_tracker = ProgressTracker(self.config.get_progress_file())
        self.logger = self._setup_logging()
        self.shutdown_requested = False
    
    def _setup_logging(self):
        """Setup logging."""
        log_file = self.config.get_log_file()
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger = logging.getLogger('ingestion_daemon')
        logger.setLevel(self.config.get_log_level())
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
    
    def collect_markdown_files(self, root: str) -> List[Path]:
        """Collect all markdown files recursively."""
        try:
            return sorted(Path(root).rglob("*.md"))
        except Exception as e:
            self.logger.error(f"Failed to collect files: {e}")
            return []
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info("Shutdown requested, stopping gracefully...")
        self.shutdown_requested = True
    
    async def process_files(self, files: List[Path], language: str, 
                          lightrag: LightRagManager, concurrency: int) -> tuple[int, int]:
        """Process files with concurrency control."""
        semaphore = asyncio.Semaphore(concurrency)
        
        async def process_file(md_path: Path):
            async with semaphore:
                if self.shutdown_requested:
                    return None
                
                filepath = str(md_path)
                self.progress_tracker.start_file(filepath)
                
                try:
                    text = md_path.read_text(encoding="utf-8", errors="ignore")
                    metadata = {
                        "source_path": filepath,
                        "filename": md_path.name,
                        "folder": str(md_path.parent),
                        "filetype": "markdown",
                        "language": language
                    }
                    
                    success = await lightrag.insert_document(text, metadata)
                    
                    if success:
                        self.progress_tracker.finish_file(filepath, True)
                        return True
                    else:
                        error = "Insert failed"
                        self.progress_tracker.finish_file(filepath, False, error)
                        return False
                        
                except Exception as e:
                    error = f"Error: {str(e)[:100]}"
                    self.progress_tracker.finish_file(filepath, False, error)
                    return False
        
        # Create and execute tasks
        tasks = [process_file(f) for f in files]
        results = []
        
        # Process in chunks to show progress
        chunk_size = concurrency * 5
        for i in range(0, len(tasks), chunk_size):
            if self.shutdown_requested:
                break
            
            chunk_tasks = tasks[i:i + chunk_size]
            chunk_results = await tqdm_asyncio.gather(*chunk_tasks, 
                                                     desc=f"Processing files {i+1}-{min(i+chunk_size, len(tasks))}")
            results.extend(chunk_results)
            
            # Log progress
            processed = i + len(chunk_tasks)
            self.logger.info(f"Progress: {processed}/{len(tasks)} files")
        
        # Count results
        successful = sum(1 for r in results if r is True)
        failed = sum(1 for r in results if r is False)
        
        return successful, failed
    
    async def run(self, force: bool = False, skip_check: bool = False):
        """Main ingestion loop."""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        root_dir = self.config.get_root_dir()
        language = self.config.get_language()
        concurrency = self.config.get_concurrency()
        
        self.logger.info(f"Starting ingestion from {root_dir}")
        self.progress_tracker.update_status="running")
        
        # Collect files
        files = self.collect_markdown_files(root_dir)
        if not files:
            self.logger.error("No markdown files found")
            self.progress_tracker.update(status="failed")
            return
        
        self.logger.info(f"Found {len(files)} markdown files")
        self.progress_tracker.update(total_files=len(files))
        
        # Connect to LightRag
        async with LightRagManager(
            base_url=self.config.get_lightrag_url(),
            api_key=self.config.get_api_key()
        ) as lightrag:
            
            if not await lightrag.check_health():
                self.logger.error("Cannot connect to LightRag server")
                self.progress_tracker.update(status="failed")
                return
            
            self.logger.info("Connected to LightRag successfully")
            
            # Determine which files to process
            files_to_process = files
            skipped_count = 0
            deleted_count = 0
            
            if not skip_check:
                if force:
                    # Delete all existing documents
                    indexed_docs = await lightrag.get_indexed_documents()
                    if indexed_docs:
                        self.logger.info(f"Deleting {len(indexed_docs)} existing documents...")
                        
                        delete_tasks = [
                            lightrag.delete_document(doc_id)
                            for doc_id in indexed_docs.values()
                        ]
                        
                        # Delete in parallel but limit concurrency
                        semaphore = asyncio.Semaphore(10)
                        async def delete_with_limit(doc_id):
                            async with semaphore:
                                return await lightrag.delete_document(doc_id)
                        
                        delete_results = await asyncio.gather(*[
                            delete_with_limit(doc_id) for doc_id in indexed_docs.values()
                        ])
                        
                        deleted_count = sum(1 for r in delete_results if r)
                        self.logger.info(f"Deleted {deleted_count} documents")
                    
                    files_to_process = files
                else:
                    # Skip already indexed files
                    indexed_docs = await lightrag.get_indexed_documents()
                    files_to_process = [f for f in files if str(f) not in indexed_docs]
                    skipped_count = len(files) - len(files_to_process)
                    
                    if skipped_count > 0:
                        self.logger.info(f"Skipping {skipped_count} already indexed files")
            else:
                self.logger.info("Skipping document check")
            
            # Process files
            if not files_to_process:
                self.logger.info("All documents are already indexed")
                self.progress_tracker.update(
                    status="completed",
                    skipped=skipped_count,
                    deleted=deleted_count
                )
                return
            
            self.progress_tracker.update(
                to_process=len(files_to_process),
                skipped=skipped_count,
                deleted=deleted_count
            )
            
            self.logger.info(f"Processing {len(files_to_process)} files...")
            
            # Process files
            successful, failed = await self.process_files(
                files_to_process, language, lightrag, concurrency
            )
            
            # Update final status
            if self.shutdown_requested:
                self.progress_tracker.update(status="interrupted")
                self.logger.info("Ingestion interrupted")
            else:
                self.progress_tracker.update(status="completed")
                self.logger.info(f"Ingestion completed: {successful} successful, {failed} failed")


async def main():
    """Entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="LightRag Ingestion Daemon")
    parser.add_argument("--force", action="store_true", help="Force re-ingestion")
    parser.add_argument("--skip-check", action="store_true", help="Skip document check")
    parser.add_argument("--root-dir", type=str, help="Override root directory")
    parser.add_argument("--language", type=str, help="Override language")
    
    args = parser.parse_args()
    
    daemon = IngestionDaemon()
    await daemon.run(force=args.force, skip_check=args.skip_check)


if __name__ == "__main__":
    asyncio.run(main())