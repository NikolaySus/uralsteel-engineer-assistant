#!/usr/bin/env python3
import asyncio
import json
from pathlib import Path
from multiprocessing import Process

import typer
from lightrag.api import AsyncLightRagClient
from tqdm.asyncio import tqdm_asyncio

app = typer.Typer()

# --------------------------
# CONFIG
# --------------------------
LIGHTRAG_URL = "http://localhost:9621"
API_KEY = None
CONCURRENCY = 1  # Reduced concurrency to avoid rate limiting
STATUS_FILE = Path("ingest_status.json")
PROCESSING_STATUS_FILE = Path("processing_status.json")
POLL_INTERVAL = 5  # seconds between status checks

# LightRag API documentation reference
# Based on investigation, we need to use insert_text() instead of upload_document()
# for text content. The method signatures are:
#
# insert_text(text: str, file_source: str) -> InsertResponse
#
# The file_source parameter should contain the file path
# insert_text() returns instantly and starts background processing
# We need to track document processing status using the returned track_id
# and respect concurrency limits to avoid rate limiting
#
# API methods for tracking:
# - get_track_status(track_id) - track document processing status
# - get_pipeline_status() - check overall pipeline status
# - get_status_counts() - get document count by status
#
# upload_document() is designed for file uploads, not text content with metadata

# --------------------------
# HELPERS
# --------------------------
def collect_markdown_files(root: str):
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
    async with semaphore:
        text = path.read_text(encoding="utf-8", errors="ignore")

        # Use the file path as file_source as requested
        # This will allow the system to track the source of the document
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

        # update status
        progress = json.loads(status_file.read_text(encoding="utf-8"))
        progress["processed"] += 1
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
    files = collect_markdown_files(root_dir)

    # Initialize status files
    status_file.write_text(json.dumps({
        "processed": 0,
        "total": len(files),
        "done": False
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
        status_file.write_text(json.dumps(progress, ensure_ascii=False))
    finally:
        await client.close()


def run_ingestion(root_dir: str, status_file: Path):
    asyncio.run(ingest_async(root_dir, status_file))

# --------------------------
# CLI
# --------------------------
@app.command()
def start(root_dir: str):
    """
    Start background ingestion
    """
    p = Process(target=run_ingestion, args=(root_dir, STATUS_FILE))
    p.start()
    print(f"🚀 Ingestion started (PID={p.pid})")
    print("Use `status` to check progress")


@app.command()
def status():
    """
    Check ingestion status
    """
    if not STATUS_FILE.exists():
        print("❌ No status file found")
        raise typer.Exit(1)

    s = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    pct = (s["processed"] / s["total"] * 100) if s["total"] else 0
    print(f"📊 Overall Progress: {s['processed']} / {s['total']} ({pct:.1f}%)")
    print(f"✅ Ingestion Done: {s['done']}")

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

if __name__ == "__main__":
    app()
