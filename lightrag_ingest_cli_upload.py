#!/usr/bin/env python3
import asyncio
import json
from pathlib import Path
from multiprocessing import Process
from typing import List

import typer
from lightrag.api import AsyncLightRagClient
from tqdm.asyncio import tqdm_asyncio

app = typer.Typer()

# --------------------------
# CONFIG
# --------------------------
LIGHTRAG_URL = "http://localhost:9621"
API_KEY = None
BATCH_SIZE = 5        # Number of documents per batch
CONCURRENCY = 5       # Number of parallel uploads
STATUS_FILE = Path("ingest_status.json")

# --------------------------
# HELPERS
# --------------------------
def collect_markdown_files(root: str) -> List[Path]:
    """Recursively collect all markdown files in root dir"""
    return sorted(Path(root).rglob("*.md"))

def prepare_batch(paths: List[Path]) -> List[dict]:
    """Prepare list of dicts for upload_document"""
    batch = []
    for p in paths:
        text = p.read_text(encoding="utf-8", errors="ignore")
        metadata = {
            "source_path": str(p),
            "filename": p.name,
            "folder": str(p.parent),
            "filetype": "markdown",
            "language": "ru"
        }
        batch.append({"text": text, "metadata": metadata})
    return batch

async def bounded_ingest(semaphore, client, batch, progress_file: Path):
    """Upload a batch and update status file"""
    async with semaphore:
        await client.upload_document(batch)
        try:
            progress = json.loads(progress_file.read_text(encoding="utf-8"))
        except FileNotFoundError:
            progress = {"processed": 0, "total": 0, "done": False}
        progress["processed"] += len(batch)
        progress_file.write_text(json.dumps(progress, ensure_ascii=False))

async def ingest_async(root_dir: str, progress_file: Path):
    """Main async ingestion logic"""
    files = collect_markdown_files(root_dir)
    progress = {"processed": 0, "total": len(files), "done": False}
    progress_file.write_text(json.dumps(progress, ensure_ascii=False))

    batches = [files[i:i+BATCH_SIZE] for i in range(0, len(files), BATCH_SIZE)]

    client = AsyncLightRagClient(base_url=LIGHTRAG_URL, api_key=API_KEY)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    try:
        tasks = [
            bounded_ingest(semaphore, client, prepare_batch(batch), progress_file)
            for batch in batches
        ]
        await tqdm_asyncio.gather(*tasks)
        progress["done"] = True
        progress_file.write_text(json.dumps(progress, ensure_ascii=False))
    finally:
        await client.close()

def run_ingestion_process(root_dir: str, progress_file: Path):
    """Run async ingestion in a subprocess"""
    asyncio.run(ingest_async(root_dir, progress_file))

# --------------------------
# CLI COMMANDS
# --------------------------
@app.command()
def start(root_dir: str):
    """
    Start ingestion of markdown files in ROOT_DIR
    """
    p = Process(target=run_ingestion_process, args=(root_dir, STATUS_FILE))
    p.start()
    print(f"🚀 Ingestion started as background process PID={p.pid}")
    print("Use `status` command to check progress")


@app.command()
def status():
    """
    Check ingestion status
    """
    if not STATUS_FILE.exists():
        print("❌ No ingestion process started or status file missing")
        raise typer.Exit(1)

    data = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    processed = data.get("processed", 0)
    total = data.get("total", 0)
    done = data.get("done", False)

    percent = (processed / total * 100) if total > 0 else 0
    print(f"📊 Progress: {processed}/{total} ({percent:.1f}%)")
    print(f"✅ Done: {done}")


if __name__ == "__main__":
    app()
