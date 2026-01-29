#!/usr/bin/env python3
import asyncio
import json
from pathlib import Path
from multiprocessing import Process, Manager
from typing import List

import typer
from lightrag.api import AsyncLightRagClient
from tqdm.asyncio import tqdm_asyncio

app = typer.Typer()

# --------------------------
# CONFIGURATION
# --------------------------
LIGHTRAG_URL = "http://localhost:9621"
API_KEY = None
BATCH_SIZE = 5
CONCURRENCY = 5
STATUS_FILE = Path("ingest_status.json")

# --------------------------
# HELPERS
# --------------------------
def collect_markdown_files(root: str) -> List[Path]:
    return sorted(Path(root).rglob("*.md"))

def prepare_batch(paths):
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

async def ingest_batch(client, batch):
    await client.insert_texts(batch)

async def bounded_ingest(semaphore, client, batch, progress_dict, idx):
    async with semaphore:
        await ingest_batch(client, batch)
        # update progress
        progress_dict["processed"] += len(batch)
        STATUS_FILE.write_text(json.dumps(progress_dict, ensure_ascii=False))

async def ingest_async(root_dir: str, progress_dict):
    files = collect_markdown_files(root_dir)
    progress_dict["total"] = len(files)

    batches = [files[i:i+BATCH_SIZE] for i in range(0, len(files), BATCH_SIZE)]

    client = AsyncLightRagClient(base_url=LIGHTRAG_URL, api_key=API_KEY)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    try:
        tasks = [
            bounded_ingest(semaphore, client, prepare_batch(batch), progress_dict, idx)
            for idx, batch in enumerate(batches)
        ]
        await tqdm_asyncio.gather(*tasks)
        progress_dict["done"] = True
        STATUS_FILE.write_text(json.dumps(progress_dict, ensure_ascii=False))
    finally:
        await client.close()


def run_ingestion_process(root_dir: str, progress_dict):
    asyncio.run(ingest_async(root_dir, progress_dict))

# --------------------------
# CLI COMMANDS
# --------------------------
@app.command()
def start(root_dir: str):
    """
    Start ingestion of markdown files in ROOT_DIR
    """
    manager = Manager()
    progress_dict = manager.dict({"processed": 0, "total": 0, "done": False})

    p = Process(target=run_ingestion_process, args=(root_dir, progress_dict))
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

    if total > 0:
        percent = processed / total * 100
    else:
        percent = 0

    print(f"📊 Progress: {processed}/{total} ({percent:.1f}%)")
    print(f"✅ Done: {done}")


if __name__ == "__main__":
    app()
