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
CONCURRENCY = 5
STATUS_FILE = Path("ingest_status.json")

# --------------------------
# HELPERS
# --------------------------
def collect_markdown_files(root: str):
    return sorted(Path(root).rglob("*.md"))

async def upload_one(semaphore, client, path: Path, status_file: Path):
    async with semaphore:
        text = path.read_text(encoding="utf-8", errors="ignore")
        metadata = {
            "source_path": str(path),
            "folder": str(path.parent),
            "filetype": "markdown",
            "language": "ru"
        }

        await client.upload_document(
            text,
            file_name=path.name,
            metadata=metadata
        )

        # update status
        progress = json.loads(status_file.read_text(encoding="utf-8"))
        progress["processed"] += 1
        status_file.write_text(json.dumps(progress, ensure_ascii=False))


async def ingest_async(root_dir: str, status_file: Path):
    files = collect_markdown_files(root_dir)

    status_file.write_text(json.dumps({
        "processed": 0,
        "total": len(files),
        "done": False
    }, ensure_ascii=False))

    client = AsyncLightRagClient(base_url=LIGHTRAG_URL, api_key=API_KEY)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    try:
        tasks = [
            upload_one(semaphore, client, path, status_file)
            for path in files
        ]
        await tqdm_asyncio.gather(*tasks)

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
    print(f"📊 {s['processed']} / {s['total']} ({pct:.1f}%)")
    print(f"✅ Done: {s['done']}")

if __name__ == "__main__":
    app()
