import asyncio
from pathlib import Path
from lightrag.api import AsyncLightRagClient
from tqdm.asyncio import tqdm_asyncio

# --------------------------
# CONFIGURATION
# --------------------------
LIGHTRAG_URL = "http://localhost:9621"
API_KEY = None  # or "your-api-key"
ROOT_DIR = "/root/rag-source/hierarchy_trailing_20260126_182731"
CONCURRENCY = 5      # parallelism for insert_texts batches
BATCH_SIZE = 5       # number of documents per batch for insert_texts

# --------------------------
# HELPERS
# --------------------------
def collect_markdown_files(root: str):
    return sorted(Path(root).rglob("*.md"))

def prepare_batch(paths):
    """
    Prepare a list of dicts for insert_texts:
    [{'text': ..., 'metadata': {...}}, ...]
    """
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

async def bounded_ingest(semaphore, client, batch):
    async with semaphore:
        await ingest_batch(client, batch)

# --------------------------
# MAIN ASYNC FUNCTION
# --------------------------
async def main():
    files = collect_markdown_files(ROOT_DIR)
    print(f"📄 Found {len(files)} markdown files")

    if not files:
        return

    # batch files
    batches = [files[i:i+BATCH_SIZE] for i in range(0, len(files), BATCH_SIZE)]

    client = AsyncLightRagClient(base_url=LIGHTRAG_URL, api_key=API_KEY)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    try:
        tasks = [
            bounded_ingest(semaphore, client, prepare_batch(batch))
            for batch in batches
        ]
        # tqdm progress bar
        await tqdm_asyncio.gather(*tasks)

        print("✅ All documents ingested successfully!")

    finally:
        await client.close()

# --------------------------
# RUN SCRIPT
# --------------------------
if __name__ == "__main__":
    asyncio.run(main())
