#!/usr/bin/env python3
"""
LightRag Markdown Ingestion CLI (Parallel & Stable)

Combines parallel upload capability with stable error handling and reprocessing logic.

Requirements addressed:
- Process multiple documents in parallel with controlled concurrency.
- Properly wait for each document to reach a final state (processed/failed).
- If a document fails, automatically reprocess it using the API endpoint.
- Handle transient errors gracefully with retries.
- Provide comprehensive status tracking and clear error messages.

Usage:
    python lightrag_ingest_cli_parallel.py <root_dir> [--concurrency N] [--reprocess-on-fail]
"""

import argparse
import asyncio
import re
import sys
from pathlib import Path

import requests
from lightrag.api import AsyncLightRagClient


# --------------------------
# CONFIG
# --------------------------
LIGHTRAG_URL = "http://localhost:9622"
API_KEY = None
POLL_INTERVAL = 5  # seconds between status checks
MAX_STATUS_ATTEMPTS = 720  # 720 * 5s = 3600s (~1 hour) per processing cycle
DEFAULT_CONCURRENCY = 4


# --------------------------
# HELPERS
# --------------------------
def collect_markdown_files(root: str, path_regex: str | None = None):
    """Collect markdown files recursively; optionally filter by regex on the full path."""
    files = sorted(Path(root).rglob("*.md"))
    if path_regex:
        pattern = re.compile(path_regex)
        files = [p for p in files if pattern.search(str(p).replace('\\', '/'))]
    return files


def fetch_indexed_paths():
    """Fetch already indexed file paths from LightRag service."""
    url = f"{LIGHTRAG_URL}/documents"
    headers = {"accept": "application/json"}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        return set(
            chunk.get("file_path")
            for chunk in data.get("statuses", {}).get("processed", [])
            if chunk.get("file_path")
        )
    except Exception as e:
        print(f"⚠️  Warning: could not fetch indexed paths, proceeding without skip check. Error: {e}")
        return set()


def reprocess_failed_documents():
    """Trigger reprocessing of failed documents (no payload; service handles all failed)."""
    url = f"{LIGHTRAG_URL}/documents/reprocess_failed"
    headers = {"accept": "application/json", "content-type": "application/json"}
    try:
        response = requests.post(url, headers=headers, timeout=15)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"⚠️  Reprocess request failed: {e}")
        return False


async def wait_for_processing(client: AsyncLightRagClient, track_id: str, file_path: Path):
    """Poll track status until it reaches a final state or exhausts attempts."""
    attempts = 0
    while attempts < MAX_STATUS_ATTEMPTS:
        attempts += 1
        try:
            status = await client.get_track_status(track_id)
            if not status.documents:
                raise RuntimeError("No documents returned for track status")

            doc_status = status.documents[0].status
            if doc_status in {"processed", "failed"}:
                return doc_status

            await asyncio.sleep(POLL_INTERVAL)
        except Exception as e:
            # Treat transient errors (network/timeouts) as retryable until max attempts
            if attempts >= MAX_STATUS_ATTEMPTS:
                raise RuntimeError(f"Status polling failed for {file_path.name}: {e}") from e
            await asyncio.sleep(POLL_INTERVAL)

    raise RuntimeError(f"Status polling exceeded max attempts for {file_path.name}")


async def process_file(
    semaphore: asyncio.Semaphore,
    client: AsyncLightRagClient,
    path: Path,
    reprocess_on_fail: bool,
    index: int,
    total: int
):
    """Upload one file and wait until it is fully processed (with semaphore for concurrency control)."""
    async with semaphore:
        print(f"\n📄 [{index}/{total}] {path.name}")
        print(f"➡️  Uploading: {path}")

        text = path.read_text(encoding="utf-8", errors="ignore")
        file_source = str(path)

        # Upload
        try:
            response = await client.insert_text(text, file_source=file_source)
        except Exception as e:
            raise RuntimeError(f"Upload failed for {path.name}: {e}") from e

        # Wait for completion, optionally reprocess and retry until processed
        attempts = 0
        while True:
            try:
                final_status = await wait_for_processing(client, response.track_id, path)
            except Exception as e:
                # Treat polling failure as retryable when reprocess is requested
                if reprocess_on_fail:
                    attempts += 1
                    print(f"🔁 Polling error, reprocess attempt {attempts} for {path}: {e}")
                    if reprocess_failed_documents():
                        await asyncio.sleep(POLL_INTERVAL)
                        continue
                raise RuntimeError(f"Processing check failed for {path.name}: {e}") from e

            if final_status == "processed":
                print(f"✅ Done: {path}")
                return

            # final_status is failed
            if not reprocess_on_fail:
                raise RuntimeError(f"Processing ended with status '{final_status}' for {path.name}")

            attempts += 1
            print(f"🔁 Reprocess attempt {attempts} for {path}")
            if reprocess_failed_documents():
                await asyncio.sleep(POLL_INTERVAL)
            else:
                raise RuntimeError(
                    f"Failed to reprocess {path.name} after {attempts} attempt(s): "
                    f"reprocess API call failed"
                )


async def ingest_parallel(
    root_dir: str,
    concurrency: int = DEFAULT_CONCURRENCY,
    path_regex: str | None = None,
    reprocess_on_fail: bool = False
):
    """Ingest markdown files in parallel with controlled concurrency."""
    files = collect_markdown_files(root_dir, path_regex)
    indexed_paths = fetch_indexed_paths()

    if indexed_paths:
        original_total = len(files)
        files = [p for p in files if str(p) not in indexed_paths]
        skipped = original_total - len(files)
        if skipped:
            print(f"ℹ️  Skipping {skipped} already indexed file(s)")

    total = len(files)
    if total == 0:
        print("🎉 Nothing to ingest. All files are already processed or no .md files found.")
        return 0

    print(f"🚀 Starting parallel ingestion of {total} file(s) with {concurrency} concurrent uploads")

    client = AsyncLightRagClient(base_url=LIGHTRAG_URL, api_key=API_KEY)
    semaphore = asyncio.Semaphore(concurrency)

    try:
        tasks = [
            process_file(semaphore, client, path, reprocess_on_fail, idx, total)
            for idx, path in enumerate(files, start=1)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check for any errors
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            print(f"\n❌ {len(errors)} file(s) failed to process:")
            for error in errors:
                print(f"  • {error}")
            return 1

        print("\n🏁 All files processed successfully.")
        return 0
    except Exception as e:
        print(f"❌ Fatal error during ingestion: {e}")
        return 1
    finally:
        await client.close()


def main():
    parser = argparse.ArgumentParser(
        description="Parallel LightRag Markdown Ingestion with stable error handling"
    )
    parser.add_argument("root_dir", help="Root directory containing markdown files")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Number of concurrent uploads (default: {DEFAULT_CONCURRENCY})",
    )
    parser.add_argument(
        "--path-regex",
        dest="path_regex",
        help="Regex applied to full file path (use forward slashes). Example: '.*Маршрутные карты УТСП/\\d{3}мк от.*'",
    )
    parser.add_argument(
        "--reprocess-on-fail",
        action="store_true",
        help="If a file fails after upload, call POST /documents/reprocess_failed and retry until processed.",
    )
    args = parser.parse_args()

    # Validate concurrency
    if args.concurrency < 1:
        print(f"❌ Concurrency must be at least 1, got {args.concurrency}")
        return 1

    try:
        exit_code = asyncio.run(
            ingest_parallel(
                args.root_dir,
                concurrency=args.concurrency,
                path_regex=args.path_regex,
                reprocess_on_fail=args.reprocess_on_fail,
            )
        )
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user. Exiting.")
        exit_code = 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
