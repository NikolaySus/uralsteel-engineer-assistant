#!/usr/bin/env python3
"""
Delete all documents that are not processed.

Fetches statuses from GET /documents, collects doc_ids for every document whose
status is NOT "processed" (across any bucket), then calls DELETE
/documents/delete_document with payload:

{
  "doc_ids": ["..."],
  "delete_file": true,
  "delete_llm_cache": true
}

Env/config:
- BASE_URL default http://localhost:9621, override via BASE_URL env var.
- DRY_RUN env var: if set to "1"/"true"/"yes", only print what would be deleted.

Usage:
  python delete_unprocessed_documents.py

Requires: requests
"""

import os
import sys
import json
import requests
from typing import List, Dict, Any


BASE_URL = os.getenv("BASE_URL", "http://localhost:9621")
DOCS_URL = f"{BASE_URL}/documents"
DELETE_URL = f"{BASE_URL}/documents/delete_document"


def is_dry_run() -> bool:
    val = os.getenv("DRY_RUN", "").lower()
    return val in {"1", "true", "yes", "y"}


def fetch_statuses() -> Dict[str, Any]:
    headers = {"accept": "application/json"}
    resp = requests.get(DOCS_URL, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


def collect_unprocessed_doc_ids(statuses: Dict[str, Any]) -> List[str]:
    doc_ids: List[str] = []

    buckets = statuses.get("statuses", {})

    for bucket_name, items in buckets.items():
        # Skip processed bucket; we only want non-processed
        if bucket_name == "processed":
            continue
        if not isinstance(items, list):
            continue
        for item in items:
            doc_id = item.get("id") or item.get("doc_id")
            if doc_id:
                doc_ids.append(doc_id)
    return doc_ids


def delete_documents(doc_ids: List[str]) -> None:
    if not doc_ids:
        print("No unprocessed documents to delete.")
        return

    payload = {
        "doc_ids": doc_ids,
        "delete_file": True,
        "delete_llm_cache": True,
    }

    if is_dry_run():
        print("[DRY RUN] Would delete doc_ids (including files and llm cache):")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    headers = {"accept": "application/json", "Content-Type": "application/json"}
    resp = requests.delete(DELETE_URL, headers=headers, json=payload, timeout=30)

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"Delete request failed: {e}\nResponse: {resp.text}")
        raise

    try:
        print("Delete response:")
        print(json.dumps(resp.json(), ensure_ascii=False, indent=2))
    except ValueError:
        print(f"Delete response (non-JSON): {resp.text}")


def main():
    try:
        statuses = fetch_statuses()
    except Exception as e:
        print(f"Failed to fetch /documents: {e}")
        sys.exit(1)

    doc_ids = collect_unprocessed_doc_ids(statuses)
    print(f"Found {len(doc_ids)} unprocessed document(s).")

    if doc_ids:
        # Also show sample mapping of doc_id -> file_path when available
        sample = []
        for bucket_name, items in statuses.get("statuses", {}).items():
            if bucket_name == "processed" or not isinstance(items, list):
                continue
            for item in items:
                doc_id = item.get("id") or item.get("doc_id")
                file_path = item.get("file_path")
                if doc_id in doc_ids and file_path:
                    sample.append({"doc_id": doc_id, "file_path": file_path, "status": bucket_name})
                if len(sample) >= 5:
                    break
            if len(sample) >= 5:
                break

        if sample:
            print("Sample of documents to delete (up to 5):")
            print(json.dumps(sample, ensure_ascii=False, indent=2))

    try:
        delete_documents(doc_ids)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()