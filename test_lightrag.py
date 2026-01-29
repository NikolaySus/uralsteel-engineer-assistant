#!/usr/bin/env python3
"""
Test LightRag API functionality.
"""
import asyncio
import json
from lightrag.api import AsyncLightRagClient


async def test_api():
    """Test LightRag API."""
    client = AsyncLightRagClient(
        base_url="http://localhost:9621",
        api_key=None
    )
    
    print("🔍 Testing LightRag API...")
    print("="*60)
    
    try:
        # Test 1: Health check
        print("\n1. Testing health...")
        health = await client.get_health()
        print(f"   Status: {health.get('status', 'unknown')}")
        print(f"   Version: {health.get('core_version', 'unknown')}")
        
        # Test 2: Insert document
        print("\n2. Testing document insertion...")
        response = await client.insert_text(
            text="This is a test document for LightRag API.",
            metadata={
                "source_path": "/test/path/test.md",
                "filename": "test.md",
                "filetype": "markdown",
                "language": "en",
                "test": "true"
            }
        )
        print(f"   Response: {json.dumps(response, indent=2)}")
        
        # Test 3: Get documents
        print("\n3. Testing get_documents...")
        try:
            # Try paginated first
            docs = await client.get_documents_paginated(page_size=10)
            print(f"   Paginated: {len(docs.get('documents', []))} documents")
        except:
            # Fall back to regular
            docs = await client.get_documents()
            count = len(docs) if isinstance(docs, list) else "unknown"
            print(f"   Regular: {count} documents")
        
        # Test 4: Query
        print("\n4. Testing query...")
        query_result = await client.query(
            query="test",
            top_k=5,
            space="default"
        )
        if query_result and "hits" in query_result:
            print(f"   Found {len(query_result['hits'])} results")
        
        # Test 5: Available methods
        print("\n5. Available methods:")
        methods = [m for m in dir(client) if not m.startswith('_')]
        print(f"   Total: {len(methods)} methods")
        print(f"   Sample: {', '.join(sorted(methods)[:10])}...")
        
        print("\n✅ All tests passed!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(test_api())