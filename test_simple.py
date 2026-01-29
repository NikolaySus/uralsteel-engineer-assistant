# test_simple.py
import asyncio
import aiohttp
import json


async def test_lightrag_api():
    """Test LightRag API directly."""
    base_url = "http://localhost:9621"
    
    print(f"Testing LightRag API at {base_url}")
    print("="*60)
    
    async with aiohttp.ClientSession() as session:
        # Test 1: Check if server is running
        print("1. Testing server connection...")
        try:
            async with session.get(f"{base_url}/") as resp:
                print(f"   GET / : {resp.status}")
                if resp.status == 200:
                    text = await resp.text()
                    print(f"   Response: {text[:100]}...")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Test 2: Check health endpoint
        print("\n2. Testing health endpoint...")
        try:
            async with session.get(f"{base_url}/health") as resp:
                print(f"   GET /health : {resp.status}")
                if resp.status == 200:
                    text = await resp.text()
                    print(f"   Response: {text}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Test 3: Check API endpoints
        print("\n3. Discovering API endpoints...")
        endpoints = [
            "/docs", "/v1", "/api", "/api/v1",
            "/openapi.json", "/swagger.json",
            "/documents", "/search"
        ]
        
        for endpoint in endpoints:
            try:
                async with session.get(f"{base_url}{endpoint}") as resp:
                    if resp.status < 400:
                        print(f"   GET {endpoint} : {resp.status}")
            except:
                pass
        
        # Test 4: Try to insert a document
        print("\n4. Testing document insertion...")
        test_doc = {
            "text": "This is a test document for LightRag.",
            "metadata": {
                "source_path": "/test/path/test.md",
                "filename": "test.md",
                "filetype": "markdown",
                "language": "en",
                "test": "true"
            }
        }
        
        # Try different endpoints
        insert_endpoints = ["/documents", "/api/documents", "/v1/documents"]
        for endpoint in insert_endpoints:
            try:
                async with session.post(
                    f"{base_url}{endpoint}",
                    json=test_doc,
                    headers={"Content-Type": "application/json"}
                ) as resp:
                    if resp.status < 400:
                        print(f"   POST {endpoint} : {resp.status}")
                        try:
                            response_data = await resp.json()
                            print(f"   Response: {json.dumps(response_data, indent=2)}")
                            break
                        except:
                            text = await resp.text()
                            print(f"   Response: {text[:200]}...")
            except Exception as e:
                print(f"   Error posting to {endpoint}: {e}")
        
        # Test 5: Try to search
        print("\n5. Testing search...")
        search_endpoints = ["/search", "/api/search", "/v1/search"]
        search_query = {"query": "test", "top_k": 5}
        
        for endpoint in search_endpoints:
            try:
                async with session.post(
                    f"{base_url}{endpoint}",
                    json=search_query,
                    headers={"Content-Type": "application/json"}
                ) as resp:
                    if resp.status < 400:
                        print(f"   POST {endpoint} : {resp.status}")
                        try:
                            response_data = await resp.json()
                            print(f"   Found {len(response_data.get('hits', []))} results")
                            break
                        except:
                            text = await resp.text()
                            print(f"   Response: {text[:200]}...")
            except Exception as e:
                print(f"   Error searching at {endpoint}: {e}")
    
    print("\n" + "="*60)
    print("✅ API discovery completed")


if __name__ == "__main__":
    asyncio.run(test_lightrag_api())