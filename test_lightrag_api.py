# test_lightrag_api.py
import asyncio
from lightrag.api import AsyncLightRagClient


async def discover_api_methods():
    """Discover available methods in AsyncLightRagClient."""
    client = AsyncLightRagClient(
        base_url="http://localhost:9621",
        api_key=None
    )
    
    print("🔍 Discovering AsyncLightRagClient methods...")
    print("="*60)
    
    # List all methods that don't start with underscore
    methods = [method for method in dir(client) if not method.startswith('_')]
    
    print(f"Found {len(methods)} public methods:")
    for i, method in enumerate(sorted(methods), 1):
        print(f"{i:3}. {method}")
    
    print("\n" + "="*60)
    print("Testing some common operations...")
    
    try:
        # Try to get the actual method signature
        print("\n1. Testing client.query...")
        try:
            query_method = getattr(client, 'query')
            print(f"   query method exists: {query_method}")
        except AttributeError:
            print("   query method NOT found")
        
        print("\n2. Testing client.insert...")
        try:
            insert_method = getattr(client, 'insert')
            print(f"   insert method exists: {insert_method}")
        except AttributeError:
            print("   insert method NOT found")
        
        print("\n3. Testing client.insert_documents...")
        try:
            insert_docs_method = getattr(client, 'insert_documents')
            print(f"   insert_documents method exists: {insert_docs_method}")
        except AttributeError:
            print("   insert_documents method NOT found")
        
        print("\n4. Testing client.delete...")
        try:
            delete_method = getattr(client, 'delete')
            print(f"   delete method exists: {delete_method}")
        except AttributeError:
            print("   delete method NOT found")
        
        print("\n5. Testing client.delete_document...")
        try:
            delete_doc_method = getattr(client, 'delete_document')
            print(f"   delete_document method exists: {delete_doc_method}")
        except AttributeError:
            print("   delete_document method NOT found")
        
        print("\n6. Testing client.health...")
        try:
            health_method = getattr(client, 'health')
            print(f"   health method exists: {health_method}")
        except AttributeError:
            print("   health method NOT found")
        
        print("\n7. Testing client.ping...")
        try:
            ping_method = getattr(client, 'ping')
            print(f"   ping method exists: {ping_method}")
        except AttributeError:
            print("   ping method NOT found")
        
        print("\n8. Testing client.search...")
        try:
            search_method = getattr(client, 'search')
            print(f"   search method exists: {search_method}")
        except AttributeError:
            print("   search method NOT found")
        
        # Try to make a simple request
        print("\n" + "="*60)
        print("Making test request to server...")
        
        # Try a simple GET request to check if server is running
        import aiohttp
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get("http://localhost:9621/health") as resp:
                    print(f"GET /health response: {resp.status}")
                    if resp.status == 200:
                        text = await resp.text()
                        print(f"Response: {text[:200]}")
            except Exception as e:
                print(f"Error connecting to server: {e}")
            
            # Try common LightRag endpoints
            endpoints = ["/docs", "/v1", "/api", "/"]
            for endpoint in endpoints:
                try:
                    async with session.get(f"http://localhost:9621{endpoint}") as resp:
                        print(f"GET {endpoint}: {resp.status}")
                except:
                    pass
    
    except Exception as e:
        print(f"Error during testing: {e}")
    
    finally:
        await client.close()
        print("\n✅ Discovery completed")


if __name__ == "__main__":
    asyncio.run(discover_api_methods())