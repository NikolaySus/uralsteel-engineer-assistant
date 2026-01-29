# test_lightrag.py
import asyncio
from lightrag.api import AsyncLightRagClient


async def test_api():
    client = AsyncLightRagClient(
        base_url="http://localhost:9621",
        api_key=None
    )
    
    try:
        # Test health check
        health = await client.health()
        print(f"Health check: {health}")
        
        # Test query
        response = await client.query(
            query="test",
            top_k=5,
            space="default"
        )
        print(f"Query response: {response.keys() if isinstance(response, dict) else response}")
        
        # Test insert
        response = await client.insert_documents(
            documents=[{
                "text": "Test document",
                "metadata": {"test": "true"}
            }]
        )
        print(f"Insert response: {response}")
        
        # List available methods
        print("\nAvailable methods:")
        for method in dir(client):
            if not method.startswith('_'):
                print(f"  - {method}")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(test_api())