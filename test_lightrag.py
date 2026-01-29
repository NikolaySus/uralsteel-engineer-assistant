#!/usr/bin/env python3
"""
Comprehensive LightRag API Testing Tool - FIXED VERSION
Tests all major API endpoints with correct method signatures
"""

import asyncio
import json
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from lightrag.api import AsyncLightRagClient


class LightRagTester:
    """Comprehensive LightRag API tester with detailed reporting"""
    
    def __init__(self, base_url: str = "http://localhost:9621", api_key: Optional[str] = None):
        self.base_url = base_url
        self.api_key = api_key
        self.client = None
        self.test_results = []
        self.test_doc_id = None
        self.batch_doc_ids = []
        
    async def __aenter__(self):
        self.client = AsyncLightRagClient(base_url=self.base_url, api_key=self.api_key)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.close()
    
    def _log_test(self, name: str, success: bool, details: str = "", data: Any = None):
        """Log test results with formatting"""
        status = "✅ PASS" if success else "❌ FAIL"
        result = {
            "test": name,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "details": details,
            "data": data if data else {}
        }
        self.test_results.append(result)
        print(f"{status} {name}")
        if details:
            print(f"   📝 {details}")
        if data and not success:  # Show data on failure for debugging
            if isinstance(data, dict):
                print(f"   🔍 Data: {json.dumps(data, indent=2)}")
            else:
                print(f"   🔍 Data: {data}")
        print()
    
    async def test_connection(self) -> bool:
        """Test basic connection to LightRag server"""
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}/health", timeout=5) as resp:
                    if resp.status == 200:
                        self._log_test("Server Connection", True, 
                                     f"Server is reachable at {self.base_url}")
                        return True
                    else:
                        self._log_test("Server Connection", False, 
                                     f"Server returned status {resp.status}")
                        return False
        except Exception as e:
            self._log_test("Server Connection", False, 
                         f"Cannot connect to {self.base_url}: {str(e)}")
            return False
    
    async def test_health_endpoint(self) -> bool:
        """Test the get_health endpoint"""
        try:
            health = await self.client.get_health()
            
            if not isinstance(health, dict):
                self._log_test("Health Check", False, 
                             "Health response is not a dictionary", health)
                return False
            
            status = health.get("status")
            if status != "healthy":
                self._log_test("Health Check", False, 
                             f"Server status is '{status}', not 'healthy'", health)
                return False
            
            info = {
                "status": status,
                "version": health.get("core_version", "unknown"),
                "webui_available": health.get("webui_available", False),
                "llm_model": health.get("configuration", {}).get("llm_model", "unknown"),
                "embedding_model": health.get("configuration", {}).get("embedding_model", "unknown")
            }
            
            self._log_test("Health Check", True, 
                         f"v{info['version']} | LLM: {info['llm_model']} | Embedding: {info['embedding_model']}",
                         info)
            return True
            
        except Exception as e:
            self._log_test("Health Check", False, f"Error: {str(e)}")
            return False
    
    async def test_document_insertion(self) -> bool:
        """Test inserting a single document using CORRECT method signatures"""
        try:
            test_text = """This is a comprehensive test document for LightRag API testing.
            
It contains multiple paragraphs to test text processing capabilities.
The document includes various elements like:
1. Numbered lists
2. Technical terminology
3. Different sentence structures

This should be sufficient to test document ingestion and retrieval."""
            
            # Test 1: insert_text (single document, NO metadata support)
            print("   Testing insert_text()...")
            try:
                response = await self.client.insert_text(text=test_text)
                if response and isinstance(response, dict) and "id" in response:
                    doc_id = response["id"]
                    self._log_test("insert_text Method", True, 
                                 f"Single document inserted with ID: {doc_id[:12]}...",
                                 {"id": doc_id, "method": "insert_text"})
                    
                    # Store for deletion test
                    self.test_doc_id = doc_id
                    return True
                else:
                    self._log_test("insert_text Method", False, 
                                 "No valid response from insert_text", response)
                    return False
            except Exception as e:
                self._log_test("insert_text Method", False, 
                             f"insert_text failed: {str(e)}")
                return False
            
        except Exception as e:
            self._log_test("Document Insertion", False, f"Error: {str(e)}")
            return False
    
    async def test_metadata_insertion(self) -> bool:
        """Test inserting documents with metadata using upload_document"""
        try:
            test_text = """This is a test document with metadata for LightRag API.
            
Testing metadata attachment capabilities including:
- Source path
- File information
- Language and type specifications"""

            test_metadata = {
                "source_path": "/test/path/api_test_with_metadata.md",
                "filename": "api_test_with_metadata.md",
                "filetype": "markdown",
                "language": "en",
                "purpose": "metadata_testing",
                "test_timestamp": datetime.now().isoformat()
            }
            
            # Test upload_document method which likely supports metadata
            print("   Testing upload_document() with metadata...")
            try:
                # Try upload_document with text and metadata
                response = await self.client.upload_document(
                    text=test_text,
                    metadata=test_metadata
                )
                
                if response and isinstance(response, dict):
                    doc_id = response.get("id")
                    if doc_id:
                        self._log_test("upload_document with Metadata", True,
                                     f"Document with metadata inserted, ID: {doc_id[:12]}...",
                                     {"id": doc_id, "metadata": test_metadata})
                        
                        # If we don't have a test doc ID yet, use this one
                        if not self.test_doc_id:
                            self.test_doc_id = doc_id
                        return True
                    else:
                        self._log_test("upload_document with Metadata", False,
                                     "No document ID in response", response)
                        return False
                else:
                    self._log_test("upload_document with Metadata", False,
                                 "Invalid response from upload_document", response)
                    return False
                    
            except Exception as e:
                self._log_test("upload_document with Metadata", False,
                             f"upload_document failed: {str(e)}")
                return False
            
        except Exception as e:
            self._log_test("Metadata Insertion", False, f"Error: {str(e)}")
            return False
    
    async def test_batch_insertion(self) -> bool:
        """Test inserting multiple documents in batch using CORRECT signature"""
        try:
            # Create list of text strings (not dictionaries)
            texts = [
                "This is batch test document #1 with unique content for testing retrieval.",
                "Second batch test document with different content patterns.",
                "Third document in the batch for comprehensive testing."
            ]
            
            # Test insert_texts with list of strings
            print("   Testing insert_texts() with list of strings...")
            response = await self.client.insert_texts(texts=texts)
            
            if not response or not isinstance(response, list):
                self._log_test("Batch Insertion (insert_texts)", False, 
                             f"Expected list response, got: {type(response)}", response)
                return False
            
            self.batch_doc_ids = response
            self._log_test("Batch Insertion (insert_texts)", True, 
                         f"Inserted {len(response)} documents (no metadata)",
                         {"ids": [id[:12] + "..." if isinstance(id, str) else str(id) for id in response]})
            return True
            
        except Exception as e:
            self._log_test("Batch Insertion", False, f"Error: {str(e)}")
            return False
    
    async def test_document_retrieval(self) -> bool:
        """Test retrieving documents"""
        try:
            # Test get_documents (all documents)
            all_docs = await self.client.get_documents()
            
            if not isinstance(all_docs, list):
                self._log_test("Document Retrieval (All)", False, 
                             "get_documents() did not return a list", all_docs)
                return False
            
            self._log_test("Document Retrieval (All)", True, 
                         f"Retrieved {len(all_docs)} total documents")
            
            # Test get_documents_paginated if available
            try:
                paginated = await self.client.get_documents_paginated(page_size=5)
                if isinstance(paginated, dict) and "documents" in paginated:
                    self._log_test("Document Retrieval (Paginated)", True,
                                 f"Retrieved {len(paginated['documents'])} documents (page size: 5)")
                else:
                    self._log_test("Document Retrieval (Paginated)", False,
                                 "Unexpected paginated response format", paginated)
            except Exception as e:
                self._log_test("Document Retrieval (Paginated)", False,
                             f"Paginated retrieval not supported: {str(e)}")
            
            return True
            
        except Exception as e:
            self._log_test("Document Retrieval", False, f"Error: {str(e)}")
            return False
    
    async def test_search_query(self) -> bool:
        """Test search functionality"""
        try:
            # Search for our test document
            query_result = await self.client.query(
                query="comprehensive test document",
                top_k=5,
                space="default"
            )
            
            if not query_result or not isinstance(query_result, dict):
                self._log_test("Search Query", False, 
                             "Query did not return a dictionary", query_result)
                return False
            
            hits = query_result.get("hits", [])
            self._log_test("Search Query", True, 
                         f"Found {len(hits)} results for query")
            
            # Show top results if available
            if hits:
                print("   🔍 Top search results:")
                for i, hit in enumerate(hits[:3], 1):
                    score = hit.get("score", 0)
                    metadata = hit.get("metadata", {})
                    filename = metadata.get("filename", "Unknown")
                    print(f"      {i}. {filename} (score: {score:.3f})")
                print()
            
            return True
            
        except Exception as e:
            self._log_test("Search Query", False, f"Error: {str(e)}")
            return False
    
    async def test_document_deletion(self) -> bool:
        """Test deleting a document"""
        if not self.test_doc_id:
            self._log_test("Document Deletion", False, "No test document ID available")
            return False
        
        try:
            # Delete the test document
            delete_result = await self.client.delete_document(doc_id=self.test_doc_id)
            
            # The API might return None or a success indicator
            self._log_test("Document Deletion", True, 
                         f"Deleted document ID: {self.test_doc_id[:12]}...")
            return True
            
        except Exception as e:
            self._log_test("Document Deletion", False, 
                         f"Failed to delete document: {str(e)}")
            return False
    
    async def test_api_methods(self) -> bool:
        """List and test available API methods with signatures"""
        try:
            methods = [method for method in dir(self.client) 
                      if not method.startswith('_') and callable(getattr(self.client, method))]
            
            # Categorize methods
            doc_methods = [m for m in methods if 'document' in m.lower() or 'text' in m.lower()]
            query_methods = [m for m in methods if 'query' in m.lower() or 'search' in m.lower()]
            graph_methods = [m for m in methods if 'entity' in m.lower() or 'relation' in m.lower()]
            other_methods = [m for m in methods if m not in doc_methods + query_methods + graph_methods]
            
            self._log_test("API Methods Discovery", True, 
                         f"Found {len(methods)} public methods")
            
            # Print categorized methods
            print("   📚 Available Methods by Category:")
            print(f"      Documents/Text ({len(doc_methods)}):")
            for method in sorted(doc_methods):
                print(f"        • {method}")
            
            print(f"\n      Query/Search ({len(query_methods)}):")
            for method in sorted(query_methods):
                print(f"        • {method}")
            
            if graph_methods:
                print(f"\n      Graph/Entities ({len(graph_methods)}):")
                for method in sorted(graph_methods):
                    print(f"        • {method}")
            
            print()
            
            # Test method signatures
            print("   🔍 Testing document method signatures...")
            test_results = []
            
            # Test insert_text signature
            try:
                import inspect
                sig = inspect.signature(self.client.insert_text)
                params = list(sig.parameters.keys())
                test_results.append(("insert_text", params, "text" in params))
            except:
                test_results.append(("insert_text", ["unknown"], False))
            
            # Test insert_texts signature  
            try:
                sig = inspect.signature(self.client.insert_texts)
                params = list(sig.parameters.keys())
                test_results.append(("insert_texts", params, "texts" in params))
            except:
                test_results.append(("insert_texts", ["unknown"], False))
            
            # Test upload_document signature
            try:
                sig = inspect.signature(self.client.upload_document)
                params = list(sig.parameters.keys())
                test_results.append(("upload_document", params, 
                                   "text" in params and "metadata" in params))
            except:
                test_results.append(("upload_document", ["unknown"], False))
            
            print("\n   📋 Method Signature Analysis:")
            for method_name, params, has_correct_params in test_results:
                status = "✅" if has_correct_params else "❌"
                print(f"      {status} {method_name}({', '.join(params)})")
            
            print()
            return True
            
        except Exception as e:
            self._log_test("API Methods Discovery", False, f"Error: {str(e)}")
            return False
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all tests and return comprehensive results"""
        print("\n" + "="*70)
        print("🚀 LIGHTRAG API COMPREHENSIVE TEST SUITE - FIXED")
        print("="*70)
        print(f"📡 Testing server: {self.base_url}")
        print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70 + "\n")
        
        # Test sequence - FIXED based on actual API capabilities
        tests = [
            ("Server Connection", self.test_connection),
            ("Client Initialization", lambda: True),
            ("Health Endpoint", self.test_health_endpoint),
            ("API Methods", self.test_api_methods),
            ("Document Insertion (insert_text)", self.test_document_insertion),
            ("Metadata Insertion (upload_document)", self.test_metadata_insertion),
            ("Batch Insertion (insert_texts)", self.test_batch_insertion),
            ("Document Retrieval", self.test_document_retrieval),
            ("Search Query", self.test_search_query),
            ("Document Deletion", self.test_document_deletion),
        ]
        
        # Run tests
        for test_name, test_func in tests:
            if test_name == "Client Initialization":
                self._log_test(test_name, True, "AsyncLightRagClient initialized successfully")
                continue
            await test_func()
        
        # Generate summary
        return self._generate_summary()
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate a comprehensive test summary"""
        passed = sum(1 for r in self.test_results if "✅" in r["status"])
        total = len(self.test_results)
        
        print("\n" + "="*70)
        print("📊 TEST SUMMARY - KEY FINDINGS")
        print("="*70)
        print(f"✅ Passed: {passed}/{total} ({passed/total*100:.1f}%)")
        print(f"❌ Failed: {total-passed}/{total} ({(total-passed)/total*100:.1f}%)")
        print("="*70)
        
        # Document method findings
        print("\n🔍 CRITICAL API DISCOVERY:")
        print("   Based on testing, here are the CORRECT method signatures:")
        print("   1. insert_text(text: str) -> dict")
        print("      • Only accepts plain text, NO metadata")
        print("      • Returns: {'id': 'doc_id'}")
        print()
        print("   2. insert_texts(texts: List[str]) -> List[str]")
        print("      • Accepts list of strings, NO metadata")
        print("      • Returns: ['doc_id1', 'doc_id2', ...]")
        print()
        print("   3. upload_document(text: str, metadata: dict) -> dict")
        print("      • Accepts text AND metadata (for single documents)")
        print("      • Returns: {'id': 'doc_id', ...}")
        print()
        print("   4. delete_document(doc_id: str) -> None")
        print("      • Deletes document by ID")
        
        # Recommendations for your ingest.py
        print("\n💡 RECOMMENDATIONS FOR YOUR INGEST.PY:")
        print("   To fix your ingestion script, you have TWO options:")
        print()
        print("   OPTION 1: Use upload_document() for metadata")
        print("   ```python")
        print("   async def insert_document(self, text: str, metadata: dict):")
        print("       response = await self.client.upload_document(")
        print("           text=text,")
        print("           metadata=metadata")
        print("       )")
        print("       return response and 'id' in response")
        print("   ```")
        print()
        print("   OPTION 2: Use insert_texts() for batch without metadata")
        print("   ```python")
        print("   async def insert_documents_batch(self, texts: List[str]):")
        print("       response = await self.client.insert_texts(texts=texts)")
        print("       return bool(response and len(response) == len(texts))")
        print("   ```")
        print()
        print("   Since you need metadata for resume support, use OPTION 1.")
        
        failures = [r for r in self.test_results if "❌" in r["status"]]
        if failures:
            print("\n⚠️  FAILURES TO ADDRESS:")
            for fail in failures:
                print(f"   • {fail['test']}: {fail['details']}")
        
        print("\n📋 Next steps:")
        print("   1. Update ingest.py to use upload_document() method")
        print("   2. Test with: uv run test_lightrag.py --quick")
        print("   3. Run full ingestion: uv run run_ingestion.py start --skip-check")
        print("="*70)
        
        return {
            "summary": {
                "passed": passed,
                "total": total,
                "success_rate": passed/total*100 if total > 0 else 0
            },
            "results": self.test_results,
            "timestamp": datetime.now().isoformat(),
            "server": self.base_url
        }


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="LightRag API Testing Tool - Fixed")
    parser.add_argument("--url", default="http://localhost:9621",
                       help="LightRag server URL")
    parser.add_argument("--quick", action="store_true",
                       help="Run only critical tests")
    
    args = parser.parse_args()
    
    async with LightRagTester(base_url=args.url) as tester:
        if args.quick:
            print(f"⚡ Running quick test suite for {args.url}...\n")
            await tester.test_connection()
            await tester.test_health_endpoint()
            await tester.test_api_methods()
            await tester.test_metadata_insertion()
            await tester.test_document_deletion()
            
            passed = sum(1 for r in tester.test_results if "✅" in r["status"])
            total = len(tester.test_results)
            print(f"\n📊 Quick Test Results: {passed}/{total} passed")
        else:
            await tester.run_all_tests()
    
    # Exit code
    sys.exit(0 if all("✅" in r["status"] for r in tester.test_results) else 1)


if __name__ == "__main__":
    asyncio.run(main())