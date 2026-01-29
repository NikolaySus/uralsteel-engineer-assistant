#!/usr/bin/env python3
"""
Comprehensive LightRag API Testing Tool
Tests all major API endpoints with detailed reporting and debugging
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
            print(f"   🔍 Data: {json.dumps(data, indent=2) if isinstance(data, dict) else data}")
        print()
    
    async def test_connection(self) -> bool:
        """Test basic connection to LightRag server"""
        try:
            # Try direct connection first
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
            
            # Validate health response structure
            if not isinstance(health, dict):
                self._log_test("Health Check", False, 
                             "Health response is not a dictionary", health)
                return False
            
            status = health.get("status")
            if status != "healthy":
                self._log_test("Health Check", False, 
                             f"Server status is '{status}', not 'healthy'", health)
                return False
            
            # Extract useful info for reporting
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
        """Test inserting a single document"""
        try:
            test_text = """This is a comprehensive test document for LightRag API testing.
            
It contains multiple paragraphs to test text processing capabilities.
The document includes various elements like:
1. Numbered lists
2. Technical terminology
3. Different sentence structures

This should be sufficient to test document ingestion and retrieval."""
            
            test_metadata = {
                "source_path": "/test/path/api_test.md",
                "filename": "api_test.md",
                "filetype": "markdown",
                "language": "en",
                "purpose": "api_testing",
                "test_timestamp": datetime.now().isoformat()
            }
            
            # Test insert_texts for single document
            response = await self.client.insert_texts(
                texts=[{
                    "content": test_text,
                    "metadata": test_metadata
                }]
            )
            
            if not response or not isinstance(response, list) or len(response) == 0:
                self._log_test("Document Insertion", False, 
                             "No document ID returned", response)
                return False
            
            self.test_doc_id = response[0]
            self._log_test("Document Insertion", True, 
                         f"Document inserted with ID: {self.test_doc_id[:12]}...",
                         {"id": self.test_doc_id, "metadata": test_metadata})
            return True
            
        except Exception as e:
            self._log_test("Document Insertion", False, f"Error: {str(e)}")
            return False
    
    async def test_batch_insertion(self) -> bool:
        """Test inserting multiple documents in batch"""
        try:
            documents = []
            for i in range(3):
                documents.append({
                    "content": f"This is batch test document #{i+1} with unique content for testing retrieval.",
                    "metadata": {
                        "source_path": f"/batch/test_{i+1}.md",
                        "filename": f"batch_test_{i+1}.md",
                        "batch_id": f"batch_test_{datetime.now().timestamp()}",
                        "order": i+1
                    }
                })
            
            response = await self.client.insert_texts(texts=documents)
            
            if not response or not isinstance(response, list) or len(response) != 3:
                self._log_test("Batch Insertion", False, 
                             f"Expected 3 IDs, got {len(response) if response else 0}", response)
                return False
            
            self.batch_doc_ids = response
            self._log_test("Batch Insertion", True, 
                         f"Inserted {len(response)} documents",
                         {"ids": [id[:12] + "..." for id in response]})
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
            # We'll consider it successful if no exception was raised
            self._log_test("Document Deletion", True, 
                         f"Deleted document ID: {self.test_doc_id[:12]}...")
            return True
            
        except Exception as e:
            self._log_test("Document Deletion", False, 
                         f"Failed to delete document: {str(e)}")
            return False
    
    async def test_api_methods(self) -> bool:
        """List and test available API methods"""
        try:
            # Get all public methods
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
            print(f"      Documents/Text ({len(doc_methods)}): {', '.join(sorted(doc_methods))}")
            print(f"      Query/Search ({len(query_methods)}): {', '.join(sorted(query_methods))}")
            if graph_methods:
                print(f"      Graph/Entities ({len(graph_methods)}): {', '.join(sorted(graph_methods))}")
            if other_methods:
                print(f"      Other ({len(other_methods)}): {', '.join(sorted(other_methods[:10]))}...")
            print()
            
            return True
            
        except Exception as e:
            self._log_test("API Methods Discovery", False, f"Error: {str(e)}")
            return False
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all tests and return comprehensive results"""
        print("\n" + "="*70)
        print("🚀 LIGHTRAG API COMPREHENSIVE TEST SUITE")
        print("="*70)
        print(f"📡 Testing server: {self.base_url}")
        print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70 + "\n")
        
        # Test sequence
        tests = [
            ("Server Connection", self.test_connection),
            ("Client Initialization", lambda: True),  # Already done in __aenter__
            ("Health Endpoint", self.test_health_endpoint),
            ("API Methods", self.test_api_methods),
            ("Document Insertion", self.test_document_insertion),
            ("Batch Insertion", self.test_batch_insertion),
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
        print("📊 TEST SUMMARY")
        print("="*70)
        print(f"✅ Passed: {passed}/{total} ({passed/total*100:.1f}%)")
        print(f"❌ Failed: {total-passed}/{total} ({(total-passed)/total*100:.1f}%)")
        print("="*70)
        
        # Show critical failures
        failures = [r for r in self.test_results if "❌" in r["status"]]
        if failures:
            print("\n⚠️  CRITICAL FAILURES:")
            for fail in failures:
                print(f"   • {fail['test']}: {fail['details']}")
        
        # Recommendations
        print("\n💡 RECOMMENDATIONS:")
        if passed == total:
            print("   • All tests passed! Your LightRag setup is ready for ingestion.")
            print("   • Run: `uv run run_ingestion.py start` to begin ingesting documents.")
        elif any("Health" in r["test"] for r in failures):
            print("   • Health check failed. Ensure LightRag server is running.")
            print("   • Check: `docker ps` and `docker logs <container_name>`")
        elif any("Connection" in r["test"] for r in failures):
            print("   • Cannot connect to server. Check URL and network.")
            print("   • Test with: `curl {self.base_url}/health`")
        elif any("Insertion" in r["test"] for r in failures):
            print("   • Document insertion failed. Check API method signatures.")
            print("   • Verify `insert_texts` method accepts 'content' and 'metadata' keys.")
        else:
            print("   • Some tests failed but core functionality may still work.")
            print("   • Check the individual test details above.")
        
        print("\n📋 Next steps:")
        print("   1. Fix any critical failures above")
        print("   2. Run `uv run test_lightrag.py` again to verify")
        print("   3. Start ingestion with `uv run run_ingestion.py start`")
        print("   4. Monitor with `uv run monitor.py status`")
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
    
    def save_report(self, filename: str = "lightrag_test_report.json"):
        """Save test results to a JSON file"""
        report = {
            "summary": {
                "passed": sum(1 for r in self.test_results if "✅" in r["status"]),
                "total": len(self.test_results),
                "timestamp": datetime.now().isoformat()
            },
            "results": self.test_results,
            "server": self.base_url
        }
        
        Path(filename).write_text(json.dumps(report, indent=2))
        print(f"\n📄 Test report saved to: {filename}")


async def main():
    """Main entry point with command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Comprehensive LightRag API Testing Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                         # Run all tests with default URL
  %(prog)s --url http://192.168.1.100:9621  # Test different server
  %(prog)s --save-report           # Save results to JSON file
  %(prog)s --quick                 # Run only critical tests
  %(prog)s --list-methods          # Just list available API methods
        """
    )
    
    parser.add_argument("--url", default="http://localhost:9621",
                       help="LightRag server URL (default: http://localhost:9621)")
    parser.add_argument("--api-key", help="API key if required")
    parser.add_argument("--save-report", action="store_true",
                       help="Save detailed test report to JSON file")
    parser.add_argument("--quick", action="store_true",
                       help="Run only critical tests (connection, health, insertion)")
    parser.add_argument("--list-methods", action="store_true",
                       help="Only list available API methods")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Show detailed debug information")
    
    args = parser.parse_args()
    
    async with LightRagTester(base_url=args.url, api_key=args.api_key) as tester:
        if args.list_methods:
            # Just list methods
            print(f"🔍 Listing API methods for {args.url}...\n")
            await tester.test_api_methods()
            return
        
        if args.quick:
            print(f"⚡ Running quick test suite for {args.url}...\n")
            # Run only critical tests
            await tester.test_connection()
            await tester.test_health_endpoint()
            await tester.test_document_insertion()
            await tester.test_document_deletion()
            
            # Quick summary
            passed = sum(1 for r in tester.test_results if "✅" in r["status"])
            total = len(tester.test_results)
            print(f"\n📊 Quick Test Results: {passed}/{total} passed")
            
            if args.save_report:
                tester.save_report("lightrag_quick_test.json")
        else:
            # Run full test suite
            results = await tester.run_all_tests()
            
            if args.save_report:
                tester.save_report()
    
    # Exit with appropriate code
    failed_tests = [r for r in tester.test_results if "❌" in r["status"]]
    if any("Connection" in t["test"] for t in failed_tests) or any("Health" in t["test"] for t in failed_tests):
        sys.exit(1)  # Critical failure
    elif failed_tests:
        sys.exit(2)  # Non-critical failures
    else:
        sys.exit(0)  # All tests passed


if __name__ == "__main__":
    asyncio.run(main())