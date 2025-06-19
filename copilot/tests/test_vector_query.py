import os, sys
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'retrieval')))
from vector_query import query_file_storage
from vector_index import process_all_markdown, upsert_to_file_storage, EMBEDDINGS_FILE

load_dotenv()


def ensure_index():
    if not os.path.exists(EMBEDDINGS_FILE):
        chunks = process_all_markdown('copilot/summaries')
        upsert_to_file_storage(chunks)


def run_query_and_print(query: str, filter=None):
    print(f"\n[TEST] Query: '{query}'" + (f" with filter {filter}" if filter else ""))
    results = query_file_storage(query, top_k=3, metadata_filter=filter)
    print(f"  Results found: {len(results)}")
    if results:
        print("  Top result metadata:", results[0]['metadata'])
        print("  Top result text:", results[0]['text'][:200])
    return len(results)


def test_vector_search_real_openai():
    ensure_index()
    queries = [
        "analytics",
        "top performing products",
        "Google Ads performance",
    ]
    found_any = False
    for q in queries:
        if run_query_and_print(q) > 0:
            found_any = True

    assert found_any, "No results returned from vector search!"


if __name__ == "__main__":
    test_vector_search_real_openai()
    print("\nAll tests completed.") 