import os
import sys
import openai
from dotenv import load_dotenv
import numpy as np
import pickle

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'retrieval')))
from vector_index import process_all_markdown, upsert_to_file_storage
from vector_query import query_file_storage

load_dotenv()

VECTOR_DIR = "copilot/vector_storage"
EMBEDDINGS_FILE = os.path.join(VECTOR_DIR, "embeddings.npy")
METADATA_FILE = os.path.join(VECTOR_DIR, "metadata.pkl")


def test_index_and_query():
    # Index all summaries
    chunks = process_all_markdown('copilot/summaries')
    upsert_to_file_storage(chunks)
    print(f"Indexed {len(chunks)} chunks.")
    # Check files exist
    assert os.path.exists(EMBEDDINGS_FILE), "Embeddings file not found!"
    assert os.path.exists(METADATA_FILE), "Metadata file not found!"
    # Run a sample query
    query = "What are the top performing products?"
    results = query_file_storage(query, top_k=3)
    assert len(results) > 0, "No results returned from file-based search!"
    print("Top result:")
    print(f"Score: {results[0]['score']:.4f}")
    print(f"Metadata: {results[0]['metadata']}")
    print(f"Text: {results[0]['text'][:200]}")

if __name__ == "__main__":
    test_index_and_query() 