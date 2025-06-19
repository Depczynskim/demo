import os
from dotenv import load_dotenv
import chromadb
from chromadb.config import Settings

load_dotenv()
COLLECTION_NAME = "copilot_summaries"
CHROMA_PERSIST_DIR = os.getenv("CHROMA_PERSIST_DIR", "chroma_db")

client = chromadb.Client(Settings(persist_directory=CHROMA_PERSIST_DIR))
collection = client.get_or_create_collection(COLLECTION_NAME)

# Chroma doesn't have a direct 'list all' API, but we can use get() with limit
try:
    # Try to get up to 20 documents
    results = collection.get(include=["metadatas", "documents"], limit=20)
    print(f"Found {len(results['ids'])} documents in collection '{COLLECTION_NAME}':\n")
    queries = [
        "analytics",
        "top performing products",
        "GA4 Product Analytics Summary",
        "events",
        "channels"
    ]
    for i in range(len(results['ids'])):
        print(f"[{i+1}] ID: {results['ids'][i]}")
        print(f"    Metadata: {results['metadatas'][i]}")
        print(f"    Text: {results['documents'][i][:200]}")
        # Brute-force substring search for each query
        doc_text = results['documents'][i].lower()
        for q in queries:
            if q.lower() in doc_text:
                print(f"    [MATCH] Query '{q}' found as substring in this document.")
        print()
except Exception as e:
    print(f"Error inspecting Chroma collection: {e}") 