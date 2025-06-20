import os
import openai
from dotenv import load_dotenv
import numpy as np
import pickle
from pathlib import Path
from typing import List
try:
    from sklearn.metrics.pairwise import cosine_similarity  # type: ignore
except ModuleNotFoundError:  # Fallback if sklearn not installed
    def cosine_similarity(a: np.ndarray, b: np.ndarray) -> np.ndarray:  # type: ignore
        """Compute cosine similarity manually for 2D arrays."""
        # Normalize vectors
        a_norm = a / np.linalg.norm(a, axis=1, keepdims=True)
        b_norm = b / np.linalg.norm(b, axis=1, keepdims=True)
        return a_norm @ b_norm.T
import argparse
from copilot.utils.openai_client import get_openai_client

# Requirements: pip install openai python-dotenv numpy scikit-learn

# Load environment variables from .env file
load_dotenv()

# Restore the missing path definitions
BASE_DIR = Path(__file__).resolve().parent.parent.parent
VECTOR_DIR = BASE_DIR / "copilot" / "vector_storage"
EMBEDDINGS_FILE = VECTOR_DIR / "embeddings.npy"
METADATA_FILE = VECTOR_DIR / "metadata.pkl"

def get_embedding(text_chunk: str, model="text-embedding-3-small") -> list[float]:
    """Generate a vector embedding for a given text chunk using OpenAI (new API)."""
    client = get_openai_client()
    text_chunk = text_chunk.replace("\n", " ")
    response = client.embeddings.create(
        input=[text_chunk], 
        model=model
    )
    return response.data[0].embedding

def embed_query(query: str):
    client = get_openai_client()
    response = client.embeddings.create(
        input=[query],
        model="text-embedding-ada-002"
    )
    return np.array(response.data[0].embedding, dtype=np.float32).reshape(1, -1)

def query_file_storage(query: str, top_k: int = 5, metadata_filter: dict = None):
    with open(EMBEDDINGS_FILE, "rb") as f:
        embeddings = np.load(f)
    with open(METADATA_FILE, "rb") as f:
        metadatas = pickle.load(f)
    if metadata_filter:
        mask = [all(meta.get(k) == v for k, v in metadata_filter.items()) for meta in metadatas]
        filtered_embeddings = embeddings[mask]
        filtered_metadatas = [m for m, keep in zip(metadatas, mask) if keep]
    else:
        filtered_embeddings = embeddings
        filtered_metadatas = metadatas
    if len(filtered_embeddings) == 0:
        return []
    query_emb = embed_query(query)
    sims = cosine_similarity(query_emb, filtered_embeddings)[0]
    top_idx = np.argsort(sims)[::-1][:top_k]
    results = []
    for idx in top_idx:
        results.append({
            "score": float(sims[idx]),
            "metadata": filtered_metadatas[idx],
            "text": filtered_metadatas[idx]["text"]
        })
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", type=str, help="Query string")
    parser.add_argument("--top_k", type=int, default=5)
    parser.add_argument("--filter", type=str, help="Filter in key=value format")
    args = parser.parse_args()
    metadata_filter = None
    if args.filter:
        k, v = args.filter.split("=", 1)
        metadata_filter = {k: v}
    results = query_file_storage(args.query, top_k=args.top_k, metadata_filter=metadata_filter)
    if not results:
        print("No results found.")
        return
    for i, r in enumerate(results):
        print(f"[{i+1}] Score: {r['score']:.4f}")
        print(f"File: {r['metadata'].get('file')}, Start line: {r['metadata'].get('start_line')}, Title: {r['metadata'].get('title', '-')}")
        print(f"Metadata: {r['metadata']}")
        print(f"Text: {r['text'][:300]}\n---\n")

def query_chroma(query: str, top_k: int = 5, metadata_filter: dict = None):
    # For compatibility with test scripts
    return query_file_storage(query, top_k, metadata_filter)

def query_vector_store(
    query: str,
    index_path: str | Path,
    k: int = 3,
    threshold: float = 0.7,
) -> List[str]:
    """Return the top k most relevant chunks for the query."""
    # Initialize OpenAI client only when needed
    client = get_openai_client()
    # ... existing code ...

if __name__ == "__main__":
    main() 