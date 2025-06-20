"""Build a vector index from summary files for semantic search."""

import glob
import hashlib
import os
import pickle
import re
from pathlib import Path
from typing import Dict, List

import numpy as np
import openai
from dotenv import load_dotenv
import yaml  # PyYAML for front-matter parsing
from copilot.utils.openai_client import get_openai_client

# Logger
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))
from logger import get_logger

logger = get_logger(__name__)

# Requirements: pip install openai python-dotenv numpy pyyaml
# Set your OpenAI API key in a .env file as OPENAI_API_KEY=sk-...

# Load environment variables from .env file
load_dotenv()

# Constants
# ----------------------------------------------------------------------
# Path to the directory containing summary files, relative to this script
BASE_DIR = Path(__file__).resolve().parent.parent.parent
VECTOR_DIR = BASE_DIR / "copilot" / "vector_storage"
EMBEDDINGS_FILE = VECTOR_DIR / "embeddings.npy"
METADATA_FILE = VECTOR_DIR / "metadata.pkl"

def get_embedding(text_chunk: str, model="text-embedding-3-small") -> list[float]:
    """Generate a vector embedding for a given text chunk using OpenAI."""
    # Initialize OpenAI client only when needed
    client = get_openai_client()
    
    text_chunk = text_chunk.replace("\n", " ")
    response = client.embeddings.create(
        input=[text_chunk],
        model=model
    )
    return response.data[0].embedding

def parse_front_matter(md_text: str) -> dict:
    match = re.match(r"^---\n(.*?)\n---", md_text, re.DOTALL)
    if match:
        try:
            return yaml.safe_load(match.group(1))
        except Exception as e:
            print(f"[WARN] Failed to parse YAML front-matter: {e}")
    return {}

def process_all_markdown(summaries_dir: str) -> List[Dict]:
    # Resolve path relative to BASE_DIR to avoid CWD issues
    summaries_path = Path(summaries_dir)
    if not summaries_path.is_absolute():
        summaries_path = BASE_DIR / summaries_path

    chunks = []
    for md_file in summaries_path.glob("*.md"):
        with open(md_file, "r") as f:
            text = f.read()
        front_matter = parse_front_matter(text)
        lines = text.splitlines()
        chunk_text: List[str] = []
        start_line = 1
        for i, line in enumerate(lines):
            chunk_text.append(line)
            joined = "\n".join(chunk_text)
            if len(joined) > 1000 or i == len(lines) - 1:
                chunk_body = joined
                # Trim leading newline if overlap fragment
                if chunk_body.startswith("\n"):
                    chunk_body = chunk_body.lstrip("\n")
                chunk = {
                    "text": chunk_body,
                    "file": os.path.basename(md_file),
                    "start_line": start_line,
                    "front_matter": front_matter
                }
                # Add title if present
                header_match = re.search(r"^# (.+)", chunk["text"], re.MULTILINE)
                if header_match:
                    chunk["title"] = header_match.group(1)
                # Deterministic id
                h = hashlib.sha256((chunk["file"] + str(start_line) + chunk["text"]).encode("utf-8")).hexdigest()[:12]
                chunk["id"] = h
                chunks.append(chunk)
                start_line = i + 2
                # Keep 200-char overlap for next chunk
                overlap_text = joined[-200:]
                chunk_text = [overlap_text]
    return chunks

def upsert_to_file_storage(chunks: List[Dict]):
    """Upsert chunks by id (skip duplicates)."""
    VECTOR_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing
    existing_embeddings = None
    existing_meta: List[Dict] = []
    if METADATA_FILE.exists() and EMBEDDINGS_FILE.exists():
        with open(METADATA_FILE, "rb") as f:
            existing_meta = pickle.load(f)
        with open(EMBEDDINGS_FILE, "rb") as f:
            existing_embeddings = np.load(f)

    existing_ids = {m.get("id") for m in existing_meta if m.get("id")}

    new_embeds = []
    new_meta = []
    for chunk in chunks:
        if chunk["id"] in existing_ids:
            continue
        emb = get_embedding(chunk["text"])
        new_embeds.append(emb)
        meta = chunk.copy()
        new_meta.append(meta)

    if new_embeds:
        new_embeds_arr = np.array(new_embeds, dtype=np.float32)
        if existing_embeddings is not None:
            embeddings = np.concatenate([existing_embeddings, new_embeds_arr])
            metadatas = existing_meta + new_meta
        else:
            embeddings = new_embeds_arr
            metadatas = new_meta
        with open(EMBEDDINGS_FILE, "wb") as f:
            np.save(f, embeddings)
        with open(METADATA_FILE, "wb") as f:
            pickle.dump(metadatas, f)
        logger.info(f"Upserted {len(new_embeds)} new embeddings (total {len(embeddings)}).")
    else:
        logger.info("No new chunks to upsert – vector store already up-to-date.")

def main():
    summary_dir = BASE_DIR / "copilot" / "summaries"
    chunks = process_all_markdown(str(summary_dir))
    print(f"Processed {len(chunks)} chunks from Markdown summaries.")
    upsert_to_file_storage(chunks)

if __name__ == "__main__":
    main() 