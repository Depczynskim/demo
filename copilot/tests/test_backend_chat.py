import os, sys
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # repo root
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

COPILOT_DIR = PROJECT_ROOT / 'copilot'

sys.path.append(str(COPILOT_DIR / 'backend'))
from app import app  # noqa: E402

client = TestClient(app)


def test_chat_endpoint_live():
    # Ensure vector index exists
    idx_path = PROJECT_ROOT / 'copilot' / 'vector_storage' / 'embeddings.npy'
    assert idx_path.exists(), "Vector index missing; run vector_index.py first."

    resp = client.post('/chat', json={
        'question': 'Give me SEO improvement suggestions for product pages',
        'top_k': 5,
        'model': 'gpt-3.5-turbo-0125',
    })
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert 'answer' in data and data['answer']
    assert 'suggestions' in data and len(data['suggestions']) == 3
    assert len(data['context']) > 0 