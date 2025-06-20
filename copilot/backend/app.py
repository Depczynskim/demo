from __future__ import annotations

"""FastAPI backend for Pops Analytics Copilot.

Run with:
    uvicorn copilot.backend.app:app --reload --port 8000

Env vars required:
    OPENAI_API_KEY
"""

import os
from typing import List, Dict

import openai
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field
from loguru import logger

from pathlib import Path
import sys

# Project imports
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR / "retrieval"))
from vector_query import query_file_storage  # noqa: E402

sys.path.append(str(BASE_DIR / "utils"))
from logger import get_logger  # noqa: E402

from copilot.llm.prompt_builder import build_messages  # noqa: E402
from copilot.memory.crud import fetch_history, log_message  # noqa: E402
from copilot.llm.suggestions import generate_suggestions  # noqa: E402

# Central config – provides the default model name
from config import OPENAI_COMPLETION_MODEL, setup_logging, PROJECT_ROOT, SUMMARIES_DIR  # noqa: E402

load_dotenv()

def get_openai_client() -> openai.OpenAI:
    """Initialize and return an OpenAI client with proper API key configuration."""
    try:
        # Try to import streamlit for deployed environment
        import streamlit as st
        if hasattr(st, 'secrets') and 'OPENAI_API_KEY' in st.secrets:
            api_key = st.secrets['OPENAI_API_KEY']
        else:
            api_key = os.getenv("OPENAI_API_KEY")
    except ImportError:
        # Fallback for non-Streamlit environments
        api_key = os.getenv("OPENAI_API_KEY")
    
    if not api_key:
        raise ValueError("OpenAI API key not found in environment or Streamlit secrets")
    
    return openai.OpenAI(api_key=api_key)

# App setup
# -----------------------------------------------------------------------------
setup_logging()

app = FastAPI(title="Pops Analytics Copilot", version="0.1.0")

# ---------------------------------------------------------------------------
# Pydantic Schemas
# ---------------------------------------------------------------------------


class ChatRequest(BaseModel):
    question: str
    top_k: int | None = 5
    window_days: int | None = 30  # Which context window to use
    model: str | None = Field(
        default=None,
        description="Optional OpenAI chat model id (e.g. gpt-4o, gpt-3.5-turbo-0125). If omitted, server default applies.",
    )


class ContextChunk(BaseModel):
    score: float
    text: str
    metadata: dict


class ChatResponse(BaseModel):
    answer: str
    context: List[ContextChunk]
    suggestions: List[str]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


# No longer needed – prompt construction handled by copilot.llm.prompt_builder


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.post("/chat", response_model=ChatResponse)
async def chat(
    req: ChatRequest,
    # Clients should send ``X-Session-Id`` header (or any custom header).
    session_id: str | None = Header(default=None, alias="X-Session-Id"),
):
    # Retrieve context
    try:
        results = query_file_storage(req.question, top_k=req.top_k or 5)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Vector storage not built. Run vector_index.py first.")
    if not results:
        raise HTTPException(status_code=404, detail="No relevant context found.")

    context_chunks = [r["text"] for r in results]

    # Build messages via new template-driven prompt builder, injecting previous
    # conversation turns if a session id is provided.
    history_msgs = fetch_history(session_id, limit=6) if session_id else []

    try:
        messages = build_messages(
            req.question,
            window=req.window_days or 30,
            context_chunks=context_chunks,
            chat_history=history_msgs,
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not client:
        raise HTTPException(status_code=500, detail="OpenAI client not initialized. Check server logs.")

    model_name = req.model or OPENAI_COMPLETION_MODEL or "gpt-3.5-turbo-0125"

    try:
        logger.info(
            "Calling OpenAI chat completion | model=%s | messages=%d | window=%s",
            model_name,
            len(messages),
            req.window_days,
        )
        response = client.chat.completions.create(
            model=model_name,
            messages=messages,
            temperature=0.3,
            max_tokens=400,
        )
    except Exception as e:
        logger.error("OpenAI chat completion failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    # Persist chat turn *after* successful LLM completion so we never store
    # requests that triggered server/LLM errors.
    if session_id:
        log_message(session_id, "user", req.question)
        log_message(session_id, "assistant", response.choices[0].message.content)

    context_resp = [ContextChunk(score=r["score"], text=r["text"], metadata=r["metadata"]) for r in results]

    # Generate tailored suggestions (LLM-backed with safe fallback)
    suggestions = generate_suggestions(req.question, response.choices[0].message.content)

    return ChatResponse(answer=response.choices[0].message.content, context=context_resp, suggestions=suggestions) 