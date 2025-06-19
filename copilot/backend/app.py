from __future__ import annotations

"""FastAPI backend for Pops Analytics Copilot.

Run with:
    uvicorn copilot.backend.app:app --reload --port 8000

Env vars required:
    OPENAI_API_KEY
"""

import os
from typing import List

import openai
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field

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
from config import OPENAI_COMPLETION_MODEL  # noqa: E402

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
logger = get_logger(__name__)

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

    model_name = req.model or OPENAI_COMPLETION_MODEL or "gpt-3.5-turbo-0125"

    logger.info(
        "Calling OpenAI chat completion | model=%s | messages=%d | window=%s",
        model_name,
        len(messages),
        req.window_days,
    )

    try:
        response = openai.chat.completions.create(
            model=model_name,
            messages=messages,
            temperature=0.2,
            max_tokens=400,
        )
        answer = response.choices[0].message.content
    except Exception as e:
        logger.error("OpenAI chat completion failed: %s", e)
        raise HTTPException(status_code=500, detail="LLM request failed.")

    # Persist chat turn *after* successful LLM completion so we never store
    # requests that triggered server/LLM errors.
    if session_id:
        log_message(session_id, "user", req.question)
        log_message(session_id, "assistant", answer)

    context_resp = [ContextChunk(score=r["score"], text=r["text"], metadata=r["metadata"]) for r in results]

    # Generate tailored suggestions (LLM-backed with safe fallback)
    suggestions = generate_suggestions(req.question, answer)

    return ChatResponse(answer=answer, context=context_resp, suggestions=suggestions) 