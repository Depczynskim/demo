from __future__ import annotations

"""Prompt construction helpers for Pops Analytics Copilot.

All LLM-facing messages should be assembled via this module so we maintain
one single source of truth for system, user and tool prompts.

Templates live in ``copilot/prompts/`` and use Jinja2 for simple variable
substitution.  Anything more complex than loops / conditionals should be
implemented in Python and passed into the template context as plain data.
"""

from pathlib import Path
import json
from typing import Any, Iterable, List, Dict

import jinja2

# ---------------------------------------------------------------------------
# Paths & Jinja environment
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent  # copilot/
PROMPTS_DIR = BASE_DIR / "prompts"
CONTEXT_DIR = BASE_DIR / "context"

# Lazy-initialised Jinja environment so we only pay the cost once.
_ENV: jinja2.Environment | None = None


def _get_env() -> jinja2.Environment:
    global _ENV
    if _ENV is None:
        _ENV = jinja2.Environment(
            loader=jinja2.FileSystemLoader(str(PROMPTS_DIR)),
            autoescape=False,  # we do not render HTML
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # --------------------------------------------------------
        # Custom Jinja filters for tidy formatting inside templates
        # --------------------------------------------------------

        def _pct(val, digits: int = 1):
            """Return *val* as '<d.dd %>' string rounded to *digits* decimals."""
            try:
                return f"{float(val):.{digits}f} %"
            except Exception:
                return val

        def _gbp(val, digits: int = 2):
            """Return *val* formatted as '£x.xx'."""
            try:
                return f"£{float(val):.{digits}f}"
            except Exception:
                return val

        _ENV.filters["pct"] = _pct
        _ENV.filters["gbp"] = _gbp
    return _ENV


# ---------------------------------------------------------------------------
# Context helpers
# ---------------------------------------------------------------------------

def load_context(window: int = 30) -> Dict[str, Any]:
    """Return the JSON context object for the given window (days)."""
    path = CONTEXT_DIR / f"context_{window}d.json"
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open() as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Public API – build the messages list
# ---------------------------------------------------------------------------

def build_messages(
    question: str,
    *,
    window: int = 30,
    context_chunks: Iterable[str] | None = None,
    chat_history: List[Dict[str, str]] | None = None,
) -> List[Dict[str, str]]:
    """Return a list of OpenAI ChatCompletion-style messages.

    Parameters
    ----------
    question
        The human question.
    window
        Context window in days (30/90/365).
    context_chunks
        Optional additional chunks (e.g. vector search snippets) that will be
        made available to the user template as ``extra_context``.
    chat_history
        Optional list of previous chat turns.
    """
    env = _get_env()

    ctx_json = load_context(window)
    tmpl_kwargs = {
        "ctx": ctx_json,
        "question": question,
        "extra_context": list(context_chunks or []),
    }

    system_prompt = env.get_template("system_prompt.jinja").render(**tmpl_kwargs)
    user_prompt = env.get_template("user_prompt.jinja").render(**tmpl_kwargs)

    messages: List[Dict[str, str]] = [
        {"role": "system", "content": system_prompt},
    ]

    # Inject previous chat turns so the model has the full conversation.
    if chat_history:
        # Expecting list[ {"role": "user"|"assistant", "content": str} ]
        messages.extend(chat_history)

    # Finally the *current* user question.
    messages.append({"role": "user", "content": user_prompt})
    return messages 