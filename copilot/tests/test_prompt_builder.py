import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import pytest

from copilot.llm.prompt_builder import build_messages


@pytest.fixture
def monkeypatched_context(monkeypatch):
    """Monkey-patch load_context so the test is independent from real JSON files."""

    def _fake_load_context(window: int = 30):
        return {
            "window_days": window,
            "generated_at": "2025-06-13T00:00:00Z",
        }

    monkeypatch.setattr("copilot.llm.prompt_builder.load_context", _fake_load_context)


def test_build_messages_basic(monkeypatched_context):
    question = "Why did ROAS drop?"
    extra_ctx = ["ads_cost rose 20%", "conversions flat"]

    messages = build_messages(question, window=30, context_chunks=extra_ctx)

    # Expect a two-message conversation: system then user
    assert len(messages) == 2
    system_msg, user_msg = messages

    assert system_msg["role"] == "system"
    assert "last 30-day window" in system_msg["content"]

    assert user_msg["role"] == "user"
    assert question in user_msg["content"]
    # Extra context chunks are included verbatim in the user prompt
    for chunk in extra_ctx:
        assert chunk in user_msg["content"] 