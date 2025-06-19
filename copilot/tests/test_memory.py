import os, sys, uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Ensure the env var points to a *temporary* sqlite DB in the pytest tmp dir.
MEM_DB_PATH = PROJECT_ROOT / "copilot" / "tests" / "test_chat_memory_tmp.db"
os.environ["COPILOT_MEMORY_DB"] = f"sqlite:///{MEM_DB_PATH}"

from copilot.memory.crud import log_message, fetch_history  # noqa: E402


def test_memory_roundtrip(tmp_path):
    # Override env to point to tmp path *within* tmp_path fixture
    tmp_db_file = tmp_path / "chat_mem.db"
    os.environ["COPILOT_MEMORY_DB"] = f"sqlite:///{tmp_db_file}"

    # Re-import db+crud modules so they pick up the new env var.
    import importlib
    import copilot.memory.db as db_module  # type: ignore
    importlib.reload(db_module)
    import copilot.memory.crud as crud_module  # type: ignore
    importlib.reload(crud_module)

    # Ensure the model metadata is registered against the *new* Base/engine
    import copilot.memory.models as models_module  # type: ignore
    importlib.reload(models_module)

    # Explicitly (re)initialise the schema for the freshly created DB
    db_module.init_db()

    session_id = str(uuid.uuid4())
    log_message(session_id, "user", "Hello there!")
    log_message(session_id, "assistant", "Hi! How can I help?")

    hist = fetch_history(session_id, limit=10)
    assert len(hist) == 2
    assert hist[0]["role"] == "user" and "Hello" in hist[0]["content"]
    assert hist[1]["role"] == "assistant" 