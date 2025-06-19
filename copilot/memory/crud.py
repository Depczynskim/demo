from typing import List, Dict

from sqlalchemy.orm import Session

from .db import SessionLocal, init_db
from .models import ChatMessage

# Ensure tables exist on first import.
init_db()


# ---------------------------------------------------------------------------
# Public helper functions
# ---------------------------------------------------------------------------

def log_message(session_id: str, role: str, content: str) -> None:
    """Persist a single chat turn to the DB."""
    if not session_id:
        # Safety guard – we never persist messages without an explicit session.
        return
    db: Session = SessionLocal()
    try:
        msg = ChatMessage(session_id=session_id, role=role, content=content)
        db.add(msg)
        db.commit()
    finally:
        db.close()


def fetch_history(session_id: str, limit: int = 6) -> List[Dict[str, str]]:
    """Return the *most recent* ``limit`` chat turns for ``session_id``.

    The list is returned in chronological order (oldest → newest) so that it
    can be appended to a prompt without additional sorting.
    """
    if not session_id:
        return []

    db: Session = SessionLocal()
    try:
        rows = (
            db.query(ChatMessage)
            .filter(ChatMessage.session_id == session_id)
            .order_by(ChatMessage.id.desc())
            .limit(limit)
            .all()
        )
        # Reverse so we go from oldest → newest.
        rows.reverse()
        return [{"role": r.role, "content": r.content} for r in rows]
    finally:
        db.close() 