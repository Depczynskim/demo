import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime

from .db import Base


class ChatMessage(Base):
    """ORM model representing a single chat turn (either user or assistant).

    Attributes
    ----------
    id
        Auto-increment primary key.
    session_id
        Arbitrary string sent by the front-end identifying the chat session.
    role
        "user" or "assistant" – mirrors OpenAI ChatCompletion roles so that we
        can later reuse the stored turns verbatim in prompts.
    content
        The natural-language message.
    ts
        Timestamp (UTC) when the message was stored.
    """

    __tablename__ = "chat_messages"

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String(64), index=True, nullable=False)
    role = Column(String(16), nullable=False)
    content = Column(Text, nullable=False)
    ts = Column(DateTime, default=datetime.datetime.utcnow, nullable=False) 