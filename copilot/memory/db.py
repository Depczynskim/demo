import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base

# ---------------------------------------------------------------------------
# Database configuration
# ---------------------------------------------------------------------------

# Allow an override via env var.  If not provided we fall back to a local file
# inside the repo (fine for local development / unit tests).
_DATABASE_URL = os.getenv("COPILOT_MEMORY_DB", "sqlite:///./copilot_chat_memory.db")

# ``check_same_thread`` must be disabled for SQLite to allow usage from FastAPI
# background threads.
_engine = create_engine(
    _DATABASE_URL,
    connect_args={"check_same_thread": False} if _DATABASE_URL.startswith("sqlite") else {},
    echo=False,
)

# Thread-local session factory so we can safely share it across async FastAPI
# endpoints (each request uses ``SessionLocal()`` which returns a short-lived
# session attached to the same connection pool).
SessionLocal = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=_engine)
)

# Declarative base class that the ORM models should inherit from.
Base = declarative_base()

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Create tables if they do not yet exist.

    Importing ``copilot.memory.models`` registers all subclasses with the Base
    metadata, after which ``metadata.create_all`` will build the schema.
    """
    # The models import needs to stay **inside** the function to avoid circular
    # imports when other modules use ``get_session`` early during start-up.
    from . import models  # noqa: F401  (side-effect import)

    Base.metadata.create_all(bind=_engine) 