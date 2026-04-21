import os
from contextlib import contextmanager

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session

from .models import Base

_engine: Engine | None = None


def get_engine(url: str | None = None) -> Engine:
    """Return (and cache) the SQLAlchemy engine.

    url defaults to the RAPIDQCMS_DB_URL environment variable, falling back to
    a local SQLite file for development. Pass url explicitly in tests to inject
    an in-memory engine without touching global state.
    """
    global _engine
    if url is not None:
        return create_engine(url)
    if _engine is None:
        _engine = create_engine(
            os.getenv("RAPIDQCMS_DB_URL", "sqlite:///data/rapidqcms.db")
        )
    return _engine


def init_db(engine: Engine | None = None) -> None:
    """Create all tables. Safe to call multiple times (CREATE IF NOT EXISTS)."""
    Base.metadata.create_all(engine or get_engine())


@contextmanager
def get_session(engine: Engine | None = None):
    """Context manager yielding a transactional Session.

    Commits on clean exit, rolls back on exception.

    Usage:
        with get_session() as session:
            session.add(record)
    """
    bound_engine = engine or get_engine()
    factory = sessionmaker(bound_engine)
    with factory() as session:
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
