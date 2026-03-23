"""Shared pytest fixtures.

DB fixtures use SQLite in-memory — no Postgres instance required for testing.
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from rapidqcms.db.models import Base
from rapidqcms.storage.local import LocalStorageBackend

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def db_engine():
    """Fresh in-memory SQLite engine per test function."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(db_engine):
    """Transactional session bound to the in-memory engine."""
    Session = sessionmaker(db_engine)
    with Session() as session:
        yield session


# ---------------------------------------------------------------------------
# Storage — local (filesystem)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def local_storage(tmp_path):
    """LocalStorageBackend rooted at a pytest-managed temp directory."""
    return LocalStorageBackend(root=tmp_path / "storage")
