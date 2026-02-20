"""Shared pytest fixtures.

DB fixtures use SQLite in-memory — no Postgres instance required for testing.
S3 fixtures use moto to mock the AWS API — no real AWS credentials required.
"""

import boto3
import pytest
from moto import mock_aws
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from rapidqcms.db.models import Base
from rapidqcms.storage.local import LocalStorageBackend
from rapidqcms.storage.s3 import S3StorageBackend

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

TEST_BUCKET = "test-rapidqcms"
TEST_REGION = "us-east-1"


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


# ---------------------------------------------------------------------------
# Storage — S3 (moto mock)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def mock_s3_ctx():
    """Start/stop the moto AWS mock context and yield the boto3 client."""
    with mock_aws():
        client = boto3.client("s3", region_name=TEST_REGION)
        client.create_bucket(Bucket=TEST_BUCKET)
        yield client


@pytest.fixture(scope="function")
def mock_s3(mock_s3_ctx):
    """S3StorageBackend pointed at the moto-mocked bucket."""
    return S3StorageBackend(bucket=TEST_BUCKET, prefix="test")
