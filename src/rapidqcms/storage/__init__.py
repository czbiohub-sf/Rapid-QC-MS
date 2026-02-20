import os
from pathlib import Path

from .base import StorageBackend
from .local import LocalStorageBackend
from .s3 import S3StorageBackend

__all__ = ["StorageBackend", "LocalStorageBackend", "S3StorageBackend", "get_storage"]


def get_storage() -> StorageBackend:
    """Return a storage backend based on environment configuration.

    RAPIDQCMS_STORAGE_BACKEND=local (default) → LocalStorageBackend
    RAPIDQCMS_STORAGE_BACKEND=s3             → S3StorageBackend

    For local dev set RAPIDQCMS_LOCAL_STORAGE_PATH to override the default
    data/storage directory.
    """
    backend = os.getenv("RAPIDQCMS_STORAGE_BACKEND", "local")
    if backend == "s3":
        bucket = os.getenv("RAPIDQCMS_S3_BUCKET")
        if not bucket:
            raise ValueError(
                "RAPIDQCMS_S3_BUCKET must be set when RAPIDQCMS_STORAGE_BACKEND=s3"
            )
        prefix = os.getenv("RAPIDQCMS_S3_PREFIX", "rapidqcms")
        return S3StorageBackend(bucket=bucket, prefix=prefix)
    else:
        path = os.getenv("RAPIDQCMS_LOCAL_STORAGE_PATH", "data/storage")
        return LocalStorageBackend(root=Path(path))
