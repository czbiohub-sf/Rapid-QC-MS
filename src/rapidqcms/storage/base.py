from pathlib import Path
from typing import Protocol, runtime_checkable


@runtime_checkable
class StorageBackend(Protocol):
    """Protocol for file storage backends.

    Implementations:
      - LocalStorageBackend  — filesystem, for local dev and testing
      - S3StorageBackend     — AWS S3, for production

    Keys are forward-slash-delimited relative paths, e.g.:
      "instruments/INSTR_001/runs/RUN_2026_001/sample.mzML"
    """

    def upload(self, local_path: Path, remote_key: str) -> None:
        """Copy a local file to storage under the given key."""
        ...

    def download(self, remote_key: str, local_path: Path) -> None:
        """Retrieve a file from storage and write it to local_path."""
        ...

    def exists(self, remote_key: str) -> bool:
        """Return True if the key exists in storage."""
        ...

    def list_keys(self, prefix: str = "") -> list[str]:
        """Return all keys under prefix (non-recursive listing uses prefix as directory)."""
        ...

    def delete(self, remote_key: str) -> None:
        """Remove a key from storage."""
        ...
