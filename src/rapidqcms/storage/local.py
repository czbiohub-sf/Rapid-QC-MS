import shutil
from pathlib import Path


class LocalStorageBackend:
    """Filesystem-backed storage for local development and testing.

    Satisfies the StorageBackend protocol without importing it, so there
    is no circular dependency. Use this backend by setting:
        RAPIDQCMS_STORAGE_BACKEND=local
        RAPIDQCMS_LOCAL_STORAGE_PATH=/path/to/storage/root
    """

    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / key

    def upload(self, local_path: Path, remote_key: str) -> None:
        dest = self._path(remote_key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, dest)

    def download(self, remote_key: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self._path(remote_key), local_path)

    def exists(self, remote_key: str) -> bool:
        return self._path(remote_key).exists()

    def list_keys(self, prefix: str = "") -> list[str]:
        search_root = self._path(prefix) if prefix else self.root
        if not search_root.exists():
            return []
        return [
            str(p.relative_to(self.root))
            for p in search_root.rglob("*")
            if p.is_file()
        ]

    def delete(self, remote_key: str) -> None:
        path = self._path(remote_key)
        if path.exists():
            path.unlink()
