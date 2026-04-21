"""Tests for LocalStorageBackend."""

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_file(tmp_path, name: str, content: str = "hello"):
    f = tmp_path / name
    f.write_text(content)
    return f


# ---------------------------------------------------------------------------
# Shared contract tests — run against both backends
# ---------------------------------------------------------------------------

def _run_contract_tests(backend, tmp_path):
    """Assert that backend satisfies the StorageBackend contract."""
    src = _write_file(tmp_path, "sample.mzml", "mzml content")

    # upload / exists
    assert not backend.exists("runs/RUN_001/sample.mzml")
    backend.upload(src, "runs/RUN_001/sample.mzml")
    assert backend.exists("runs/RUN_001/sample.mzml")

    # download
    dest = tmp_path / "downloaded.mzml"
    backend.download("runs/RUN_001/sample.mzml", dest)
    assert dest.read_text() == "mzml content"

    # list_keys
    for i in range(2):
        f = _write_file(tmp_path, f"extra_{i}.txt", f"data {i}")
        backend.upload(f, f"runs/RUN_001/extra_{i}.txt")

    keys = backend.list_keys("runs/RUN_001")
    assert len(keys) == 3

    # delete
    backend.delete("runs/RUN_001/sample.mzml")
    assert not backend.exists("runs/RUN_001/sample.mzml")


class TestLocalStorageBackend:
    def test_contract(self, local_storage, tmp_path):
        _run_contract_tests(local_storage, tmp_path)

    def test_upload_creates_parent_dirs(self, local_storage, tmp_path):
        src = _write_file(tmp_path, "deep.txt")
        local_storage.upload(src, "a/b/c/d/deep.txt")
        assert local_storage.exists("a/b/c/d/deep.txt")

    def test_list_keys_empty_prefix(self, local_storage, tmp_path):
        src = _write_file(tmp_path, "file.txt")
        local_storage.upload(src, "file.txt")
        keys = local_storage.list_keys()
        assert "file.txt" in keys

    def test_list_keys_nonexistent_prefix(self, local_storage):
        assert local_storage.list_keys("does/not/exist") == []

    def test_delete_nonexistent_is_safe(self, local_storage):
        local_storage.delete("ghost.txt")  # should not raise


