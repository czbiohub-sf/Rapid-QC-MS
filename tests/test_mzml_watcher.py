"""Tests for mzml_watcher experiment-type classification and file matching."""

import threading
from unittest import mock
import pytest

from rapidqcms.service.watchers.mzml_watcher import (
    MzmlWatcherConfig,
    MzmlEventHandler,
    _classify_experiment,
    _backfill,
)
from pathlib import Path


class TestClassifyExperiment:
    def test_hilic_is_metabolomics(self):
        assert _classify_experiment("Sample_HILIC_Pos_001") == ("metabolomics", "HILIC")

    def test_lipid_is_lipidomics(self):
        assert _classify_experiment("Sample_Lipid_Pos_001") == ("lipidomics", None)

    def test_other_is_proteomics(self):
        assert _classify_experiment("HeLa_DDA_run001") == ("proteomics", None)

    def test_hilic_takes_priority_over_lipid(self):
        # HILIC check comes first in the function
        assert _classify_experiment("HILIC_Lipid_sample") == ("metabolomics", "HILIC")

    def test_case_sensitive_hilic(self):
        # "hilic" (lowercase) should not match — filenames use uppercase HILIC
        assert _classify_experiment("sample_hilic_001")[0] == "proteomics"

    def test_case_sensitive_lipid(self):
        assert _classify_experiment("sample_lipid_001")[0] == "proteomics"


class TestDeriveRunId:
    def _handler(self, tmp_path):
        cfg = MzmlWatcherConfig(watch_path=tmp_path)
        return MzmlEventHandler(cfg)

    def test_derives_run_id_from_first_subdir(self, tmp_path):
        h = self._handler(tmp_path)
        mzml = tmp_path / "TLG1025" / "raw" / "sample_HILIC.mzML"
        assert h._derive_run_id(mzml) == "TLG1025"

    def test_falls_back_when_file_is_flat(self, tmp_path):
        h = self._handler(tmp_path)
        mzml = tmp_path / "sample_HILIC.mzML"
        # flat file → falls back to watch_path name
        assert h._derive_run_id(mzml) == tmp_path.name

    def test_explicit_run_id_overrides_path(self, tmp_path):
        cfg = MzmlWatcherConfig(watch_path=tmp_path, run_id="MYRUN")
        h = MzmlEventHandler(cfg)
        mzml = tmp_path / "TLG1025" / "sample_HILIC.mzML"
        assert h._derive_run_id(mzml) == "MYRUN"


class TestMzmlEventHandlerMatches:
    def _handler(self, tmp_path):
        cfg = MzmlWatcherConfig(watch_path=tmp_path)
        return MzmlEventHandler(cfg)

    def test_mzml_matches(self, tmp_path):
        h = self._handler(tmp_path)
        assert h._matches(Path("sample.mzML")) is True

    def test_mzml_case_insensitive(self, tmp_path):
        h = self._handler(tmp_path)
        assert h._matches(Path("sample.mzml")) is True

    def test_raw_does_not_match(self, tmp_path):
        h = self._handler(tmp_path)
        assert h._matches(Path("sample.raw")) is False

    def test_txt_does_not_match(self, tmp_path):
        h = self._handler(tmp_path)
        assert h._matches(Path("sample.txt")) is False


class TestBackfill:
    def _handler(self, tmp_path):
        cfg = MzmlWatcherConfig(watch_path=tmp_path)
        return MzmlEventHandler(cfg)

    def test_queues_unprocessed_files(self, tmp_path):
        (tmp_path / "RUN001").mkdir()
        mzml = tmp_path / "RUN001" / "sample_HILIC.mzML"
        mzml.touch()

        with mock.patch.object(MzmlEventHandler, "_run_qc") as mock_run:
            _backfill(self._handler(tmp_path), tmp_path)
            # wait for the pool thread to finish
            import time; time.sleep(0.2)
            mock_run.assert_called_once_with(mzml)

    def test_skips_already_gated_files(self, tmp_path):
        (tmp_path / "RUN001").mkdir()
        mzml = tmp_path / "RUN001" / "sample_HILIC.mzML"
        mzml.touch()
        (tmp_path / "RUN001" / "sample_HILIC.qc_pass").write_text(
            '{"status":"Pass","stage":"pre_search","module":"pipeline","metrics":{},"timestamp":"2026-01-01T00:00:00+00:00"}'
        )

        with mock.patch.object(MzmlEventHandler, "_run_qc") as mock_run:
            _backfill(self._handler(tmp_path), tmp_path)
            import time; time.sleep(0.2)
            mock_run.assert_not_called()

    def test_no_op_when_directory_empty(self, tmp_path):
        with mock.patch.object(MzmlEventHandler, "_run_qc") as mock_run:
            _backfill(self._handler(tmp_path), tmp_path)
            import time; time.sleep(0.1)
            mock_run.assert_not_called()
