"""In-memory QC library loader.

Reads qc_config.yaml once at first use and caches the result.
Provides typed accessors for the data structures QC modules need.

Override the default config path with:
    RAPIDQCMS_QC_CONFIG=/path/to/your/config.yaml
"""

import functools
import os
from pathlib import Path

import pandas as pd
import yaml

_DEFAULT_CONFIG = Path(__file__).parent / "qc_config.yaml"
_ENV_VAR = "RAPIDQCMS_QC_CONFIG"


@functools.lru_cache(maxsize=1)
def _load(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_qc_config() -> dict:
    """Load and cache the QC library config.

    Reads RAPIDQCMS_QC_CONFIG env var for an override path;
    falls back to the bundled qc_config.yaml.
    """
    path = os.environ.get(_ENV_VAR) or str(_DEFAULT_CONFIG)
    return _load(path)


def get_internal_standards(chromatography: str, polarity: str) -> list[dict]:
    """Return IS entries for the given chromatography method and polarity.

    Each entry is a dict with keys: name, mz, rt.
    Returns an empty list if no IS are configured for this combination.
    """
    config = load_qc_config()
    return (
        config
        .get("internal_standards", {})
        .get(chromatography, {})
        .get(polarity, [])
    )


def get_internal_standards_df(chromatography: str, polarity: str) -> pd.DataFrame:
    """Return IS as a DataFrame with columns: name, precursor_mz, retention_time.

    Returns an empty DataFrame (with those columns) if none are configured.
    """
    rows = get_internal_standards(chromatography, polarity)
    if not rows:
        return pd.DataFrame(columns=["name", "precursor_mz", "retention_time"])
    return pd.DataFrame([
        {"name": r["name"], "precursor_mz": r["mz"], "retention_time": r["rt"]}
        for r in rows
    ])


def _reset_cache() -> None:
    """Clear the config cache (for use in tests only)."""
    _load.cache_clear()
