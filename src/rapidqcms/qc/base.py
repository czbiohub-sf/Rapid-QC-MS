from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class QCStatus(str, Enum):
    PASS = "Pass"
    WARN = "Warn"
    FAIL = "Fail"


@dataclass
class QCResult:
    status: QCStatus
    module: str
    metrics: dict = field(default_factory=dict)
    details: dict = field(default_factory=dict)
    message: str = ""


class QCModule(ABC):
    """Abstract base class for all QC modules.

    Subclasses implement analyze() and are registered via config/qc_modules.toml.
    The from_config() classmethod is the standard constructor used by the registry.
    """

    def __init__(self, config: dict):
        self.config = config

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable module identifier used in QCResult records and the database."""
        ...

    @abstractmethod
    def analyze(self, input_path: Path, context: dict) -> QCResult:
        """Run QC checks on the given input file.

        Args:
            input_path: Path to input file.
                - Pre-search modules receive an mzML file.
                - Post-search modules receive a search engine output
                  (msstats.csv for FragPipe DDA, report.tsv for DIANN).
            context: Instrument/run metadata dictionary with at minimum:
                - instrument_id (str)
                - run_id (str)
                - experiment_type (str)

        Returns:
            QCResult with status, scalar metrics dict, and per-feature details dict.
        """
        ...

    @classmethod
    def from_config(cls, config: dict) -> "QCModule":
        return cls(config)
