import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Settings:
    """Application settings populated from environment variables.

    Production (RDS):
        RAPIDQCMS_DB_URL=postgresql://user:pass@host/rapidqcms

    Local dev (SQLite):
        (defaults below apply — no env vars needed)
    """

    # Database
    db_url: str

    # Notifications
    slack_bot_token: str | None
    slack_channel: str | None

    # QC module config path
    qc_modules_config: Path

    # Listener / acquisition — deployment-stable, set once per instrument computer
    extension: str           # file extension to watch, e.g. ".raw", ".d", ".mzML"
    experiment_type: str     # "metabolomics" | "proteomics"
    chromatography: str      # "HILIC", "C18", etc.
    polarity: str            # "Pos" | "Neg"
    qc_stage: str            # e.g. "pre_search"
    msconvert_exe: Path | None   # None → MSConvert step skipped
    msdial_exe: Path | None      # None → MS-DIAL step skipped
    msdial_params: Path | None   # None → MS-DIAL step skipped

    @classmethod
    def from_env(cls) -> "Settings":
        def _optional_path(var: str) -> Path | None:
            val = os.getenv(var)
            return Path(val) if val else None

        return cls(
            db_url=os.getenv("RAPIDQCMS_DB_URL", "sqlite:///data/rapidqcms.db"),
            slack_bot_token=os.getenv("RAPIDQCMS_SLACK_BOT_TOKEN"),
            slack_channel=os.getenv("RAPIDQCMS_SLACK_CHANNEL"),
            qc_modules_config=Path(
                os.getenv(
                    "RAPIDQCMS_QC_MODULES_CONFIG",
                    str(Path(__file__).parent / "qc_modules.toml"),
                )
            ),
            extension=os.getenv("RAPIDQCMS_EXTENSION", ".raw"),
            experiment_type=os.getenv("RAPIDQCMS_EXPERIMENT_TYPE", "metabolomics"),
            chromatography=os.getenv("RAPIDQCMS_CHROMATOGRAPHY", "HILIC"),
            polarity=os.getenv("RAPIDQCMS_POLARITY", "Pos"),
            qc_stage=os.getenv("RAPIDQCMS_QC_STAGE", "pre_search"),
            msconvert_exe=_optional_path("RAPIDQCMS_MSCONVERT_EXE"),
            msdial_exe=_optional_path("RAPIDQCMS_MSDIAL_EXE"),
            msdial_params=_optional_path("RAPIDQCMS_MSDIAL_PARAMS"),
        )


_settings: Settings | None = None


def get_settings() -> Settings:
    """Return the cached Settings singleton (populated from environment on first call)."""
    global _settings
    if _settings is None:
        _settings = Settings.from_env()
    return _settings
