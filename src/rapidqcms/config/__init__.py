import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Settings:
    """Application settings populated from environment variables.

    Production (RDS + S3):
        RAPIDQCMS_DB_URL=postgresql://user:pass@host/rapidqcms
        RAPIDQCMS_STORAGE_BACKEND=s3
        RAPIDQCMS_S3_BUCKET=my-bucket
        RAPIDQCMS_S3_PREFIX=rapidqcms

    Local dev (SQLite + filesystem):
        (defaults below apply — no env vars needed)

    Okta SSO (Phase 4):
        RAPIDQCMS_OKTA_DOMAIN=czbiohub.okta.com
        RAPIDQCMS_OKTA_CLIENT_ID=...
        RAPIDQCMS_OKTA_CLIENT_SECRET=...
    """

    # Database
    db_url: str

    # Storage
    storage_backend: str           # "local" or "s3"
    s3_bucket: str | None
    s3_prefix: str
    local_storage_path: str

    # Notifications
    slack_bot_token: str | None
    slack_channel: str | None

    # Auth (Okta, Phase 5)
    okta_domain: str | None
    okta_client_id: str | None
    okta_client_secret: str | None
    session_secret: str | None    # RAPIDQCMS_SESSION_SECRET

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
            storage_backend=os.getenv("RAPIDQCMS_STORAGE_BACKEND", "local"),
            s3_bucket=os.getenv("RAPIDQCMS_S3_BUCKET"),
            s3_prefix=os.getenv("RAPIDQCMS_S3_PREFIX", "rapidqcms"),
            local_storage_path=os.getenv(
                "RAPIDQCMS_LOCAL_STORAGE_PATH", "data/storage"
            ),
            slack_bot_token=os.getenv("RAPIDQCMS_SLACK_BOT_TOKEN"),
            slack_channel=os.getenv("RAPIDQCMS_SLACK_CHANNEL"),
            okta_domain=os.getenv("RAPIDQCMS_OKTA_DOMAIN"),
            okta_client_id=os.getenv("RAPIDQCMS_OKTA_CLIENT_ID"),
            okta_client_secret=os.getenv("RAPIDQCMS_OKTA_CLIENT_SECRET"),
            session_secret=os.getenv("RAPIDQCMS_SESSION_SECRET"),
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
