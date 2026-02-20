import datetime
from datetime import UTC

from sqlalchemy import DateTime, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Instrument(Base):
    __tablename__ = "instruments"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    vendor: Mapped[str | None] = mapped_column(String, nullable=True)
    experiment_type: Mapped[str | None] = mapped_column(String, nullable=True)

    runs: Mapped[list["Run"]] = relationship("Run", back_populates="instrument")


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    instrument_id: Mapped[str] = mapped_column(
        String, ForeignKey("instruments.id"), nullable=False, index=True
    )
    experiment_type: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="active")
    started_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, default=lambda: datetime.datetime.now(UTC)
    )
    completed_at: Mapped[datetime.datetime | None] = mapped_column(DateTime, nullable=True)

    instrument: Mapped["Instrument"] = relationship("Instrument", back_populates="runs")
    qc_results: Mapped[list["QCResult"]] = relationship("QCResult", back_populates="run")


class QCResult(Base):
    __tablename__ = "qc_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    run_id: Mapped[str] = mapped_column(
        String, ForeignKey("runs.id"), nullable=False, index=True
    )
    sample_id: Mapped[str] = mapped_column(String, nullable=False)
    experiment_type: Mapped[str] = mapped_column(String, nullable=False, index=True)
    qc_stage: Mapped[str] = mapped_column(String, nullable=False)  # pre_search / post_search
    qc_module: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)    # Pass / Warn / Fail
    acquired_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, nullable=True, index=True
    )
    metrics: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    details: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    message: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=lambda: datetime.datetime.now(UTC)
    )

    run: Mapped["Run"] = relationship("Run", back_populates="qc_results")
