import argparse
import logging
import sys
import webbrowser


def _open_browser(host: str, port: int) -> None:
    url = f"http://127.0.0.1:{port}/"
    if sys.platform == "win32":
        chrome_path = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
        webbrowser.register("chrome", None, webbrowser.BackgroundBrowser(chrome_path))
        webbrowser.get("chrome").open(url)
    elif sys.platform == "darwin":
        webbrowser.get("chrome").open(url, new=1)
    else:
        webbrowser.open(url)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="rapidqcms",
        description="Rapid QC-MS — instrument quality control service",
    )
    subparsers = parser.add_subparsers(dest="command")

    # ── listen ────────────────────────────────────────────────────────────────
    # Runs on the instrument computer. Watches a data acquisition directory and
    # routes completed raw files through the QC pipeline.
    listen_parser = subparsers.add_parser(
        "listen", help="Start the acquisition listener on an instrument computer"
    )
    listen_parser.add_argument("--instrument", required=True, help="Instrument ID")
    listen_parser.add_argument(
        "--path", required=True, help="Data acquisition directory to watch"
    )
    listen_parser.add_argument("--run-id", required=True, help="Run / job ID")

    # ── watch ─────────────────────────────────────────────────────────────────
    # Always-on root watcher. Reads lab_config.toml on startup, syncs
    # instruments to the DB, then watches for new run subdirectories.
    watch_parser = subparsers.add_parser(
        "watch", help="Start the always-on root directory watcher"
    )
    watch_parser.add_argument(
        "--path", required=True,
        help="Root directory to watch (e.g. /hpc/projects/mass_spec/projects)",
    )
    watch_parser.add_argument(
        "--config", required=True, metavar="CONFIG",
        help="Path to lab_config.toml",
    )

    # ── mzml-watch ────────────────────────────────────────────────────────────
    # Watches a directory for .mzML files whose names contain HILIC and
    # Metabolome, then triggers metabolomics pre-search QC automatically.
    # Works on macOS (FSEvents) and Linux/HPC (inotify or --polling for NFS).
    mzml_watch_parser = subparsers.add_parser(
        "mzml-watch",
        help="Watch a directory for HILIC metabolomics mzML files and run QC",
    )
    mzml_watch_parser.add_argument(
        "--path",
        default=None,
        help=(
            "Directory to watch (default: RAPIDQCMS_MZML_WATCH_PATH env var, "
            "then ./data/mzml)"
        ),
    )
    mzml_watch_parser.add_argument(
        "--instrument",
        default=None,
        help="Instrument ID written to the DB (default: RAPIDQCMS_INSTRUMENT_ID or 'unknown')",
    )
    mzml_watch_parser.add_argument(
        "--run-id",
        default=None,
        help=(
            "Run ID written to the DB. Defaults to RAPIDQCMS_RUN_ID, "
            "then SLURM_JOB_ID, then the watch directory name."
        ),
    )
    mzml_watch_parser.add_argument(
        "--polarity",
        default=None,
        choices=["Pos", "Neg"],
        help="Ion polarity for IS lookup (default: RAPIDQCMS_POLARITY or Pos)",
    )
    mzml_watch_parser.add_argument(
        "--filter",
        dest="filters",
        metavar="TOKEN",
        nargs="+",
        default=None,
        help="Filename tokens that must ALL be present (default: HILIC Metabolome)",
    )
    mzml_watch_parser.add_argument(
        "--polling",
        action="store_true",
        help=(
            "Use a stat-based PollingObserver instead of inotify/FSEvents. "
            "Required on NFS, Lustre, and GPFS (HPC shared filesystems)."
        ),
    )
    mzml_watch_parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip DB writes; only gate files are written next to each mzML.",
    )

    # ── migrate ───────────────────────────────────────────────────────────────
    # One-shot import from a legacy per-instrument Settings.db into the new DB.
    migrate_parser = subparsers.add_parser(
        "migrate",
        help="Import internal standards and QC configs from a legacy Settings.db",
    )
    migrate_parser.add_argument(
        "--settings-db",
        required=True,
        metavar="PATH",
        help="Path to the legacy Settings.db SQLite file",
    )

    # ── serve ─────────────────────────────────────────────────────────────────
    # Runs on the shared server. Serves the Dash dashboard for all users.
    serve_parser = subparsers.add_parser(
        "serve", help="Start the dashboard server (shared server deployment)"
    )
    serve_parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    serve_parser.add_argument("--port", default=8050, type=int, help="Bind port")
    serve_parser.add_argument("--debug", action="store_true")
    serve_parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Skip opening a browser tab (use for headless server deployments)",
    )

    args = parser.parse_args()

    logging.basicConfig(filename="rapid-qc-ms.log", level=logging.INFO)

    if args.command == "listen":
        from pathlib import Path
        from rapidqcms.service.listener import ListenerConfig, start_listener
        from rapidqcms.config import get_settings
        from rapidqcms.db.connection import get_engine
        from rapidqcms.storage import get_storage

        s = get_settings()
        cfg = ListenerConfig(
            instrument_id=args.instrument,
            run_id=args.run_id,
            watch_path=Path(args.path),
            extension=s.extension,
            experiment_type=s.experiment_type,
            chromatography=s.chromatography,
            polarity=s.polarity,
            stage=s.qc_stage,
            msconvert_exe=s.msconvert_exe,
            msdial_exe=s.msdial_exe,
            msdial_params=s.msdial_params,
        )
        start_listener(cfg, db_engine=get_engine(), storage=get_storage())

    elif args.command == "watch":
        from pathlib import Path
        from rapidqcms.config.lab_config import load_lab_config
        from rapidqcms.service.watcher import start_watcher
        from rapidqcms.db.connection import get_engine

        config = load_lab_config(Path(args.config))
        start_watcher(
            root_path=Path(args.path),
            config=config,
            db_engine=get_engine(),
        )

    elif args.command == "mzml-watch":
        from pathlib import Path
        from rapidqcms.service.mzml_watcher import MzmlWatcherConfig, start_mzml_watcher

        watch_path = Path(args.path) if args.path else None
        cfg = MzmlWatcherConfig.from_env(watch_path=watch_path)

        # CLI args override env-var defaults
        if args.instrument:
            cfg.instrument_id = args.instrument
        if args.run_id:
            cfg.run_id = args.run_id
        if args.polarity:
            cfg.polarity = args.polarity
        if args.filters:
            cfg.filename_filters = args.filters
        if args.polling:
            cfg.use_polling = True

        db_engine = None
        if not args.no_db:
            from rapidqcms.db.connection import get_engine
            db_engine = get_engine()

        start_mzml_watcher(cfg, db_engine=db_engine)

    elif args.command == "migrate":
        from pathlib import Path
        from sqlalchemy.orm import sessionmaker
        from rapidqcms.db.connection import get_engine
        from rapidqcms.db.migration import import_internal_standards, import_qc_configurations

        settings_db = Path(args.settings_db)
        engine = get_engine()
        Session = sessionmaker(engine)
        with Session() as session:
            n_is = import_internal_standards(settings_db, session)
            n_qc = import_qc_configurations(settings_db, session)
            session.commit()
        print(f"Imported {n_is} internal standards and {n_qc} QC configurations.")

    elif args.command == "serve":
        from rapidqcms.dashboard.app import app
        if not args.no_browser:
            _open_browser(args.host, args.port)
        app.run_server(threaded=False, debug=args.debug, host=args.host, port=args.port)

    else:
        # Legacy: bare `rapidqcms` with no subcommand retains the original
        # single-machine desktop behaviour for backward compatibility.
        from rapidqcms.dashboard.app import app
        _open_browser("127.0.0.1", 8050)
        app.run_server(threaded=False, debug=False, port=8050)


if __name__ == "__main__":
    main()