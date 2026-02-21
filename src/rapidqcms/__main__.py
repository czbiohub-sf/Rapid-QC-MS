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