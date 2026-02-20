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
        from rapidqcms.AcquisitionListener import start_listener
        start_listener(args.path, args.instrument, args.run_id)

    elif args.command == "serve":
        from rapidqcms.DashWebApp import app
        if not args.no_browser:
            _open_browser(args.host, args.port)
        app.run_server(threaded=False, debug=args.debug, host=args.host, port=args.port)

    else:
        # Legacy: bare `rapidqcms` with no subcommand retains the original
        # single-machine desktop behaviour for backward compatibility.
        from rapidqcms.DashWebApp import app
        _open_browser("127.0.0.1", 8050)
        app.run_server(threaded=False, debug=False, port=8050)


if __name__ == "__main__":
    main()