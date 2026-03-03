"""Performance tab callbacks."""
import datetime as _dt
import json

from dash import Input, Output
from dash.exceptions import PreventUpdate

from rapidqcms.db.connection import get_session
from rapidqcms.db.settings import get_instrument_run_qc_summary, list_instruments


def register(app):

    @app.callback(
        Output("perf-instrument", "options"),
        Input("main-tabs", "active_tab"),
    )
    def populate_perf_instrument_options(active_tab):
        if active_tab != "performance":
            raise PreventUpdate
        with get_session() as session:
            instruments = list_instruments(session)
            return [{"label": i.name or i.id, "value": i.id} for i in instruments]

    @app.callback(
        Output("perf-data", "data"),
        Output("perf-status-chart", "figure"),
        Output("perf-detection-chart", "figure"),
        Output("perf-issues-chart", "figure"),
        Output("perf-scan-chart", "figure"),
        Output("perf-metabolomics-section", "style"),
        Output("perf-proteomics-section", "style"),
        Output("perf-card-runs", "children"),
        Output("perf-card-passrate", "children"),
        Output("perf-card-detection", "children"),
        Output("perf-card-lastrun", "children"),
        Input("main-tabs", "active_tab"),
        Input("perf-instrument", "value"),
        Input("perf-exp-type", "value"),
        Input("perf-date-range", "value"),
    )
    def update_performance_dashboard(active_tab, instrument_ids, exp_type, date_range):
        if active_tab != "performance":
            raise PreventUpdate

        now = _dt.datetime.now(_dt.timezone.utc)
        since = None
        if date_range == "1m":
            since = now - _dt.timedelta(days=30)
        elif date_range == "3m":
            since = now - _dt.timedelta(days=90)
        elif date_range == "6m":
            since = now - _dt.timedelta(days=180)

        with get_session() as session:
            summaries = get_instrument_run_qc_summary(
                session,
                instrument_ids=instrument_ids or None,
                experiment_type=exp_type or None,
                since=since,
            )

        from rapidqcms.dashboard.plots import (
            build_is_detection_chart,
            build_qc_issues_chart,
            build_qc_status_chart,
            build_scan_count_chart,
        )

        has_metabolomics = any(s["experiment_type"] == "metabolomics" for s in summaries)
        has_proteomics   = any(s["experiment_type"] == "proteomics"   for s in summaries)

        status_fig    = build_qc_status_chart(summaries)
        detection_fig = build_is_detection_chart(summaries) if has_metabolomics else {}
        issues_fig    = build_qc_issues_chart(summaries)    if has_metabolomics else {}
        scan_fig      = build_scan_count_chart(summaries)   if has_proteomics   else {}

        metab_style = {"display": "block"} if has_metabolomics else {"display": "none"}
        proto_style = {"display": "block"} if has_proteomics   else {"display": "none"}

        all_pass = [s["pass_rate"]         for s in summaries if s["pass_rate"] is not None]
        all_fill = [s["avg_fill_fraction"] for s in summaries if s["avg_fill_fraction"] is not None]
        last_dt  = max((s["started_at"] for s in summaries if s["started_at"]), default=None)

        return (
            json.dumps(summaries, default=str),
            status_fig, detection_fig, issues_fig, scan_fig,
            metab_style, proto_style,
            str(len(summaries)) if summaries else "—",
            f"{sum(all_pass)/len(all_pass):.0%}" if all_pass else "—",
            f"{sum(all_fill)/len(all_fill):.1%}" if all_fill else "—",
            last_dt.strftime("%Y-%m-%d") if last_dt else "—",
        )
