# dashboard.py
import sys, json, math, itertools, logging
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from pyodide.http import open_url  # browser-aware, returns a file-like StringIO (sync)  # docs: pyodide.http.open_url

# ---------- logging ----------
logger = logging.getLogger("pb.pipeline")
logger.setLevel(logging.DEBUG)
_sh = logging.StreamHandler(sys.stdout)
_sh.setLevel(logging.DEBUG)
_sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
if not logger.handlers:
    logger.addHandler(_sh)

def build_dashboard(dataset: str = "d6yy-54nr"):
    logger.info("bootstrap python logging started")

    # ---------- fetch ----------
    base = (
        f"https://data.ny.gov/resource/{dataset}.json?"
        "$select=draw_date,winning_numbers,multiplier&"
        "$order=draw_date%20ASC&$limit=50000"
    )
    logger.info("fetch begin")
    # open_url -> file-like object that pandas.read_json can read synchronously
    df = pd.read_json(open_url(base))  # pyodide docs confirm this returns StringIO-compatible object
    logger.info("fetch ok")

    # Keep draw_date as *timezone-aware UTC* (pandas returns datetime64[ns, UTC] with utc=True)
    df["draw_date"] = pd.to_datetime(df["draw_date"], utc=True)  # docs: pandas.to_datetime(utc=True)

    # ---------- parse numbers ----------
    logger.info("parse numbers begin")
    parts = df["winning_numbers"].str.split(" ", expand=True).astype(int)
    whites = parts[[0, 1, 2, 3, 4]]
    powerball = parts[5]

    # Important: preserve tz-aware dtype by NOT using .values on draw_date
    white_df = pd.DataFrame({
        "draw_date": df["draw_date"].repeat(5).reset_index(drop=True),
        "white_ball": whites.stack().reset_index(level=1, drop=True).astype(int).reset_index(drop=True),
    })
    pb_df = pd.DataFrame({"draw_date": df["draw_date"], "powerball": powerball})
    logger.info("parse numbers done", extra={"white_rows": int(white_df.shape[0])})

    # ---------- timezone hardening (guard) ----------
    # If white_df['draw_date'] somehow became naive, localize; if aware, convert to UTC.
    if getattr(white_df["draw_date"].dtype, "tz", None) is None:
        white_df["draw_date"] = white_df["draw_date"].dt.tz_localize("UTC")   # localize naive → aware (no clock shift)
    else:
        white_df["draw_date"] = white_df["draw_date"].dt.tz_convert("UTC")    # convert aware → UTC (preserve instant)

    # ---------- sanity logs ----------
    logger.info(f"dtypes after parse: {df.dtypes.to_dict()}")
    logger.info(f"draw_date dtype: {df['draw_date'].dtype}")                  # expect datetime64[ns, UTC]
    logger.info(f"white_df.draw_date dtype: {white_df['draw_date'].dtype}")   # expect datetime64[ns, UTC]

    # ---------- aggregates ----------
    # Be explicit about UTC for 'today'
    today = df["draw_date"].max().tz_convert("UTC")

    last_seen = white_df.groupby("white_ball")["draw_date"].max()
    logger.info(f"last_seen dtype: {last_seen.dtype}")                         # expect datetime64[ns, UTC]

    overdue = (today - last_seen).dt.days
    overdue_df = (
        overdue.sort_values(ascending=False)
               .rename("days")
               .reset_index()
               .rename(columns={"index": "white_ball"})
    )

    df["year"] = df["draw_date"].dt.year
    df["dow"] = df["draw_date"].dt.day_name()
    pp_year = df.groupby(["year", "multiplier"]).size().reset_index(name="count")
    dow = df["dow"].value_counts().sort_index()

    # Top 30 white-ball pairs
    def row_pairs(row):
        balls = row.values
        return list(itertools.combinations(sorted(balls), 2))

    pair_counts = {}
    for i in range(whites.shape[0]):
        for pair in row_pairs(whites.iloc[i]):
            pair_counts[pair] = pair_counts.get(pair, 0) + 1
    top_pairs = sorted(pair_counts.items(), key=lambda kv: kv[1], reverse=True)[:30]
    pairs_df = pd.DataFrame([(a, b, c) for ((a, b), c) in top_pairs], columns=["a", "b", "count"])

    # Sum & spread over time
    sums = whites.sum(axis=1)
    spreads = whites.max(axis=1) - whites.min(axis=1)
    trend_df = pd.DataFrame({"draw_date": df["draw_date"], "sum": sums, "spread": spreads})

    # ---------- figures ----------
    import plotly.express as px
    import plotly.graph_objects as go

    fig_white_hist = px.histogram(white_df, x="white_ball", nbins=69, title="White-ball Frequency (1–69)")
    fig_pb_hist = px.histogram(pb_df, x="powerball", nbins=26, title="Powerball Frequency (1–26)")
    fig_overdue = px.bar(overdue_df, x="white_ball", y="days", title=f"Overdue (days) as of {today.date()}")
    fig_pp_year = px.bar(pp_year, x="year", y="count", color="multiplier", barmode="stack", title="Power Play usage by year")
    fig_dow = px.bar(dow, title="Draws by Day of Week")

    mat = pairs_df.pivot_table(index="a", columns="b", values="count").fillna(0)
    fig_pairs_heatmap = go.Figure(
        data=go.Heatmap(z=mat.values, x=mat.columns, y=mat.index, colorbar=dict(title="count"))
    )
    fig_pairs_heatmap.update_layout(title="Top Pair Combinations (counts)")

    fig_sum_spread = go.Figure()
    fig_sum_spread.add_trace(go.Scatter(x=trend_df["draw_date"], y=trend_df["sum"], mode="lines", name="sum(whites)"))
    fig_sum_spread.add_trace(go.Scatter(x=trend_df["draw_date"], y=trend_df["spread"], mode="lines", name="spread(max-min)"))
    fig_sum_spread.update_layout(title="Sum & Spread over Time")

    # ---------- (optional) Vizro export ----------
    try:
        import vizro
        import vizro.components as vz
        from vizro.models import Page, Dashboard

        page1 = Page(
            title="Explorer",
            components=[
                vz.Graph(figure=fig_white_hist),
                vz.Graph(figure=fig_pb_hist),
                vz.Graph(figure=fig_overdue),
                vz.Graph(figure=fig_pp_year),
                vz.Graph(figure=fig_dow),
            ],
        )
        page2 = Page(
            title="Combos & Trends",
            components=[
                vz.Graph(figure=fig_pairs_heatmap),
                vz.Graph(figure=fig_sum_spread),
            ],
        )
        app = Dashboard(pages=[page1, page2])

        CAN_EXPORT = False
        assert isinstance(CAN_EXPORT, bool)
        if CAN_EXPORT:
            return {"html": app.to_html()}  # adjust for your Vizro version if different
    except Exception as vizro_err:
        logger.info(f"vizro export unavailable -> fallback: {vizro_err}")

    # ---------- JSON fallback for front-end rendering ----------
    return {
        "figures": {
            "white_hist": json.loads(fig_white_hist.to_json()),
            "pb_hist": json.loads(fig_pb_hist.to_json()),
            "overdue": json.loads(fig_overdue.to_json()),
            "pp_year": json.loads(fig_pp_year.to_json()),
            "dow": json.loads(fig_dow.to_json()),
            "pairs_heatmap": json.loads(fig_pairs_heatmap.to_json()),
            "sum_spread": json.loads(fig_sum_spread.to_json()),
        }
    }
