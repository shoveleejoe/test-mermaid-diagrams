# dashboard.py
import sys, json, itertools, logging
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from pyodide.http import open_url  # file-like for pandas.read_json (Pyodide)  # docs: open_url returns StringIO

# Vizro (models + PX wrapper + capture for custom charts)
import vizro.models as vm
import vizro.plotly.express as vpx
from vizro.models.types import capture

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
    # open_url -> StringIO-like; pandas.read_json accepts file-like objects
    df = pd.read_json(open_url(base))
    logger.info("fetch ok")

    # timezone-aware UTC datetimes
    df["draw_date"] = pd.to_datetime(df["draw_date"], utc=True)  # tz-aware UTC

    # ---------- parse numbers ----------
    logger.info("parse numbers begin")
    parts = df["winning_numbers"].str.split(" ", expand=True).astype(int)
    whites = parts[[0, 1, 2, 3, 4]]
    powerball = parts[5]

    # preserve tz-aware dtype by not using .values on draw_date
    white_df = pd.DataFrame({
        "draw_date": df["draw_date"].repeat(5).reset_index(drop=True),
        "white_ball": (
            whites.stack()
                  .reset_index(level=1, drop=True)
                  .astype(int)
                  .reset_index(drop=True)
        ),
    })
    pb_df = pd.DataFrame({"draw_date": df["draw_date"], "powerball": powerball})
    logger.info("parse numbers done", extra={"white_rows": int(white_df.shape[0])})

    # ---------- timezone guard ----------
    if getattr(white_df["draw_date"].dtype, "tz", None) is None:
        white_df["draw_date"] = white_df["draw_date"].dt.tz_localize("UTC")
    else:
        white_df["draw_date"] = white_df["draw_date"].dt.tz_convert("UTC")

    # ---------- sanity logs ----------
    logger.info(f"dtypes after parse: {df.dtypes.to_dict()}")
    logger.info(f"draw_date dtype: {df['draw_date'].dtype}")                # expect datetime64[ns, UTC]
    logger.info(f"white_df.draw_date dtype: {white_df['draw_date'].dtype}") # expect datetime64[ns, UTC]

    # ---------- aggregates ----------
    today = df["draw_date"].max().tz_convert("UTC")

    last_seen = white_df.groupby("white_ball")["draw_date"].max()
    logger.info(f"last_seen dtype: {last_seen.dtype}")                      # expect datetime64[ns, UTC]
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
    dow = df["dow"].value_counts().sort_index().rename_axis("dow").reset_index(name="count")

    # --- combinations heatmap prep (top 30 pairs) ---
    pair_counts = {}
    for i in range(whites.shape[0]):
        balls = sorted(whites.iloc[i].values)
        for a, b in itertools.combinations(balls, 2):
            pair_counts[(a, b)] = pair_counts.get((a, b), 0) + 1
    top_pairs = sorted(pair_counts.items(), key=lambda kv: kv[1], reverse=True)[:30]
    pairs_df = pd.DataFrame([(a, b, c) for ((a, b), c) in top_pairs], columns=["a", "b", "count"])

    # --- sum & spread over time ---
    sums = whites.sum(axis=1)
    spreads = whites.max(axis=1) - whites.min(axis=1)
    trend_df = pd.DataFrame({"draw_date": df["draw_date"], "sum": sums, "spread": spreads})

    # ---------- Captured custom charts (for GO / custom logic) ----------
    @capture("graph")
    def pairs_heatmap(data_frame):
        import plotly.graph_objects as go
        mat = data_frame.pivot_table(index="a", columns="b", values="count").fillna(0)
        fig = go.Figure(go.Heatmap(z=mat.values, x=mat.columns, y=mat.index, colorbar=dict(title="count")))
        fig.update_layout(title="Top Pair Combinations (counts)")
        return fig

    @capture("graph")
    def sum_spread_lines(data_frame):
        import plotly.graph_objects as go
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=data_frame["draw_date"], y=data_frame["sum"], mode="lines", name="sum(whites)"))
        fig.add_trace(go.Scatter(x=data_frame["draw_date"], y=data_frame["spread"], mode="lines", name="spread(max-min)"))
        fig.update_layout(title="Sum & Spread over Time")
        return fig

    # ---------- Vizro pages (use Vizro PX wrapper; pass data_frame ONCE via keyword) ----------
    page1 = vm.Page(
        title="Explorer",
        components=[
            vm.Graph(figure=vpx.histogram(data_frame=white_df, x="white_ball", nbins=69,
                                          title="White-ball Frequency (1–69)")),
            vm.Graph(figure=vpx.histogram(data_frame=pb_df, x="powerball", nbins=26,
                                          title="Powerball Frequency (1–26)")),
            vm.Graph(figure=vpx.bar(data_frame=overdue_df, x="white_ball", y="days",
                                    title=f"Overdue (days) as of {today.date()}")),
            vm.Graph(figure=vpx.bar(data_frame=pp_year, x="year", y="count", color="multiplier",
                                    barmode="stack", title="Power Play usage by year")),
            vm.Graph(figure=vpx.bar(data_frame=dow, x="dow", y="count",
                                    title="Draws by Day of Week")),
        ],
    )

    page2 = vm.Page(
        title="Combos & Trends",
        components=[
            vm.Graph(figure=pairs_heatmap(data_frame=pairs_df)),
            vm.Graph(figure=sum_spread_lines(data_frame=trend_df)),
        ],
    )

    dashboard = vm.Dashboard(pages=[page1, page2])

    # ---------- Optional Vizro export (keep False under Pyodide) ----------
    CAN_EXPORT = False
    try:
        assert isinstance(CAN_EXPORT, bool)
        if CAN_EXPORT:
            return {"engine": "vizro", "html": dashboard.to_html()}
    except Exception as vizro_err:
        logger.info(f"vizro export unavailable -> fallback: {vizro_err}")

    # ---------- Plotly JSON fallback ----------
    import plotly.express as px
    import plotly.graph_objects as go

    fig_white_hist = px.histogram(white_df, x="white_ball", nbins=69, title="White-ball Frequency (1–69)")
    fig_pb_hist = px.histogram(pb_df, x="powerball", nbins=26, title="Powerball Frequency (1–26)")
    fig_overdue = px.bar(overdue_df, x="white_ball", y="days", title=f"Overdue (days) as of {today.date()}")
    fig_pp_year = px.bar(pp_year, x="year", y="count", color="multiplier", barmode="stack", title="Power Play usage by year")
    fig_dow = px.bar(dow, x="dow", y="count", title="Draws by Day of Week")

    mat = pairs_df.pivot_table(index="a", columns="b", values="count").fillna(0)
    fig_pairs_heatmap = go.Figure(go.Heatmap(z=mat.values, x=mat.columns, y=mat.index, colorbar=dict(title="count")))
    fig_pairs_heatmap.update_layout(title="Top Pair Combinations (counts)")

    fig_sum_spread = go.Figure()
    fig_sum_spread.add_trace(go.Scatter(x=trend_df["draw_date"], y=trend_df["sum"], mode="lines", name="sum(whites)"))
    fig_sum_spread.add_trace(go.Scatter(x=trend_df["draw_date"], y=trend_df["spread"], mode="lines", name="spread(max-min)"))
    fig_sum_spread.update_layout(title="Sum & Spread over Time")

    return {
        "engine": "plotly",
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
