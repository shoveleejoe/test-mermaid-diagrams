// Pyodide Worker: loads Pyodide, fetches NY Powerball via SODA (SoQL),
// computes Explorer figs + Combos&Trends figs, and returns either:
// - Vizro HTML (if available), or
// - Plotly figure JSON (fallback).

let pyodide = null;

function sendStatus(msg){ postMessage({ type: "status", payload: msg }); }
function sendResult(id, payload, error){
  postMessage({ type: "result", id, payload, error: error ? String(error) : undefined });
}

self.onmessage = async (evt) => {
  const { type, id, payload } = evt.data || {};
  try {
    if (type === "init"){
      await initPyodide(payload);
      sendResult(id, { ok: true });
    } else if (type === "build_dashboard"){
      const res = await buildDashboard(payload);
      sendResult(id, res);
    }
  } catch (e){
    console.error(e);
    sendResult(id, null, e);
  }
};

async function initPyodide({ pyodideIndexURL, pyodidePackages = [], micropipPackages = [] }){
  sendStatus("loading pyodide…");
  const { loadPyodide } = await import("https://cdn.jsdelivr.net/pyodide/v0.28.2/full/pyodide.mjs");
  pyodide = await loadPyodide({ indexURL: pyodideIndexURL });
  sendStatus("pyodide ready.");

  for (const pkg of pyodidePackages){
    try { sendStatus("loading distro pkg: " + pkg); await pyodide.loadPackage(pkg); }
    catch (e){ console.warn("loadPackage failed:", pkg, e); }
  }

  if (micropipPackages.length){
    await pyodide.loadPackage("micropip");
    const code = `
import micropip
pkgs = ${JSON.stringify(micropipPackages)}
for name in pkgs:
    try:
        await micropip.install(name)
    except Exception as e:
        print(f"[micropip warn] {name} -> {e}")
`;
    await pyodide.runPythonAsync(code);
  }
  sendStatus("initialization complete.");
}

// Build two-tab content.
// If Vizro can export static HTML (client-side), return {"html": "..."}.
// Otherwise return {"figures": {...}} with Plotly JSON figs (fallback).
async function buildDashboard({ dataset }){
  sendStatus("fetching & computing…");
  const py = `
import json, itertools, math
from datetime import datetime, timezone
import pandas as pd
import numpy as np

# 1) Load data via Socrata SODA (SoQL). We request relevant fields only. :contentReference[oaicite:3]{index=3}
base = "https://data.ny.gov/resource/${dataset}.json?$select=draw_date,winning_numbers,multiplier&$order=draw_date%20ASC&$limit=50000"
df = pd.read_json(base)
df["draw_date"] = pd.to_datetime(df["draw_date"], utc=True).dt.date

# Parse winning numbers: 5 whites + 1 powerball
parts = df["winning_numbers"].str.split(" ", expand=True).astype(int)
whites = parts[[0,1,2,3,4]]
powerball = parts[5]
white_df = pd.DataFrame({
    "draw_date": df["draw_date"].repeat(5).values,
    "white_ball": whites.stack().reset_index(level=1, drop=True).values
})
pb_df = pd.DataFrame({"draw_date": df["draw_date"], "powerball": powerball})

# Helper: overdue days (relative to latest draw_date)
today = df["draw_date"].max()
last_seen = white_df.groupby("white_ball")["draw_date"].max()
overdue = (today - last_seen).dt.days.sort_values(ascending=False)
overdue_df = overdue.reset_index(names="white_ball").rename(columns={0:"days"})

# Power Play by year
df["year"] = pd.to_datetime(df["draw_date"]).dt.year
pp_year = df.groupby(["year","multiplier"]).size().reset_index(name="count")

# Day of week
df["dow"] = pd.to_datetime(df["draw_date"]).dt.day_name()
dow = df["dow"].value_counts().sort_index()

# --- Combos & Trends ---
# Top 30 two-number combinations among white balls
def row_pairs(row):
    balls = row.values
    return list(itertools.combinations(sorted(balls), 2))
pair_counts = {}
for i in range(whites.shape[0]):
    for pair in row_pairs(whites.iloc[i]):
        pair_counts[pair] = pair_counts.get(pair, 0) + 1
top_pairs = sorted(pair_counts.items(), key=lambda kv: kv[1], reverse=True)[:30]
pairs_df = pd.DataFrame([(a,b,c) for ((a,b),c) in top_pairs], columns=["a","b","count"])

# Sum & spread over time
sums = whites.sum(axis=1)
spreads = whites.max(axis=1) - whites.min(axis=1)
trend_df = pd.DataFrame({"draw_date": df["draw_date"], "sum": sums, "spread": spreads})

# Try Vizro first (if present). Otherwise, serialize Plotly figs for fallback.
result = {}

try:
    import plotly.express as px
    import plotly.graph_objects as go

    # Explorer figs
    fig_white_hist = px.histogram(white_df, x="white_ball", nbins=69, title="White-ball Frequency (1–69)")
    fig_pb_hist = px.histogram(pb_df, x="powerball", nbins=26, title="Powerball Frequency (1–26)")
    fig_overdue = px.bar(overdue_df, x="white_ball", y="days", title=f"Overdue (days) as of {today}")
    fig_pp_year = px.bar(pp_year, x="year", y="count", color="multiplier", barmode="stack", title="Power Play usage by year")
    fig_dow = px.bar(dow, title="Draws by Day of Week")

    # Heatmap of top pairs
    # Build a square-ish matrix using the 30 pairs (sparse); or show as annotated heatmap by pivoting on a,b.
    mat = pairs_df.pivot_table(index="a", columns="b", values="count").fillna(0)
    fig_pairs_heatmap = go.Figure(data=go.Heatmap(z=mat.values, x=mat.columns, y=mat.index, colorbar=dict(title="count")))
    fig_pairs_heatmap.update_layout(title="Top Pair Combinations (counts)")

    # Sum/spread vs time
    fig_sum_spread = go.Figure()
    fig_sum_spread.add_trace(go.Scatter(x=trend_df["draw_date"], y=trend_df["sum"], mode="lines", name="sum(whites)"))
    fig_sum_spread.add_trace(go.Scatter(x=trend_df["draw_date"], y=trend_df["spread"], mode="lines", name="spread(max-min)"))
    fig_sum_spread.update_layout(title="Sum & Spread over Time")

    # Attempt Vizro → two-page (two-tab) dashboard
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

        # NOTE: Depending on Vizro version, a direct "export to HTML" API may or may not be available.
        # If available in your build, replace the next two lines with that API (e.g., app.to_html()).
        # Otherwise we fall back to Plotly JSON for rendering in JS. :contentReference[oaicite:4]{index=4}
        CAN_EXPORT = False
        if CAN_EXPORT:
            html = app.to_html()  # (placeholder if your version supports it)
            result["html"] = html
        else:
            raise RuntimeError("no vizro export; use plotly fallback")

    except Exception as e:
        # Plotly fallback payload (both tabs)
        result["figures"] = {
            "white_hist": json.loads(fig_white_hist.to_json()),
            "pb_hist": json.loads(fig_pb_hist.to_json()),
            "overdue": json.loads(fig_overdue.to_json()),
            "pp_year": json.loads(fig_pp_year.to_json()),
            "dow": json.loads(fig_dow.to_json()),
            "pairs_heatmap": json.loads(fig_pairs_heatmap.to_json()),
            "sum_spread": json.loads(fig_sum_spread.to_json()),
        }

except Exception as e:
    result = {"error": str(e)}

json.dumps(result)
`;
  const out = await pyodide.runPythonAsync(py);
  return JSON.parse(out);
}
