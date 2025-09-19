// py-worker.js — Pyodide Web Worker with structured logging, stdout/err capture,
// performance marks, and a Vizro-or-Plotly build pipeline.
//
// This worker supports two messages:
//   { type: "init", id, payload: { pyodideIndexURL, pyodidePackages[], micropipPackages[] } }
//   { type: "build_dashboard", id, payload: { dataset: "d6yy-54nr" } }
//
// It returns:
//   { type: "result", id, payload: { html } }   // if Vizro HTML export is available
//   { type: "result", id, payload: { figures } } // Plotly JSON fallback
// or an error: { type: "result", id, error: "..." }
//
// Docs referenced in the chat message:
//
//  - Pyodide in a Web Worker (pattern, postMessage plumbing) — pyodide.org
//  - Redirecting Python stdout/stderr to JS — pyodide.setStdout / setStderr
//  - Loading packages — pyodide.loadPackage, loadPackagesFromImports, micropip.install
//  - Performance.mark/measure support in workers — MDN

let pyodide = null;

// --------------- worker-side logging & timing ---------------
function sendStatus(msg, level = "info", extra = undefined) {
  postMessage({ type: "status", payload: msg, level, extra });
}
function sendResult(id, payload, error) {
  postMessage({ type: "result", id, payload, error: error ? String(error) : undefined });
}
function wlog(level, scope, msg, extra = {}) {
  (console[level] || console.log)(`[${scope}] ${msg}`, extra);
  sendStatus(`[${scope}] ${msg}`, level, extra);
}
function wmark(name) {
  try { performance.mark(name); } catch {}
}
function wmeasure(name, start, end) {
  try { performance.measure(name, start, end); } catch {}
}

// --------------- message handler ---------------
self.onmessage = async (evt) => {
  const { type, id, payload } = evt.data || {};
  try {
    if (type === "init") {
      await initPyodideAndPackages(payload || {});
      sendResult(id, { ok: true });
      return;
    }
    if (type === "build_dashboard") {
      const out = await buildDashboard(payload || {});
      sendResult(id, out);
      return;
    }
  } catch (e) {
    console.error(e);
    wlog("error", "worker", "unhandled error", { error: String(e && e.stack ? e.stack : e) });
    sendResult(id, null, e);
  }
};

// --------------- initialization ---------------
async function initPyodideAndPackages(opts) {
  const {
    pyodideIndexURL = "https://cdn.jsdelivr.net/pyodide/v0.28.2/full/",
    pyodidePackages = [],
    micropipPackages = []
  } = opts;

  wlog("info", "init", "loading pyodide", { indexURL: pyodideIndexURL });
  wmark("init:pyodide:start");

  const { loadPyodide } = await import(`${pyodideIndexURL}pyodide.mjs`);
  pyodide = await loadPyodide({
    indexURL: pyodideIndexURL,
    // You can also pass stdout/stderr callbacks here; we’ll use setStdout/Err below.
  });

  wmark("init:pyodide:end"); wmeasure("init:pyodide", "init:pyodide:start", "init:pyodide:end");
  wlog("info", "init", "pyodide ready");

  // Redirect Python stdout/stderr → worker logs → main UI
  try {
    pyodide.setStdout({ batched: (s) => s && wlog("info", "py.stdout", s) });
    pyodide.setStderr({ batched: (s) => s && wlog("error", "py.stderr", s) });
    // pyodide.setDebug?.(true); // uncomment for very verbose diagnostics
    wlog("info", "init", "wired py stdout/stderr");
  } catch (e) {
    wlog("warn", "init", "setStdout/Err failed", { error: String(e) });
  }

  // Load packages from the Pyodide distribution (fast, prebuilt)
  for (const pkg of pyodidePackages) {
    try {
      wlog("info", "init", `load distro package`, { pkg });
      wmark(`init:pkg:${pkg}:start`);
      await pyodide.loadPackage(pkg);
      wmark(`init:pkg:${pkg}:end`); wmeasure(`init:pkg:${pkg}`, `init:pkg:${pkg}:start`, `init:pkg:${pkg}:end`);
    } catch (e) {
      wlog("warn", "init", "loadPackage failed", { pkg, error: String(e) });
    }
  }

  // Install pure-Python wheels via micropip (best-effort)
  if (micropipPackages && micropipPackages.length) {
    try {
      wlog("info", "init", "loading micropip");
      await pyodide.loadPackage("micropip");
      const code = `
import micropip, sys
pkgs = ${JSON.stringify(micropipPackages)}
for name in pkgs:
    try:
        print(f"[micropip] installing {name}…")
        await micropip.install(name)
        print(f"[micropip] ok: {name}")
    except Exception as e:
        print(f"[micropip warn] {name}: {e}", file=sys.stderr)
`;
      wmark("init:micropip:start");
      await pyodide.runPythonAsync(code);
      wmark("init:micropip:end"); wmeasure("init:micropip", "init:micropip:start", "init:micropip:end");
    } catch (e) {
      wlog("warn", "init", "micropip install phase failed", { error: String(e) });
    }
  }

  wlog("info", "init", "initialization complete");
}

// --------------- dashboard build ---------------
async function buildDashboard({ dataset = "d6yy-54nr" } = {}) {
  wlog("info", "build", "fetching & computing", { dataset });

  const py = `
import sys, json, math, itertools, logging
from datetime import datetime, timezone
import pandas as pd
import numpy as np

# ---------- logging ----------
logger = logging.getLogger("pb.pipeline")
logger.setLevel(logging.DEBUG)
_sh = logging.StreamHandler(sys.stdout)
_sh.setLevel(logging.DEBUG)
_sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
if not logger.handlers:
    logger.addHandler(_sh)
logger.info("bootstrap python logging started")

# ---------- fetch ----------
base = f"https://data.ny.gov/resource/${dataset}.json?$select=draw_date,winning_numbers,multiplier&$order=draw_date%20ASC&$limit=50000"
logger.info("fetch begin", extra={"stage":"fetch","endpoint":base})
df = pd.read_json(base)
logger.info("fetch ok", extra={"rows": int(df.shape[0])})

# normalize draw_date to date (UTC->date)
df["draw_date"] = pd.to_datetime(df["draw_date"], utc=True).dt.date

# ---------- parse ----------
logger.info("parse numbers begin")
parts = df["winning_numbers"].str.split(" ", expand=True).astype(int)
whites = parts[[0,1,2,3,4]]
powerball = parts[5]
white_df = pd.DataFrame({
    "draw_date": df["draw_date"].repeat(5).values,
    "white_ball": whites.stack().reset_index(level=1, drop=True).values
})
pb_df = pd.DataFrame({"draw_date": df["draw_date"], "powerball": powerball})
logger.info("parse numbers done", extra={"white_rows": int(white_df.shape[0])})

# ---------- aggregates ----------
today = df["draw_date"].max()
last_seen = white_df.groupby("white_ball")["draw_date"].max()
overdue = (today - last_seen).dt.days.sort_values(ascending=False)
overdue_df = overdue.reset_index(names="white_ball").rename(columns={0:"days"})

df["year"] = pd.to_datetime(df["draw_date"]).dt.year
pp_year = df.groupby(["year","multiplier"]).size().reset_index(name="count")

df["dow"] = pd.to_datetime(df["draw_date"]).dt.day_name()
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
pairs_df = pd.DataFrame([(a,b,c) for ((a,b),c) in top_pairs], columns=["a","b","count"])

# Sum & spread over time
sums = whites.sum(axis=1)
spreads = whites.max(axis=1) - whites.min(axis=1)
trend_df = pd.DataFrame({"draw_date": df["draw_date"], "sum": sums, "spread": spreads})

# ---------- figures (Plotly), attempt Vizro export if available ----------
result = {}

try:
    import plotly.express as px
    import plotly.graph_objects as go

    fig_white_hist = px.histogram(white_df, x="white_ball", nbins=69, title="White-ball Frequency (1–69)")
    fig_pb_hist    = px.histogram(pb_df,   x="powerball",  nbins=26, title="Powerball Frequency (1–26)")
    fig_overdue    = px.bar(overdue_df, x="white_ball", y="days", title=f"Overdue (days) as of {today}")
    fig_pp_year    = px.bar(pp_year, x="year", y="count", color="multiplier", barmode="stack", title="Power Play usage by year")
    fig_dow        = px.bar(dow, title="Draws by Day of Week")

    mat = pairs_df.pivot_table(index="a", columns="b", values="count").fillna(0)
    fig_pairs_heatmap = go.Figure(data=go.Heatmap(z=mat.values, x=mat.columns, y=mat.index, colorbar=dict(title="count")))
    fig_pairs_heatmap.update_layout(title="Top Pair Combinations (counts)")

    fig_sum_spread = go.Figure()
    fig_sum_spread.add_trace(go.Scatter(x=trend_df["draw_date"], y=trend_df["sum"],    mode="lines", name="sum(whites)"))
    fig_sum_spread.add_trace(go.Scatter(x=trend_df["draw_date"], y=trend_df["spread"], mode="lines", name="spread(max-min)"))
    fig_sum_spread.update_layout(title="Sum & Spread over Time")

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

        # If your Vizro build exposes an HTML export (e.g., app.to_html()), flip CAN_EXPORT=True.
        CAN_EXPORT = False
        if CAN_EXPORT:
            html = app.to_html()  # replace with the correct API for your version
            result["html"] = html
        else:
            raise RuntimeError("vizro export disabled; returning plotly JSON fallback")

    except Exception as vizro_err:
        logger.info(f"vizro export unavailable -> fallback: {vizro_err}")
        result["figures"] = {
            "white_hist": json.loads(fig_white_hist.to_json()),
            "pb_hist":    json.loads(fig_pb_hist.to_json()),
            "overdue":    json.loads(fig_overdue.to_json()),
            "pp_year":    json.loads(fig_pp_year.to_json()),
            "dow":        json.loads(fig_dow.to_json()),
            "pairs_heatmap": json.loads(fig_pairs_heatmap.to_json()),
            "sum_spread":    json.loads(fig_sum_spread.to_json()),
        }

except Exception as e:
    result = {"error": str(e)}

json.dumps(result)
`;

  wmark("build:py:start");
  const out = await pyodide.runPythonAsync(py);
  wmark("build:py:end"); wmeasure("build:py", "build:py:start", "build:py:end");

  const obj = JSON.parse(out);

  if (obj && obj.html) {
    wlog("info", "build", "produced Vizro HTML");
    return { html: obj.html };
  }
  if (obj && obj.figures) {
    wlog("info", "build", "produced Plotly figures", {
      keys: Object.keys(obj.figures || {})
    });
    return { figures: obj.figures };
  }
  if (obj && obj.error) {
    wlog("error", "build", "python error", { error: obj.error });
    throw new Error(obj.error);
  }

  wlog("warn", "build", "no payload generated");
  return {};
}
