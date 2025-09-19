// app.js — main thread controller for Vizro + Pyodide (Web Worker)
// - Structured logging panel + console
// - Performance marks/measures
// - Worker plumbing with robust error handling
// - Vizro-HTML (iframe) path OR Plotly fallback (two tabs)
//
// References:
//   • Pyodide in a Web Worker: https://pyodide.org/en/stable/usage/webworker.html
//   • Redirecting Python stdout/stderr to JS: https://pyodide.org/en/stable/usage/streams.html
//   • Performance.mark/measure (works in workers and main thread): MDN docs
// (See citations in the chat message.)

// ---------- DOM hooks ----------
const statusEl = document.getElementById("status");
const iframe = document.getElementById("vizro-iframe");
const tabsEl = document.getElementById("tabs");
const panels = Array.from(document.querySelectorAll(".panel"));
const logView = document.getElementById("log-view");

// ---------- Logging helpers ----------
function setStatus(t) {
  statusEl.textContent = t;
}
function logLine(level, scope, msg, extra = {}) {
  const entry = { t: new Date().toISOString(), lvl: level, scope, msg, ...extra };
  try {
    (console[level] || console.log)(`[${scope}] ${msg}`, extra);
  } catch {
    console.log(`[${scope}] ${msg}`, extra);
  }
  if (logView) {
    logView.textContent += JSON.stringify(entry) + "\n";
    // Keep panel from growing unbounded
    if (logView.textContent.length > 200_000) {
      logView.textContent = logView.textContent.slice(-150_000);
    }
  }
}

// ---------- Performance helpers ----------
function mark(name) {
  try { performance.mark(name); } catch {}
}
function measure(name, start, end) {
  try { performance.measure(name, start, end); } catch {}
}

// ---------- Simple tabs for Plotly fallback ----------
tabsEl.addEventListener("click", (e) => {
  if (e.target.tagName !== "BUTTON") return;
  const id = e.target.getAttribute("data-tab");
  for (const btn of tabsEl.querySelectorAll("button")) {
    btn.classList.toggle("active", btn === e.target);
  }
  for (const p of panels) {
    p.classList.toggle("active", p.id === id);
  }
});

// ---------- Worker plumbing ----------
const worker = new Worker("./py-worker.js", { type: "module" });
let seq = 1;
const waits = new Map();

worker.addEventListener("error", (e) => {
  logLine("error", "worker", "uncaught worker error", {
    message: e.message, filename: e.filename, lineno: e.lineno, colno: e.colno
  });
  setStatus("worker error — see console/logs");
});
worker.addEventListener("messageerror", (e) => {
  logLine("error", "worker", "message deserialization failed", { data: e.data });
  setStatus("worker messageerror — see console/logs");
});

worker.onmessage = (evt) => {
  const { type, id, payload, error, level, extra } = evt.data || {};
  if (type === "status") {
    // Worker status updates (and Python stdout/stderr relayed by worker)
    logLine(level || "info", "worker:status", String(payload ?? ""), extra);
    setStatus(String(payload ?? ""));
    return;
  }
  if (type === "result") {
    const w = waits.get(id);
    waits.delete(id);
    if (!w) return;
    if (error) {
      logLine("error", "worker:result", "error payload", { error });
      setStatus("error: " + error);
      w.reject(new Error(error));
      return;
    }
    w.resolve(payload);
  }
};

function call(kind, payload) {
  return new Promise((resolve, reject) => {
    const id = seq++;
    waits.set(id, { resolve, reject });
    worker.postMessage({ type: kind, id, payload });
  });
}

// ---------- Boot sequence ----------
try {
  logLine("info", "main", "booting app");
  setStatus("booting…");

  // 1) Initialize Pyodide + packages
  mark("init:start");
  logLine("info", "init", "requesting worker init");

  await call("init", {
    pyodideIndexURL: "https://cdn.jsdelivr.net/pyodide/v0.28.2/full/",
    // Prefer Pyodide distro packages for speed & compatibility.
    pyodidePackages: ["numpy", "pandas"],
    // Use micropip only for pure-Python wheels; installation is best-effort.
    micropipPackages: ["vizro", "plotly", "dash"]
  });

  mark("init:end"); measure("init", "init:start", "init:end");
  logLine("info", "init", "worker init complete");

  // 2) Build dashboard (Vizro HTML when available, else Plotly JSON)
  mark("build:start");
  logLine("info", "build", "requesting dashboard build");
  const out = await call("build_dashboard", { dataset: "d6yy-54nr" });
  mark("build:end"); measure("build", "build:start", "build:end");

  // 3) Render
  if (out && out.html) {
    // Vizro (WASM) path: self-contained HTML
    logLine("info", "render", "rendering Vizro HTML in iframe");
    iframe.srcdoc = out.html;
    iframe.style.display = "block";
    // Hide fallback UI
    tabsEl.style.display = "none";
    for (const p of panels) p.style.display = "none";
    setStatus("rendered Vizro dashboard via Pyodide (WASM).");
  } else if (out && out.figures) {
    // Plotly fallback: render both tabs
    const figs = out.figures;
    const P = window.Plotly;
    if (!P) {
      setStatus("plotly.js not loaded");
      logLine("error", "render", "plotly missing");
      throw new Error("plotly not available");
    }

    const plot = async (id, fig) => {
      // Safety: ensure target exists
      const el = document.getElementById(id);
      if (!el) {
        logLine("warn", "render", `target #${id} not found`);
        return;
      }
      console.time(`plot:${id}`);
      await P.newPlot(el, fig.data, fig.layout || {}, { responsive: true, displayModeBar: true });
      console.timeEnd(`plot:${id}`);
      logLine("info", "render", `plotted #${id}`, {
        traces: Array.isArray(fig.data) ? fig.data.length : 0
      });
    };

    await plot("fig_white_hist", figs.white_hist);
    await plot("fig_pb_hist", figs.pb_hist);
    await plot("fig_overdue", figs.overdue);
    await plot("fig_pp_year", figs.pp_year);
    await plot("fig_dow", figs.dow);
    await plot("fig_pairs_heatmap", figs.pairs_heatmap);
    await plot("fig_sum_spread", figs.sum_spread);

    setStatus("rendered Plotly fallback (two tabs) from Pyodide worker.");
  } else {
    setStatus("no renderable payload from worker");
    logLine("warn", "render", "no payload");
  }
} catch (err) {
  logLine("error", "main", "fatal error", { error: String(err && err.stack ? err.stack : err) });
  setStatus("fatal error — see console/logs");
}
