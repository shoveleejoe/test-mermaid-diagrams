// app.js — main thread controller for the Pyodide worker
const statusEl = document.getElementById("status");
const vizroIframe = document.getElementById("vizro-html");
const plotlyRoot = document.getElementById("plotly-root");

// important for GitHub Pages subpath hosting: use a relative worker URL
const worker = new Worker("./py-worker.js", { type: "module" });

// simple request/response correlate by id
let nextId = 1;
const pending = new Map();

worker.onmessage = (evt) => {
  const { type, id, payload, error } = evt.data || {};
  if (type === "status") {
    statusEl.textContent = payload;
    return;
  }
  if (type === "result") {
    const resolver = pending.get(id);
    pending.delete(id);
    if (!resolver) return;

    if (error) {
      statusEl.textContent = `error: ${error}`;
      console.error(error);
      resolver.reject(new Error(error));
      return;
    }

    // Prefer Vizro HTML if present
    if (payload && payload.html) {
      const html = payload.html;
      // write the HTML into the iframe via srcdoc
      vizroIframe.srcdoc = html;
      vizroIframe.style.display = "block";
      plotlyRoot.style.display = "none";
      statusEl.textContent = "rendered Vizro HTML from Pyodide.";
      resolver.resolve(payload);
      return;
    }

    // Fallback: Plotly JSON
    if (payload && payload.plotly) {
      const { figure, layout } = payload.plotly;
      vizroIframe.style.display = "none";
      plotlyRoot.style.display = "block";
      if (window.Plotly) {
        window.Plotly.newPlot("plotly-root", figure.data, layout || figure.layout || {}, {
          responsive: true,
          displayModeBar: true
        });
        statusEl.textContent = "rendered Plotly (fallback) from Pyodide.";
      } else {
        statusEl.textContent = "plotly.js not available for fallback.";
      }
      resolver.resolve(payload);
      return;
    }

    // Nothing recognized
    statusEl.textContent = "worker returned no renderable payload.";
    resolver.resolve(payload);
  }
};

function callWorker(kind, data = {}) {
  return new Promise((resolve, reject) => {
    const id = nextId++;
    pending.set(id, { resolve, reject });
    worker.postMessage({ type: kind, id, payload: data });
  });
}

// 1) initialize pyodide + install packages (vizro attempt)
await callWorker("init", {
  pyodideIndexURL: "https://cdn.jsdelivr.net/pyodide/v0.28.2/full/",
  // You can pin specific versions here; leave as-is to try latest compatible wheels
  micropipPackages: [
    // Try Vizro first; if this fails in Pyodide, fallback code will still run.
    "vizro",
    // dash/plotly are pure-Python and typically fine; some vizro builds may already depend upon them.
    "dash",
    "plotly",
  ],
  // Preload some common scientific packages from the Pyodide distribution
  pyodidePackages: [
    "numpy",
    "pandas"
  ]
});

// 2) ask the worker to run our "vizro or plotly" one-pager
await callWorker("run_vizro_or_plotly", {
  title: "Vizro One-Page Demo",
  note: "Rendered entirely in-browser via Pyodide. Falls back to Plotly if Vizro import fails."
});
