const statusEl = document.getElementById("status");
const iframe = document.getElementById("vizro-iframe");
const tabsEl = document.getElementById("tabs");
const panels = Array.from(document.querySelectorAll(".panel"));

function setStatus(t){ statusEl.textContent = t; }

// Basic tabs (fallback UI)
tabsEl.addEventListener("click", (e) => {
  if (e.target.tagName !== "BUTTON") return;
  const id = e.target.getAttribute("data-tab");
  for (const btn of tabsEl.querySelectorAll("button")) btn.classList.toggle("active", btn === e.target);
  for (const p of panels) p.classList.toggle("active", p.id === id);
});

// Worker plumbing
const worker = new Worker("./py-worker.js", { type: "module" });
let seq = 1;
const waits = new Map();
worker.onmessage = (evt) => {
  const { type, id, payload, error } = evt.data || {};
  if (type === "status"){ setStatus(payload); return; }
  if (type === "result"){
    const w = waits.get(id); waits.delete(id);
    if (!w) return;
    if (error){ w.reject(new Error(error)); setStatus("error: " + error); return; }
    w.resolve(payload);
  }
};
function call(kind, payload){
  return new Promise((resolve, reject) => {
    const id = seq++;
    waits.set(id, { resolve, reject });
    worker.postMessage({ type: kind, id, payload });
  });
}

// 1) Initialize Pyodide + packages
await call("init", {
  pyodideIndexURL: "https://cdn.jsdelivr.net/pyodide/v0.28.2/full/",
  pyodidePackages: ["numpy", "pandas"],    // distro packages (fast) :contentReference[oaicite:2]{index=2}
  micropipPackages: ["vizro", "plotly", "dash"] // pure-Python wheels via micropip (best-effort)
});

// 2) Ask worker to build Vizro HTML (if possible) OR return Plotly JSON for both tabs
const out = await call("build_dashboard", { dataset: "d6yy-54nr" });

if (out && out.html){
  // Vizro (WASM) path: show exported HTML in iframe (if your Vizro version provides this)
  iframe.srcdoc = out.html;
  iframe.style.display = "block";
  // hide fallback UI
  tabsEl.style.display = "none";
  for (const p of panels) p.style.display = "none";
  setStatus("rendered Vizro dashboard via Pyodide (WASM).");
} else if (out && out.figures) {
  // Fallback: render Plotly figs for both tabs
  const figs = out.figures;
  const P = window.Plotly;
  const plot = (id, fig) => P.newPlot(id, fig.data, fig.layout || {}, { responsive: true });
  await plot("fig_white_hist", figs.white_hist);
  await plot("fig_pb_hist", figs.pb_hist);
  await plot("fig_overdue", figs.overdue);
  await plot("fig_pp_year", figs.pp_year);
  await plot("fig_dow", figs.dow);
  await plot("fig_pairs_heatmap", figs.pairs_heatmap);
  await plot("fig_sum_spread", figs.sum_spread);
  setStatus("rendered Plotly fallback (two tabs) from Pyodide worker.");
} else {
  setStatus("No renderable payload from worker.");
}
