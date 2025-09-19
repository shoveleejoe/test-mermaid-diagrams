// py-worker.js — Pyodide Web Worker with module-based Python + hot-reload
//
// What this worker does:
// - Loads Pyodide in a Web Worker (official pattern)                     [docs]
// - Fetches ./dashboard.py, writes it to the Pyodide VFS, imports it     [docs]
// - Hot-reloads the module on each build (invalidate caches + reimport)   [docs]
// - Calls dashboard.build_dashboard(dataset) and returns either HTML or Plotly JSON
//
// Refs:
// • Web worker usage: https://pyodide.org/en/stable/usage/webworker.html
// • Virtual FS + loading custom code: https://pyodide.org/en/stable/usage/loading-custom-python-code.html
// • VFS behavior: https://pyodide.org/en/stable/usage/file-system.html
// • pyodide.pyimport / JS API: https://pyodide.org/en/stable/usage/api/js-api.html
// • importlib.reload / sys.modules: https://docs.python.org/3/library/importlib.html
// • pyodide.http.open_url (used inside dashboard.py): https://pyodide.org/en/stable/usage/api/python-api/http.html

let pyodide = null;

/* ---------------- logging helpers ---------------- */
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
function wmark(name) { try { performance.mark(name); } catch {} }
function wmeasure(name, start, end) { try { performance.measure(name, start, end); } catch {} }

/* ---------------- message handler ---------------- */
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
    if (type === "hot_reload") {
      // optional control message to just re-fetch/re-import module
      await ensureDashboardModule(true);
      sendResult(id, { reloaded: true });
      return;
    }
  } catch (e) {
    console.error(e);
    wlog("error", "worker", "unhandled error", { error: String(e && e.stack ? e.stack : e) });
    sendResult(id, null, e);
  }
};

/* ---------------- init ---------------- */
async function initPyodideAndPackages(opts) {
  const {
    pyodideIndexURL = "https://cdn.jsdelivr.net/pyodide/v0.28.2/full/",
    pyodidePackages = [],
    micropipPackages = []
  } = opts;

  wlog("info", "init", "loading pyodide", { indexURL: pyodideIndexURL });
  wmark("init:pyodide:start");
  const { loadPyodide } = await import(`${pyodideIndexURL}pyodide.mjs`);
  pyodide = await loadPyodide({ indexURL: pyodideIndexURL });
  wmark("init:pyodide:end"); wmeasure("init:pyodide", "init:pyodide:start", "init:pyodide:end");
  wlog("info", "init", "pyodide ready");

  try {
    pyodide.setStdout({ batched: (s) => s && wlog("info", "py.stdout", s) });
    pyodide.setStderr({ batched: (s) => s && wlog("error", "py.stderr", s) });
    wlog("info", "init", "wired py stdout/stderr");
  } catch (e) {
    wlog("warn", "init", "setStdout/Err failed", { error: String(e) });
  }

  // Preload distro packages (numpy/pandas etc.)
  for (const pkg of pyodidePackages) {
    try {
      wlog("info", "init", `load distro package`, { pkg });
      wmark(`init:pkg:${pkg}:start`);
      await pyodide.loadPackage(pkg);
      wmark(`init:pkg:${pkg}:end`);
      wmeasure(`init:pkg:${pkg}`, `init:pkg:${pkg}:start`, `init:pkg:${pkg}:end`);
    } catch (e) {
      wlog("warn", "init", "loadPackage failed", { pkg, error: String(e) });
    }
  }

  // Install pure-Python wheels via micropip (vizro, plotly, dash, …)
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

/* ---------------- Python module loading & hot-reload ---------------- */

/**
 * Ensure ./dashboard.py is present in the Pyodide VFS and imported as module "dashboard".
 * If hotReload is true, force a re-import by invalidating caches and dropping sys.modules entry.
 *
 * This uses:
 *  - FS.writeFile + sys.path tweak to make "/app" importable  (Pyodide VFS)     [docs]
 *  - importlib.invalidate_caches() + sys.modules deletion or reload (hot)       [docs]
 *  - pyodide.pyimport("dashboard") to get a callable PyProxy in JS              [docs]
 */
async function ensureDashboardModule(hotReload = true, url = "./dashboard.py", vfsPath = "/app/dashboard.py", modName = "dashboard") {
  wlog("info", "module", `fetching ${url}`, { hotReload });
  const resp = await fetch(`${url}?v=${Date.now()}`, { cache: "no-cache" }); // dev-friendly cache-bust
  if (!resp.ok) throw new Error(`Failed to fetch ${url}: ${resp.status}`);
  const code = await resp.text();

  // Write/overwrite file in the in-memory FS; ensure directory exists first.
  pyodide.FS.mkdirTree("/app");
  pyodide.FS.writeFile(vfsPath, code, { encoding: "utf8" }); // VFS write  :contentReference[oaicite:4]{index=4}

  // Make sure Python can import from /app
  await pyodide.runPythonAsync(`
import sys
if "/app" not in sys.path:
    sys.path.insert(0, "/app")
`);

  // Hot reload: prefer importlib.reload when module exists; else clean sys.modules and reimport.
  if (hotReload) {
    await pyodide.runPythonAsync(`
import sys, importlib
importlib.invalidate_caches()
if "${modName}" in sys.modules:
    try:
        importlib.reload(sys.modules["${modName}"])  # official reload API
    except Exception:
        # fallback: drop and let a fresh import occur
        del sys.modules["${modName}"]
`);
  }

  // Import the module into JS space as a PyProxy
  // (pyodide.pyimport is the documented API for this)
  const mod = pyodide.pyimport(modName); // PyProxy  :contentReference[oaicite:5]{index=5}
  return mod;
}

/* ---------------- build ---------------- */
async function buildDashboard({ dataset = "d6yy-54nr", hotReload = true } = {}) {
  wlog("info", "build", "loading dashboard.py as module", { dataset, hotReload });

  const dash = await ensureDashboardModule(hotReload);
  try {
    wmark("build:py:start");

    // Call Python: dashboard.build_dashboard(dataset) -> Python dict
    const resultPy = dash.build_dashboard(dataset);

    // Convert Python dict (PyProxy) to a plain JS object
    const obj = resultPy.toJs({ dict_converter: Object.fromEntries }); //  :contentReference[oaicite:6]{index=6}

    // Clean up proxies to avoid leaks (optional but good hygiene)
    resultPy.destroy?.();
    dash.destroy?.();

    wmark("build:py:end"); wmeasure("build:py", "build:py:start", "build:py:end");

    if (obj && obj.html) {
      wlog("info", "build", "produced Vizro HTML");
      return { html: obj.html };
    }
    if (obj && obj.figures) {
      wlog("info", "build", "produced Plotly figures", { keys: Object.keys(obj.figures || {}) });
      return { figures: obj.figures };
    }
    if (obj && obj.error) {
      wlog("error", "build", "python error", { error: obj.error });
      throw new Error(obj.error);
    }

    wlog("warn", "build", "no payload generated");
    return {};
  } catch (e) {
    // If execution fails, include some context
    wlog("error", "build", "exception during build", { error: String(e && e.stack ? e.stack : e) });
    throw e;
  }
}
