// py-worker.js — Web Worker that runs Python with Pyodide
// This worker loads Pyodide once, installs packages (micropip + distro),
// and then executes a small script that tries Vizro first, Plotly second.

let pyodide = null;

function postStatus(msg) {
  postMessage({ type: "status", payload: msg });
}
function postResult(id, payload, error) {
  if (error) {
    postMessage({ type: "result", id, error: String(error) });
  } else {
    postMessage({ type: "result", id, payload });
  }
}

self.onmessage = async (evt) => {
  const { type, id, payload } = evt.data || {};
  try {
    if (type === "init") {
      await initPyodideAndPackages(payload);
      postResult(id, { ok: true });
      return;
    }
    if (type === "run_vizro_or_plotly") {
      const out = await runVizroOrPlotly(payload);
      postResult(id, out);
      return;
    }
  } catch (err) {
    console.error(err);
    postResult(id, null, err);
  }
};

async function initPyodideAndPackages(opts) {
  const { pyodideIndexURL, micropipPackages = [], pyodidePackages = [] } = opts || {};

  postStatus("loading pyodide…");
  // dynamic import of pyodide ES module
  const { loadPyodide } = await import(
    "https://cdn.jsdelivr.net/pyodide/v0.28.2/full/pyodide.mjs"
  );

  pyodide = await loadPyodide({
    indexURL: pyodideIndexURL || "https://cdn.jsdelivr.net/pyodide/v0.28.2/full/",
  });
  postStatus("pyodide ready.");

  // Preload from the Pyodide distribution
  for (const pkg of pyodidePackages) {
    postStatus(`loading distro package: ${pkg}`);
    try {
      await pyodide.loadPackage(pkg);
    } catch (e) {
      console.warn(`failed to load distro package ${pkg}`, e);
    }
  }

  // Install pure-Python wheels via micropip
  if (micropipPackages && micropipPackages.length) {
    postStatus("installing micropip + pure-python wheels…");
    await pyodide.loadPackage("micropip");
    const code = `
import micropip
pkgs = ${JSON.stringify(micropipPackages)}
for name in pkgs:
    try:
        await micropip.install(name)
    except Exception as e:
        # don't fail the entire init if a package can't be installed in Pyodide
        print(f"[micropip warning] failed to install {name}: {e}")
`;
    await pyodide.runPythonAsync(code);
  }

  postStatus("initialization complete.");
}

async function runVizroOrPlotly(params) {
  const py = `
import json
import sys

result = {"mode": None}

# Try a tiny Vizro page first
try:
    import vizro
    import plotly.express as px

    df = [{"x": i, "y": i*i} for i in range(20)]
    fig = px.line(df, x="x", y="y", title="${(params.title || "Vizro Demo").replace(/"/g, '\\"')}")
    # The exact Vizro API may differ by version; we keep this intentionally simple.
    # Below tries a generic component->page approach; if the API differs, this block may raise.
    try:
        # Example using a hypothetical minimal API surface
        # (Adjust to your installed Vizro version’s components/page builder.)
        from vizro import components as vz
        from vizro import page as vz_page

        graph = vz.Graph(figure=fig)
        page = vz_page.Page(title="${(params.title || "Vizro Demo").replace(/"/g, '\\"')}", components=[graph])
        # Many dashboards frameworks offer "to_html" or a similar export; if Vizro doesn’t, this will raise.
        html = page.to_html()  # <-- swap to whatever export Vizro exposes in your version

        result["mode"] = "vizro_html"
        result["html"] = html
    except Exception as e:
        # As a fallback, send Plotly JSON back to main thread for rendering.
        result["mode"] = "plotly_json_fallback"
        result["plotly"] = json.loads(fig.to_json())

except Exception as e:
    # Total Vizro failure -> fallback to a bare Plotly figure
    try:
        import plotly.express as px
        df = [{"x": i, "y": i*i} for i in range(20)]
        fig = px.scatter(df, x="x", y="y", title="Plotly Fallback (Vizro import failed)")
        result["mode"] = "plotly_json"
        result["plotly"] = json.loads(fig.to_json())
    except Exception as inner:
        result["mode"] = "error"
        result["error"] = f"Vizro and fallback both failed: {inner}"

json.dumps(result)
`;
  postStatus("executing python…");
  const out = await pyodide.runPythonAsync(py);
  const obj = JSON.parse(out);

  if (obj.mode === "vizro_html" && obj.html) {
    return { html: obj.html };
  }
  if (obj.plotly) {
    return { plotly: obj.plotly };
  }
  if (obj.error) {
    throw new Error(obj.error);
  }
  return {};
}
