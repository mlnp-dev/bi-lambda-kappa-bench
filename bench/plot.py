"""Figuras y cifras del informe a partir de bench/results/ (nada se escribe a mano).

Lee ingest_raw.json y api_raw.json y genera:
- bench/results/fig_bench.png y report/figs/fig_bench.pdf
- report/tab_ingest.tex, report/tab_api.tex (tablas LaTeX)
- report/numbers.tex (macros \\Num... con cada cifra citada en el texto)

    python bench/plot.py
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results"
REPORT = ROOT / "report"

# Paleta categórica validada (slot 1 azul, slot 2 naranja) + trama para impresión en gris.
COLORS = {"lambda": "#2a78d6", "kappa": "#eb6834", "rest": "#2a78d6", "graphql": "#eb6834"}
HATCH = {"lambda": "", "kappa": "///", "rest": "", "graphql": "///"}
LABEL = {"lambda": "Lambda", "kappa": "Kappa", "rest": "REST", "graphql": "GraphQL"}
SCEN = {"hourly": "hourly", "top-sedes": "top-sedes", "post-events": "POST events"}


def load(name: str) -> dict:
    return json.loads((RESULTS / name).read_text(encoding="utf-8"))


def fmt(x, nd: int = 0) -> str:
    """Número en formato del informe: espacio fino para miles y coma decimal (1\\,234,5)."""
    if x is None:
        return "--"
    return f"{x:,.{nd}f}".replace(",", " ").replace(".", ",").replace(" ", "\\,")


def tex_escape(s: str) -> str:
    return s.replace("_", "\\_").replace("%", "\\%").replace("&", "\\&")


# --- figura ----------------------------------------------------------------------------------
def style(ax) -> None:
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color("#8a8984")
    ax.tick_params(colors="#52514e", labelsize=7, length=0)
    ax.yaxis.grid(True, color="#e4e3df", linewidth=0.6)
    ax.set_axisbelow(True)


def bar(ax, x, h, key, width):
    return ax.bar(x, h, width, color=COLORS[key], hatch=HATCH[key], edgecolor="white", linewidth=1.0,
                  label=LABEL[key])


def figure(ingest: dict, api: dict) -> None:
    plt.rcParams.update({"font.family": "serif", "font.size": 7, "hatch.color": "white", "hatch.linewidth": 0.8})
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(7.1, 2.0), gridspec_kw={"width_ratios": [1, 2.3]})

    # (a) Ingesta: latencia e2e p50/p95/p99 a carga constante.
    s = {r["arch"]: r for r in ingest["summary"]}
    pct = ["p50", "p95", "p99"]
    w = 0.38
    for i, arch in enumerate(("lambda", "kappa")):
        xs = [j + (i - 0.5) * w for j in range(3)]
        bar(a1, xs, [s[arch][f"latency_{p}_ms"] for p in pct], arch, w)
    a1.set_xticks(range(3), pct)
    a1.set_ylabel("ms", color="#52514e")
    a1.set_title(f"(a) Latencia e2e a {ingest['params']['rate']:,.0f} ev/s".replace(",", " "), fontsize=7.5, loc="left")
    a1.legend(frameon=False, fontsize=6.5, loc="upper left")
    style(a1)

    # (b) API: req/s con concurrencia máxima, por escenario y vista.
    conc = max(api["params"]["conc"])
    rows = [r for r in api["summary"] if r["conc"] == conc]
    keys = [(sc, a) for sc in ("hourly", "top-sedes", "post-events") for a in ("lambda", "kappa", "-")
            if any(r["scenario"] == sc and r["arch"] == a for r in rows)]
    for i, apiname in enumerate(("rest", "graphql")):
        vals = [next(r["rps"] for r in rows if (r["scenario"], r["arch"], r["api"]) == (sc, a, apiname))
                for sc, a in keys]
        bar(a2, [j + (i - 0.5) * w for j in range(len(keys))], vals, apiname, w)
    a2.set_xticks(range(len(keys)), [SCEN[sc] + ("" if a == "-" else f"\n{LABEL[a]}") for sc, a in keys])
    a2.set_ylabel("req/s", color="#52514e")
    a2.set_title(f"(b) Throughput de la API, concurrencia {conc} (1 worker)", fontsize=7.5, loc="left")
    a2.set_ylim(0, a2.get_ylim()[1] * 1.15)
    a2.legend(frameon=False, fontsize=6.5, loc="upper center", ncol=2)
    style(a2)

    fig.tight_layout(pad=0.4, w_pad=1.5)
    (REPORT / "figs").mkdir(parents=True, exist_ok=True)
    fig.savefig(REPORT / "figs" / "fig_bench.pdf")
    fig.savefig(RESULTS / "fig_bench.png", dpi=200)
    plt.close(fig)


# --- tablas y macros -------------------------------------------------------------------------
def ingest_table(ingest: dict) -> str:
    s = {r["arch"]: r for r in ingest["summary"]}
    c = ingest["complexity"]
    L, K = s["lambda"], s["kappa"]
    rows = [
        ("Throughput backlog (ev/s)", fmt(L["throughput_ev_s"]), fmt(K["throughput_ev_s"])),
        ("Tiempo de ingesta (s)", fmt(L["ingest_seconds"], 1), fmt(K["ingest_seconds"], 1)),
        ("Latencia e2e p50 (ms)", fmt(L["latency_p50_ms"]), fmt(K["latency_p50_ms"])),
        ("Latencia e2e p95 (ms)", fmt(L["latency_p95_ms"]), fmt(K["latency_p95_ms"])),
        ("Latencia e2e p99 (ms)", fmt(L["latency_p99_ms"]), fmt(K["latency_p99_ms"])),
        ("Reproceso completo (s)", fmt(L["reprocess_seconds"], 2), fmt(K["reprocess_seconds"], 2)),
        ("CPU procesos, media (\\%)", fmt(L["app_cpu_mean_pct"], 0), fmt(K["app_cpu_mean_pct"], 0)),
        ("RAM procesos, máx. (MiB)", fmt(L["app_mem_max_mib"], 0), fmt(K["app_mem_max_mib"], 0)),
        ("CPU PostgreSQL, media (\\%)", fmt(L["db_cpu_mean_pct"], 0), fmt(K["db_cpu_mean_pct"], 0)),
        ("Líneas de código propias", fmt(c["lambda"]["loc"]), fmt(c["kappa"]["loc"])),
        ("Servicios a mantener", str(len(c["lambda"]["services"])), str(len(c["kappa"]["services"]))),
        ("Cuadra con la referencia", "sí" if L["consistent"] else "no", "sí" if K["consistent"] else "no"),
    ]
    body = "\n".join(f"{a} & {b} & {k} \\\\" for a, b, k in rows)
    return ("\\begin{tabular}{@{}lrr@{}}\n\\toprule\nMétrica & Lambda & Kappa \\\\\n\\midrule\n"
            f"{body}\n\\bottomrule\n\\end{{tabular}}\n")


def api_table(api: dict) -> str:
    rows = api["summary"]
    keys = sorted({(r["scenario"], r["arch"], r["conc"]) for r in rows},
                  key=lambda k: (list(SCEN).index(k[0]), k[1], k[2]))
    lines = []
    for sc, arch, conc in keys:
        cells = []
        for apiname in ("rest", "graphql"):
            r = next(x for x in rows if (x["scenario"], x["arch"], x["conc"], x["api"]) == (sc, arch, conc, apiname))
            cells += [fmt(r["rps"]), fmt(r["p50_ms"], 1), fmt(r["p99_ms"], 1)]
        vista = "--" if arch == "-" else LABEL[arch]
        lines.append(f"{SCEN[sc]} & {vista} & {conc} & " + " & ".join(cells) + " \\\\")
    return ("\\begin{tabular}{@{}lllrrrrrr@{}}\n\\toprule\n"
            "& & & \\multicolumn{3}{c}{REST} & \\multicolumn{3}{c}{GraphQL} \\\\\n"
            "\\cmidrule(lr){4-6}\\cmidrule(l){7-9}\n"
            "Escenario & Vista & $c$ & req/s & p50 & p99 & req/s & p50 & p99 \\\\\n\\midrule\n"
            + "\n".join(lines) + "\n\\bottomrule\n\\end{tabular}\n")


def numbers(ingest: dict, api: dict) -> str:
    s = {r["arch"]: r for r in ingest["summary"]}
    L, K = s["lambda"], s["kappa"]
    p, e = ingest["params"], ingest["environment"]
    ap = api["params"]
    rows = api["summary"]

    def rps(apiname, sc, arch, conc):
        return next(r["rps"] for r in rows if (r["api"], r["scenario"], r["arch"], r["conc"]) == (apiname, sc, arch, conc))

    cmax = max(ap["conc"])
    ratios = {}
    for sc, arch in (("hourly", "kappa"), ("top-sedes", "kappa"), ("post-events", "-")):
        ratios[sc] = rps("rest", sc, arch, cmax) / rps("graphql", sc, arch, cmax)
    top_kl = rps("rest", "top-sedes", "kappa", cmax) / rps("rest", "top-sedes", "lambda", cmax)
    max_err = max(r["error_rate"] or 0 for r in rows)
    m = {
        "NumEvents": fmt(p["events_in_topic"]),
        "NumReps": str(p["reps"]),
        "NumRate": fmt(p["rate"]),
        "NumLatSeconds": fmt(p["seconds"]),
        "NumWarmup": fmt(p["warmup_s"]),
        "NumBatchEvery": fmt(p["batch_every_s"]),
        "NumTpKappa": fmt(K["throughput_ev_s"]),
        "NumTpLambda": fmt(L["throughput_ev_s"]),
        "NumTpRatio": fmt(K["throughput_ev_s"] / L["throughput_ev_s"], 1),
        "NumRpKappa": fmt(K["reprocess_seconds"], 1),
        "NumRpLambda": fmt(L["reprocess_seconds"], 2),
        "NumBatchSeconds": fmt(L["batch_seconds"], 2),
        "NumPfiftyKappa": fmt(K["latency_p50_ms"]),
        "NumPfiftyLambda": fmt(L["latency_p50_ms"]),
        "NumPninetynineKappa": fmt(K["latency_p99_ms"]),
        "NumPninetynineLambda": fmt(L["latency_p99_ms"]),
        "NumLocKappa": fmt(ingest["complexity"]["kappa"]["loc"]),
        "NumLocLambda": fmt(ingest["complexity"]["lambda"]["loc"]),
        "NumLocShared": fmt(ingest["complexity"]["shared_loc"]),
        "NumDbCpuKappa": fmt(K["db_cpu_mean_pct"]),
        "NumDbCpuLambda": fmt(L["db_cpu_mean_pct"]),
        "NumApiSeconds": fmt(ap["seconds"]),
        "NumApiWarmup": fmt(ap["warmup_s"]),
        "NumApiConc": " y ".join(str(c) for c in ap["conc"]),
        "NumApiCmax": str(cmax),
        "NumApiReps": str(ap["reps"]),
        "NumRatioHourly": fmt(ratios["hourly"], 1),
        "NumRatioTop": fmt(ratios["top-sedes"], 1),
        "NumRatioPost": fmt(ratios["post-events"], 1),
        "NumTopKappaOverLambda": fmt(top_kl, 1),
        "NumMaxErr": fmt(max_err * 100, 2),
        "NumPostEvents": str(ap["post_events_per_request"]),
        "EnvCpu": tex_escape(e["host_cpu"]),
        "EnvRam": fmt(e["host_ram_gb"], 0),
        "EnvDocker": tex_escape(e["docker"]),
        "EnvPostgres": tex_escape(e["postgres"].split(" on ")[0]),
        "EnvRedpanda": tex_escape(e["redpanda"][0].split()[-1]) if e["redpanda"] else "--",
        "EnvPython": tex_escape(e["app_python"].split()[0]),
        "EnvVmCpu": tex_escape(e["docker_vm"].split(" CPU")[0]),
        "EnvVmRam": fmt(int(e["docker_vm"].split(", ")[1].split()[0]) / 2**30, 1),
    }
    return "% Generado por bench/plot.py a partir de bench/results/. No editar a mano.\n" + "".join(
        f"\\newcommand{{\\{k}}}{{{v}}}\n" for k, v in m.items())


def main() -> None:
    ingest, api = load("ingest_raw.json"), load("api_raw.json")
    figure(ingest, api)
    REPORT.mkdir(exist_ok=True)
    (REPORT / "tab_ingest.tex").write_text(ingest_table(ingest), encoding="utf-8")
    (REPORT / "tab_api.tex").write_text(api_table(api), encoding="utf-8")
    (REPORT / "numbers.tex").write_text(numbers(ingest, api), encoding="utf-8")
    print("figuras y tablas en bench/results/ y report/")


if __name__ == "__main__":
    main()
