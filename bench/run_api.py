"""Benchmark de API: REST frente a GraphQL, cruzado con la vista Lambda y Kappa.

Escenarios: hourly (serie por hora de una sede, un mes), top-sedes y POST events (10 eventos por
petición). Cada combinación: concurrencia 10 y 50, 10 s de calentamiento (descartado) y 30 s de
medición con oha; 3 repeticiones y se reporta la mediana.

oha corre en un contenedor en la misma red de Docker que las APIs (un worker cada una). Durante
la prueba las APIs publican en un topic aparte (`bench_api`) para no alterar el topic real; al
terminar se recrean con la configuración normal.

    python bench/run_api.py [--reps 3] [--seconds 30] [--warmup 10] [--conc 10 50]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import time
import uuid

from common import ROOT, DockerStats, create_topic, delete_topic, load_env, log, median, run, write_json

OHA = "ghcr.io/hatoo/oha:1.16.0"
NETWORK = "lkbench_default"
BENCH_TOPIC = "bench_api"
HOSTS = {"rest": "http://api-rest:8000", "graphql": "http://api-graphql:8001"}
SEDE, FROM, TO = 1, "2019-04-01T00:00:00-05:00", "2019-05-01T00:00:00-05:00"   # sede PUNO, abril 2019

GQL_HOURLY = ("query($s:Int!,$f:DateTime,$t:DateTime,$a:Arch!){hourly(sedeId:$s,from:$f,to:$t,arch:$a)"
              "{bucket marcaciones tardanzas}}")
GQL_TOP = "query($a:Arch!){topSedes(limit:5,arch:$a){sedeId nombre marcaciones tardanzas}}"
GQL_INGEST = "mutation($e:[EventInput!]!){ingestEvents(events:$e)}"


def sample_events(n: int = 10) -> list[dict]:
    """Eventos fijos (event_id determinista): reenviarlos no duplica nada aguas abajo."""
    tipos = ("ENTRADA", "REF_SALIDA", "REF_RETORNO", "SALIDA")
    return [{"event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"bench-api-{i}")), "staff_id": 1000 + i,
             "sede_id": 1 + i % 16, "tipo": tipos[i % 4], "tardanza": i % 4 == 0,
             "event_ts": f"2021-06-01T08:{i:02d}:00-05:00", "produced_at": 0} for i in range(n)]


def request(api: str, scenario: str, arch: str | None) -> list[str]:
    """Argumentos de oha (método, cuerpo y URL) para un escenario."""
    base = HOSTS[api]
    if api == "rest":
        if scenario == "hourly":
            return [f"{base}/api/v1/sedes/{SEDE}/hourly?from={FROM.replace('+', '%2B')}&to={TO}&arch={arch}"]
        if scenario == "top-sedes":
            return [f"{base}/api/v1/top-sedes?limit=5&arch={arch}"]
        return ["-m", "POST", "-T", "application/json", "-d", json.dumps(sample_events()),
                f"{base}/api/v1/events"]
    if scenario == "hourly":
        body = {"query": GQL_HOURLY, "variables": {"s": SEDE, "f": FROM, "t": TO, "a": arch.upper()}}
    elif scenario == "top-sedes":
        body = {"query": GQL_TOP, "variables": {"a": arch.upper()}}
    else:
        evs = [{"eventId": e["event_id"], "staffId": e["staff_id"], "sedeId": e["sede_id"], "tipo": e["tipo"],
                "tardanza": e["tardanza"], "eventTs": e["event_ts"]} for e in sample_events()]
        body = {"query": GQL_INGEST, "variables": {"e": evs}}
    return ["-m", "POST", "-T", "application/json", "-d", json.dumps(body), f"{base}/graphql"]


def oha(args: list[str], conc: int, seconds: float) -> dict:
    cmd = ["docker", "run", "--rm", "--network", NETWORK, OHA, "-z", f"{seconds}s", "-c", str(conc), "-w",
           "--no-tui", "--output-format", "json", *args]
    return json.loads(run(cmd, timeout=seconds + 120))


def check_responses(api: str, scenario: str, arch: str | None) -> None:
    """Una petición de control: debe ser 2xx y, en GraphQL, sin errores en el cuerpo."""
    args = request(api, scenario, arch)
    url = args[-1].replace("api-rest", "localhost").replace("api-graphql", "localhost")
    cmd = ["curl", "-s", "-o", "-", "-w", "\n%{http_code}"]
    if "-d" in args:
        cmd += ["-X", "POST", "-H", "Content-Type: application/json", "--data-binary", args[args.index("-d") + 1]]
    body, code = run(cmd + [url]).rsplit("\n", 1)
    if not code.startswith("2") or (api == "graphql" and '"errors"' in body):
        raise RuntimeError(f"{api} {scenario} {arch}: HTTP {code} {body[:300]}")


def apis_up(topic: str | None) -> None:
    env = {**os.environ}
    if topic:
        env["TOPIC"] = topic
    subprocess.run(["docker", "compose", "up", "-d", "--build", "--force-recreate", "--wait", "api-rest",
                    "api-graphql"], cwd=ROOT, env=env, check=True, capture_output=True)


def summarize(runs: list[dict]) -> list[dict]:
    keys = sorted({(r["api"], r["scenario"], r["arch"] or "-", r["conc"]) for r in runs})
    rows = []
    for api, scenario, arch, conc in keys:
        rs = [r for r in runs if (r["api"], r["scenario"], r["arch"] or "-", r["conc"]) == (api, scenario, arch, conc)]
        rows.append({"api": api, "scenario": scenario, "arch": arch, "conc": conc, "reps": len(rs),
                     **{k: median([r[k] for r in rs]) for k in ("rps", "p50_ms", "p95_ms", "p99_ms", "error_rate",
                                                                "api_cpu_mean_pct", "db_cpu_mean_pct")}})
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--warmup", type=float, default=10)
    ap.add_argument("--conc", type=int, nargs="+", default=[10, 50])
    a = ap.parse_args()
    load_env()

    scenarios = [("hourly", "lambda"), ("hourly", "kappa"), ("top-sedes", "lambda"), ("top-sedes", "kappa"),
                 ("post-events", None)]
    params = {"reps": a.reps, "seconds": a.seconds, "warmup_s": a.warmup, "conc": a.conc, "oha": OHA,
              "workers_per_api": 1, "hourly": {"sede": SEDE, "from": FROM, "to": TO}, "top_limit": 5,
              "post_events_per_request": len(sample_events()), "post_topic": BENCH_TOPIC}
    create_topic(BENCH_TOPIC)
    run(["docker", "pull", "-q", OHA], timeout=600)
    runs: list[dict] = []
    try:
        apis_up(BENCH_TOPIC)
        for api in HOSTS:
            for sc, arch in scenarios:
                check_responses(api, sc, arch)
        for rep in range(1, a.reps + 1):
            apis = list(HOSTS) if rep % 2 else list(HOSTS)[::-1]   # alternar el orden entre repeticiones
            for sc, arch in scenarios:
                for conc in a.conc:
                    for api in apis:
                        args = request(api, sc, arch)
                        oha(args, conc, a.warmup)                                  # calentamiento
                        with DockerStats((f"lkbench-api-{api}", "lkbench-db", "lkbench-redpanda")) as ds:
                            d = oha(args, conc, a.seconds)
                        res = ds.summary()
                        ok = sum(v for k, v in d["statusCodeDistribution"].items() if k.startswith("2"))
                        total = sum(d["statusCodeDistribution"].values()) + sum(d["errorDistribution"].values())
                        p = d["latencyPercentiles"]
                        runs.append({
                            "api": api, "scenario": sc, "arch": arch, "conc": conc, "rep": rep,
                            "requests": total, "rps": d["summary"]["requestsPerSec"],
                            "p50_ms": p["p50"] * 1000, "p95_ms": p["p95"] * 1000, "p99_ms": p["p99"] * 1000,
                            "error_rate": 1 - ok / total if total else None,
                            "status": d["statusCodeDistribution"], "errors": d["errorDistribution"],
                            "bytes_per_request": d["summary"]["sizePerRequest"],
                            "api_cpu_mean_pct": next((v["cpu_mean_pct"] for k, v in res.items()
                                                      if f"api-{api}" in k), None),
                            "db_cpu_mean_pct": res.get("lkbench-db-1", {}).get("cpu_mean_pct"),
                            "resources": res,
                        })
                        r = runs[-1]
                        log(f"rep {rep} {api:7} {sc:11} {arch or '-':6} c={conc:<3} {r['rps']:8.0f} req/s "
                            f"p50 {r['p50_ms']:6.1f} p99 {r['p99_ms']:6.1f} ms err {r['error_rate']:.3f}")
                        write_json("api_raw.json", {"params": params, "runs": runs})
                        time.sleep(1)
    finally:
        apis_up(None)
        delete_topic(BENCH_TOPIC)

    summary = summarize(runs)
    write_json("api_raw.json", {"params": params, "runs": runs, "summary": summary})
    with open(ROOT / "bench" / "results" / "api_summary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)


if __name__ == "__main__":
    main()
