"""Benchmark de ingesta: Lambda frente a Kappa.

Por repetición y arquitectura (cada corrida en un esquema de PostgreSQL desechable; los
procesadores corren en contenedores, igual que en producción):

- Throughput: backlog completo del topic principal (marcaciones reales, sin inflar).
    Kappa: procesador v1 hasta alcanzar el final. Lambda: master sink + speed layer en paralelo.
- Reproceso: Kappa, replay completo a v2; Lambda, recálculo batch completo desde master_events.
- Consistencia: las vistas de servicio deben cuadrar exactamente con master_events / el CSV.
- Latencia e2e (p50/p95/p99): carga constante (por defecto 2 000 ev/s) reemitiendo marcaciones
    reales con produced_at actual a un topic nuevo; latencia = escritura en la BD - produced_at.
    En Lambda corre además el batch cada BATCH_EVERY segundos (frescura del batch).
- Recursos: CPU y RAM con docker stats.

    python bench/run_ingest.py [--reps 3] [--rate 2000] [--seconds 30]
"""

from __future__ import annotations

import argparse
import csv
import subprocess
import time
import uuid

from common import (
    APP_SERVICE,
    ROOT,
    DockerStats,
    compose_run,
    create_schema,
    create_topic,
    delete_topic,
    docker_logs,
    drop_schema,
    load_env,
    log,
    median,
    remove,
    run,
    stats_lines,
    topic_size,
    wait_log,
    write_json,
)

LATENCY_SQL = """
    WITH l AS (SELECT extract(epoch FROM committed_at) * 1000 - produced_at AS ms, produced_at
               FROM e2e_latency WHERE consumer = %(c)s),
         w AS (SELECT min(produced_at) + %(warm)s AS t0 FROM l)
    SELECT count(*), avg(ms),
           percentile_cont(0.50) WITHIN GROUP (ORDER BY ms),
           percentile_cont(0.95) WITHIN GROUP (ORDER BY ms),
           percentile_cont(0.99) WITHIN GROUP (ORDER BY ms), max(ms)
    FROM l, w WHERE l.produced_at >= w.t0
"""


def settings_for(schema: str):
    from lkbench.config import settings

    return settings().with_(pg_schema=schema)


def offsets_sum(schema: str, consumer: str) -> int:
    from lkbench import db

    with db.connect(settings_for(schema)) as conn:
        return conn.execute("SELECT coalesce(sum(next_offset), 0) FROM consumer_offsets WHERE consumer = %s",
                            (consumer,)).fetchone()[0]


def objects(schema: str, arch: str) -> list[str]:
    from lkbench import db

    pat = ["kappa%"] if arch == "kappa" else ["master_events", "batch%", "speed%", "lambda%"]
    with db.connect(settings_for(schema)) as conn:
        rows = conn.execute("SELECT table_name FROM information_schema.tables "
                            "WHERE table_schema = %s AND table_name LIKE ANY(%s) ORDER BY 1",
                            (schema, pat)).fetchall()
    return [r[0] for r in rows] + ["consumer_offsets"]


def consistency(schema: str, arch: str, csv_ref) -> dict:
    from lkbench import db
    from lkbench.consistency import compare, master_reference

    with db.connect(settings_for(schema)) as conn:
        if arch == "kappa":
            d = compare(conn, "kappa_serving", csv_ref)
            return {"kappa_serving_vs_csv": d.ok, "rows": d.actual_rows}
        master = master_reference(conn)
        d = compare(conn, "lambda_serving", master)
        return {"lambda_serving_vs_master": d.ok, "master_vs_csv": master == csv_ref, "rows": d.actual_rows}


# --- throughput + reproceso ------------------------------------------------------------------
def throughput_run(arch: str, rep: int, topic: str, n: int, csv_ref) -> dict:
    schema = f"bench_tp_{arch}_{rep}"
    env = {"PG_SCHEMA": schema, "TOPIC": topic}
    create_schema(schema)
    out: dict = {"arch": arch, "rep": rep, "kind": "throughput", "events_in_topic": n}
    try:
        if arch == "kappa":
            name = f"lkb-tp-kappa-{rep}"
            with DockerStats(("lkb-tp-", "lkbench-db", "lkbench-redpanda")) as ds:
                st = stats_lines(run(compose_run(["lkbench.kappa_arch.processor", "--version", "1",
                                                  "--until-caught-up", "--json"], env, name)))[-1]
            out.update(read=st["read"], seconds=st["seconds"], throughput=st["read"] / st["seconds"],
                       resources=ds.summary())
            with DockerStats(("lkb-rp-", "lkbench-db")) as ds:
                rp = stats_lines(run(compose_run(["lkbench.kappa_arch.promote", "--json"], env,
                                                 f"lkb-rp-kappa-{rep}")))[-1]
            out["reprocess"] = {"what": "replay completo a v2", "seconds": rp["seconds"], "read": rp["read"],
                                "resources": ds.summary()}
        else:
            names = {m: f"lkb-tp-{m}-{rep}" for m in ("master_sink", "speed_layer")}
            with DockerStats(("lkb-tp-", "lkbench-db", "lkbench-redpanda")) as ds:
                procs = {m: subprocess.Popen(compose_run([f"lkbench.lambda_arch.{m}", "--until-caught-up",
                                                          "--json"], env, nm), cwd=ROOT,
                                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                             encoding="utf-8")
                         for m, nm in names.items()}
                res = {m: p.communicate(timeout=900) for m, p in procs.items()}
            for m, (o, e) in res.items():
                if procs[m].returncode or not stats_lines(o):
                    raise RuntimeError(f"{m} falló (código {procs[m].returncode}):\n{o[-1500:]}\n{e[-1500:]}")
            sts = {m: stats_lines(o)[-1] for m, (o, _) in res.items()}
            window = max(s["ended_at"] for s in sts.values()) - min(s["started_at"] for s in sts.values())
            out.update(read=sts["master_sink"]["read"], seconds=window, throughput=sts["master_sink"]["read"] / window,
                       components={m: {"seconds": s["seconds"], "read": s["read"]} for m, s in sts.items()},
                       resources=ds.summary())
            with DockerStats(("lkb-rp-", "lkbench-db")) as ds:
                bt = stats_lines(run(compose_run(["lkbench.lambda_arch.batch_layer", "--json"], env,
                                                 f"lkb-rp-lambda-{rep}")))[-1]
            out["reprocess"] = {"what": "recálculo batch completo", "seconds": bt["seconds"],
                                "events": bt["events"], "resources": ds.summary()}
        out["consistency"] = consistency(schema, arch, csv_ref)
        out["objects"] = objects(schema, arch)
    finally:
        drop_schema(schema)
    return out


# --- latencia de extremo a extremo -----------------------------------------------------------
def latency_run(arch: str, rep: int, rate: float, seconds: float, warmup_s: float, batch_every: float) -> dict:
    tag = uuid.uuid4().hex[:6]
    schema, topic = f"bench_lat_{arch}_{rep}", f"lat_{arch}_{rep}_{tag}"
    n = int(rate * seconds)
    env = {"PG_SCHEMA": schema, "TOPIC": topic, "TRACK_LATENCY": "1"}
    create_schema(schema)
    create_topic(topic)
    if arch == "kappa":
        procs = {"kappa_v1": ["lkbench.kappa_arch.processor", "--version", "1"]}
    else:
        procs = {"lambda_master": ["lkbench.lambda_arch.master_sink"],
                 "lambda_speed": ["lkbench.lambda_arch.speed_layer"],
                 "batch": ["lkbench.lambda_arch.batch_layer", "--every", str(batch_every), "--json"]}
    names = {c: f"lkb-lat-{c.replace('_', '-')}-{rep}" for c in procs}
    out: dict = {"arch": arch, "rep": rep, "kind": "latency", "rate": rate, "events": n, "warmup_s": warmup_s}
    try:
        for c, args in procs.items():
            run(compose_run(args, env, names[c], detach=True))
        for c in procs:
            if c != "batch":
                wait_log(names[c], "desde offsets")
        consumers = [c for c in procs if c != "batch"]
        with DockerStats(("lkb-lat-", "lkbench-db", "lkbench-redpanda")) as ds:
            t0 = time.time()
            run(compose_run(["lkbench.producer", "--rate", str(rate), "--limit", str(n)], {"TOPIC": topic},
                            f"lkb-prod-{arch}-{rep}", data=True))
            produce_s = time.time() - t0
            while any(offsets_sum(schema, c) < n for c in consumers):
                if time.time() - t0 > seconds + 300:
                    raise TimeoutError("los consumidores no alcanzaron el final del topic")
                time.sleep(0.5)
            drain_s = time.time() - t0 - produce_s
        out.update(produce_seconds=produce_s, drain_after_produce_s=drain_s, resources=ds.summary())
        if arch == "lambda":
            batches = stats_lines(docker_logs(names["batch"]))
            out["batch"] = {"every_s": batch_every, "runs": len(batches),
                            "seconds_median": median([b["seconds"] for b in batches])}
        from lkbench import db

        with db.connect(settings_for(schema)) as conn:
            for c in consumers:
                cnt, mean, p50, p95, p99, mx = conn.execute(LATENCY_SQL, {"c": c, "warm": warmup_s * 1000}).fetchone()
                out.setdefault("latency_ms", {})[c] = {"n": cnt, "mean": mean, "p50": p50, "p95": p95,
                                                       "p99": p99, "max": mx}
    finally:
        remove(*names.values())
        drop_schema(schema)
        delete_topic(topic)
    return out


# --- complejidad -----------------------------------------------------------------------------
def loc(path) -> int:
    """Líneas de código sin blancos, comentarios ni docstrings."""
    import ast

    src = path.read_text(encoding="utf-8")
    doc = set()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(getattr(first, "value", None), ast.Constant) \
                    and isinstance(first.value.value, str):
                doc.update(range(first.lineno, first.end_lineno + 1))
    return sum(1 for i, line in enumerate(src.splitlines(), 1)
               if line.strip() and not line.strip().startswith("#") and i not in doc)


def complexity() -> dict:
    src = ROOT / "src" / "lkbench"
    shared = ["stream.py", "events.py", "kafka.py", "db.py", "config.py"]
    services = run(["docker", "compose", "--profile", "stream", "config", "--services"]).split()
    out = {"shared_loc": sum(loc(src / f) for f in shared), "shared_files": shared}
    for arch, folder, prefix in (("kappa", "kappa_arch", "kappa"), ("lambda", "lambda_arch", "lambda-")):
        files = sorted((src / folder).glob("*.py"))
        out[arch] = {"loc": sum(loc(f) for f in files), "files": [f.name for f in files],
                     "services": sorted(s for s in services if s.startswith(prefix))}
    return out


def host_cpu() -> str:
    try:
        import winreg

        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DESCRIPTION\System\CentralProcessor\0") as k:
            return winreg.QueryValueEx(k, "ProcessorNameString")[0].strip()
    except (ImportError, OSError):
        import platform

        return platform.processor()


def host_ram_gb() -> float | None:
    try:
        import ctypes

        class MemStatus(ctypes.Structure):
            _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]

        m = MemStatus()
        m.dwLength = ctypes.sizeof(m)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m))
        return round(m.ullTotalPhys / 2**30, 1)
    except (AttributeError, OSError):
        return None


def environment() -> dict:
    import platform

    from lkbench import db
    from lkbench.config import settings

    def cmd(*c):
        try:
            return subprocess.run(c, capture_output=True, text=True, encoding="utf-8", timeout=60, check=False).stdout.strip()
        except Exception as exc:  # noqa: BLE001
            return f"n/d ({exc})"

    with db.connect(settings()) as conn:
        pg = conn.execute("SELECT version()").fetchone()[0]
    return {
        "host_os": platform.platform(), "host_cpu": host_cpu(), "host_ram_gb": host_ram_gb(),
        "docker": cmd("docker", "version", "--format", "{{.Server.Version}}"),
        "docker_vm": cmd("docker", "info", "--format", "{{.NCPU}} CPU, {{.MemTotal}} bytes, {{.KernelVersion}}"),
        "postgres": pg,
        "redpanda": cmd("docker", "compose", "exec", "-T", "redpanda", "rpk", "version").splitlines()[:1],
        "app_python": cmd("docker", "compose", "run", "--rm", "--no-deps", "-T", "kappa", "python", "-c",
                          "import sys, psycopg, confluent_kafka, fastapi, strawberry, uvicorn; "
                          "from importlib.metadata import version as v; "
                          "print(sys.version.split()[0], 'psycopg', psycopg.__version__, 'confluent-kafka', "
                          "confluent_kafka.version()[0], 'fastapi', fastapi.__version__, 'strawberry', "
                          "v('strawberry-graphql'), 'uvicorn', uvicorn.__version__)"),
    }


# --- resumen ---------------------------------------------------------------------------------
def summarize(runs: list[dict]) -> list[dict]:
    rows = []
    for arch in ("kappa", "lambda"):
        tp = [r for r in runs if r["arch"] == arch and r["kind"] == "throughput"]
        lat = [r for r in runs if r["arch"] == arch and r["kind"] == "latency"]
        main = "kappa_v1" if arch == "kappa" else "lambda_speed"

        def app_res(rs, key):
            vals = [sum(v[key] for n, v in r["resources"].items() if n.startswith("lkb-")) for r in rs]
            return median(vals)

        rows.append({
            "arch": arch, "reps": len(tp),
            "throughput_ev_s": median([r["throughput"] for r in tp]),
            "ingest_seconds": median([r["seconds"] for r in tp]),
            "reprocess_seconds": median([r["reprocess"]["seconds"] for r in tp]),
            "latency_p50_ms": median([r["latency_ms"][main]["p50"] for r in lat]),
            "latency_p95_ms": median([r["latency_ms"][main]["p95"] for r in lat]),
            "latency_p99_ms": median([r["latency_ms"][main]["p99"] for r in lat]),
            "batch_seconds": median([r["batch"]["seconds_median"] for r in lat]) if arch == "lambda" else None,
            "batch_every_s": lat[0]["batch"]["every_s"] if arch == "lambda" and lat else None,
            "app_cpu_mean_pct": app_res(tp, "cpu_mean_pct"),
            "app_mem_max_mib": app_res(tp, "mem_max_mib"),
            "db_cpu_mean_pct": median([r["resources"].get("lkbench-db-1", {}).get("cpu_mean_pct") for r in tp]),
            "consistent": all(all(v for k, v in r["consistency"].items() if k != "rows") for r in tp),
        })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--rate", type=float, default=2000, help="eventos/s en la prueba de latencia")
    ap.add_argument("--seconds", type=float, default=30, help="duración de la carga constante")
    ap.add_argument("--warmup", type=float, default=5, help="segundos iniciales excluidos de la latencia")
    ap.add_argument("--batch-every", type=float, default=15, help="intervalo del batch Lambda en la prueba de latencia")
    a = ap.parse_args()
    load_env()

    from lkbench.config import settings
    from lkbench.events import aggregate
    from lkbench.producer import DEFAULT_CSV, read_csv

    topic = settings().topic
    n = topic_size(topic)
    csv_ref = aggregate(read_csv(ROOT / DEFAULT_CSV))
    log(f"topic {topic}: {n} eventos; CSV: {sum(m for m, _ in csv_ref.values())} eventos únicos")
    run(["docker", "compose", "build", "-q", APP_SERVICE])

    params = {"reps": a.reps, "rate": a.rate, "seconds": a.seconds, "warmup_s": a.warmup,
              "batch_every_s": a.batch_every, "topic": topic, "events_in_topic": n}
    runs: list[dict] = []
    for rep in range(1, a.reps + 1):
        order = ("kappa", "lambda") if rep % 2 else ("lambda", "kappa")   # alternar para repartir sesgos
        for arch in order:
            log(f"rep {rep} {arch}: throughput + reproceso")
            runs.append(throughput_run(arch, rep, topic, n, csv_ref))
            r = runs[-1]
            log(f"  {r['throughput']:,.0f} ev/s, reproceso {r['reprocess']['seconds']:.2f} s, {r['consistency']}")
            log(f"rep {rep} {arch}: latencia a {a.rate:.0f} ev/s")
            runs.append(latency_run(arch, rep, a.rate, a.seconds, a.warmup, a.batch_every))
            log(f"  {runs[-1]['latency_ms']}")
            write_json("ingest_raw.json", {"params": params, "runs": runs})

    summary = summarize(runs)
    write_json("ingest_raw.json", {"params": params, "environment": environment(), "complexity": complexity(),
                                   "runs": runs, "summary": summary})
    with open(ROOT / "bench" / "results" / "ingest_summary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)
    for row in summary:
        log(str(row))


if __name__ == "__main__":
    main()
