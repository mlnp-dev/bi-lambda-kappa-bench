"""Tareas del proyecto (reemplazo de Makefile, que no existe en Windows).

    python tasks.py up       levanta Postgres y Redpanda y crea el topic
    python tasks.py down     detiene los contenedores (conserva los volúmenes)
    python tasks.py reset    detiene y BORRA los volúmenes (BD y topic vacíos)
    python tasks.py anon     genera data/marcaciones.csv desde fuentes/MARCACIONES.xlsx
    python tasks.py seed     carga la dimensión de sedes en la BD
    python tasks.py check    verifica esquema, sedes y topic con 3 particiones
    python tasks.py produce  envía data/marcaciones.csv al topic (args extra: --limit N --rate EV/S)
    python tasks.py kappa    procesador Kappa continuo sobre la versión activa (Ctrl+C para parar)
    python tasks.py replay   reproceso Kappa: replay desde el offset 0 a v(N+1) y promoción
    python tasks.py lambda   master sink + speed layer en paralelo (args extra: --until-caught-up)
    python tasks.py batch    batch layer Lambda: recálculo completo A/B (args extra: --every SEG)
    python tasks.py api      construye y levanta api-rest (:8000/docs) y api-graphql (:8001/graphql)
    python tasks.py verify   comprueba que las vistas Kappa y Lambda cuadran con master_events y el CSV
    python tasks.py bench    benchmark completo: ingesta, API y figuras en bench/results/ (args: ingest|api|plot)
    python tasks.py report   compila report/informe.pdf (tectonic) con las cifras de bench/results/
"""

from __future__ import annotations

import csv
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))


def load_env() -> None:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def sh(*cmd: str) -> None:
    print("$", " ".join(cmd))
    env = {**os.environ, "PYTHONUTF8": "1", "PYTHONPATH": os.pathsep.join(filter(None, [str(SRC), os.environ.get("PYTHONPATH")]))}
    subprocess.run(cmd, cwd=ROOT, check=True, env=env)


def py(module: str, *args: str) -> None:
    sh(sys.executable, "-m", module, *args)


def dsn() -> str:
    e = os.environ
    return (f"host=localhost port={e['POSTGRES_PORT']} dbname={e['POSTGRES_DB']} "
            f"user={e['POSTGRES_USER']} password={e['POSTGRES_PASSWORD']}")


def up() -> None:
    sh("docker", "compose", "up", "-d", "--wait", "db", "redpanda")
    sh("docker", "compose", "run", "--rm", "topic-init")


def down() -> None:
    sh("docker", "compose", "down")


def reset() -> None:
    sh("docker", "compose", "down", "-v")


def anon() -> None:
    sh(sys.executable, "-W", "ignore", "db/anonymize.py")


def seed() -> None:
    import psycopg

    with open(ROOT / "db" / "sedes.csv", encoding="utf-8") as f:
        rows = [(int(r["sede_id"]), r["nombre"]) for r in csv.DictReader(f)]
    with psycopg.connect(dsn()) as conn:
        conn.cursor().executemany(
            "INSERT INTO sedes (sede_id, nombre) VALUES (%s, %s) "
            "ON CONFLICT (sede_id) DO UPDATE SET nombre = EXCLUDED.nombre",
            rows,
        )
    print(f"sedes cargadas: {len(rows)}")


def check() -> None:
    import psycopg
    from confluent_kafka.admin import AdminClient

    esperadas = {"sedes", "consumer_offsets", "master_events", "kappa_view_v1", "batch_view_a",
                 "batch_view_b", "speed_view", "speed_seen", "lambda_cutoff", "kappa_serving", "batch_active",
                 "lambda_serving"}
    with psycopg.connect(dsn()) as conn:
        nombres = {r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")}
        n_sedes = conn.execute("SELECT count(*) FROM sedes").fetchone()[0]
    faltan = esperadas - nombres
    print("esquema:", "OK" if not faltan else f"faltan {sorted(faltan)}")
    print("sedes:", n_sedes)

    topic = os.environ["TOPIC"]
    md = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]}).list_topics(topic, timeout=10)
    parts = len(md.topics[topic].partitions) if md.topics[topic].error is None else 0
    print(f"topic {topic}: {parts} particiones")
    if faltan or n_sedes == 0 or parts != 3:
        sys.exit(1)


def produce(*args: str) -> None:
    py("lkbench.producer", *args)


def kappa(*args: str) -> None:
    py("lkbench.kappa_arch.processor", *args)


def replay(*args: str) -> None:
    py("lkbench.kappa_arch.promote", *args)


def lambda_(*args: str) -> None:
    env = {**os.environ, "PYTHONUTF8": "1", "PYTHONPATH": str(SRC)}
    procs = [subprocess.Popen([sys.executable, "-m", f"lkbench.lambda_arch.{m}", *args], cwd=ROOT, env=env)
             for m in ("master_sink", "speed_layer")]
    try:
        codes = [p.wait() for p in procs]
    except KeyboardInterrupt:
        codes = [p.wait() for p in procs]   # cada proceso recibe Ctrl+C y cierra su lote
    if any(codes):
        sys.exit(1)


lambda_.__name__ = "lambda"


def batch(*args: str) -> None:
    py("lkbench.lambda_arch.batch_layer", *args)


def api() -> None:
    sh("docker", "compose", "up", "-d", "--build", "--wait", "api-rest", "api-graphql")
    print("REST:    http://localhost:8000/docs")
    print("GraphQL: http://localhost:8001/graphql")


def verify() -> None:
    from lkbench import db
    from lkbench.config import settings
    from lkbench.consistency import compare, master_reference
    from lkbench.events import aggregate
    from lkbench.kappa_arch.processor import active_version, latest_version, view_table
    from lkbench.lambda_arch.batch_layer import active_table
    from lkbench.producer import DEFAULT_CSV, read_csv

    csv_ref = aggregate(read_csv(ROOT / DEFAULT_CSV))
    with db.connect(settings()) as conn:
        master = master_reference(conn)
        print(f"kappa_serving -> {view_table(active_version(conn))}; batch_active -> {active_table(conn)}")
        print(f"master_events vs CSV: {'OK' if master == csv_ref else 'DIFERENTE'}")
        kappas = ["kappa_serving"] + [view_table(v) for v in range(1, latest_version(conn) + 1)]
        diffs = [compare(conn, r, master) for r in kappas + ["lambda_serving"]]
    for d in diffs:
        print(d)
    if master != csv_ref or not all(d.ok for d in diffs):
        sys.exit(1)


def bench(*parts: str) -> None:
    parts = parts or ("ingest", "api", "plot")
    if "ingest" in parts:
        sh(sys.executable, "bench/run_ingest.py")
    if "api" in parts:
        sh(sys.executable, "bench/run_api.py")
    if "plot" in parts:
        sh(sys.executable, "bench/plot.py")


def report() -> None:
    sh(sys.executable, "bench/plot.py")            # regenera report/numbers.tex y las figuras
    sh("tectonic", "-X", "compile", "report/informe.tex")


TASKS = {f.__name__: f for f in (up, down, reset, anon, seed, check, produce, kappa, replay, lambda_, batch,
                                  api, verify, bench, report)}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in TASKS:
        sys.exit(__doc__)
    load_env()
    TASKS[sys.argv[1]](*sys.argv[2:])
