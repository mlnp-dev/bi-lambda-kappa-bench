"""Tareas del proyecto (reemplazo de Makefile, que no existe en Windows).

    python tasks.py up       levanta Postgres y Redpanda y crea el topic
    python tasks.py down     detiene los contenedores (conserva los volúmenes)
    python tasks.py reset    detiene y BORRA los volúmenes (BD y topic vacíos)
    python tasks.py anon     genera data/marcaciones.csv desde fuentes/MARCACIONES.xlsx
    python tasks.py seed     carga la dimensión de sedes en la BD
    python tasks.py check    verifica esquema, sedes y topic con 3 particiones
"""

from __future__ import annotations

import csv
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def load_env() -> None:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def sh(*cmd: str) -> None:
    print("$", " ".join(cmd))
    subprocess.run(cmd, cwd=ROOT, check=True)


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
                 "batch_view_b", "speed_view", "lambda_cutoff", "kappa_serving", "batch_active",
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


TASKS = {f.__name__: f for f in (up, down, reset, anon, seed, check)}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in TASKS:
        sys.exit(__doc__)
    load_env()
    TASKS[sys.argv[1]]()
