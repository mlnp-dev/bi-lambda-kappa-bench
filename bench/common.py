"""Utilidades del benchmark: contenedores, docker stats, esquemas y topics desechables."""

from __future__ import annotations

import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Self

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results"
sys.path.insert(0, str(ROOT / "src"))

APP_SERVICE = "kappa"   # cualquier servicio con la imagen de la aplicación sirve para `compose run`


def load_env() -> None:
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def log(msg: str) -> None:
    print(time.strftime("%H:%M:%S"), msg, flush=True)


# --- contenedores ----------------------------------------------------------------------------
def compose_run(module_args: list[str], env: dict[str, str], name: str, detach: bool = False,
                data: bool = False) -> list[str]:
    cmd = ["docker", "compose", "run", "--rm", "--no-deps", "--name", name]
    cmd += ["-d"] if detach else ["-T"]
    for k, v in env.items():
        cmd += ["-e", f"{k}={v}"]
    if data:
        cmd += ["-v", f"{ROOT / 'data'}:/app/data:ro"]
    return cmd + [APP_SERVICE, "python", "-m", *module_args]


def run(cmd: list[str], timeout: float = 900) -> str:
    r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=timeout, check=False)
    if r.returncode != 0:
        raise RuntimeError(f"falló {' '.join(cmd[:8])}...:\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}")
    return r.stdout


def stats_lines(text: str) -> list[dict]:
    """Líneas 'STATS {json}' que imprimen los procesadores con --json."""
    return [json.loads(line[6:]) for line in text.splitlines() if line.startswith("STATS ")]


def wait_log(name: str, needle: str, timeout: float = 60) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = subprocess.run(["docker", "logs", name], capture_output=True, text=True, encoding="utf-8", check=False)
        if needle in r.stdout + r.stderr:
            return
        time.sleep(0.5)
    raise TimeoutError(f"{name} no mostró {needle!r}")


def docker_logs(name: str) -> str:
    r = subprocess.run(["docker", "logs", name], capture_output=True, text=True, encoding="utf-8", check=False)
    return r.stdout


def remove(*names: str) -> None:
    subprocess.run(["docker", "rm", "-f", *names], capture_output=True, check=False)


# --- recursos (docker stats) -----------------------------------------------------------------
_UNITS = {"B": 1 / 2**20, "KiB": 1 / 1024, "kB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}


def _mib(text: str) -> float:
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", text.strip())
    return float(m.group(1)) * _UNITS.get(m.group(2), 1) if m else 0.0


class DockerStats:
    """Muestrea `docker stats` en segundo plano para los contenedores cuyo nombre contiene `match`."""

    def __init__(self, match: tuple[str, ...]):
        self.match = match
        self.samples: dict[str, list[tuple[float, float]]] = {}
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._loop, daemon=True)

    def __enter__(self) -> Self:
        self._t.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._t.join(10)

    def _loop(self) -> None:
        while not self._stop.is_set():
            r = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"],
                               capture_output=True, text=True, encoding="utf-8", check=False)
            for line in r.stdout.splitlines():
                d = json.loads(line)
                if any(m in d["Name"] for m in self.match):
                    cpu = float(d["CPUPerc"].rstrip("%") or 0)
                    self.samples.setdefault(d["Name"], []).append((cpu, _mib(d["MemUsage"].split("/")[0])))

    def summary(self) -> dict[str, dict[str, float]]:
        return {n: {"cpu_mean_pct": round(statistics.fmean(c for c, _ in s), 1),
                    "cpu_max_pct": round(max(c for c, _ in s), 1),
                    "mem_max_mib": round(max(m for _, m in s), 1), "samples": len(s)}
                for n, s in self.samples.items() if s}


# --- esquemas y topics desechables -----------------------------------------------------------
def create_schema(name: str) -> None:
    import psycopg

    from lkbench.config import settings

    with psycopg.connect(settings().dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
        conn.execute(f'CREATE SCHEMA "{name}"')
        conn.execute(f'SET search_path TO "{name}"')
        conn.execute((ROOT / "db" / "schema.sql").read_text(encoding="utf-8"))
        conn.execute(f'INSERT INTO "{name}".sedes SELECT * FROM public.sedes')


def drop_schema(name: str) -> None:
    import psycopg

    from lkbench.config import settings

    with psycopg.connect(settings().dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')


def admin():
    from confluent_kafka.admin import AdminClient

    from lkbench.config import settings

    return AdminClient({"bootstrap.servers": settings().kafka_bootstrap})


def create_topic(name: str, partitions: int = 3) -> None:
    from confluent_kafka.admin import NewTopic

    a = admin()
    if name in a.list_topics(timeout=10).topics:
        delete_topic(name)
    a.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])[name].result(30)


def delete_topic(name: str) -> None:
    a = admin()
    if name in a.list_topics(timeout=10).topics:
        a.delete_topics([name])[name].result(30)
        time.sleep(1)


def topic_size(name: str) -> int:
    from confluent_kafka import Consumer, TopicPartition

    from lkbench.config import settings

    c = Consumer({"bootstrap.servers": settings().kafka_bootstrap, "group.id": "lkbench-size"})
    try:
        parts = c.list_topics(name, timeout=10).topics[name].partitions
        total = 0
        for p in parts:
            lo, hi = c.get_watermark_offsets(TopicPartition(name, p), timeout=10)
            total += hi - lo
        return total
    finally:
        c.close()


# --- resultados ------------------------------------------------------------------------------
def write_json(name: str, obj) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / name
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return path


def median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None
