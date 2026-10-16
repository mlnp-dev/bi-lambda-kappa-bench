"""Generador: reemite las marcaciones anonimizadas al topic con produced_at actual.

    python -m lkbench.producer [--limit N] [--rate EV_POR_SEG] [--csv data/marcaciones.csv]
"""

from __future__ import annotations

import argparse
import csv
import time
from collections.abc import Iterable, Iterator
from pathlib import Path

from lkbench import kafka
from lkbench.config import settings
from lkbench.events import Event

DEFAULT_CSV = Path("data/marcaciones.csv")


def read_csv(path: Path, limit: int | None = None) -> Iterator[Event]:
    with path.open(encoding="utf-8", newline="") as f:
        for i, row in enumerate(csv.DictReader(f)):
            if limit is not None and i >= limit:
                return
            yield Event.from_csv_row(row)


def send(prod, topic: str, events: Iterable[Event], rate: float = 0) -> int:
    """Envía los eventos (clave = sede_id). rate > 0 limita a esa cantidad por segundo."""
    n = 0
    t0 = time.perf_counter()
    for e in events:
        while True:
            try:
                prod.produce(topic, key=e.key, value=e.stamped().to_json())
                break
            except BufferError:   # cola local llena: esperar confirmaciones del broker
                prod.poll(0.05)
        n += 1
        if n % 1000 == 0:
            prod.poll(0)
        if rate > 0:
            delay = n / rate - (time.perf_counter() - t0)
            if delay > 0:
                time.sleep(delay)
    if prod.flush(60) != 0:
        raise RuntimeError("no se confirmaron todos los mensajes")
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--rate", type=float, default=0, help="eventos/s (0 = sin límite)")
    a = ap.parse_args()
    s = settings()
    t0 = time.perf_counter()
    n = send(kafka.producer(s.kafka_bootstrap), s.topic, read_csv(a.csv, a.limit), a.rate)
    secs = time.perf_counter() - t0
    print(f"enviados {n} eventos a {s.topic} en {secs:.1f} s ({n / secs:,.0f} ev/s)")


if __name__ == "__main__":
    main()
