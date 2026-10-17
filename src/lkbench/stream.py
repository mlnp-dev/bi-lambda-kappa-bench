"""Bucle de consumo común con offsets guardados en la BD (exactly-once por efecto).

Cada lote se copia a una tabla temporal `stage` y la subclase la vuelca a sus tablas con SQL.
El volcado y los offsets se confirman en UNA transacción; al arrancar se leen los offsets de
la BD y se hace seek. Los offsets de Kafka no se usan (enable.auto.commit = false).
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field

import psycopg
from confluent_kafka import OFFSET_BEGINNING, KafkaError, TopicPartition

from lkbench import db, kafka
from lkbench.config import Settings
from lkbench.events import Event

log = logging.getLogger("stream")

COLTYPES = {
    "event_id": "uuid", "staff_id": "integer", "sede_id": "smallint", "tipo": "text",
    "tardanza": "boolean", "event_ts": "timestamptz", "bucket": "timestamptz",
    "produced_at": "bigint", "kafka_partition": "smallint", "kafka_offset": "bigint",
}


def stored_offsets(conn: psycopg.Connection, consumer: str) -> dict[int, int]:
    rows = conn.execute("SELECT partition, next_offset FROM consumer_offsets WHERE consumer = %s",
                        (consumer,)).fetchall()
    return {p: o for p, o in rows}


@dataclass
class Stats:
    read: int = 0        # mensajes leídos del topic
    counted: int = 0     # eventos nuevos escritos (sin duplicados)
    invalid: int = 0     # mensajes mal formados (se saltan, pero avanzan el offset)
    batches: int = 0
    seconds: float = 0.0
    started_at: float = 0.0   # epoch (s) al empezar a consumir
    ended_at: float = 0.0
    offsets: dict[int, int] = field(default_factory=dict)

    @property
    def throughput(self) -> float:
        return self.read / self.seconds if self.seconds else 0.0


class StreamConsumer:
    """Subclases: definen `name`, `columns`, `write()` y, si hace falta, `setup()`."""

    name: str
    columns: tuple[str, ...]

    def __init__(self, s: Settings, batch_size: int = 5000):
        self.s = s
        self.batch_size = batch_size
        # Con TRACK_LATENCY se copia también produced_at para medir la latencia de extremo a extremo.
        extra = ("produced_at",) if s.track_latency and "produced_at" not in self.columns else ()
        self._cols = self.columns + extra

    # --- puntos de extensión -------------------------------------------------------------
    def setup(self, conn: psycopg.Connection) -> None:
        """Crea las tablas propias si no existen."""

    def initial_offsets(self, conn: psycopg.Connection, stored: dict[int, int]) -> dict[int, int]:
        return stored

    def write(self, cur: psycopg.Cursor) -> int:
        """Vuelca `stage` a las tablas destino y devuelve cuántos eventos nuevos escribió."""
        raise NotImplementedError

    def _before_commit(self) -> None:
        """Punto de enganche para los tests (simular una caída antes del commit)."""

    # --- bucle ---------------------------------------------------------------------------
    def run(self, until_caught_up: bool = False, max_batches: int | None = None) -> Stats:
        st = Stats()
        with db.connect(self.s) as conn:
            self.setup(conn)
            if self.s.track_latency:
                conn.execute("CREATE TABLE IF NOT EXISTS e2e_latency (consumer text NOT NULL, "
                             "produced_at bigint NOT NULL, committed_at timestamptz NOT NULL)")
            cols = ", ".join(f"{c} {COLTYPES[c]}" for c in self._cols)
            conn.execute(f"CREATE TEMP TABLE IF NOT EXISTS stage ({cols}) ON COMMIT DELETE ROWS")
            conn.commit()
            nxt = self.initial_offsets(conn, stored_offsets(conn, self.name))
            conn.commit()

            c = kafka.consumer(self.s.kafka_bootstrap, f"lkbench-{self.name}")
            try:
                parts = kafka.partitions(c, self.s.topic)
                c.assign([TopicPartition(self.s.topic, p, nxt.get(p, OFFSET_BEGINNING)) for p in parts])
                target = kafka.high_watermarks(c, self.s.topic, parts) if until_caught_up else {}
                log.info("%s desde offsets %s (objetivo %s)", self.name, nxt, target or "continuo")
                t0, st.started_at = time.perf_counter(), time.time()
                while max_batches is None or st.batches < max_batches:
                    msgs = [m for m in c.consume(self.batch_size, timeout=0.5) if not _error(m)]
                    if msgs:
                        self._process(conn, msgs, nxt, st)
                    if until_caught_up and self._caught_up(c, parts, nxt, target):
                        break
                st.seconds, st.ended_at = time.perf_counter() - t0, time.time()
            finally:
                c.close()
        st.offsets = dict(nxt)
        return st

    def _process(self, conn: psycopg.Connection, msgs, nxt: dict[int, int], st: Stats) -> None:
        batch_off: dict[int, int] = {}
        rows = []
        for m in msgs:
            batch_off[m.partition()] = m.offset() + 1
            try:
                e = Event.from_json(m.value())
            except (ValueError, KeyError, TypeError) as exc:
                st.invalid += 1
                log.warning("%s: mensaje inválido p%d@%d: %s", self.name, m.partition(), m.offset(), exc)
                continue
            rows.append(self._row(e, m))
        with conn.transaction(), conn.cursor() as cur:
            with cur.copy(f"COPY stage ({', '.join(self._cols)}) FROM STDIN") as cp:
                for r in rows:
                    cp.write_row(r)
            counted = self.write(cur)
            if self.s.track_latency:
                # Hora de escritura dentro de la transacción, justo antes del commit.
                cur.execute("INSERT INTO e2e_latency SELECT %s, produced_at, clock_timestamp() FROM stage",
                            (self.name,))
            cur.executemany(
                "INSERT INTO consumer_offsets (consumer, partition, next_offset) VALUES (%s, %s, %s) "
                "ON CONFLICT (consumer, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset",
                [(self.name, p, o) for p, o in batch_off.items()])
            self._before_commit()
        nxt.update(batch_off)
        st.read += len(msgs)
        st.counted += counted
        st.batches += 1

    def _row(self, e: Event, m) -> tuple:
        v = {"event_id": e.event_id, "staff_id": e.staff_id, "sede_id": e.sede_id, "tipo": e.tipo,
             "tardanza": e.tardanza, "event_ts": e.event_ts, "bucket": e.bucket,
             "produced_at": e.produced_at, "kafka_partition": m.partition(), "kafka_offset": m.offset()}
        return tuple(v[c] for c in self._cols)

    def _caught_up(self, c, parts: list[int], nxt: dict[int, int], target: dict[int, int]) -> bool:
        # La posición del consumidor cubre offsets sin mensaje (p. ej. marcadores de control).
        pos = {tp.partition: tp.offset for tp in c.position([TopicPartition(self.s.topic, p) for p in parts])}
        return all(max(nxt.get(p, 0), pos.get(p, 0)) >= target[p] for p in parts)


def _error(m) -> bool:
    err = m.error()
    if err is None:
        return False
    if err.code() == KafkaError._PARTITION_EOF:
        return True
    raise RuntimeError(f"error de Kafka: {err}")


def print_stats(obj) -> None:
    """Línea legible por máquina para el benchmark (bench/run_ingest.py)."""
    print("STATS " + json.dumps(asdict(obj), default=str), flush=True)


def run_cli(consumer: StreamConsumer, until_caught_up: bool, as_json: bool = False) -> None:
    try:
        st = consumer.run(until_caught_up=until_caught_up)
    except KeyboardInterrupt:
        log.info("%s detenido; los offsets en la BD reflejan el último lote confirmado", consumer.name)
        return
    log.info("%s: %d leídos, %d nuevos, %d inválidos, %d lotes, %.1f s (%.0f ev/s)", consumer.name,
             st.read, st.counted, st.invalid, st.batches, st.seconds, st.throughput)
    if as_json:
        print_stats(st)
