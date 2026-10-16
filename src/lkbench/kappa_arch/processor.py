"""Procesador de streaming Kappa: topic -> kappa_view_vN.

Garantía exactly-once (efecto): por cada lote, el agregado, los event_id vistos y los offsets
se escriben en UNA transacción de PostgreSQL. Al arrancar se leen los offsets de la BD y se
hace seek; los offsets de Kafka no se usan (enable.auto.commit = false).

Idempotencia: kappa_seen_vN guarda los event_id ya contados, así reenviar el mismo archivo al
topic no duplica marcaciones. Cuesta un índice más por evento (exactitud frente a rendimiento).

    python -m lkbench.kappa_arch.processor [--version N] [--until-caught-up]
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass, field

import psycopg
from confluent_kafka import OFFSET_BEGINNING, KafkaError, TopicPartition
from psycopg import sql

from lkbench import db, kafka
from lkbench.config import Settings, settings
from lkbench.events import Event

log = logging.getLogger("kappa")

VIEW_RE = re.compile(r"^kappa_view_v(\d+)$")


def view_table(v: int) -> str:
    return f"kappa_view_v{v}"


def seen_table(v: int) -> str:
    return f"kappa_seen_v{v}"


def consumer_name(v: int) -> str:
    return f"kappa_v{v}"


def ensure_version(conn: psycopg.Connection, v: int) -> None:
    with conn.transaction():
        conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (LIKE agg_template INCLUDING ALL)")
                     .format(sql.Identifier(view_table(v))))
        conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (event_id uuid PRIMARY KEY)")
                     .format(sql.Identifier(seen_table(v))))


def active_version(conn: psycopg.Connection) -> int:
    """Versión a la que apunta hoy la vista kappa_serving."""
    rows = conn.execute(
        "SELECT table_name FROM information_schema.view_table_usage "
        "WHERE view_schema = current_schema() AND view_name = 'kappa_serving'").fetchall()
    versions = [int(m.group(1)) for (t,) in rows if (m := VIEW_RE.match(t))]
    if len(versions) != 1:
        raise RuntimeError(f"kappa_serving no apunta a una sola versión: {rows}")
    return versions[0]


def latest_version(conn: psycopg.Connection) -> int:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = current_schema() AND table_name LIKE 'kappa_view_v%'").fetchall()
    return max((int(m.group(1)) for (t,) in rows if (m := VIEW_RE.match(t))), default=0)


def stored_offsets(conn: psycopg.Connection, consumer: str) -> dict[int, int]:
    rows = conn.execute("SELECT partition, next_offset FROM consumer_offsets WHERE consumer = %s",
                        (consumer,)).fetchall()
    return {p: o for p, o in rows}


@dataclass
class Stats:
    read: int = 0        # mensajes leídos del topic
    counted: int = 0     # eventos nuevos sumados al agregado (sin duplicados)
    invalid: int = 0     # mensajes mal formados (se saltan, pero avanzan el offset)
    batches: int = 0
    seconds: float = 0.0
    offsets: dict[int, int] = field(default_factory=dict)

    @property
    def throughput(self) -> float:
        return self.read / self.seconds if self.seconds else 0.0


class Processor:
    def __init__(self, s: Settings, version: int, batch_size: int = 5000):
        self.s = s
        self.version = version
        self.batch_size = batch_size
        self.consumer = consumer_name(version)
        view, seen = sql.Identifier(view_table(version)), sql.Identifier(seen_table(version))
        self._upsert = sql.SQL("""
            WITH nuevos AS (
                INSERT INTO {seen} (event_id)
                SELECT DISTINCT event_id FROM kappa_stage
                ON CONFLICT DO NOTHING
                RETURNING event_id
            ), agg AS (
                SELECT s.sede_id, s.bucket, count(*) AS m, count(*) FILTER (WHERE s.tardanza) AS t
                FROM (SELECT DISTINCT ON (event_id) * FROM kappa_stage) s
                JOIN nuevos USING (event_id)
                GROUP BY s.sede_id, s.bucket
            ), upsert AS (
                INSERT INTO {view} AS v (sede_id, bucket, marcaciones, tardanzas)
                SELECT sede_id, bucket, m, t FROM agg
                ON CONFLICT (sede_id, bucket) DO UPDATE
                   SET marcaciones = v.marcaciones + EXCLUDED.marcaciones,
                       tardanzas   = v.tardanzas   + EXCLUDED.tardanzas
            )
            SELECT count(*) FROM nuevos
        """).format(seen=seen, view=view)

    def run(self, until_caught_up: bool = False, max_batches: int | None = None) -> Stats:
        st = Stats()
        with db.connect(self.s) as conn:
            ensure_version(conn, self.version)
            conn.execute("CREATE TEMP TABLE IF NOT EXISTS kappa_stage (event_id uuid, sede_id smallint, "
                         "bucket timestamptz, tardanza boolean) ON COMMIT DELETE ROWS")
            conn.commit()
            nxt = stored_offsets(conn, self.consumer)

            c = kafka.consumer(self.s.kafka_bootstrap, f"lkbench-{self.consumer}")
            try:
                parts = kafka.partitions(c, self.s.topic)
                c.assign([TopicPartition(self.s.topic, p, nxt.get(p, OFFSET_BEGINNING)) for p in parts])
                target = kafka.high_watermarks(c, self.s.topic, parts) if until_caught_up else {}
                log.info("v%d desde offsets %s (objetivo %s)", self.version, nxt, target or "continuo")
                t0 = time.perf_counter()
                while max_batches is None or st.batches < max_batches:
                    msgs = c.consume(self.batch_size, timeout=0.5)
                    good = [m for m in msgs if not _error(m)]
                    if good:
                        self._process(conn, good, nxt, st)
                    if until_caught_up and self._caught_up(c, parts, nxt, target):
                        break
                st.seconds = time.perf_counter() - t0
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
                log.warning("mensaje inválido p%d@%d: %s", m.partition(), m.offset(), exc)
                continue
            rows.append((e.event_id, e.sede_id, e.bucket, e.tardanza))
        with conn.transaction(), conn.cursor() as cur:
            with cur.copy("COPY kappa_stage (event_id, sede_id, bucket, tardanza) FROM STDIN") as cp:
                for r in rows:
                    cp.write_row(r)
            counted = cur.execute(self._upsert).fetchone()[0]
            cur.executemany(
                "INSERT INTO consumer_offsets (consumer, partition, next_offset) VALUES (%s, %s, %s) "
                "ON CONFLICT (consumer, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset",
                [(self.consumer, p, o) for p, o in batch_off.items()])
            self._before_commit()
        nxt.update(batch_off)
        st.read += len(msgs)
        st.counted += counted
        st.batches += 1

    def _before_commit(self) -> None:
        """Punto de enganche para los tests (simular una caída antes del commit)."""

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


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--version", type=int, help="versión vN (por defecto, la que sirve kappa_serving)")
    ap.add_argument("--until-caught-up", action="store_true", help="terminar al alcanzar el final del topic")
    ap.add_argument("--batch-size", type=int, default=5000)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    s = settings()
    v = a.version
    if v is None:
        with db.connect(s) as conn:
            v = active_version(conn)
    try:
        st = Processor(s, v, a.batch_size).run(until_caught_up=a.until_caught_up)
    except KeyboardInterrupt:
        log.info("detenido; los offsets en la BD reflejan el último lote confirmado")
        return
    log.info("v%d: %d leídos, %d nuevos, %d inválidos, %d lotes, %.1f s (%.0f ev/s)",
             v, st.read, st.counted, st.invalid, st.batches, st.seconds, st.throughput)


if __name__ == "__main__":
    main()
