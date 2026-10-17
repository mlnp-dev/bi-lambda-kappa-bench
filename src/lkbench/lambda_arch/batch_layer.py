"""Batch layer de Lambda: recalcula TODO el agregado desde master_events hasta un cutoff.

1. cutoff[p] = offset confirmado por el master sink en la partición p, alineado hacia abajo
   al bloque de la speed layer (nunca retrocede).
2. Recalcula el agregado en la tabla inactiva (batch_view_a / batch_view_b).
3. En la MISMA transacción: actualiza el cutoff, purga speed_view y speed_seen por debajo del
   cutoff y cambia batch_active a la tabla recién calculada. Quien lee lambda_serving ve el
   estado anterior completo o el nuevo completo, nunca una mezcla.

    python -m lkbench.lambda_arch.batch_layer [--every SEGUNDOS]
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass

import psycopg
from psycopg import sql

from lkbench import db
from lkbench.config import Settings, settings
from lkbench.lambda_arch.master_sink import NAME as MASTER
from lkbench.lambda_arch.speed_layer import cutoffs
from lkbench.stream import print_stats, stored_offsets

log = logging.getLogger("batch")

TABLES = ("batch_view_a", "batch_view_b")


@dataclass
class BatchStats:
    table: str               # tabla que quedó activa
    cutoff: dict[int, int]
    rows: int                # filas (sede, hora) del agregado
    events: int              # eventos de master_events cubiertos
    seconds: float


def active_table(conn: psycopg.Connection) -> str:
    rows = [t for (t,) in conn.execute(
        "SELECT table_name FROM information_schema.view_table_usage "
        "WHERE view_schema = current_schema() AND view_name = 'batch_active'")]
    if len(rows) != 1 or rows[0] not in TABLES:
        raise RuntimeError(f"batch_active no apunta a una tabla A/B: {rows}")
    return rows[0]


def run_batch(s: Settings) -> BatchStats:
    t0 = time.perf_counter()
    b = s.lambda_block
    with db.connect(s) as conn, conn.transaction():
        old = cutoffs(conn)
        master = stored_offsets(conn, MASTER)
        cut = {p: max(old.get(p, 0), master.get(p, 0) // b * b) for p in old.keys() | master.keys()}
        parts, offs = list(cut), [cut[p] for p in cut]
        target = TABLES[1 - TABLES.index(active_table(conn))]
        tbl = sql.Identifier(target)

        # Recalculo completo (la tabla inactiva no la lee nadie).
        conn.execute(sql.SQL("TRUNCATE {}").format(tbl))
        conn.execute(sql.SQL("""
                INSERT INTO {} (sede_id, bucket, marcaciones, tardanzas)
                SELECT m.sede_id, date_trunc('hour', m.event_ts, 'UTC'),
                       count(*), count(*) FILTER (WHERE m.tardanza)
                FROM master_events m
                JOIN unnest(%s::smallint[], %s::bigint[]) AS c(p, off)
                  ON m.kafka_partition = c.p AND m.kafka_offset < c.off
                GROUP BY 1, 2
            """).format(tbl), (parts, offs))
        rows, events = conn.execute(sql.SQL("SELECT count(*), coalesce(sum(marcaciones), 0) FROM {}")
                                    .format(tbl)).fetchone()

        # Cambio atómico: cutoff, purga de la speed layer y vista activa.
        conn.execute("""
                INSERT INTO lambda_cutoff (partition, cutoff_offset)
                SELECT * FROM unnest(%s::smallint[], %s::bigint[])
                ON CONFLICT (partition) DO UPDATE SET cutoff_offset = EXCLUDED.cutoff_offset
            """, (parts, offs))
        for purged in ("speed_view", "speed_seen"):
            conn.execute(sql.SQL("""
                    DELETE FROM {} s USING lambda_cutoff c
                    WHERE s.kafka_partition = c.partition AND s.offset_block < c.cutoff_offset / %s
                """).format(sql.Identifier(purged)), (b,))
        conn.execute(sql.SQL("CREATE OR REPLACE VIEW batch_active AS SELECT * FROM {}").format(tbl))
    return BatchStats(target, cut, rows, int(events), time.perf_counter() - t0)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--every", type=float, help="repetir cada N segundos (por defecto, una sola vez)")
    ap.add_argument("--json", action="store_true", help="imprimir las estadísticas en JSON")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    s = settings()
    try:
        while True:
            st = run_batch(s)
            log.info("batch -> %s: %d eventos, %d filas, cutoff %s, %.2f s",
                     st.table, st.events, st.rows, st.cutoff, st.seconds)
            if a.json:
                print_stats(st)
            if a.every is None:
                break
            time.sleep(a.every)
    except KeyboardInterrupt:
        log.info("batch detenido")


if __name__ == "__main__":
    main()
