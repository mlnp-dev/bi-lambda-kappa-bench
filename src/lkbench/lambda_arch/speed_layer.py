"""Speed layer de Lambda: agrega de forma incremental los eventos posteriores al cutoff.

- Agrega en speed_view por (sede, hora, partición, bloque de offsets). Como el cutoff del batch
  está alineado a bloques, la purga borra exactamente lo que el batch ya cubre.
- Exactitud frente a reenvíos: un evento se cuenta solo si (a) su offset es >= cutoff,
  (b) el batch no lo contó ya (no está en master_events con offset < cutoff) y (c) la speed
  layer no lo contó antes (speed_seen).
- Cada lote toma un bloqueo compartido sobre lambda_cutoff: el batch no puede mover el cutoff
  ni purgar mientras un lote está a medias, y viceversa.

    python -m lkbench.lambda_arch.speed_layer [--until-caught-up]
"""

from __future__ import annotations

import argparse
import logging

import psycopg

from lkbench.config import Settings, settings
from lkbench.stream import StreamConsumer, run_cli

NAME = "lambda_speed"


def cutoffs(conn: psycopg.Connection) -> dict[int, int]:
    return {p: c for p, c in conn.execute("SELECT partition, cutoff_offset FROM lambda_cutoff")}


class SpeedLayer(StreamConsumer):
    name = NAME
    columns = ("event_id", "sede_id", "bucket", "tardanza", "kafka_partition", "kafka_offset")

    def __init__(self, s: Settings, batch_size: int = 5000):
        super().__init__(s, batch_size)
        self.block = s.lambda_block

    def setup(self, conn: psycopg.Connection) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS speed_seen (event_id uuid PRIMARY KEY, "
                     "kafka_partition smallint NOT NULL, offset_block bigint NOT NULL)")

    def initial_offsets(self, conn: psycopg.Connection, stored: dict[int, int]) -> dict[int, int]:
        # Lo anterior al cutoff ya está en el batch: no hace falta leerlo.
        cut = cutoffs(conn)
        return {p: max(stored.get(p, 0), cut.get(p, 0)) for p in stored.keys() | cut.keys()}

    def write(self, cur: psycopg.Cursor) -> int:
        cur.execute("SELECT 1 FROM lambda_cutoff FOR SHARE")
        return cur.execute("""
            WITH s AS (
                SELECT DISTINCT ON (st.event_id) st.*
                FROM stage st
                JOIN lambda_cutoff c ON c.partition = st.kafka_partition
                WHERE st.kafka_offset >= c.cutoff_offset
                  AND NOT EXISTS (SELECT 1 FROM master_events m
                                  WHERE m.event_id = st.event_id AND m.kafka_offset < c.cutoff_offset)
                ORDER BY st.event_id, st.kafka_offset
            ), nuevos AS (
                INSERT INTO speed_seen (event_id, kafka_partition, offset_block)
                SELECT event_id, kafka_partition, kafka_offset / %(b)s FROM s
                ON CONFLICT DO NOTHING
                RETURNING event_id
            ), agg AS (
                SELECT s.sede_id, s.bucket, s.kafka_partition, s.kafka_offset / %(b)s AS blk,
                       count(*) AS m, count(*) FILTER (WHERE s.tardanza) AS t
                FROM s JOIN nuevos USING (event_id)
                GROUP BY 1, 2, 3, 4
            ), upsert AS (
                INSERT INTO speed_view AS v (sede_id, bucket, kafka_partition, offset_block, marcaciones, tardanzas)
                SELECT sede_id, bucket, kafka_partition, blk, m, t FROM agg
                ON CONFLICT (sede_id, bucket, kafka_partition, offset_block) DO UPDATE
                   SET marcaciones = v.marcaciones + EXCLUDED.marcaciones,
                       tardanzas   = v.tardanzas   + EXCLUDED.tardanzas
            )
            SELECT count(*) FROM nuevos
        """, {"b": self.block}).fetchone()[0]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--until-caught-up", action="store_true")
    ap.add_argument("--batch-size", type=int, default=5000)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run_cli(SpeedLayer(settings(), a.batch_size), a.until_caught_up)


if __name__ == "__main__":
    main()
