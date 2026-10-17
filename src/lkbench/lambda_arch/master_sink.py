"""Master sink de Lambda: guarda cada evento tal cual en master_events (solo inserción).

event_id es la clave primaria: un reenvío se ignora y queda la primera aparición. Como la clave
del mensaje es sede_id, todas las copias de un evento caen en la misma partición, así que la
primera aparición es la de menor offset. Se guardan partición y offset para el cutoff del batch.

    python -m lkbench.lambda_arch.master_sink [--until-caught-up]
"""

from __future__ import annotations

import argparse
import logging

import psycopg

from lkbench.config import settings
from lkbench.stream import StreamConsumer, run_cli

NAME = "lambda_master"


class MasterSink(StreamConsumer):
    name = NAME
    columns = ("event_id", "staff_id", "sede_id", "tipo", "tardanza", "event_ts", "produced_at",
               "kafka_partition", "kafka_offset")

    def write(self, cur: psycopg.Cursor) -> int:
        cols = ", ".join(self.columns)
        cur.execute(f"""
            INSERT INTO master_events ({cols})
            SELECT DISTINCT ON (event_id) {cols} FROM stage ORDER BY event_id, kafka_offset
            ON CONFLICT (event_id) DO NOTHING
        """)
        return cur.rowcount


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--until-caught-up", action="store_true")
    ap.add_argument("--batch-size", type=int, default=5000)
    ap.add_argument("--json", action="store_true", help="imprimir las estadísticas en JSON")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run_cli(MasterSink(settings(), a.batch_size), a.until_caught_up, a.json)


if __name__ == "__main__":
    main()
