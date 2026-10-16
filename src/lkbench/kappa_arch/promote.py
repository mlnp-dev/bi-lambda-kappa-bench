"""Reproceso Kappa: replay completo desde el offset 0 hacia v(N+1) y cambio de la vista de servicio.

    python -m lkbench.kappa_arch.promote            # replay a una versión nueva y promoción
    python -m lkbench.kappa_arch.promote --to 2     # solo cambia kappa_serving a v2

Tras promover, el procesador continuo debe reiniciarse: toma la versión activa y sigue
desde los offsets guardados de esa versión.
"""

from __future__ import annotations

import argparse
import logging

import psycopg
from psycopg import sql

from lkbench import db
from lkbench.config import Settings, settings
from lkbench.kappa_arch.processor import Processor, Stats, latest_version, view_table

log = logging.getLogger("kappa")


def promote(conn: psycopg.Connection, v: int) -> None:
    """Cambia kappa_serving a kappa_view_vN en un solo paso atómico."""
    with conn.transaction():
        conn.execute(sql.SQL("CREATE OR REPLACE VIEW kappa_serving AS SELECT * FROM {}")
                     .format(sql.Identifier(view_table(v))))


def replay(s: Settings, batch_size: int = 5000) -> tuple[int, Stats]:
    """Reprocesa todo el topic en una versión nueva (grupo y offsets propios) y la promueve."""
    with db.connect(s) as conn:
        v = latest_version(conn) + 1
    st = Processor(s, v, batch_size).run(until_caught_up=True)
    with db.connect(s) as conn:
        promote(conn, v)
    return v, st


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--to", type=int, help="solo promover esta versión existente, sin replay")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    s = settings()
    if a.to is not None:
        with db.connect(s) as conn:
            promote(conn, a.to)
        log.info("kappa_serving -> %s", view_table(a.to))
        return
    v, st = replay(s)
    log.info("replay v%d: %d eventos en %.1f s (%.0f ev/s); kappa_serving -> %s",
             v, st.read, st.seconds, st.throughput, view_table(v))


if __name__ == "__main__":
    main()
