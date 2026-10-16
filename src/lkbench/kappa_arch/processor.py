"""Procesador de streaming Kappa: topic -> kappa_view_vN.

Exactly-once (efecto): agregado, event_id vistos y offsets en una sola transacción (ver stream.py).
Idempotencia: kappa_seen_vN guarda los event_id ya contados, así reenviar el mismo archivo al
topic no duplica marcaciones. Cuesta un índice más por evento (exactitud frente a rendimiento).

    python -m lkbench.kappa_arch.processor [--version N] [--until-caught-up]
"""

from __future__ import annotations

import argparse
import logging
import re

import psycopg
from psycopg import sql

from lkbench import db
from lkbench.config import Settings, settings
from lkbench.stream import Stats, StreamConsumer, run_cli, stored_offsets

__all__ = ["Processor", "Stats", "active_version", "latest_version", "stored_offsets", "view_table"]

VIEW_RE = re.compile(r"^kappa_view_v(\d+)$")


def view_table(v: int) -> str:
    return f"kappa_view_v{v}"


def seen_table(v: int) -> str:
    return f"kappa_seen_v{v}"


def consumer_name(v: int) -> str:
    return f"kappa_v{v}"


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


class Processor(StreamConsumer):
    columns = ("event_id", "sede_id", "bucket", "tardanza")

    def __init__(self, s: Settings, version: int, batch_size: int = 5000):
        super().__init__(s, batch_size)
        self.version = version
        self.name = consumer_name(version)
        self._view, self._seen = sql.Identifier(view_table(version)), sql.Identifier(seen_table(version))
        self._upsert = sql.SQL("""
            WITH nuevos AS (
                INSERT INTO {seen} (event_id)
                SELECT DISTINCT event_id FROM stage
                ON CONFLICT DO NOTHING
                RETURNING event_id
            ), agg AS (
                SELECT s.sede_id, s.bucket, count(*) AS m, count(*) FILTER (WHERE s.tardanza) AS t
                FROM (SELECT DISTINCT ON (event_id) * FROM stage) s
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
        """).format(seen=self._seen, view=self._view)

    def setup(self, conn: psycopg.Connection) -> None:
        conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (LIKE agg_template INCLUDING ALL)")
                     .format(self._view))
        conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (event_id uuid PRIMARY KEY)").format(self._seen))

    def write(self, cur: psycopg.Cursor) -> int:
        return cur.execute(self._upsert).fetchone()[0]


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
    run_cli(Processor(s, v, a.batch_size), a.until_caught_up)


if __name__ == "__main__":
    main()
