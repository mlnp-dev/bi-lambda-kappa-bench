"""Repositorio compartido por las APIs REST y GraphQL: mismas consultas, misma publicación.

`arch` se resuelve con una lista blanca a un identificador SQL; nunca se concatena texto del
usuario. Todas las consultas están parametrizadas.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from psycopg import sql
from psycopg_pool import AsyncConnectionPool

from lkbench import kafka
from lkbench.config import Settings
from lkbench.events import Event

ARCH_VIEWS = {"lambda": "lambda_serving", "kappa": "kappa_serving"}
ARCHS = tuple(ARCH_VIEWS)
LIMA = dt.timezone(dt.timedelta(hours=-5))
MAX_EVENTS = 1000   # eventos por petición de ingesta


def relation(arch: str) -> sql.Identifier:
    try:
        return sql.Identifier(ARCH_VIEWS[arch])
    except KeyError:
        raise ValueError(f"arch debe ser uno de {ARCHS}") from None


def _aware(ts: dt.datetime | None) -> dt.datetime | None:
    """Fechas sin zona se interpretan en hora de Perú (-05:00)."""
    return ts.replace(tzinfo=LIMA) if ts is not None and ts.tzinfo is None else ts


@dataclass(frozen=True)
class HourlyPoint:
    bucket: dt.datetime
    marcaciones: int
    tardanzas: int


@dataclass(frozen=True)
class SedeTotal:
    sede_id: int
    nombre: str
    marcaciones: int
    tardanzas: int


@dataclass(frozen=True)
class Summary:
    sede_id: int
    nombre: str
    marcaciones: int
    tardanzas: int
    tasa_tardanza: float      # tardanzas / marcaciones
    horas: int                # horas con al menos una marcación
    desde: dt.datetime | None
    hasta: dt.datetime | None


class Repository:
    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def ping(self) -> bool:
        async with self.pool.connection() as conn:
            await conn.execute("SELECT 1")
        return True

    async def hourly(self, sede_id: int, desde: dt.datetime | None, hasta: dt.datetime | None,
                     arch: str) -> list[HourlyPoint]:
        q = sql.SQL("""
            SELECT bucket, marcaciones, tardanzas FROM {}
            WHERE sede_id = %(sede)s
              AND (%(desde)s::timestamptz IS NULL OR bucket >= %(desde)s)
              AND (%(hasta)s::timestamptz IS NULL OR bucket <  %(hasta)s)
            ORDER BY bucket
        """).format(relation(arch))
        async with self.pool.connection() as conn:
            cur = await conn.execute(q, {"sede": sede_id, "desde": _aware(desde), "hasta": _aware(hasta)})
            return [HourlyPoint(*r) for r in await cur.fetchall()]

    async def top_sedes(self, limit: int, arch: str) -> list[SedeTotal]:
        q = sql.SQL("""
            SELECT s.sede_id, s.nombre, sum(v.marcaciones)::bigint, sum(v.tardanzas)::bigint
            FROM {} v JOIN sedes s USING (sede_id)
            GROUP BY s.sede_id, s.nombre
            ORDER BY 3 DESC, s.sede_id
            LIMIT %s
        """).format(relation(arch))
        async with self.pool.connection() as conn:
            cur = await conn.execute(q, (limit,))
            return [SedeTotal(*r) for r in await cur.fetchall()]

    async def summary(self, sede_id: int, arch: str) -> Summary | None:
        q = sql.SQL("""
            SELECT s.sede_id, s.nombre,
                   coalesce(sum(v.marcaciones), 0)::bigint, coalesce(sum(v.tardanzas), 0)::bigint,
                   count(v.bucket), min(v.bucket), max(v.bucket)
            FROM sedes s LEFT JOIN {} v ON v.sede_id = s.sede_id
            WHERE s.sede_id = %s
            GROUP BY s.sede_id, s.nombre
        """).format(relation(arch))
        async with self.pool.connection() as conn:
            cur = await conn.execute(q, (sede_id,))
            r = await cur.fetchone()
        if r is None:
            return None
        sid, nombre, m, t, horas, desde, hasta = r
        return Summary(sid, nombre, m, t, round(t / m, 4) if m else 0.0, horas, desde, hasta)


class EventPublisher:
    """Publica eventos en Redpanda; responde cuando el broker confirmó la escritura."""

    def __init__(self, s: Settings):
        self.topic = s.topic
        self.producer = kafka.producer(s.kafka_bootstrap)

    async def publish(self, events: list[Event]) -> int:
        if len(events) > MAX_EVENTS:
            raise ValueError(f"máximo {MAX_EVENTS} eventos por petición")
        for e in events:
            e = e if e.produced_at else e.stamped()
            while True:
                try:
                    self.producer.produce(self.topic, key=e.key, value=e.to_json())
                    break
                except BufferError:
                    await asyncio.to_thread(self.producer.poll, 0.05)
        pending = await asyncio.to_thread(self.producer.flush, 10)
        if pending:
            raise RuntimeError(f"{pending} eventos sin confirmar por el broker")
        return len(events)

    def close(self) -> None:
        self.producer.flush(10)


@dataclass
class Services:
    repo: Repository
    publisher: EventPublisher


@asynccontextmanager
async def open_services(s: Settings, pool_max: int = 10) -> AsyncIterator[Services]:
    pool = AsyncConnectionPool(s.dsn, min_size=2, max_size=pool_max, open=False,
                               kwargs={"options": f"-c search_path={s.pg_schema} -c TimeZone=America/Lima",
                                       "autocommit": True})
    await pool.open(wait=True, timeout=10)
    publisher = EventPublisher(s)
    try:
        yield Services(Repository(pool), publisher)
    finally:
        publisher.close()
        await pool.close()
