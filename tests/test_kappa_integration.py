"""Kappa de punta a punta sobre un topic y un esquema de prueba propios (no toca los datos reales).

Cubre: consistencia exacta, idempotencia ante reenvíos, reanudación desde los offsets de la BD,
atomicidad agregado+offsets ante una caída y replay a v2 con promoción.
"""

import datetime as dt
import random
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[1]
TZ = dt.timezone(dt.timedelta(hours=-5))


@pytest.fixture(scope="module")
def s():
    psycopg = pytest.importorskip("psycopg")
    from confluent_kafka.admin import AdminClient, NewTopic

    from lkbench.config import settings

    try:
        base = settings()
        with psycopg.connect(base.dsn, connect_timeout=3):
            pass
        admin = AdminClient({"bootstrap.servers": base.kafka_bootstrap, "socket.timeout.ms": 3000})
        admin.list_topics(timeout=5)
    except Exception as exc:  # noqa: BLE001 - cualquier fallo de conexión = entorno no disponible
        pytest.skip(f"Postgres/Redpanda no disponibles: {exc}")

    tag = uuid.uuid4().hex[:8]
    t = base.with_(pg_schema=f"test_{tag}", topic=f"test_{tag}")
    with psycopg.connect(base.dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{t.pg_schema}"')
        conn.execute(f'SET search_path TO "{t.pg_schema}"')
        conn.execute((ROOT / "db" / "schema.sql").read_text(encoding="utf-8"))
    admin.create_topics([NewTopic(t.topic, num_partitions=3, replication_factor=1)])[t.topic].result(10)
    yield t
    admin.delete_topics([t.topic])[t.topic].result(10)
    with psycopg.connect(base.dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{t.pg_schema}" CASCADE')


@pytest.fixture(scope="module")
def events():
    from lkbench.events import Event

    rnd = random.Random(42)
    base = dt.datetime(2019, 4, 1, 6, 0, tzinfo=TZ)
    return [Event(uuid.uuid4(), rnd.randint(1, 460), rnd.randint(1, 16),
                  rnd.choice(["ENTRADA", "SALIDA", "REF_SALIDA", "REF_RETORNO"]), rnd.random() < 0.2,
                  base + dt.timedelta(minutes=rnd.randint(0, 60 * 24 * 5))) for _ in range(600)]


def test_kappa_punta_a_punta(s, events):
    from lkbench import db, kafka
    from lkbench.consistency import compare
    from lkbench.events import aggregate
    from lkbench.kappa_arch.processor import Processor, active_version, stored_offsets
    from lkbench.kappa_arch.promote import replay
    from lkbench.producer import send

    prod = kafka.producer(s.kafka_bootstrap)
    send(prod, s.topic, events)
    send(prod, s.topic, events[:100])          # reenvío: no debe duplicar
    expected = aggregate(events)

    # 1) Caída antes del commit: no queda ni agregado ni offset.
    class Falla(Processor):
        def _before_commit(self):
            raise RuntimeError("caída simulada")

    with pytest.raises(RuntimeError, match="caída simulada"):
        Falla(s, 1, batch_size=50).run(until_caught_up=True)
    with db.connect(s) as conn:
        assert stored_offsets(conn, "kappa_v1") == {}
        assert conn.execute("SELECT count(*) FROM kappa_view_v1").fetchone()[0] == 0

    # 2) Procesa dos lotes, se detiene y se reanuda desde los offsets de la BD.
    p1 = Processor(s, 1, batch_size=50).run(until_caught_up=True, max_batches=2)
    p2 = Processor(s, 1, batch_size=50).run(until_caught_up=True)
    assert p1.batches == 2
    assert p1.read + p2.read == 700
    assert p1.counted + p2.counted == 600
    with db.connect(s) as conn:
        assert sum(stored_offsets(conn, "kappa_v1").values()) == 700
        assert compare(conn, "kappa_view_v1", expected).ok

    # 3) Volver a correr sin mensajes nuevos no cambia nada.
    assert Processor(s, 1).run(until_caught_up=True).read == 0

    # 4) Replay desde el offset 0 a v2 y promoción.
    v, st = replay(s, batch_size=200)
    assert (v, st.read, st.counted) == (2, 700, 600)
    with db.connect(s) as conn:
        assert active_version(conn) == 2
        assert compare(conn, "kappa_view_v2", expected).ok
        assert compare(conn, "kappa_serving", expected).ok
