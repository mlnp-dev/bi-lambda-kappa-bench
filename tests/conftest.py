import datetime as dt
import os
import random
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TZ = dt.timezone(dt.timedelta(hours=-5))

# En local se usan las variables de .env; en CI no existe y los tests de integración se saltan.
_env = ROOT / ".env"
if _env.exists():
    for line in _env.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


@pytest.fixture
def s():
    """Esquema de Postgres y topic de 3 particiones propios del test; se borran al terminar."""
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
    t = base.with_(pg_schema=f"test_{tag}", topic=f"test_{tag}", lambda_block=50)
    with psycopg.connect(base.dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{t.pg_schema}"')
        conn.execute(f'SET search_path TO "{t.pg_schema}"')
        conn.execute((ROOT / "db" / "schema.sql").read_text(encoding="utf-8"))
    admin.create_topics([NewTopic(t.topic, num_partitions=3, replication_factor=1)])[t.topic].result(10)
    yield t
    admin.delete_topics([t.topic])[t.topic].result(10)
    with psycopg.connect(base.dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{t.pg_schema}" CASCADE')


def make_events(n: int, seed: int = 42):
    from lkbench.events import Event

    rnd = random.Random(seed)
    base = dt.datetime(2019, 4, 1, 6, 0, tzinfo=TZ)
    return [Event(uuid.UUID(int=rnd.getrandbits(128)), rnd.randint(1, 460), rnd.randint(1, 16),
                  rnd.choice(["ENTRADA", "SALIDA", "REF_SALIDA", "REF_RETORNO"]), rnd.random() < 0.2,
                  base + dt.timedelta(minutes=rnd.randint(0, 60 * 24 * 5))) for _ in range(n)]


@pytest.fixture
def events():
    return make_events(600)
