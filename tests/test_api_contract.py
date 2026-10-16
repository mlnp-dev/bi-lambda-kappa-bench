"""Contrato de las APIs: REST y GraphQL devuelven lo mismo para las mismas operaciones.

Los datos se cargan directamente en un esquema de prueba: la misma información en Kappa
(kappa_view_v1) y en Lambda repartida entre batch y speed, para que ambas vistas coincidan.
"""

import datetime as dt
import re
import uuid

import pytest

pytestmark = pytest.mark.integration

TZ = dt.timezone(dt.timedelta(hours=-5))
H = [dt.datetime(2019, 4, 2, h, tzinfo=TZ) for h in (7, 8, 9)]
# (sede, hora, marcaciones, tardanzas)
ROWS = [(1, H[0], 10, 0), (1, H[1], 30, 6), (1, H[2], 5, 1), (2, H[1], 50, 20), (3, H[0], 2, 0)]
SEDES = [(1, "PUNO"), (2, "JULIACA"), (3, "ILAVE"), (4, "SIN DATOS")]


@pytest.fixture
def clients(s):
    from fastapi.testclient import TestClient

    from lkbench import db
    from lkbench.api import graphql_app, rest_app

    with db.connect(s) as conn:
        conn.cursor().executemany("INSERT INTO sedes VALUES (%s, %s)", SEDES)
        conn.cursor().executemany("INSERT INTO kappa_view_v1 VALUES (%s, %s, %s, %s)", ROWS)
        # Lambda: la primera fila de la sede 1 en speed (en dos bloques) y el resto en batch.
        conn.cursor().executemany("INSERT INTO batch_view_a VALUES (%s, %s, %s, %s)", ROWS[1:])
        conn.execute("INSERT INTO speed_view VALUES (1, %s, 0, 0, 4, 0), (1, %s, 1, 7, 6, 0)", (H[0], H[0]))
    with TestClient(rest_app.create_app(s)) as rest, TestClient(graphql_app.create_app(s)) as gql:
        yield rest, gql


def gql(client, query, **variables):
    r = client.post("/graphql", json={"query": query, "variables": variables})
    assert r.status_code == 200
    return r.json()


def camel_to_snake(obj):
    if isinstance(obj, list):
        return [camel_to_snake(x) for x in obj]
    if isinstance(obj, dict):
        return {re.sub(r"(?<!^)([A-Z])", r"_\1", k).lower(): camel_to_snake(v) for k, v in obj.items()}
    return obj


def same_instants(a, b):
    """Compara respuestas normalizando fechas ISO a instantes (la zona del texto puede variar)."""
    def norm(x):
        if isinstance(x, list):
            return [norm(i) for i in x]
        if isinstance(x, dict):
            return {k: norm(v) for k, v in x.items()}
        if isinstance(x, str) and re.match(r"^\d{4}-\d\d-\d\dT", x):
            return dt.datetime.fromisoformat(x).timestamp()
        return x
    return norm(a) == norm(b)


@pytest.mark.parametrize("arch", ["lambda", "kappa"])
def test_hourly(clients, arch):
    rest, g = clients
    r = rest.get("/api/v1/sedes/1/hourly", params={"arch": arch, "from": "2019-04-02T08:00:00-05:00"})
    assert r.status_code == 200
    q = gql(g, "query($a: Arch!, $f: DateTime) { hourly(sedeId: 1, from: $f, arch: $a) "
               "{ bucket marcaciones tardanzas } }", a=arch.upper(), f="2019-04-02T08:00:00-05:00")
    assert [(p["marcaciones"], p["tardanzas"]) for p in r.json()] == [(30, 6), (5, 1)]
    assert same_instants(r.json(), q["data"]["hourly"])


def test_hourly_lambda_suma_batch_y_speed(clients):
    rest, _ = clients
    for arch in ("lambda", "kappa"):
        first = rest.get("/api/v1/sedes/1/hourly", params={"arch": arch, "to": "2019-04-02T08:00:00"}).json()
        assert [(p["marcaciones"], p["tardanzas"]) for p in first] == [(10, 0)]   # fecha sin zona = -05:00


@pytest.mark.parametrize("arch", ["lambda", "kappa"])
def test_top_sedes(clients, arch):
    rest, g = clients
    r = rest.get("/api/v1/top-sedes", params={"limit": 2, "arch": arch}).json()
    q = gql(g, "query($a: Arch!) { topSedes(limit: 2, arch: $a) { sedeId nombre marcaciones tardanzas } }",
            a=arch.upper())
    assert r == [{"sede_id": 2, "nombre": "JULIACA", "marcaciones": 50, "tardanzas": 20},
                 {"sede_id": 1, "nombre": "PUNO", "marcaciones": 45, "tardanzas": 7}]
    assert camel_to_snake(q["data"]["topSedes"]) == r


@pytest.mark.parametrize("arch", ["lambda", "kappa"])
def test_summary(clients, arch):
    rest, g = clients
    r = rest.get("/api/v1/sedes/1/summary", params={"arch": arch}).json()
    q = gql(g, "query($a: Arch!) { summary(sedeId: 1, arch: $a) { sedeId nombre marcaciones tardanzas "
               "tasaTardanza horas desde hasta } }", a=arch.upper())
    assert (r["marcaciones"], r["tardanzas"], r["horas"], r["tasa_tardanza"]) == (45, 7, 3, round(7 / 45, 4))
    assert same_instants(camel_to_snake(q["data"]["summary"]), r)


def test_summary_sede_sin_datos_e_inexistente(clients):
    rest, g = clients
    vacia = rest.get("/api/v1/sedes/4/summary").json()
    assert (vacia["marcaciones"], vacia["horas"], vacia["desde"]) == (0, 0, None)
    assert rest.get("/api/v1/sedes/99/summary").status_code == 404
    assert gql(g, "{ summary(sedeId: 99) { sedeId } }")["data"] == {"summary": None}


@pytest.mark.parametrize("arch", ["batch_active", "lambda;DROP TABLE sedes", "LAMBDA"])
def test_arch_solo_lista_blanca(clients, arch):
    rest, g = clients
    assert rest.get("/api/v1/top-sedes", params={"arch": arch}).status_code == 422
    assert "errors" in gql(g, f'{{ topSedes(arch: {arch.split(";")[0].lower()}) {{ sedeId }} }}')


def test_validaciones(clients):
    rest, g = clients
    assert rest.get("/api/v1/top-sedes", params={"limit": 0}).status_code == 422
    assert "errors" in gql(g, "{ topSedes(limit: 0) { sedeId } }")
    assert rest.post("/api/v1/events", json=[]).status_code == 422


def _event(i, **kw):
    e = {"event_id": str(uuid.uuid4()), "staff_id": 100 + i, "sede_id": 1 + i % 3, "tipo": "ENTRADA",
         "tardanza": i % 2 == 0, "event_ts": f"2021-06-01T08:0{i}:00-05:00"}
    e.update(kw)
    return e


def test_ingesta_llega_a_redpanda(clients, s):
    from confluent_kafka import Consumer, TopicPartition

    from lkbench.events import Event

    rest, g = clients
    r = rest.post("/api/v1/events", json=[_event(i) for i in range(3)])
    assert (r.status_code, r.json()) == (202, {"accepted": 3})
    sin_zona = rest.post("/api/v1/events", json=[_event(0, event_ts="2021-06-01T08:00:00")])
    assert sin_zona.status_code == 422

    evs = [{"eventId": e["event_id"], "staffId": e["staff_id"], "sedeId": e["sede_id"], "tipo": e["tipo"],
            "tardanza": e["tardanza"], "eventTs": e["event_ts"], "producedAt": 1792180000000}
           for e in (_event(i) for i in range(2))]
    q = gql(g, "mutation($e: [EventInput!]!) { ingestEvents(events: $e) }", e=evs)
    assert q == {"data": {"ingestEvents": 2}}

    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": "contract", "enable.auto.commit": False})
    c.assign([TopicPartition(s.topic, p, 0) for p in range(3)])
    msgs = []
    while len(msgs) < 5:
        batch = c.consume(10, timeout=5)
        assert batch, "faltan mensajes en el topic"
        msgs += [Event.from_json(m.value()) for m in batch if m.error() is None]
    c.close()
    assert len(msgs) == 5
    fijos = [m for m in msgs if m.produced_at == 1792180000000]
    assert len(fijos) == 2                                                   # el de GraphQL se respeta
    assert all(m.produced_at > 1792180000000 for m in msgs if m not in fijos)  # REST lo pone la API


def test_salud_y_documentacion(clients):
    rest, g = clients
    assert rest.get("/health").json() == {"status": "ok"}
    assert g.get("/health").json() == {"status": "ok"}
    assert rest.get("/docs").status_code == 200
    assert "graphiql" in g.get("/graphql", headers={"Accept": "text/html"}).text.lower()
