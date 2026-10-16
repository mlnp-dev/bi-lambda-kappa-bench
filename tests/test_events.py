import datetime as dt
import uuid

import pytest

from lkbench.events import Event, aggregate

TZ = dt.timezone(dt.timedelta(hours=-5))


def ev(minute=5, sede=3, tardanza=True, eid=None, hour=8):
    return Event(eid or uuid.uuid4(), 48213, sede, "ENTRADA", tardanza, dt.datetime(2019, 4, 2, hour, minute, tzinfo=TZ))


def test_json_ida_y_vuelta():
    e = ev().stamped(1792180000000)
    assert Event.from_json(e.to_json()) == e


def test_csv_row():
    e = Event.from_csv_row({"event_id": "fdcd5a4f-8640-5ee6-8c1a-c4a8e9841734", "staff_id": "4344733",
                            "sede_id": "7", "tipo": "SALIDA", "tardanza": "False",
                            "event_ts": "2019-01-01T17:56:00-05:00"})
    assert (e.sede_id, e.tipo, e.tardanza, e.produced_at) == (7, "SALIDA", False, 0)
    assert e.bucket == dt.datetime(2019, 1, 1, 17, tzinfo=TZ)


def test_bucket_y_clave():
    e = ev(minute=59)
    assert e.bucket == dt.datetime(2019, 4, 2, 8, 0, tzinfo=TZ)
    assert e.key == b"3"


def test_stamped_pone_hora_actual():
    assert ev().stamped().produced_at > 1_700_000_000_000


@pytest.mark.parametrize("campo,valor", [("tipo", "OTRO"), ("event_ts", "2019-04-02T08:05:00")])
def test_rechaza_invalidos(campo, valor):
    d = {"event_id": str(uuid.uuid4()), "staff_id": 1, "sede_id": 1, "tipo": "ENTRADA",
         "tardanza": False, "event_ts": "2019-04-02T08:05:00-05:00"}
    d[campo] = valor
    with pytest.raises(ValueError):
        Event.from_dict(d)


def test_aggregate_ignora_duplicados():
    a, b = ev(tardanza=True), ev(tardanza=False, minute=30)
    otra_hora = ev(hour=17, tardanza=False)
    agg = aggregate([a, b, a, otra_hora])
    assert agg == {(3, a.bucket): (2, 1), (3, otra_hora.bucket): (1, 0)}
