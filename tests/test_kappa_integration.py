"""Kappa de punta a punta sobre un topic y un esquema de prueba propios (no toca los datos reales).

Cubre: consistencia exacta, idempotencia ante reenvíos, reanudación desde los offsets de la BD,
atomicidad agregado+offsets ante una caída y replay a v2 con promoción.
"""

import pytest

pytestmark = pytest.mark.integration


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
