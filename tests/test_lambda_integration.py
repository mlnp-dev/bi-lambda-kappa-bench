"""Lambda de punta a punta sobre un topic y un esquema de prueba propios (bloque = 50 offsets).

Cubre: lambda_serving = batch + speed cuadra exactamente con master_events en cada momento,
reenvíos antes y después del cutoff, alternancia A/B, purga exacta de la speed layer y
ejecución concurrente del batch con el master sink y la speed layer.
"""

import threading

import pytest
from conftest import make_events

pytestmark = pytest.mark.integration


def check(s, expected):
    from lkbench import db
    from lkbench.consistency import compare, master_reference

    with db.connect(s) as conn:
        assert master_reference(conn) == expected
        d = compare(conn, "lambda_serving", expected)
        assert d.ok, str(d)


def speed_blocks_below_cutoff(s) -> int:
    from lkbench import db

    with db.connect(s) as conn:
        return conn.execute(
            "SELECT count(*) FROM speed_view v JOIN lambda_cutoff c ON c.partition = v.kafka_partition "
            "WHERE v.offset_block < c.cutoff_offset / %s", (s.lambda_block,)).fetchone()[0]


def test_lambda_punta_a_punta(s, events):
    from lkbench import kafka
    from lkbench.events import aggregate
    from lkbench.lambda_arch.batch_layer import run_batch
    from lkbench.lambda_arch.master_sink import MasterSink
    from lkbench.lambda_arch.speed_layer import SpeedLayer
    from lkbench.producer import send

    prod = kafka.producer(s.kafka_bootstrap)
    first, rest = events[:300], events[300:]

    # 1) Solo speed layer (batch vacío).
    send(prod, s.topic, first)
    assert MasterSink(s, 40).run(until_caught_up=True).counted == 300
    assert SpeedLayer(s, 40).run(until_caught_up=True).counted == 300
    check(s, aggregate(first))

    # 2) Primer batch: pasa a la tabla B, mueve el cutoff y purga la speed layer.
    b1 = run_batch(s)
    assert b1.table == "batch_view_b"
    assert all(c % s.lambda_block == 0 for c in b1.cutoff.values())
    assert 0 < b1.events <= 300
    assert speed_blocks_below_cutoff(s) == 0
    check(s, aggregate(first))

    # 3) Nuevos eventos + reenvíos de eventos ya en el batch y de eventos aún en la speed layer.
    send(prod, s.topic, rest)
    send(prod, s.topic, first[:100] + rest[:50])
    sp = SpeedLayer(s, 40).run(until_caught_up=True)   # la speed layer va por delante del master
    ms = MasterSink(s, 40).run(until_caught_up=True)
    assert ms.counted == 300 and ms.read == 450
    assert (sp.read, sp.counted) == (450, 300)           # nunca cuenta reenvíos
    check(s, aggregate(events))

    # 4) Segundo batch: vuelve a la tabla A y todo sigue cuadrando.
    b2 = run_batch(s)
    assert b2.table == "batch_view_a"
    assert speed_blocks_below_cutoff(s) == 0
    check(s, aggregate(events))


def test_lambda_concurrente(s):
    from lkbench import kafka
    from lkbench.events import aggregate
    from lkbench.lambda_arch.batch_layer import run_batch
    from lkbench.lambda_arch.master_sink import MasterSink
    from lkbench.lambda_arch.speed_layer import SpeedLayer
    from lkbench.producer import send

    evs = make_events(1500, seed=7)
    produced, stop, errors, batches = threading.Event(), threading.Event(), [], []

    def guard(fn):
        def wrapped():
            try:
                fn()
            except Exception as exc:  # noqa: BLE001 - se reporta en el hilo principal
                errors.append(exc)
        return wrapped

    def producer():
        prod = kafka.producer(s.kafka_bootstrap)
        send(prod, s.topic, evs, rate=3000)
        send(prod, s.topic, evs[:300] + evs[1000:1200], rate=3000)
        produced.set()

    def consume(cls):
        def loop():
            while True:
                done = produced.is_set()
                st = cls(s, 25).run(until_caught_up=True)
                if done and st.read == 0:
                    return
        return loop

    def batch_loop():
        while not stop.is_set():
            batches.append(run_batch(s))

    threads = [threading.Thread(target=guard(f)) for f in
               (producer, consume(MasterSink), consume(SpeedLayer))]
    bt = threading.Thread(target=guard(batch_loop))
    for t in threads + [bt]:
        t.start()
    for t in threads:
        t.join(120)
    stop.set()
    bt.join(60)
    assert not errors, errors
    assert len(batches) > 3   # el batch corrió varias veces en paralelo con la ingesta
    check(s, aggregate(evs))
    run_batch(s)
    check(s, aggregate(evs))


def test_batch_no_purga_un_lote_speed_a_medias(s, events):
    """Fuerza la carrera: el batch corre mientras un lote de la speed layer aún no confirma.

    Con el bloqueo compartido sobre lambda_cutoff, el batch espera al commit del lote y luego lo
    purga; sin el bloqueo, purgaría antes y el lote quedaría contado dos veces (batch + speed).
    """
    from lkbench import kafka
    from lkbench.events import aggregate
    from lkbench.lambda_arch.batch_layer import run_batch
    from lkbench.lambda_arch.master_sink import MasterSink
    from lkbench.lambda_arch.speed_layer import SpeedLayer
    from lkbench.producer import send

    send(kafka.producer(s.kafka_bootstrap), s.topic, events)
    MasterSink(s).run(until_caught_up=True)
    batches = []

    class SpeedConBatchEnMedio(SpeedLayer):
        fired = False

        def _before_commit(self):
            if not self.fired:
                self.fired = True
                t = threading.Thread(target=lambda: batches.append(run_batch(s)))
                t.start()
                t.join(2)          # con bloqueo: el batch sigue esperando; sin bloqueo: ya terminó
                self.batch_thread = t

    sp = SpeedConBatchEnMedio(s, 100)
    sp.run(until_caught_up=True)
    sp.batch_thread.join(30)
    assert len(batches) == 1       # el batch terminó (después del commit del lote)
    check(s, aggregate(events))
