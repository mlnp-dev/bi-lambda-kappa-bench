"""Fábricas de productor y consumidor de Redpanda (API de Kafka)."""

from __future__ import annotations

from confluent_kafka import Consumer, Producer, TopicPartition


def producer(bootstrap: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,   # sin duplicados por reintentos del productor
        "acks": "all",
        "linger.ms": 5,
        "batch.size": 256 * 1024,
        "compression.type": "lz4",
    })


def consumer(bootstrap: str, group: str) -> Consumer:
    # Los offsets viven en la BD (misma transacción que el agregado), nunca en Kafka.
    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
    })


def partitions(c: Consumer, topic: str) -> list[int]:
    md = c.list_topics(topic, timeout=10).topics[topic]
    if md.error is not None:
        raise RuntimeError(f"topic {topic}: {md.error}")
    return sorted(md.partitions)


def high_watermarks(c: Consumer, topic: str, parts: list[int]) -> dict[int, int]:
    return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
