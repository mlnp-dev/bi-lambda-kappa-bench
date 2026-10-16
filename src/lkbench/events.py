"""Modelo del evento de marcación y su serialización."""

from __future__ import annotations

import datetime as dt
import json
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, replace

TIPOS = ("ENTRADA", "SALIDA", "REF_SALIDA", "REF_RETORNO")

Agg = dict[tuple[int, dt.datetime], tuple[int, int]]   # (sede, hora) -> (marcaciones, tardanzas)


@dataclass(frozen=True)
class Event:
    event_id: uuid.UUID
    staff_id: int
    sede_id: int
    tipo: str
    tardanza: bool
    event_ts: dt.datetime   # con zona horaria (-05:00)
    produced_at: int = 0    # epoch en ms; se fija al enviar

    @property
    def bucket(self) -> dt.datetime:
        """Hora de la marcación (truncada), en la que se agrega."""
        return self.event_ts.replace(minute=0, second=0, microsecond=0)

    @property
    def key(self) -> bytes:
        return str(self.sede_id).encode()

    def to_json(self) -> bytes:
        return json.dumps({
            "event_id": str(self.event_id),
            "staff_id": self.staff_id,
            "sede_id": self.sede_id,
            "tipo": self.tipo,
            "tardanza": self.tardanza,
            "event_ts": self.event_ts.isoformat(),
            "produced_at": self.produced_at,
        }, separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, raw: bytes | str) -> Event:
        return cls.from_dict(json.loads(raw))

    @classmethod
    def from_dict(cls, d: dict) -> Event:
        return cls(
            event_id=uuid.UUID(str(d["event_id"])),
            staff_id=int(d["staff_id"]),
            sede_id=int(d["sede_id"]),
            tipo=_tipo(d["tipo"]),
            tardanza=_bool(d["tardanza"]),
            event_ts=_aware(d["event_ts"]),
            produced_at=int(d.get("produced_at") or 0),
        )

    @classmethod
    def from_csv_row(cls, r: dict[str, str]) -> Event:
        return cls.from_dict(r)

    def stamped(self, produced_at: int | None = None) -> Event:
        """Copia con produced_at = ahora (ms), para medir la latencia de extremo a extremo."""
        ms = produced_at if produced_at is not None else time.time_ns() // 1_000_000
        return replace(self, produced_at=ms)


def _tipo(t: str) -> str:
    if t not in TIPOS:
        raise ValueError(f"tipo inválido: {t!r}")
    return t


def _bool(v) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("true", "1")


def _aware(v) -> dt.datetime:
    ts = v if isinstance(v, dt.datetime) else dt.datetime.fromisoformat(str(v))
    if ts.tzinfo is None:
        raise ValueError("event_ts debe incluir zona horaria")
    return ts


def aggregate(events: Iterable[Event]) -> Agg:
    """Agregado de referencia, sin duplicados por event_id (sirve para verificar las vistas)."""
    seen: set[uuid.UUID] = set()
    out: dict[tuple[int, dt.datetime], list[int]] = {}
    for e in events:
        if e.event_id in seen:
            continue
        seen.add(e.event_id)
        acc = out.setdefault((e.sede_id, e.bucket), [0, 0])
        acc[0] += 1
        acc[1] += int(e.tardanza)
    return {k: (m, t) for k, (m, t) in out.items()}
