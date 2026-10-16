"""Verificación de consistencia: una vista agregada debe cuadrar exactamente con la referencia.

En la fase Kappa la referencia es el CSV anonimizado (deduplicado por event_id); con Lambda
será master_events.
"""

from __future__ import annotations

from dataclasses import dataclass

import psycopg
from psycopg import sql

from lkbench.events import Agg


@dataclass
class Diff:
    relation: str
    expected_rows: int
    actual_rows: int
    mismatches: list   # (clave, esperado, obtenido), máximo 10

    @property
    def ok(self) -> bool:
        return not self.mismatches and self.expected_rows == self.actual_rows

    def __str__(self) -> str:
        estado = "OK" if self.ok else f"DIFERENCIAS (ejemplos: {self.mismatches[:3]})"
        return f"{self.relation}: {self.actual_rows}/{self.expected_rows} filas -> {estado}"


def read_view(conn: psycopg.Connection, relation: str) -> Agg:
    q = sql.SQL("SELECT sede_id, bucket, marcaciones, tardanzas FROM {}").format(sql.Identifier(relation))
    return {(s, b): (m, t) for s, b, m, t in conn.execute(q)}


def compare(conn: psycopg.Connection, relation: str, expected: Agg) -> Diff:
    actual = read_view(conn, relation)
    bad = [(k, expected.get(k), actual.get(k))
           for k in expected.keys() | actual.keys() if expected.get(k) != actual.get(k)]
    return Diff(relation, len(expected), len(actual), sorted(bad, key=str)[:10])
