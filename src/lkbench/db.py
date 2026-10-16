"""Conexión a PostgreSQL."""

from __future__ import annotations

import psycopg

from lkbench.config import Settings


def connect(s: Settings, autocommit: bool = False) -> psycopg.Connection:
    # search_path permite aislar los tests en un esquema propio.
    return psycopg.connect(s.dsn, autocommit=autocommit, options=f"-c search_path={s.pg_schema}")
