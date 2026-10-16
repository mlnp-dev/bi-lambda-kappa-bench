"""Configuración leída solo de variables de entorno."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    pg_schema: str
    kafka_bootstrap: str
    topic: str
    lambda_block: int = 1000   # bloque de offsets de la speed layer; fijo durante la vida del topic

    @property
    def dsn(self) -> str:
        return (f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
                f"user={self.pg_user} password={self.pg_password}")

    def with_(self, **kw) -> Settings:
        return replace(self, **kw)


def settings() -> Settings:
    e = os.environ
    return Settings(
        pg_host=e.get("POSTGRES_HOST", "localhost"),
        pg_port=int(e.get("POSTGRES_PORT", "5432")),
        pg_db=e["POSTGRES_DB"],
        pg_user=e["POSTGRES_USER"],
        pg_password=e["POSTGRES_PASSWORD"],
        pg_schema=e.get("PG_SCHEMA", "public"),
        kafka_bootstrap=e.get("KAFKA_BOOTSTRAP", "localhost:19092"),
        topic=e.get("TOPIC", "marcaciones"),
        lambda_block=int(e.get("LAMBDA_BLOCK_SIZE", "1000")),
    )
