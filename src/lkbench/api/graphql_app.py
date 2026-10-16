"""API GraphQL (Strawberry sobre FastAPI). GraphiQL en /graphql.

    uvicorn --factory lkbench.api.graphql_app:create_app --port 8001 --workers 1
"""

from __future__ import annotations

import datetime as dt
import enum
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Annotated, NewType

import strawberry
from fastapi import FastAPI, Request
from strawberry.fastapi import GraphQLRouter
from strawberry.schema.config import StrawberryConfig
from strawberry.types import Info

from lkbench.api.repository import MAX_EVENTS, Services, open_services
from lkbench.config import Settings, settings
from lkbench.events import Event

# Int de GraphQL es de 32 bits: un epoch en milisegundos no cabe.
BigInt = NewType("BigInt", int)
BIGINT = strawberry.scalar(name="BigInt", serialize=int, parse_value=int,
                           description="Entero de 64 bits (epoch en milisegundos)")


@strawberry.enum(description="Vista a consultar")
class Arch(enum.Enum):
    LAMBDA = "lambda"
    KAPPA = "kappa"


@strawberry.enum
class Tipo(enum.Enum):
    ENTRADA = "ENTRADA"
    SALIDA = "SALIDA"
    REF_SALIDA = "REF_SALIDA"
    REF_RETORNO = "REF_RETORNO"


@strawberry.type
class HourlyPoint:
    bucket: dt.datetime
    marcaciones: int
    tardanzas: int


@strawberry.type
class SedeTotal:
    sede_id: int
    nombre: str
    marcaciones: int
    tardanzas: int


@strawberry.type
class Summary:
    sede_id: int
    nombre: str
    marcaciones: int
    tardanzas: int
    tasa_tardanza: float
    horas: int
    desde: dt.datetime | None
    hasta: dt.datetime | None


@strawberry.input
class EventInput:
    event_id: uuid.UUID
    staff_id: int
    sede_id: int
    tipo: Tipo
    tardanza: bool
    event_ts: dt.datetime
    produced_at: BigInt = BigInt(0)


def _svc(info: Info) -> Services:
    return info.context["svc"]


@strawberry.type
class Query:
    @strawberry.field(description="Serie por hora de una sede en [from, to)")
    async def hourly(self, info: Info, sede_id: int,
                     from_: Annotated[dt.datetime | None, strawberry.argument(name="from")] = None,
                     to: dt.datetime | None = None, arch: Arch = Arch.LAMBDA) -> list[HourlyPoint]:
        rows = await _svc(info).repo.hourly(sede_id, from_, to, arch.value)
        return [HourlyPoint(**asdict(r)) for r in rows]

    @strawberry.field(description="Ranking de sedes por cantidad de marcaciones")
    async def top_sedes(self, info: Info, limit: int = 5, arch: Arch = Arch.LAMBDA) -> list[SedeTotal]:
        if not 1 <= limit <= 100:
            raise ValueError("limit debe estar entre 1 y 100")
        return [SedeTotal(**asdict(r)) for r in await _svc(info).repo.top_sedes(limit, arch.value)]

    @strawberry.field(description="Totales de una sede (null si no existe)")
    async def summary(self, info: Info, sede_id: int, arch: Arch = Arch.LAMBDA) -> Summary | None:
        r = await _svc(info).repo.summary(sede_id, arch.value)
        return Summary(**asdict(r)) if r else None


@strawberry.type
class Mutation:
    @strawberry.mutation(description="Publica eventos en Redpanda; devuelve cuántos aceptó el broker")
    async def ingest_events(self, info: Info, events: list[EventInput]) -> int:
        if not 1 <= len(events) <= MAX_EVENTS:
            raise ValueError(f"se aceptan entre 1 y {MAX_EVENTS} eventos por petición")
        evs = [Event.from_dict({**vars(e), "tipo": e.tipo.value}) for e in events]
        return await _svc(info).publisher.publish(evs)


schema = strawberry.Schema(query=Query, mutation=Mutation, config=StrawberryConfig(scalar_map={BigInt: BIGINT}))


def create_app(s: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with open_services(s or settings()) as svc:
            app.state.svc = svc
            yield

    async def context(request: Request) -> dict:
        return {"svc": request.app.state.svc}

    app = FastAPI(title="Marcaciones - API GraphQL", version="0.4.0", lifespan=lifespan,
                  docs_url=None, redoc_url=None)
    app.include_router(GraphQLRouter(schema, context_getter=context, graphql_ide="graphiql"), prefix="/graphql")

    @app.get("/health")
    async def health(request: Request) -> dict[str, str]:
        await request.app.state.svc.repo.ping()
        return {"status": "ok"}

    return app

