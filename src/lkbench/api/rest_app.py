"""API REST (FastAPI). Documentación en /docs.

    uvicorn --factory lkbench.api.rest_app:create_app --port 8000 --workers 1
"""

from __future__ import annotations

import datetime as dt
import uuid
from contextlib import asynccontextmanager
from typing import Annotated, Literal

from fastapi import Body, FastAPI, HTTPException, Path, Query, Request
from pydantic import AwareDatetime, BaseModel, Field

from lkbench.api.repository import MAX_EVENTS, HourlyPoint, SedeTotal, Services, Summary, open_services
from lkbench.config import Settings, settings
from lkbench.events import TIPOS, Event

Arch = Literal["lambda", "kappa"]
ArchQ = Annotated[Arch, Query(description="Vista a consultar: lambda_serving o kappa_serving")]
SedeId = Annotated[int, Path(ge=1, le=32767)]


class EventIn(BaseModel):
    event_id: uuid.UUID
    staff_id: int = Field(ge=0)
    sede_id: int = Field(ge=1, le=32767)
    tipo: Literal[TIPOS]  # type: ignore[valid-type]
    tardanza: bool
    event_ts: AwareDatetime
    produced_at: int = Field(0, ge=0, description="epoch en ms; si es 0 la API pone la hora actual")


class Accepted(BaseModel):
    accepted: int


def create_app(s: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with open_services(s or settings()) as svc:
            app.state.svc = svc
            yield

    app = FastAPI(title="Marcaciones - API REST", version="0.4.0", lifespan=lifespan,
                  description="Agregados de marcaciones por sede y hora sobre las vistas Lambda y Kappa.")

    def svc(request: Request) -> Services:
        return request.app.state.svc

    @app.get("/health", tags=["salud"])
    async def health(request: Request) -> dict[str, str]:
        await svc(request).repo.ping()
        return {"status": "ok"}

    @app.get("/api/v1/sedes/{sede_id}/hourly", tags=["consultas"])
    async def hourly(request: Request, sede_id: SedeId,
                     from_: Annotated[dt.datetime | None, Query(alias="from")] = None,
                     to: dt.datetime | None = None, arch: ArchQ = "lambda") -> list[HourlyPoint]:
        """Serie por hora de una sede: marcaciones y tardanzas en [from, to)."""
        return await svc(request).repo.hourly(sede_id, from_, to, arch)

    @app.get("/api/v1/top-sedes", tags=["consultas"])
    async def top_sedes(request: Request, limit: Annotated[int, Query(ge=1, le=100)] = 5,
                        arch: ArchQ = "lambda") -> list[SedeTotal]:
        """Ranking de sedes por cantidad de marcaciones."""
        return await svc(request).repo.top_sedes(limit, arch)

    @app.get("/api/v1/sedes/{sede_id}/summary", tags=["consultas"])
    async def summary(request: Request, sede_id: SedeId, arch: ArchQ = "lambda") -> Summary:
        """Totales de una sede: marcaciones, tardanzas, tasa de tardanza y rango de fechas."""
        r = await svc(request).repo.summary(sede_id, arch)
        if r is None:
            raise HTTPException(404, "sede no encontrada")
        return r

    @app.post("/api/v1/events", status_code=202, tags=["ingesta"])
    async def ingest(request: Request,
                     events: Annotated[list[EventIn], Body(min_length=1, max_length=MAX_EVENTS)]) -> Accepted:
        """Publica eventos en Redpanda (202: confirmados por el broker, aún no procesados)."""
        n = await svc(request).publisher.publish([Event(**e.model_dump()) for e in events])
        return Accepted(accepted=n)

    return app
