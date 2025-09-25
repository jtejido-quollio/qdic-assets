# app/main.py
import asyncio
import logging
from fastapi import FastAPI
from dependency_injector import containers, providers

from app.core.logging import configure_logging
from app.core.config import settings
from app.api.v1.routes import routers
from app.services.registration import register
from app.core.database import Database
from app.infrastructure.db.repositories.asset import AssetRepository
from app.infrastructure.db.repositories.event import EventRepository
from app.services.event import EventProcessorWorkerPool
from app.infrastructure.messaging.consumers.event import EventsRuntime

configure_logging()
log = logging.getLogger(__name__)

ENABLE_REGISTRATION = (settings.ENV or "").lower() != "local"


class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        modules=[
            "app.infrastructure.messaging.consumers.event",
        ]
    )
    db = providers.Singleton(Database, db_url=settings.DATABASE_URL)
    asset_repo = providers.Factory(AssetRepository, session_factory=db.provided.session)
    event_repo = providers.Factory(EventRepository, session_factory=db.provided.session)
    processor_pool = providers.Factory(
        EventProcessorWorkerPool,
        event_repo=event_repo,
        asset_repo=asset_repo,
        max_workers=settings.EVENT_PROCESSOR_WORKER_POOL_SIZE,
        queue_capacity_factor=5,
    )


container = Container()


async def lifespan(app: FastAPI):
    reg_task = None
    events_runtime = None
    try:
        if ENABLE_REGISTRATION:
            reg_task = asyncio.create_task(register(), name="service-register")

        processor_pool = container.processor_pool()
        events_runtime = EventsRuntime(processor_pool)
        await events_runtime.start()

        app.state.container = container
        app.state.events_runtime = events_runtime
        app.state.reg_task = reg_task

        yield

    finally:
        if events_runtime:
            await events_runtime.stop()

        if reg_task:
            reg_task.cancel()
            await asyncio.gather(reg_task, return_exceptions=True)


app = FastAPI(
    title="QDIC Assets",
    version="0.0.1",
    lifespan=lifespan,
)


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "tenant": settings.TENANT_ID}


app.include_router(routers, prefix="/api/v1")
