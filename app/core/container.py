from dependency_injector import containers, providers

from app.core.config import settings
from app.core.database import Database
from app.infrastructure.db.repositories.asset import AssetRepository
from app.infrastructure.db.repositories.event import EventRepository

from app.services.asset import AssetService
from app.services.search import SearchService


class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        modules=[
            "app.api.v1.endpoint.asset",
            "app.api.v1.endpoint.search",
        ]
    )

    db = providers.Singleton(Database, db_url=settings.DATABASE_URL)

    asset_repository = providers.Factory(
        AssetRepository, session_factory=db.provided.session
    )

    event_repository = providers.Factory(
        EventRepository, session_factory=db.provided.session
    )

    asset_service = providers.Factory(
        AssetService, asset_repo=asset_repository, event_repo=event_repository
    )

    search_service = providers.Factory(SearchService, asset_repo=asset_repository)
