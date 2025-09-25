from typing import List, Optional, Protocol
from app.infrastructure.db.models.event import Event
from app.domain.repositories.base import Repository
from app.domain.schemas.events import Event as EventSchema


class EventRepository(Repository, Protocol):
    async def create_event(
        self,
        body: Optional[str],
        tenant_id: str,
        user_id: str,
        event_type: str,
        operation: str,
    ) -> Event: ...

    async def get_duplicate_records(self, event: EventSchema) -> List[Event]: ...

    async def get_wait_time(self, event_id: str) -> Optional[int]: ...

    async def update_status_and_times(
        self,
        event_id: str,
        status: str,
        wait_time: Optional[int] = None,
        completed_in_seconds: Optional[int] = None,
    ) -> None: ...

    async def get_dependent_records(self, record: EventSchema) -> List[Event]: ...
