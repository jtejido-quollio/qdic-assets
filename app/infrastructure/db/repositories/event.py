from __future__ import annotations

from typing import Callable, Optional, List, AsyncContextManager
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update as sql_update
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db.repositories.base import BaseRepository
from app.infrastructure.db.models.event import Event
from app.domain.services.id import new_global_id
from app.domain.schemas.types import Type
from app.domain.schemas.events import EventStatus
from app.domain.schemas.events import Event as EventSchema


class EventRepository(BaseRepository):
    def __init__(
        self, session_factory: Callable[[], AsyncContextManager[AsyncSession]]
    ):
        super().__init__(session_factory, Event)

    async def create_event(
        self,
        body: Optional[str],
        user_id: str,
        event_type: str,
        operation: str,
    ) -> Event:
        async with self.session_factory() as session:
            now = datetime.now(timezone.utc)
            expires_at = now + timedelta(days=1)

            event = Event(
                id=new_global_id(Type.Event),
                event_type=event_type,
                body=body or "",
                status=EventStatus.PENDING,
                user_id=user_id,
                operation=operation,
                expires_at=expires_at,
                updated_by=user_id,
                created_at=now,
                is_authorized=True,
            )

            session.add(event)
            await session.commit()
            await session.refresh(event)
            return event

    async def get_duplicate_records(self, event: EventSchema) -> List[Event]:
        async with self.session_factory() as session:
            stmt = (
                select(Event)
                .where(Event.event_type == event.event_type)
                .where(Event.body == event.body)
                .where(Event.created_at > event.created_at)
                .where(Event.id != event.id)
                .order_by(Event.created_at.asc())
            )
            rows = (await session.execute(stmt)).scalars().all()
            return rows

    async def get_wait_time(self, event_id: str) -> Optional[int]:
        async with self.session_factory() as session:
            stmt = select(Event.wait_time).where(Event.id == event_id).limit(1)
            return await session.scalar(stmt)

    async def update_status_and_times(
        self,
        event_id: str,
        status: str,
        wait_time: Optional[int] = None,
        completed_in_seconds: Optional[int] = None,
    ) -> None:
        values = {"status": status}
        if wait_time is not None:
            values["wait_time"] = wait_time
        if completed_in_seconds is not None:
            values["completed_in_seconds"] = completed_in_seconds

        async with self.session_factory() as session:
            stmt = sql_update(Event).where(Event.id == event_id).values(**values)
            await session.execute(stmt)
            await session.commit()

    async def get_dependent_records(self, record: EventSchema) -> List[Event]:
        async with self.session_factory() as session:
            stmt = (
                select(Event)
                .where(Event.operation == record.operation)
                .where(Event.created_at < record.created_at)
            )
            if record.user_id:
                stmt = stmt.where(Event.user_id == record.user_id)

            stmt = stmt.order_by(Event.created_at.asc())
            rows = (await session.execute(stmt)).scalars().all()
            return rows
