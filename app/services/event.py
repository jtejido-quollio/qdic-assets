import asyncio
import logging
from typing import Iterable, List, Optional, Union
from app.domain.schemas.events import (
    Event,
    EventContext,
    EventStatus,
    Operation,
    EventType,
)
from app.infrastructure.db.repositories.event import EventRepository
from app.infrastructure.db.repositories.asset import AssetRepository
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
DEPENDENCY_POLL_TIMEOUT_SECONDS = 120
EVENT_TABLE_POLLER_INTERVAL_MS = 100
REVISIBILITY_DELAY_SECONDS = 20
MAX_RETRY_COUNT = 4
dependencies: dict[Operation, list[EventType]] = {
    Operation.UPDATE_USER_GROUP: [],
    Operation.DELETE_USER_GROUP: [],
    Operation.CREATE_USER_GROUP: [],
    Operation.UPDATE_ASSET_GROUP: [],
    Operation.DELETE_ASSET_GROUP: [],
    Operation.LIST_ASSET_GROUP_MEMBERS_TREE: [],
    Operation.DELETE_TAG_GROUP: [
        EventType.UPDATE_RULES,
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_BI_DATAS,
        EventType.UPDATE_TAGS,
        EventType.UPDATE_USERS,
        EventType.UPDATE_CUSTOM_CATEGORIES,
    ],
    Operation.UPDATE_TAG_CATEGORY: [
        EventType.UPDATE_RULES,
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_BI_DATAS,
        EventType.UPDATE_TAGS,
        EventType.UPDATE_CUSTOM_CATEGORIES,
    ],
    Operation.DELETE_TAG_CATEGORY: [
        EventType.UPDATE_RULES,
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_BI_DATAS,
        EventType.UPDATE_TAGS,
        EventType.UPDATE_USERS,
        EventType.UPDATE_CUSTOM_CATEGORIES,
        EventType.DELETE_MISSING_COMMENTS,
    ],
    Operation.TAG_UPDATE_TAG: [
        EventType.UPDATE_RULES,
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_BI_DATAS,
        EventType.UPDATE_TAGS,
        EventType.UPDATE_CUSTOM_CATEGORIES,
    ],
    Operation.TAG_DELETE_TAG: [
        EventType.UPDATE_RULES,
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_BI_DATAS,
        EventType.UPDATE_TAGS,
        EventType.UPDATE_USERS,
        EventType.UPDATE_CUSTOM_CATEGORIES,
        EventType.DELETE_MISSING_COMMENTS,
    ],
    Operation.UPDATE_RULE_SET: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_TAGS,
    ],
    Operation.DELETE_RULE_SET: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_TAGS,
        EventType.UPDATE_USERS,
    ],
    Operation.CREATE_RULE: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_TAGS,
    ],
    Operation.UPDATE_RULE: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_TAGS,
    ],
    Operation.DELETE_RULE: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_TAGS,
    ],
    Operation.APPLY_RULE: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_TAGS,
    ],
    Operation.CREATE_PROPERTY_SET: [EventType.UPDATE_USER_GROUP_PROPERTY_SETS],
    Operation.UPDATE_PROPERTY_SET: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_TAGS,
    ],
    Operation.DELETE_PROPERTY_SET: [
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_BI_DATAS,
        EventType.UPDATE_TAGS,
        EventType.DELETE_MISSING_COMMENTS,
        EventType.UPDATE_USER_GROUP_PROPERTY_SETS,
    ],
    Operation.CREATE_PROPERTY: [EventType.UPDATE_USER_GROUP_PROPERTY],
    Operation.PROPERTY_UPDATE_PROPERTY: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_TAGS,
    ],
    Operation.DELETE_PROPERTY: [
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_BI_DATAS,
        EventType.UPDATE_TAGS,
        EventType.DELETE_MISSING_COMMENTS,
        EventType.UPDATE_USER_GROUP_PROPERTY,
    ],
    Operation.EXPORT_DATA: [],
    Operation.DELETE_COMMENT: [],
    Operation.DELETE_ASSETS: [
        EventType.DELETE_ASSETS,
        EventType.UPDATE_TAGS,
        EventType.UPDATE_USERS,
        EventType.DELETE_MISSING_COMMENTS,
    ],
    # deprecated
    Operation.UPDATE_METADATA: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_ASSETS,
        EventType.UPDATE_TAGS,
    ],
    Operation.ASSET_UPDATE_TAG: [],  # deprecated
    Operation.UPDATE_ASSET_DETAILS: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_ASSETS,
        EventType.UPDATE_TAGS,
    ],
    Operation.ASSET_UPDATE_PROPERTY: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_TAGS,
    ],
    Operation.EXT_TAG_DELETE_TAG: [
        EventType.UPDATE_RULES,
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.UPDATE_TAGS,
        EventType.UPDATE_USERS,
        EventType.UPDATE_CUSTOM_CATEGORIES,
    ],
    Operation.EXT_UPDATE_PARENT_TAG: [
        EventType.UPDATE_RULES,
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.UPDATE_TAGS,
        EventType.UPDATE_CUSTOM_CATEGORIES,
    ],
    Operation.EXT_DELETE_PARENT_TAG: [
        EventType.UPDATE_RULES,
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.UPDATE_TAGS,
        EventType.UPDATE_USERS,
        EventType.UPDATE_CUSTOM_CATEGORIES,
        EventType.DELETE_MISSING_COMMENTS,
    ],
    Operation.EXT_UPDATE_CHILD_TAG: [
        EventType.UPDATE_RULES,
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.UPDATE_TAGS,
    ],
    Operation.EXT_DELETE_CHILD_TAG: [
        EventType.UPDATE_RULES,
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.UPDATE_TAGS,
        EventType.UPDATE_USERS,
    ],
    Operation.EXT_DELETE_ASSETS: [
        EventType.DELETE_ASSETS,
        EventType.UPDATE_TAGS,
        EventType.UPDATE_USERS,
    ],
    Operation.EXT_UPDATE_METADATA: [
        EventType.APPLY_RULE,
        EventType.UPDATE_ASSETS,
        EventType.UPDATE_TAGS,
    ],
    Operation.EXT_ASSET_UPDATE_TAG: [],
    Operation.EXT_ASSET_UPDATE_PROPERTY: [EventType.APPLY_RULE, EventType.UPDATE_TAGS],
    Operation.ASSETS_BULK_UPDATE: [],
    Operation.TAGS_BULK_UPDATE: [],
    Operation.RULES_BULK_UPDATE: [],
    Operation.BULK_ASSETS: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_TAGS,
    ],
    Operation.BULK_TAGS: [EventType.UPDATE_RULES, EventType.UPDATE_TAGS],
    Operation.BULK_RULES: [
        EventType.APPLY_RULE,
        EventType.APPLY_RULE_BI_DATA,
        EventType.UPDATE_TAGS,
    ],
    Operation.UPDATE_WORKFLOW_TASK: [],
    Operation.DELETE_WORKFLOW_TASK: [],
}

OPTIONAL_EVENT_SET: set[tuple[Operation, EventType]] = {
    # Bulk rules / assets
    (Operation.BULK_RULES, EventType.APPLY_RULE),
    (Operation.BULK_RULES, EventType.APPLY_RULE_BI_DATA),
    (Operation.BULK_ASSETS, EventType.APPLY_RULE),
    (Operation.BULK_ASSETS, EventType.APPLY_RULE_BI_DATA),
    # Update/Property operations
    (Operation.EXT_UPDATE_METADATA, EventType.UPDATE_ASSETS),
    (Operation.ASSET_UPDATE_PROPERTY, EventType.APPLY_RULE),
    (Operation.ASSET_UPDATE_PROPERTY, EventType.APPLY_RULE_BI_DATA),
    # deprecated (kept for parity)
    (Operation.UPDATE_METADATA, EventType.APPLY_RULE),
    (Operation.UPDATE_METADATA, EventType.APPLY_RULE_BI_DATA),  # deprecated
    (Operation.UPDATE_METADATA, EventType.UPDATE_ASSETS),  # deprecated
    (Operation.UPDATE_ASSET_DETAILS, EventType.APPLY_RULE),
    (Operation.UPDATE_ASSET_DETAILS, EventType.APPLY_RULE_BI_DATA),
    (Operation.UPDATE_ASSET_DETAILS, EventType.UPDATE_ASSETS),
    # Property create/update/delete
    (Operation.DELETE_PROPERTY, EventType.APPLY_RULE),
    (Operation.CREATE_PROPERTY, EventType.UPDATE_USER_GROUP_PROPERTY),
    (Operation.DELETE_PROPERTY, EventType.UPDATE_ASSETS),
    (Operation.DELETE_PROPERTY, EventType.APPLY_RULE_BI_DATA),
    (Operation.DELETE_PROPERTY, EventType.UPDATE_BI_DATAS),
    (Operation.DELETE_PROPERTY, EventType.UPDATE_USER_GROUP_PROPERTY),
    (Operation.PROPERTY_UPDATE_PROPERTY, EventType.APPLY_RULE),
    (Operation.PROPERTY_UPDATE_PROPERTY, EventType.APPLY_RULE_BI_DATA),
    # Property set create/update/delete
    (Operation.CREATE_PROPERTY_SET, EventType.UPDATE_USER_GROUP_PROPERTY_SETS),
    (Operation.DELETE_PROPERTY_SET, EventType.APPLY_RULE),
    (Operation.DELETE_PROPERTY_SET, EventType.UPDATE_ASSETS),
    (Operation.DELETE_PROPERTY_SET, EventType.APPLY_RULE_BI_DATA),
    (Operation.DELETE_PROPERTY_SET, EventType.UPDATE_BI_DATAS),
    (Operation.DELETE_PROPERTY_SET, EventType.UPDATE_USER_GROUP_PROPERTY_SETS),
    (Operation.UPDATE_PROPERTY_SET, EventType.APPLY_RULE),
    (Operation.UPDATE_PROPERTY_SET, EventType.APPLY_RULE_BI_DATA),
    # Rule CRUD + apply
    (Operation.APPLY_RULE, EventType.APPLY_RULE),
    (Operation.APPLY_RULE, EventType.APPLY_RULE_BI_DATA),
    (Operation.DELETE_RULE, EventType.APPLY_RULE),
    (Operation.DELETE_RULE, EventType.APPLY_RULE_BI_DATA),
    (Operation.UPDATE_RULE, EventType.APPLY_RULE),
    (Operation.UPDATE_RULE, EventType.APPLY_RULE_BI_DATA),
    (Operation.CREATE_RULE, EventType.APPLY_RULE),
    (Operation.CREATE_RULE, EventType.APPLY_RULE_BI_DATA),
    (Operation.DELETE_RULE_SET, EventType.APPLY_RULE),
    (Operation.DELETE_RULE_SET, EventType.APPLY_RULE_BI_DATA),
    (Operation.UPDATE_RULE_SET, EventType.APPLY_RULE),
    (Operation.UPDATE_RULE_SET, EventType.APPLY_RULE_BI_DATA),
    # Tag group / category / tag ops
    (Operation.DELETE_TAG_GROUP, EventType.UPDATE_ASSETS),
    (Operation.DELETE_TAG_GROUP, EventType.APPLY_RULE),
    (Operation.DELETE_TAG_GROUP, EventType.APPLY_RULE_BI_DATA),
    (Operation.DELETE_TAG_GROUP, EventType.UPDATE_BI_DATAS),
    (Operation.UPDATE_TAG_CATEGORY, EventType.UPDATE_ASSETS),
    (Operation.UPDATE_TAG_CATEGORY, EventType.APPLY_RULE),
    (Operation.UPDATE_TAG_CATEGORY, EventType.APPLY_RULE_BI_DATA),
    (Operation.UPDATE_TAG_CATEGORY, EventType.UPDATE_BI_DATAS),
    (Operation.DELETE_TAG_CATEGORY, EventType.UPDATE_ASSETS),
    (Operation.DELETE_TAG_CATEGORY, EventType.APPLY_RULE),
    (Operation.DELETE_TAG_CATEGORY, EventType.APPLY_RULE_BI_DATA),
    (Operation.DELETE_TAG_CATEGORY, EventType.UPDATE_BI_DATAS),
    (Operation.TAG_UPDATE_TAG, EventType.UPDATE_ASSETS),
    (Operation.TAG_UPDATE_TAG, EventType.APPLY_RULE),
    (Operation.TAG_UPDATE_TAG, EventType.APPLY_RULE_BI_DATA),
    (Operation.TAG_UPDATE_TAG, EventType.UPDATE_BI_DATAS),
    (Operation.TAG_DELETE_TAG, EventType.UPDATE_ASSETS),
    (Operation.TAG_DELETE_TAG, EventType.APPLY_RULE),
    (Operation.TAG_DELETE_TAG, EventType.APPLY_RULE_BI_DATA),
    (Operation.TAG_DELETE_TAG, EventType.UPDATE_BI_DATAS),
    # Workflow subtasks/notifications
    (Operation.UPDATE_WORKFLOW_TASK, EventType.UPDATE_WORKFLOW_SUBTASKS),
    (Operation.DELETE_WORKFLOW_TASK, EventType.DELETE_WORKFLOW_TASK_NOTIFICATIONS),
}


def _to_operation(op: Union[Operation, str]) -> Operation:
    return op if isinstance(op, Operation) else Operation(op)


def _to_event_type(et: Union[EventType, str]) -> EventType:
    return et if isinstance(et, EventType) else EventType(et)


class EventProcessorWorkerPool:
    def __init__(
        self,
        event_repo: EventRepository,
        asset_repo: AssetRepository,
        max_workers: int = 1000,
        queue_capacity_factor: int = 5,
    ):
        self.max_workers = max_workers
        self.queue = asyncio.Queue(maxsize=max_workers * queue_capacity_factor)
        self.workers: List[asyncio.Task] = []
        self.is_running = False
        self.event_repo = event_repo
        self.asset_repo = asset_repo

    async def worker(self):
        """Worker that processes events from the queue"""
        while self.is_running:
            try:
                event_ctx = await self.queue.get()

                if event_ctx is None:  # Poison pill
                    break

                await self.process_event(event_ctx)
                self.queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in event processor worker: {e}")

    async def process_event(self, event_ctx: EventContext):
        """Process a single event"""
        ev = event_ctx.event
        try:
            # Update event status to executing
            await self.update_event_status(event_ctx, EventStatus.EXECUTING, True)

            # Check for duplicate records
            dupe = await self.has_duplicate_records(event_ctx)
            if dupe:
                await self.update_event_status(event_ctx, EventStatus.SKIPPED, False)
                return

            actual_deps_for_op = self.get_dependencies(ev)
            if actual_deps_for_op:
                start = datetime.now(timezone.utc)

                while True:
                    db_events_for_op = await self.get_dependent_records(ev)
                    if self._all_dependencies_completed(
                        ev, actual_deps_for_op, db_events_for_op
                    ):
                        break

                    # timeout?
                    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
                    if elapsed > DEPENDENCY_POLL_TIMEOUT_SECONDS:
                        # increase retry_count and "requeue".
                        ev.retry_count += 1

                        if ev.retry_count <= MAX_RETRY_COUNT:
                            await self.event_repo.update_status_and_times(
                                event_id=ev.id, status=EventStatus.PENDING.value
                            )

                            # INTERNAL REQUEUE: push back into our own worker queue after delay
                            await asyncio.sleep(REVISIBILITY_DELAY_SECONDS)
                            await self.add_event(event_ctx)
                        else:
                            # give up gracefully: mark completed to avoid deadlock (Go unblocks dependents)
                            await self.update_event_status(
                                event_ctx, EventStatus.COMPLETED, False
                            )

                        return  # end processing for now

                    await asyncio.sleep(EVENT_TABLE_POLLER_INTERVAL_MS / 1000)

            ev.is_dependency_resolved = True
            await self.update_event_status(event_ctx, EventStatus.EXECUTING, True)
            # Process the event
            await self.bulk_update(event_ctx)

            # Update event status to completed
            await self.update_event_status(event_ctx, EventStatus.COMPLETED, False)

        except Exception as e:
            ev.retry_count += 1
            if ev.retry_count <= MAX_RETRY_COUNT:
                await self.event_repo.update_status_and_times(
                    event_id=ev.id, status=EventStatus.PENDING.value
                )
                await asyncio.sleep(REVISIBILITY_DELAY_SECONDS)
                await self.add_event(event_ctx)
            else:
                await self.update_event_status(event_ctx, EventStatus.COMPLETED, False)
            logger.error(f"Error processing event {ev.id}: {e}")

    def _get_dependencies_until_self(
        self, event: Event, deps: List[EventType]
    ) -> List[EventType]:
        for i, dep in enumerate(deps):
            if dep == event.event_type:
                return deps[:i]
        return []

    def _is_event_present_in_db(
        self, events: Iterable[Event], event_type: EventType
    ) -> bool:
        for e in events:
            if e.event_type == event_type:
                return True
        return False

    def _optional_event(
        self, operation: Operation, event_type: EventType, events: Iterable[Event]
    ) -> bool:
        return self._is_optional_event(
            operation, event_type
        ) and not self._is_event_present_in_db(events, event_type)

    def _all_dependencies_completed(
        self,
        event: Event,
        actual_dependencies_for_operation: List[EventType],
        db_events_for_operation: List[Event],
    ) -> bool:
        deps = self._get_dependencies_until_self(
            event, actual_dependencies_for_operation
        )
        for dep in deps:
            # Optional dep not present → fine, skip
            if self._optional_event(event.operation, dep, db_events_for_operation):
                continue

            # If the dependency IS this event's own type, true immediately
            if dep == event.event_type:
                return True

            dep_found = False
            for db_event in db_events_for_operation:
                if db_event.event_type == dep:
                    dep_found = True
                    if db_event.status not in (
                        EventStatus.COMPLETED,
                        EventStatus.SKIPPED,
                    ):
                        return False

            if not dep_found:
                return False

        return True

    async def get_dependent_records(self, event: Event) -> List[Event]:
        return [
            Event.model_validate(r, from_attributes=True)
            for r in await self.event_repo.get_dependent_records(event)
        ]

    async def has_duplicate_records(self, event_ctx: EventContext) -> bool:
        duplicates = await self.event_repo.get_duplicate_records(event_ctx.event)
        return len(duplicates) > 0

    def get_dependencies(self, event: Event) -> List[EventType]:
        return dependencies.get(event.operation, [])

    def _is_optional_event(
        self, operation: Union[Operation, str], event_type: Union[EventType, str]
    ) -> bool:
        try:
            return (
                _to_operation(operation),
                _to_event_type(event_type),
            ) in OPTIONAL_EVENT_SET
        except ValueError:
            return False

    async def update_event_status(
        self, event_ctx: EventContext, status: EventStatus, log_only_wait_time: bool
    ):
        e = event_ctx.event

        now = datetime.now(timezone.utc)
        created_at = self._ensure_aware_utc(e.created_at)

        if log_only_wait_time:
            # First phase (EXECUTING): compute & store wait_time
            e.wait_time = int((now - created_at).total_seconds())
            completed_in_seconds: Optional[int] = None
        else:
            # Second phase (COMPLETED/FAILED/SKIPPED): compute completed_in_seconds using existing wait_time
            # If message didn’t carry wait_time (e.g., consumer restarted), fetch it from DB.
            wait_time = e.wait_time
            if wait_time is None:
                wait_time = await self.event_repo.get_wait_time(e.id)
                if wait_time is None:
                    # Fallback: recompute (won’t be exact if there was a long EXECUTING phase, but keeps logic moving)
                    wait_time = int((now - created_at).total_seconds())

            e.wait_time = int(wait_time)
            completed_in_seconds = max(
                0,
                int((now - created_at).total_seconds()) - int(e.wait_time or 0),
            )
            e.completed_in_seconds = completed_in_seconds

        e.status = status

        await self.event_repo.update_status_and_times(
            event_id=e.id,
            status=e.status.value,
            wait_time=e.wait_time,
            completed_in_seconds=e.completed_in_seconds,
        )

    def _ensure_aware_utc(self, dt: datetime) -> datetime:
        if dt is None:
            return datetime.now(timezone.utc)
        if dt.tzinfo is None:
            # assume stored as UTC-naive from DB; attach UTC
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    async def bulk_update(self, event_ctx: EventContext):
        event_type = event_ctx.event.event_type

        try:
            if event_type == EventType.DELETE_ASSETS:
                await self._delete_assets(event_ctx)
            # Add other event types as needed...
            else:
                logger.error(f"Unknown event type: {event_type}")
                raise ValueError(f"Unknown event type: {event_type}")

        except Exception as e:
            logger.error(f"Error in bulk_update for {event_type}: {e}")
            raise

    async def _delete_assets(self, event_ctx: EventContext):
        logger.info(
            "Processing event %s with id: %s",
            EventType.DELETE_ASSETS,
            event_ctx.event.id,
        )

        if event_ctx.event.operation != Operation.DELETE_ASSETS:
            raise ValueError(f"Unsupported operation: {event_ctx.event.operation}")

        asset_id = event_ctx.event.body

        logger.info("Collecting descendants for asset %s", asset_id)
        descendants = await self.asset_repo.fetch_descendants(
            ancestor_id=asset_id,
            min_depth=0,
            max_depth=2,
            project_fields=None,
        )
        ids = [a.id for a in descendants]
        ids.append(asset_id)

        # 2) Bulk delete in one go (repo handles session/commit)
        logger.info("Deleting %d assets (including root)", len(ids))
        deleted = await self.asset_repo.delete_by_ids(ids)

        logger.info("Deleted %d assets (requested %d)", deleted, len(ids))
        return {"deleted": deleted, "requested": len(ids)}

    def start(self):
        self.is_running = True
        for i in range(self.max_workers):
            worker_task = asyncio.create_task(
                self.worker(), name=f"event-processor-{i}"
            )
            self.workers.append(worker_task)

        logger.info(f"Started event processor pool with {self.max_workers} workers")

    async def add_event(self, event_ctx: EventContext):
        await self.queue.put(event_ctx)

    async def stop(self):
        self.is_running = False

        # Send poison pills to all workers
        for _ in range(self.max_workers):
            await self.queue.put(None)

        # Wait for all workers to complete
        if self.workers:
            await asyncio.gather(*self.workers, return_exceptions=True)

        logger.info("Event processor pool stopped")
