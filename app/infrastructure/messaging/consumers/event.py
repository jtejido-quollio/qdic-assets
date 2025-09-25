import asyncio
import logging
import json
from typing import List
import aio_pika
from app.core.config import settings
from app.domain.schemas.events import EventContext
from app.services.event import EventProcessorWorkerPool

logger = logging.getLogger(__name__)


class EventConsumerPool:
    def __init__(self, processor_pool: EventProcessorWorkerPool):
        self.processor_pool = processor_pool
        self._tasks: List[asyncio.Task] = []
        self._running = False

    async def _consumer_worker(self, worker_id: int):
        conn = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        try:
            channel = await conn.channel()
            await channel.set_qos(prefetch_count=1)

            queue = await channel.declare_queue(
                settings.RABBITMQ_EVENTS_QUEUE,
                durable=True,
                arguments={"x-queue-type": "stream"},
            )
            logger.info("consumer-%s: started", worker_id)

            async with queue.iterator() as it:
                async for msg in it:
                    if not self._running:
                        break
                    try:
                        data = json.loads(msg.body.decode())
                        after = data.get("after") or {}
                        if (after.get("status") or "").lower() != "pending":
                            await msg.ack()
                            continue

                        ctx = EventContext.from_debezium_message(msg.body.decode())
                        await self.processor_pool.add_event(ctx)
                        await msg.ack()
                    except Exception as e:
                        logger.exception("consumer-%s: error: %s", worker_id, e)
                        # consider msg.nack(requeue=True) if you want retries
                        await msg.ack()
        finally:
            await conn.close()

    async def start(self, workers: int):
        if self._running:
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(self._consumer_worker(i), name=f"event-consumer-{i}")
            for i in range(workers)
        ]
        logger.info("started %s consumer workers", workers)

    async def stop(self):
        if not self._running:
            return
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("consumer pool stopped")


class EventsRuntime:
    def __init__(self, processor_pool: EventProcessorWorkerPool):
        self.processor_pool = processor_pool
        self.consumer_pool = EventConsumerPool(processor_pool)
        self._started = False

    async def start(self):
        if self._started:
            return
        self.processor_pool.start()
        await self.consumer_pool.start(settings.EVENT_CONSUMER_WORKER_POOL_SIZE)
        self._started = True

    async def stop(self):
        if not self._started:
            return
        await self.consumer_pool.stop()
        self.processor_pool.stop()
        self._started = False
