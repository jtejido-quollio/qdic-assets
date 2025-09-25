import asyncio, json, logging, aio_pika
from opensearchpy import OpenSearch
from app.core.config import settings

log = logging.getLogger(__name__)


async def run_indexer():
    if not settings.RABBITMQ_URL or not settings.OPENSEARCH_URL:
        log.info("Indexer disabled (missing RABBITMQ_URL/OPENSEARCH_URL)")
        return
    conn = await aio_pika.connect_robust(settings.RABBITMQ_URL)
    ch = await conn.channel()
    exch = await ch.declare_exchange(
        "catalog.events", aio_pika.ExchangeType.TOPIC, durable=True
    )
    q = await ch.declare_queue(f"catalog-indexer-{settings.TENANT_ID}", durable=True)
    await q.bind(exch, routing_key="#")
    osx = OpenSearch(settings.OPENSEARCH_URL)
    async with q.iterator() as it:
        async for msg in it:
            async with msg.process():
                try:
                    evt = json.loads(msg.body.decode())
                    doc = evt.get("after") or evt.get("payload", {})
                    idx = f"{settings.SITE_NAME}-asset-{settings.TENANT_ID}".lower()
                    if "id" in doc:
                        osx.index(index=idx, id=doc["id"], body=doc)
                except Exception as e:
                    log.exception("indexer error: %s", e)
