import logging
import os
from typing import Optional
from urllib.parse import urlparse

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from app.core.config import settings

log = logging.getLogger(__name__)


def _should_register() -> bool:
    if (settings.ENV or "").lower() == "local":
        return False
    if not settings.CONTROLLER_URL:
        log.warning("service registration disabled: CONTROLLER_URL is empty")
        return False
    if not settings.ADVERTISE_ENDPOINT:
        log.warning("service registration disabled: ADVERTISE_ENDPOINT is empty")
        return False
    return True


class _RetryableError(Exception): ...


def _raise_for_retryable(resp: httpx.Response) -> None:
    if 500 <= resp.status_code < 600:
        raise _RetryableError(f"{resp.status_code} {resp.reason_phrase}")


def _parse_db_url(url: str) -> dict:
    """
    Accepts sync or async pg URLs:
      postgresql://user:pass@host:5432/db
      postgresql+asyncpg://user:pass@host:5432/db
    """
    p = urlparse(url)
    # strip +asyncpg
    scheme = p.scheme.split("+", 1)[0]
    if scheme not in ("postgresql", "postgres"):
        raise ValueError(f"Unsupported DB scheme in DATABASE_URL: {p.scheme}")
    host = p.hostname or ""
    port = int(p.port or 5432)
    db = (p.path or "").lstrip("/") or ""
    user = p.username or ""
    return {"db_host": host, "db_port": port, "db_name": db, "db_user": user}


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=6),
    retry=retry_if_exception_type((_RetryableError, httpx.RequestError)),
)
async def _upsert_tenant(client: httpx.AsyncClient) -> None:
    tenant_id = getattr(settings, "TENANT_ID", None)
    db_url = os.getenv("DATABASE_URL") or getattr(settings, "DATABASE_URL", "")
    if not tenant_id or not db_url:
        raise ValueError("TENANT_ID or DATABASE_URL missing; cannot upsert tenant")

    db = _parse_db_url(db_url)
    payload = {
        "id": tenant_id,
        "name": getattr(settings, "SITE_NAME", tenant_id),
        "db_name": db["db_name"],
        "db_host": db["db_host"],
        "db_port": db["db_port"],
        "db_user": db["db_user"],
        # Until Vault/ExternalSecrets wiring is used, put a placeholder so column is non-null.
        "db_password_secret": "n/a",
    }

    resp = await client.post("/v1/tenants", json=payload)
    _raise_for_retryable(resp)
    resp.raise_for_status()


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=6),
    retry=retry_if_exception_type((_RetryableError, httpx.RequestError)),
)
async def _register_service(client: httpx.AsyncClient) -> str:
    payload = {
        "tenant_id": getattr(settings, "TENANT_ID", None) or "",
        "service_name": "catalog",
        "protocol": "http",
        "endpoint": settings.ADVERTISE_ENDPOINT,
        "readiness_path": getattr(settings, "READINESS_PATH", "/healthz"),
        "version": getattr(settings, "APP_VERSION", "0.0.0"),
        "metadata": {"site": getattr(settings, "SITE_NAME", "unknown")},
    }
    payload = {k: v for k, v in payload.items() if v not in ("", None)}

    resp = await client.post("/v1/services/register", json=payload)
    _raise_for_retryable(resp)
    resp.raise_for_status()
    data = resp.json()
    sid = data.get("id") or data.get("service_id")
    if not sid:
        raise ValueError("controller register response missing 'id'")
    return sid


async def register() -> Optional[str]:
    if not _should_register():
        log.info("service registration skipped (env/flags)")
        return None

    base = settings.CONTROLLER_URL.rstrip("/")
    timeout = httpx.Timeout(connect=5, read=10, write=10, pool=5)

    try:
        async with httpx.AsyncClient(base_url=base, timeout=timeout) as client:
            # 1) Upsert tenant (idempotent)
            await _upsert_tenant(client)

            # 2) Register (UPSERT) service
            sid = await _register_service(client)
            log.info(
                "service registered",
                extra={"service_id": sid, "endpoint": settings.ADVERTISE_ENDPOINT},
            )
            return sid
    except httpx.HTTPStatusError as e:
        log.error(
            "service registration failed (HTTP error)",
            extra={"status_code": e.response.status_code, "body": e.response.text},
        )
    except httpx.RequestError as e:
        log.error(f"service registration failed (network): {e!r}")
    except Exception as e:
        log.exception(f"service registration failed: {e}")
    return None
