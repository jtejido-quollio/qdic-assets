import os
import logging
from typing import Optional

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
    # Skip in local by default unless explicitly enabled
    if (settings.ENV or "").lower() == "local":
        return False
    # Require controller URL + endpoint to be present
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


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=6),
    retry=retry_if_exception_type((_RetryableError, httpx.RequestError)),
)
async def _do_register(client: httpx.AsyncClient) -> str:
    payload = {
        # keep tenant_id if your controller expects it; otherwise remove
        "tenant_id": getattr(settings, "TENANT_ID", None) or "",
        "service_name": "catalog",
        "protocol": "http",
        "endpoint": settings.ADVERTISE_ENDPOINT,
        "readiness_path": getattr(settings, "READINESS_PATH", "/healthz"),
        "version": getattr(settings, "APP_VERSION", "0.0.0"),
        "metadata": {"site": getattr(settings, "SITE_NAME", "unknown")},
    }
    # strip empties the controller may not like
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
    sid: Optional[str] = None

    try:
        async with httpx.AsyncClient(base_url=base, timeout=timeout) as client:
            sid = await _do_register(client)
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
