from functools import wraps
from inspect import iscoroutinefunction
from dependency_injector.wiring import inject as di_inject
from loguru import logger

from app.services.base import BaseService


def inject(func):
    @di_inject
    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        _cleanup(kwargs)
        return result

    @di_inject
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        result = await func(*args, **kwargs)
        _cleanup(kwargs)
        return result

    def _cleanup(kwargs):
        injected_services = [
            arg for arg in kwargs.values() if isinstance(arg, BaseService)
        ]
        if injected_services:
            try:
                injected_services[-1].close_scoped_session()
            except Exception as e:
                logger.error(e)

    return async_wrapper if iscoroutinefunction(func) else sync_wrapper
