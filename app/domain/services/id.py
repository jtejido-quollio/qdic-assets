import uuid
from app.domain.schemas.types import lookup_prefix


def new_global_id(t: str) -> str:
    """Generate a new global ID with type prefix"""
    return f"{lookup_prefix(t)}{new_uuid()}"


def new_uuid() -> str:
    """Generate a new random UUID"""
    return str(uuid.uuid4())
