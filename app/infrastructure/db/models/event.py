from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    Text,
    BigInteger,
)
from app.infrastructure.db.models.base import Base
from sqlalchemy.sql import func
from app.domain.schemas.events import EventStatus


class Event(Base):
    __tablename__ = "events"

    id = Column(String, primary_key=True, index=True)
    event_type = Column(String(100), nullable=False)
    body = Column(Text)
    status = Column(String(20), default=EventStatus.PENDING)
    user_id = Column(String, nullable=False)
    operation = Column(String(100), nullable=False)
    expires_at = Column(DateTime, nullable=False)
    updated_by = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=func.now())
    is_authorized = Column(Boolean, default=True)
    is_fast_track = Column(Boolean, default=False)
    error = Column(Text)
    retry_count = Column(Integer, default=0)
    wait_time = Column(Integer)
    is_dependency_resolved = Column(Boolean, default=False)
    completed_in_seconds = Column(BigInteger)
    receipt_handle = Column(String)
