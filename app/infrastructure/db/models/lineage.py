from sqlalchemy import (
    Column,
    String,
    DateTime,
    ForeignKey,
    Index,
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from app.infrastructure.db.models.base import Base


class Lineage(Base):
    __tablename__ = "lineages"

    id = Column(String, primary_key=True)
    version = Column(String)
    object_type = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(String)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    updated_by = Column(String)
    source_asset_id = Column(
        String,
        ForeignKey(
            "assets.id",
        ),
        nullable=False,
    )
    parent_asset_id = Column(String, ForeignKey("assets.id"))
    target_asset_id = Column(String, ForeignKey("assets.id"), nullable=False)

    source_asset = relationship(
        "Asset", foreign_keys=[source_asset_id], back_populates="outgoing_lineages"
    )
    target_asset = relationship(
        "Asset", foreign_keys=[target_asset_id], back_populates="incoming_lineages"
    )
    parent_asset = relationship("Asset", foreign_keys=[parent_asset_id])

    __table_args__ = (
        Index("idx_lineage_source_target", "source_asset_id", "target_asset_id"),
    )
