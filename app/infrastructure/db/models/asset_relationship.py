from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base


class AssetRelationship(Base):
    __tablename__ = "asset_relationships"
    parent_asset_id = Column(
        String, ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True
    )
    child_asset_id = Column(
        String, ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True
    )

    parent = relationship(
        "Asset",
        foreign_keys=[parent_asset_id],
        backref="children_links",
        passive_deletes=True,
    )
    child = relationship(
        "Asset",
        foreign_keys=[child_asset_id],
        backref="parent_links",
        passive_deletes=True,
    )
