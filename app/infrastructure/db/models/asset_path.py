from sqlalchemy import (
    Column,
    String,
    ForeignKey,
    UniqueConstraint,
    Integer,
    Boolean,
    Index,
)
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base


class AssetPath(Base):
    __tablename__ = "asset_paths"

    asset_id = Column(
        String, ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True
    )
    ancestor_id = Column(
        String, ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True
    )
    ancestor_name = Column(String, nullable=False)
    ancestor_type = Column(String, nullable=False)
    depth = Column(Integer)  # N..0
    path_order = Column(Integer)  # 0..N
    path_layer = Column(String)

    # Relationships
    asset = relationship(
        "Asset", foreign_keys=[asset_id], back_populates="paths", passive_deletes=True
    )
    ancestor = relationship("Asset", foreign_keys=[ancestor_id], passive_deletes=True)

    __table_args__ = (
        UniqueConstraint("asset_id", "path_order", name="uq_asset_path_order"),
        Index("ix_asset_paths_ancestor_id", "ancestor_id"),  # For finding descendants
        Index(
            "ix_asset_paths_asset_id_depth", "asset_id", "depth"
        ),  # For path reconstruction
    )
