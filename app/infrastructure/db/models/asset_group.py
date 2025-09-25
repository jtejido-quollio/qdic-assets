from sqlalchemy import Column, String, ForeignKey
from app.infrastructure.db.models.base import Base
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.asset_asset_group import asset_asset_group


class AssetGroup(Base):
    __tablename__ = "asset_groups"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    authority_user_group_id = Column(String, nullable=True)

    assets = relationship(
        "Asset",
        secondary=asset_asset_group,
        back_populates="asset_groups",
    )
