from sqlalchemy import Table, Column, String, ForeignKey
from app.infrastructure.db.models.base import Base

asset_asset_group = Table(
    "asset_asset_group",
    Base.metadata,
    Column(
        "asset_id",
        String,
        ForeignKey("assets.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "asset_group_id",
        String,
        ForeignKey("asset_groups.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)
