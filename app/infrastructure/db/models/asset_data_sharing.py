from sqlalchemy import Table, Column, String, ForeignKey
from app.infrastructure.db.models.base import Base

asset_data_sharing = Table(
    "asset_data_sharing",
    Base.metadata,
    Column("asset_id", String, ForeignKey("assets.id"), primary_key=True),
    Column("data_sharing_id", String, ForeignKey("data_sharing.id"), primary_key=True),
)
