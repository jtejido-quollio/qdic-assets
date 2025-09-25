from sqlalchemy import Table, Column, String, ForeignKey
from app.infrastructure.db.models.base import Base

asset_ext_owner = Table(
    "asset_ext_owners",
    Base.metadata,
    Column("asset_id", String, ForeignKey("assets.id"), primary_key=True),
    Column("ext_owner_id", String, ForeignKey("ext_owners.id"), primary_key=True),
)
