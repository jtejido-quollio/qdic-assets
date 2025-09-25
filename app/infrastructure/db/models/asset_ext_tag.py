from sqlalchemy import Table, Column, String, ForeignKey
from app.infrastructure.db.models.base import Base

asset_ext_tag = Table(
    "asset_ext_tags",
    Base.metadata,
    Column("asset_id", String, ForeignKey("assets.id"), primary_key=True),
    Column("ext_tag_id", String, ForeignKey("ext_tags.id"), primary_key=True),
)
