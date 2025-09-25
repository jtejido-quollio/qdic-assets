from sqlalchemy import Table, Column, String, ForeignKey
from app.infrastructure.db.models.base import Base

asset_ext_connection = Table(
    "asset_ext_connections",
    Base.metadata,
    Column("asset_id", String, ForeignKey("assets.id"), primary_key=True),
    Column(
        "ext_connection_id", String, ForeignKey("ext_connections.id"), primary_key=True
    ),
)
