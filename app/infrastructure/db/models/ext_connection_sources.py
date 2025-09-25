from sqlalchemy import Table, Column, String, ForeignKey, Index
from app.infrastructure.db.models.base import Base

ext_connection_source = Table(
    "ext_connection_sources",
    Base.metadata,
    Column(
        "ext_connection_id",
        String,
        ForeignKey("ext_connections.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "ext_source_id",
        String,
        ForeignKey("ext_sources.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)

Index("ix_ext_conn_source_conn", ext_connection_source.c.ext_connection_id)
Index("ix_ext_conn_source_src", ext_connection_source.c.ext_source_id)
