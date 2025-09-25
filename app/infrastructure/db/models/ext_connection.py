from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base
from app.infrastructure.db.models.asset_ext_connection import asset_ext_connection
from app.infrastructure.db.models.ext_connection_sources import ext_connection_source


class ExtConnection(Base):
    __tablename__ = "ext_connections"

    id = Column(String, primary_key=True, index=True)
    possible_global_ids = Column(ARRAY(String))
    ext_table_name = Column(String)
    ext_table_name_path = Column(String)
    ext_description = Column(String)
    ext_service_name = Column(String)

    assets = relationship(
        "Asset",
        secondary=asset_ext_connection,
        back_populates="ext_connections",
        lazy="selectin",
    )

    ext_sources = relationship(
        "ExtSource",
        secondary=ext_connection_source,
        back_populates="ext_connections",
        lazy="selectin",
    )
