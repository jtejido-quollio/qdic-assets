from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base
from app.infrastructure.db.models.ext_connection_sources import ext_connection_source


class ExtSource(Base):
    __tablename__ = "ext_sources"

    id = Column(String, primary_key=True, index=True)
    source_name = Column(String)
    source_type = Column(String)

    ext_connections = relationship(
        "ExtConnection",
        secondary=ext_connection_source,
        back_populates="ext_sources",
        lazy="selectin",
    )
