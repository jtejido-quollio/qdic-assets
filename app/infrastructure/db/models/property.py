from sqlalchemy import Column, String, ARRAY
from app.infrastructure.db.models.base import Base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship


class Property(Base):
    __tablename__ = "properties"

    id = Column(String, primary_key=True, index=True)
    title = Column(String, nullable=False)
    type = Column(String)
    options = Column(JSONB)
    values = Column(ARRAY(String))

    attachments = relationship("PropertyAttachment", back_populates="property")
    property_links = relationship("PropertySetProperty", back_populates="property")
