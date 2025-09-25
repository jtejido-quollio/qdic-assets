from sqlalchemy import Column, String, Integer, DateTime, ForeignKey
from app.infrastructure.db.models.base import Base
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship


class PropertyAttachment(Base):
    __tablename__ = "property_attachments"
    id = Column(String, primary_key=True)
    property_id = Column(String, ForeignKey("properties.id"), nullable=False)
    file_name = Column(String)
    file_path = Column(String)
    file_size = Column(Integer)
    content_type = Column(String)
    uploaded_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    uploaded_by = Column(String)

    property = relationship("Property", back_populates="attachments")
