from sqlalchemy import Column, Boolean, String, ForeignKey
from app.infrastructure.db.models.base import Base
from sqlalchemy.orm import relationship


class PropertySet(Base):
    __tablename__ = "property_sets"

    id = Column(String, primary_key=True, index=True)
    title = Column(String, nullable=False)
    is_activated = Column(Boolean, default=False, nullable=False)

    asset_links = relationship("AssetPropertySet", back_populates="property_set")
    property_links = relationship("PropertySetProperty", back_populates="property_set")
