from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base


class PropertySetProperty(Base):
    __tablename__ = "property_set_property"

    property_set_id = Column(String, ForeignKey("property_sets.id"), primary_key=True)
    property_id = Column(String, ForeignKey("properties.id"), primary_key=True)
    order = Column(Integer)

    property_set = relationship("PropertySet", back_populates="property_links")
    property = relationship("Property", back_populates="property_links")
