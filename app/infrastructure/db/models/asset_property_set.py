from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base


class AssetPropertySet(Base):
    __tablename__ = "asset_property_sets"
    asset_id = Column(String, ForeignKey("assets.id"), primary_key=True)
    property_set_id = Column(String, ForeignKey("property_sets.id"), primary_key=True)
    order = Column(Integer)

    asset = relationship("Asset", back_populates="property_sets")
    property_set = relationship("PropertySet", back_populates="asset_links")
