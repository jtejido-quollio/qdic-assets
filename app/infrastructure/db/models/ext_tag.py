from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base
from app.infrastructure.db.models.asset_ext_tag import asset_ext_tag


class ExtTag(Base):
    __tablename__ = "ext_tags"

    id = Column(String, primary_key=True, index=True)
    ext_tag_description = Column(String)
    ext_tag_name = Column(String)

    assets = relationship("Asset", secondary=asset_ext_tag, back_populates="ext_tags")
