from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base
from app.infrastructure.db.models.asset_ext_owner import asset_ext_owner


class ExtOwner(Base):
    __tablename__ = "ext_owners"

    id = Column(String, primary_key=True, index=True)
    email_address = Column(String)
    display_name = Column(String)

    assets = relationship(
        "Asset", secondary=asset_ext_owner, back_populates="ext_owners"
    )
