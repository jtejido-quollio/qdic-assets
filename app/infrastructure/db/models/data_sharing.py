from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base
from app.infrastructure.db.models.asset_data_sharing import asset_data_sharing


class DataSharing(Base):
    __tablename__ = "data_sharing"

    id = Column(String, primary_key=True, index=True)
    sharing_name = Column(String)
    physical_name = Column(String)
    sharing_type = Column(String)
    error_reason = Column(String)

    assets = relationship(
        "Asset", secondary=asset_data_sharing, back_populates="data_sharing"
    )
