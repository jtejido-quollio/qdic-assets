from sqlalchemy import Column, String, Integer, Boolean, Float, ForeignKey, BigInteger
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base


class Statistics(Base):
    __tablename__ = "statistics"

    id = Column(String, primary_key=True, index=True)
    stats_size = Column(Float)
    stats_count = Column(BigInteger)
    stats_max = Column(String)
    stats_min = Column(String)
    stats_mean = Column(String)
    stats_median = Column(String)
    stats_mode = Column(String)
    stats_stddev = Column(String)
    stats_number_of_null = Column(BigInteger)
    stats_number_of_unique = Column(BigInteger)
    asset_id = Column(
        String, ForeignKey("assets.id", ondelete="CASCADE"), nullable=False
    )

    asset = relationship("Asset", back_populates="statistics", passive_deletes=True)
