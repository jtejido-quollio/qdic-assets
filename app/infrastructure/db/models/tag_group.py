from sqlalchemy import Column, String, ForeignKey, Boolean, UniqueConstraint, DateTime
from app.infrastructure.db.models.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func


# tag_group_projection
class TagGroup(Base):
    __tablename__ = "tag_groups"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    description = Column(String)
    color = Column(String)
    is_archived = Column(Boolean, default=False, server_default="false", nullable=False)
    is_deleted = Column(Boolean, default=False, server_default="false", nullable=False)
    version = Column(String)
    object_type = Column(String)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_by = Column(String)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    updated_by = Column(String)

    tag_links = relationship("AssetTagLink", back_populates="tag_group")
    categories = relationship(
        "Tag",
        back_populates="tag_group",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
