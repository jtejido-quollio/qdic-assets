from sqlalchemy import Column, String, ForeignKey, Boolean, DateTime
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base
from sqlalchemy.sql import func
from sqlalchemy.ext.associationproxy import association_proxy


# tag projection
class Tag(Base):
    __tablename__ = "tags"
    id = Column(String, primary_key=True, index=True)
    tag_group_id = Column(
        String, ForeignKey("tag_groups.id", ondelete="CASCADE"), nullable=False
    )
    parent_tag_id = Column(
        String, ForeignKey("tags.id", ondelete="CASCADE"), nullable=True
    )
    name = Column(String, nullable=False)
    description = Column(String)
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

    child_links = relationship(
        "AssetTagLink",
        foreign_keys="[AssetTagLink.child_tag_id]",
        back_populates="child_tag",
    )
    parent_links = relationship(
        "AssetTagLink",
        foreign_keys="[AssetTagLink.parent_tag_id]",
        back_populates="parent_tag",
    )
    parent = relationship(
        "Tag",
        remote_side=lambda: [Tag.id],
        foreign_keys=[parent_tag_id],
        back_populates="children",
        uselist=False,
        lazy="selectin",
    )
    children = relationship(
        "Tag",
        back_populates="parent",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    tag_group = relationship("TagGroup", back_populates="categories")
    rule_links = relationship(
        "RuleTargetTag",
        back_populates="tag",
        cascade="all, delete-orphan",
        lazy="selectin",
        foreign_keys="[RuleTargetTag.tag_id]",
    )
    parent_rule_links = relationship(
        "RuleTargetTag",
        back_populates="parent_tag",
        cascade="all, delete-orphan",
        lazy="selectin",
        foreign_keys="[RuleTargetTag.parent_tag_id]",
    )
    rules = association_proxy("rule_links", "rule")
    rules_as_category = association_proxy("parent_rule_links", "rule")
