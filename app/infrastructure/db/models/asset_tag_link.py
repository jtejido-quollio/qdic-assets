from sqlalchemy import Column, String, Enum, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base
from app.domain.schemas.tag import LinkType


# asset_tag_link projection
class AssetTagLink(Base):
    __tablename__ = "asset_tag_links"
    id = Column(String, primary_key=True)
    asset_id = Column(
        String, ForeignKey("assets.id", ondelete="CASCADE"), nullable=False
    )
    child_tag_id = Column(String, ForeignKey("tags.id"))
    parent_tag_id = Column(String, ForeignKey("tags.id"))
    tag_group_id = Column(String, ForeignKey("tag_groups.id"))
    link_type = Column(
        Enum(LinkType, name="link_type_enum"), nullable=False, default=LinkType.manual
    )

    asset = relationship("Asset", back_populates="tags", passive_deletes=True)
    child_tag = relationship(
        "Tag", foreign_keys=[child_tag_id], back_populates="child_links"
    )
    parent_tag = relationship(
        "Tag", foreign_keys=[parent_tag_id], back_populates="parent_links"
    )
    tag_group = relationship("TagGroup", back_populates="tag_links")

    __table_args__ = (
        UniqueConstraint(
            "asset_id", "parent_tag_id", "child_tag_id", name="uq_asset_tag"
        ),
    )
