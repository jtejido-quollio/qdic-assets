from sqlalchemy import (
    Column,
    String,
    Integer,
    Boolean,
    BigInteger,
    ForeignKey,
    DateTime,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from app.infrastructure.db.models.base import Base
from app.infrastructure.db.models.asset_relationship import AssetRelationship
from sqlalchemy.sql import func
from app.infrastructure.db.models.asset_ext_tag import asset_ext_tag
from app.infrastructure.db.models.asset_ext_owner import asset_ext_owner
from app.infrastructure.db.models.asset_ext_connection import asset_ext_connection
from app.infrastructure.db.models.asset_data_sharing import asset_data_sharing
from app.infrastructure.db.models.asset_asset_group import asset_asset_group
from app.domain.schemas.types import lookup_prefix, Type
from sqlalchemy import and_
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy import literal


class Asset(Base):
    __tablename__ = "assets"
    id = Column(String, primary_key=True, index=True)
    object_type = Column(String)
    asset_type = Column(String)
    comment_on_ddl = Column(String)
    ddl_statement = Column(String)
    data_type = Column(String)
    ordinal_position = Column(Integer)
    is_archived = Column(Boolean, default=False, server_default="false", nullable=False)
    is_csv_imported = Column(
        Boolean, default=False, server_default="false", nullable=False
    )
    is_lost = Column(Boolean, default=False, server_default="false", nullable=False)

    logical_name = Column(String)
    physical_name = Column(String)
    data_source_id = Column(String)

    service_name = Column(String)  # for now, assume ingestion_job.datasource_type
    version = Column(String)
    description = Column(String)
    record_updated_at = Column(String)

    # external stuff
    ext_url = Column(String)
    ext_access_count = Column(BigInteger)
    ext_name = Column(String)
    ext_description = Column(String)
    is_deleted = Column(Boolean, default=False, server_default="false", nullable=False)

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

    asset_groups = relationship(
        "AssetGroup",
        secondary=asset_asset_group,
        back_populates="assets",
    )
    data_sharing = relationship(
        "DataSharing",
        secondary=asset_data_sharing,
        back_populates="assets",
        cascade="all",
    )
    ext_tags = relationship(
        "ExtTag",
        secondary=asset_ext_tag,
        back_populates="assets",
        cascade="all",
    )
    ext_owners = relationship(
        "ExtOwner",
        secondary=asset_ext_owner,
        back_populates="assets",
        cascade="all",
    )
    ext_connections = relationship(
        "ExtConnection",
        secondary=asset_ext_connection,
        back_populates="assets",
        cascade="all",
    )
    statistics = relationship("Statistics", back_populates="asset", uselist=False)
    property_sets = relationship("AssetPropertySet", back_populates="asset")
    tags = relationship("AssetTagLink", back_populates="asset")
    paths = relationship(
        "AssetPath",
        foreign_keys="[AssetPath.asset_id]",
        back_populates="asset",
        cascade="all, delete-orphan",
        order_by="AssetPath.path_order",
        lazy="selectin",
    )
    parents = relationship(
        "Asset",
        secondary=AssetRelationship.__table__,
        primaryjoin="Asset.id==AssetRelationship.child_asset_id",
        secondaryjoin="Asset.id==AssetRelationship.parent_asset_id",
        backref="children",
        overlaps="children_links,parent_links,parent,child",
    )

    @hybrid_method
    def has_type(self, t: str) -> bool:
        pfx = lookup_prefix(t)
        return bool(pfx and (self.id or "").startswith(pfx))

    @has_type.expression
    def has_type(cls, t: str):
        pfx = lookup_prefix(t)
        if not pfx:
            return literal(False)
        return cls.id.like(f"{pfx}%")

    parent_dashboards = relationship(
        "Asset",
        secondary=AssetRelationship.__table__,
        primaryjoin=lambda: Asset.id == AssetRelationship.child_asset_id,
        secondaryjoin=lambda: and_(
            Asset.id == AssetRelationship.parent_asset_id,
            Asset.has_type(Type.Dashboard.value),
        ),
        viewonly=True,
        lazy="selectin",
        overlaps="parents,children",
    )
    outgoing_lineages = relationship(
        "Lineage",
        foreign_keys="Lineage.source_asset_id",
        back_populates="source_asset",
        cascade="all, delete-orphan",
    )

    incoming_lineages = relationship(
        "Lineage",
        foreign_keys="Lineage.target_asset_id",
        back_populates="target_asset",
        cascade="all, delete-orphan",
    )
