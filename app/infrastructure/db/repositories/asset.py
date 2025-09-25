from uuid import uuid4
import re
from typing import Any, Optional, Callable, AsyncContextManager, Dict, List, Set, Tuple
from sqlalchemy import select, func, or_, not_, and_, update as sql_update, delete
from sqlalchemy.orm import Session, joinedload, selectinload, load_only
from sqlalchemy.ext.asyncio import AsyncSession
from app.domain.schemas.asset import AssetCreate, AssetObjectType, FullTag
from app.domain.schemas.types import Type
from app.infrastructure.db.models.asset_tag_link import AssetTagLink
from app.infrastructure.db.repositories.base import BaseRepository
from app.infrastructure.db.models.asset import Asset
from app.infrastructure.db.models.data_sharing import DataSharing
from app.infrastructure.db.models.ext_tag import ExtTag
from app.infrastructure.db.models.ext_owner import ExtOwner
from app.infrastructure.db.models.ext_connection import ExtConnection
from app.infrastructure.db.models.ext_source import ExtSource
from app.infrastructure.db.models.statistics import Statistics
from app.infrastructure.db.models.asset_group import AssetGroup
from app.infrastructure.db.models.asset_path import AssetPath
from app.infrastructure.db.models.property_set import PropertySet
from app.infrastructure.db.models.asset_property_set import AssetPropertySet
from app.infrastructure.db.models.property_set_property import PropertySetProperty
from app.infrastructure.db.models.property import Property
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.utils.casting import to_float, to_int
from app.domain.schemas.search import SimpleClause
from sqlalchemy.sql.elements import ColumnElement
import inspect

FIELD_MAP: dict[str, ColumnElement] = {
    "asset.service_name": Asset.service_name,
    "asset.physical_name": Asset.physical_name,
    "asset.logical_name": Asset.logical_name,
    "asset.description": Asset.description,
}


class AssetRepository(BaseRepository):
    def __init__(
        self, session_factory: Callable[[], AsyncContextManager[AsyncSession]]
    ):
        super().__init__(session_factory, Asset)

    async def find_or_create(self, id: str) -> Asset:
        async with self.session_factory() as session:
            res = await session.execute(select(self.model).where(self.model.id == id))
            asset = res.scalar_one_or_none()
            if asset:
                if getattr(asset, "is_deleted", False):
                    raise LookupError("asset deleted")
                return asset

            asset = self.model(id=id)
            session.add(asset)
            await session.commit()
            await session.refresh(asset)
            return asset

    async def get_by_id(self, id: str) -> Optional[Asset]:
        async with self.session_factory() as session:
            res = await session.execute(
                select(self.model).where(
                    self.model.id == id,
                    getattr(self.model, "is_deleted", False) == False,  # noqa: E712
                )
            )
            return res.scalar_one_or_none()

    async def exists(self, id: str) -> bool:
        async with self.session_factory() as session:
            stmt = select(1).where(self.model.id == id).limit(1)
            if hasattr(self.model, "is_deleted"):
                stmt = stmt.where(self.model.is_deleted.is_(False))
            return (await session.scalar(stmt)) is not None

    async def update_object_type(
        self, id: str, schema: AssetObjectType
    ) -> Optional[Asset]:
        async with self.session_factory() as session:
            obj = (
                (await session.execute(select(self.model).where(self.model.id == id)))
                .scalars()
                .first()
            )
            if obj is None:
                return None

            obj.object_type = schema.object_type
            await session.commit()
            await session.refresh(obj)
            return obj

    async def fetch_asset_with_all_nested_data(self, asset_id: str) -> Optional[Asset]:
        async with self.session_factory() as session:
            stmt = select(self.model).where(self.model.id == asset_id)

            # Soft-delete filter if present
            if hasattr(self.model, "is_deleted"):
                stmt = stmt.where(self.model.is_deleted.is_(False))

            stmt = stmt.options(
                # paths -> ancestor
                selectinload(self.model.paths).joinedload(AssetPath.ancestor),
                # simple collections
                selectinload(self.model.children),
                selectinload(self.model.parent_dashboards),
                selectinload(self.model.tags),
                selectinload(self.model.statistics),
                selectinload(self.model.data_sharing),
                selectinload(self.model.ext_tags),
                selectinload(self.model.ext_owners),
                selectinload(self.model.asset_groups),
                # ext_connections -> ext_sources
                selectinload(self.model.ext_connections).selectinload(
                    ExtConnection.ext_sources
                ),
                # property_sets -> property_set -> property_links -> property -> attachments
                joinedload(self.model.property_sets)
                .joinedload(AssetPropertySet.property_set)
                .joinedload(PropertySet.property_links)
                .joinedload(PropertySetProperty.property)
                .joinedload(Property.attachments),
            ).limit(1)

            result = await session.execute(stmt)
            return result.scalars().first()

    async def bulk_upsert(
        self,
        schemas: List[AssetCreate],
        default_asset_group_id: str,
        is_partial: bool,
    ) -> List[Asset]:
        """
        Bulk upsert Assets, then upsert/attach relationships efficiently.
        Returns the list of ORM Asset rows (refreshed).
        """
        if not schemas:
            return []

        async with self.session_factory() as session:
            # 1) Upsert main asset rows, return ORM objects (already persistent)
            assets = await self._bulk_upsert_assets(session, schemas, is_partial)

            # 2) Build id->Asset map
            asset_by_id: Dict[str, Asset] = {a.id: a for a in assets}

            # 3) Relationships (each helper handles its own selects/adds)
            await self._process_bulk_data_sharing(session, asset_by_id, schemas)
            await self._process_bulk_ext_tags(session, asset_by_id, schemas)
            await self._process_bulk_ext_owners(session, asset_by_id, schemas)
            await self._process_bulk_ext_connections(session, asset_by_id, schemas)
            await self._process_bulk_statistics(session, asset_by_id, schemas)
            await self._process_bulk_asset_groups(
                session, asset_by_id, schemas, default_asset_group_id
            )

            # 4) Commit once
            await session.commit()

            # 5) Refresh assets to reflect relationship changes
            for a in assets:
                await session.refresh(a)

            return assets

    async def _bulk_upsert_assets(
        self, session: AsyncSession, schemas: List[AssetCreate], is_partial: bool
    ) -> List[Asset]:
        """Bulk upsert top-level Asset fields using INSERT ... ON CONFLICT DO UPDATE ... RETURNING."""
        insert_rows: List[Dict[str, Any]] = []
        non_null_columns: Set[str] = set()

        for s in schemas:
            # Custom logical_name rules
            logical_name = s.logical_name
            description = s.csv_custom_data.description if s.csv_custom_data else None

            if logical_name is not None and s.override_logical_name == "new_asset":
                # Only override for existing rows; if brand-new it’s fine to keep the provided one
                exists_stmt = (
                    select(func.count()).select_from(Asset).where(Asset.id == s.id)
                )
                exists = (await session.scalar(exists_stmt)) or 0
                if exists and s.csv_custom_data:
                    logical_name = s.csv_custom_data.logical_name
                elif exists:
                    logical_name = None

            row = self._get_updatable_columns(s, logical_name, description, is_partial)
            row.update({"id": s.id})

            # Ensure all expected columns present (fill None)
            expected = [
                "service_name",
                "physical_name",
                "is_lost",
                "is_csv_imported",
                "is_archived",
                "asset_type",
                "comment_on_ddl",
                "ddl_statement",
                "data_type",
                "ordinal_position",
                "record_updated_at",
                "ext_url",
                "ext_access_count",
                "ext_name",
                "ext_description",
                "logical_name",
                "description",
                "version",
            ]
            for col in expected:
                row.setdefault(col, None)

            insert_rows.append(row)
            non_null_columns.update(k for k, v in row.items() if v is not None)

        # SET clause only for non-null incoming fields
        excluded = pg_insert(self.model).excluded
        set_map = {
            col: getattr(excluded, col) for col in non_null_columns if col not in ("id")
        }

        stmt = pg_insert(self.model).values(insert_rows)

        stmt = stmt.on_conflict_do_update(
            index_elements=[self.model.id],
            set_=set_map,
        ).returning(self.model)

        res = await session.execute(stmt)
        return list(res.scalars())

    def _get_updatable_columns(
        self,
        s: AssetCreate,
        logical_name: Optional[str],
        description: Optional[str],
        is_partial: bool,
    ) -> Dict[str, Any]:
        cols: Dict[str, Any] = {
            "service_name": s.service_name,
            "physical_name": s.physical_name,
            "is_lost": s.is_lost,
            "is_csv_imported": s.is_csv_imported,
            "is_archived": s.is_archived,
            "asset_type": s.asset_type,
            "comment_on_ddl": s.comment_on_ddl,
            "ddl_statement": s.ddl_statement,
            "data_type": s.data_type,
            "ordinal_position": s.ordinal_position,
            "record_updated_at": s.record_updated_at,
            "ext_url": s.ext_url,
            "ext_access_count": s.ext_access_count,
            "ext_name": s.ext_name,
            "ext_description": s.ext_description,
            "logical_name": logical_name,
            "description": description,
        }
        if not is_partial:
            cols["version"] = s.version or "latest"
        return cols

    async def _process_bulk_data_sharing(
        self,
        session: AsyncSession,
        asset_by_id: Dict[str, Asset],
        schemas: List[AssetCreate],
    ) -> None:
        all_ids: Set[str] = set()
        desired_map: Dict[str, Set[str]] = {}

        for s in schemas:
            if s.data_sharing is None:
                continue
            desired = {o.global_id for o in s.data_sharing}
            desired_map[s.id] = desired
            all_ids |= desired

        existing: Dict[str, DataSharing] = {}
        if all_ids:
            rs = await session.execute(
                select(DataSharing).where(DataSharing.id.in_(all_ids))
            )
            existing = {ds.id: ds for ds in rs.scalars()}

        for s in schemas:
            if s.data_sharing is None:
                continue
            asset = asset_by_id[s.id]
            if not s.is_csv_imported or not asset.data_sharing:
                asset.data_sharing.clear()

            desired = desired_map.get(s.id, set())
            # upsert-ish for each related row (small scale; OK to do per row)
            for o in s.data_sharing:
                ds = existing.get(o.global_id)
                if not ds:
                    ds = DataSharing(
                        id=o.global_id,
                        sharing_name=o.sharing_name,
                        physical_name=o.physical_name,
                        sharing_type=o.sharing_type,
                        error_reason=o.error_reason,
                    )
                    session.add(ds)
                    existing[o.global_id] = ds
                else:
                    ds.sharing_name = o.sharing_name
                    ds.physical_name = o.physical_name
                    ds.sharing_type = o.sharing_type
                    ds.error_reason = o.error_reason

                if ds not in asset.data_sharing:
                    asset.data_sharing.append(ds)

            if s.is_csv_imported:
                asset.data_sharing[:] = [
                    ds for ds in asset.data_sharing if ds.id in desired
                ]

    async def _process_bulk_ext_tags(
        self,
        session: AsyncSession,
        asset_by_id: Dict[str, Asset],
        schemas: List[AssetCreate],
    ) -> None:
        all_ids: Set[str] = set()
        desired_map: Dict[str, Set[str]] = {}

        for s in schemas:
            if s.ext_tag is None:
                continue
            desired = {t.ext_tag_id for t in s.ext_tag}
            desired_map[s.id] = desired
            all_ids |= desired

        existing: Dict[str, ExtTag] = {}
        if all_ids:
            rs = await session.execute(select(ExtTag).where(ExtTag.id.in_(all_ids)))
            existing = {t.id: t for t in rs.scalars()}

        for s in schemas:
            if s.ext_tag is None:
                continue
            asset = asset_by_id[s.id]
            if not s.is_csv_imported or not asset.ext_tags:
                asset.ext_tags.clear()

            desired = desired_map.get(s.id, set())
            for t in s.ext_tag:
                tag = existing.get(t.ext_tag_id)
                if not tag:
                    tag = ExtTag(
                        id=t.ext_tag_id,
                        ext_tag_name=t.ext_tag_name,
                        ext_tag_description=t.ext_tag_description,
                    )
                    session.add(tag)
                    existing[t.ext_tag_id] = tag
                else:
                    tag.ext_tag_name = t.ext_tag_name
                    tag.ext_tag_description = t.ext_tag_description

                if tag not in asset.ext_tags:
                    asset.ext_tags.append(tag)

            if s.is_csv_imported:
                asset.ext_tags[:] = [t for t in asset.ext_tags if t.id in desired]

    async def _process_bulk_ext_owners(
        self,
        session: AsyncSession,
        asset_by_id: Dict[str, Asset],
        schemas: List[AssetCreate],
    ) -> None:
        all_ids: Set[str] = set()
        desired_map: Dict[str, Set[str]] = {}

        for s in schemas:
            if s.ext_owner is None:
                continue
            desired = {o.ext_owner_id for o in s.ext_owner}
            desired_map[s.id] = desired
            all_ids |= desired

        existing: Dict[str, ExtOwner] = {}
        if all_ids:
            rs = await session.execute(select(ExtOwner).where(ExtOwner.id.in_(all_ids)))
            existing = {o.id: o for o in rs.scalars()}

        for s in schemas:
            if s.ext_owner is None:
                continue
            asset = asset_by_id[s.id]
            if not s.is_csv_imported or not asset.ext_owners:
                asset.ext_owners.clear()

            desired = desired_map.get(s.id, set())
            for o in s.ext_owner:
                owner = existing.get(o.ext_owner_id)
                if not owner:
                    owner = ExtOwner(
                        id=o.ext_owner_id,
                        display_name=o.display_name,
                        email_address=o.email_address,
                    )
                    session.add(owner)
                    existing[o.ext_owner_id] = owner
                else:
                    owner.display_name = o.display_name
                    owner.email_address = o.email_address

                if owner not in asset.ext_owners:
                    asset.ext_owners.append(owner)

            if s.is_csv_imported:
                asset.ext_owners[:] = [o for o in asset.ext_owners if o.id in desired]

    async def _process_bulk_ext_connections(
        self,
        session: AsyncSession,
        asset_by_id: Dict[str, Asset],
        schemas: List[AssetCreate],
    ) -> None:
        # 1) Collect desired state
        all_conn_ids: Set[str] = set()
        all_source_ids: Set[str] = set()
        desired_conns_by_asset: Dict[str, List[Dict[str, Any]]] = {}

        for s in schemas:
            if not s.ext_connection:
                continue
            items: List[Dict[str, Any]] = []
            for c in s.ext_connection:
                row = {
                    "id": c.ext_table_id,
                    "ext_table_name": c.ext_table_name,
                    "ext_table_name_path": c.ext_table_name_path,
                    "ext_description": c.ext_description,
                    "ext_service_name": c.ext_service_name,
                    "possible_global_ids": getattr(c, "possible_global_ids", None),
                    "sources": [
                        {
                            "id": src.id,
                            "source_name": src.source_name,
                            "source_type": src.source_type,
                        }
                        for src in (getattr(c, "sources", []) or [])
                    ],
                }
                items.append(row)
                all_conn_ids.add(row["id"])
                for src in row["sources"]:
                    all_source_ids.add(src["id"])
            desired_conns_by_asset[s.id] = items

        if not all_conn_ids and not all_source_ids:
            return

        # 2) Preload existing ExtConnection / ExtSource
        existing_conn: Dict[str, ExtConnection] = {}
        if all_conn_ids:
            rs = await session.execute(
                select(ExtConnection).where(ExtConnection.id.in_(all_conn_ids))
            )
            existing_conn = {x.id: x for x in rs.scalars()}

        existing_src: Dict[str, ExtSource] = {}
        if all_source_ids:
            rs = await session.execute(
                select(ExtSource).where(ExtSource.id.in_(all_source_ids))
            )
            existing_src = {x.id: x for x in rs.scalars()}

        # 3) Upsert connections + sources; sync links
        for s in schemas:
            if s.id not in desired_conns_by_asset:
                continue

            asset = asset_by_id[s.id]

            # If not CSV-imported or asset has no connections yet, reset the list
            if not getattr(s, "is_csv_imported", False) or not asset.ext_connections:
                asset.ext_connections.clear()

            desired_rows = desired_conns_by_asset[s.id]
            seen_conn_ids: Set[str] = set()
            updated_conns: List[ExtConnection] = []

            for row in desired_rows:
                cid = row["id"]
                seen_conn_ids.add(cid)

                conn = existing_conn.get(cid)
                if not conn:
                    conn = ExtConnection(
                        id=cid,
                        ext_table_name=row["ext_table_name"],
                        ext_table_name_path=row["ext_table_name_path"],
                        ext_description=row["ext_description"],
                        ext_service_name=row["ext_service_name"],
                        possible_global_ids=row["possible_global_ids"],
                    )
                    session.add(conn)
                    existing_conn[cid] = conn
                else:
                    conn.ext_table_name = row["ext_table_name"]
                    conn.ext_table_name_path = row["ext_table_name_path"]
                    conn.ext_description = row["ext_description"]
                    conn.ext_service_name = row["ext_service_name"]
                    conn.possible_global_ids = row["possible_global_ids"]

                # Upsert & attach sources (M2M)
                # Build desired source objects for this connection
                conn_desired_sources: List[ExtSource] = []
                for srow in row["sources"]:
                    sid = srow["id"]
                    src = existing_src.get(sid)
                    if not src:
                        src = ExtSource(
                            id=sid,
                            source_name=srow["source_name"],
                            source_type=srow["source_type"],
                        )
                        session.add(src)
                        existing_src[sid] = src
                    else:
                        src.source_name = srow["source_name"]
                        src.source_type = srow["source_type"]

                    conn_desired_sources.append(src)

                # Sync connection↔sources list (replace to desired)
                conn.ext_sources[:] = conn_desired_sources

                updated_conns.append(conn)

            # Sync asset↔connections (CSV import keeps only seen; else replace)
            if getattr(s, "is_csv_imported", False):
                asset.ext_connections[:] = [
                    c for c in updated_conns if c.id in seen_conn_ids
                ]
            else:
                asset.ext_connections[:] = updated_conns

    async def _process_bulk_statistics(
        self,
        session: AsyncSession,
        asset_by_id: Dict[str, Asset],
        schemas: List[AssetCreate],
    ) -> None:
        for s in schemas:
            asset = asset_by_id[s.id]
            incoming_has_stats = self._has_any_stats(s)

            if incoming_has_stats and asset.statistics is None:
                asset.statistics = Statistics(id=str(uuid4()))

            if not incoming_has_stats:
                continue

            st = asset.statistics
            # guard again
            if st is None:
                st = Statistics(id=str(uuid4()))
                asset.statistics = st

            self._assign_if_not_none(st, "stats_size", s.stats_size, convert=to_float)
            self._assign_if_not_none(st, "stats_count", s.stats_count, convert=to_int)
            self._assign_if_not_none(st, "stats_max", s.stats_max)
            self._assign_if_not_none(st, "stats_min", s.stats_min)
            self._assign_if_not_none(st, "stats_mean", s.stats_mean)
            self._assign_if_not_none(st, "stats_median", s.stats_median)
            self._assign_if_not_none(st, "stats_mode", s.stats_mode)
            self._assign_if_not_none(st, "stats_stddev", s.stats_stddev)
            self._assign_if_not_none(
                st, "stats_number_of_null", s.stats_number_of_null, convert=to_int
            )
            self._assign_if_not_none(
                st, "stats_number_of_unique", s.stats_number_of_unique, convert=to_int
            )

    async def _process_bulk_asset_groups(
        self,
        session: AsyncSession,
        asset_by_id: Dict[str, Asset],
        schemas: List[AssetCreate],
        default_asset_group_id: str,
    ) -> None:
        all_group_ids: Set[str] = set([default_asset_group_id])
        for s in schemas:
            if s.asset_group_ids:
                all_group_ids |= set(s.asset_group_ids)

        groups_by_id: Dict[str, AssetGroup] = {}
        if all_group_ids:
            rs = await session.execute(
                select(AssetGroup).where(AssetGroup.id.in_(all_group_ids))
            )
            groups_by_id = {g.id: g for g in rs.scalars()}

        for s in schemas:
            asset = asset_by_id[s.id]
            desired = (
                set(s.asset_group_ids or [])
                if s.asset_group_ids is not None
                else {default_asset_group_id}
            )
            current_ids = {g.id for g in asset.asset_groups}

            # Remove groups not in desired (only when asset_group_ids explicitly provided)
            if s.asset_group_ids is not None:
                to_keep = [g for g in asset.asset_groups if g.id in desired]
                asset.asset_groups[:] = to_keep

            # Add missing
            for gid in desired:
                if gid not in current_ids:
                    g = groups_by_id.get(gid)
                    if g and g not in asset.asset_groups:
                        asset.asset_groups.append(g)

    def _has_any_stats(self, s: AssetCreate) -> bool:
        return any(
            not self._nullish(v)
            for v in (
                s.stats_size,
                s.stats_count,
                s.stats_max,
                s.stats_min,
                s.stats_mean,
                s.stats_median,
                s.stats_mode,
                s.stats_stddev,
                s.stats_number_of_null,
                s.stats_number_of_unique,
            )
        )

    @staticmethod
    def _nullish(x: Any) -> bool:
        return x is None or (
            isinstance(x, str) and x.strip().lower() in {"", "null", "none", "nan"}
        )

    @staticmethod
    def _assign_if_not_none(
        obj: Any, attr: str, incoming: Any, *, convert=None
    ) -> None:
        if AssetRepository._nullish(incoming):
            return
        setattr(obj, attr, convert(incoming) if convert else incoming)

    async def search_simple(
        self,
        logical_filters: List[SimpleClause],
        from_: int,
        size: int,
        sort: Optional[str],
        order: str,  # "asc" | "desc"
    ) -> Tuple[int, List[Asset]]:

        async with self.session_factory() as session:
            loaders = (
                selectinload(self.model.paths).joinedload(AssetPath.ancestor),
                selectinload(self.model.children),
                selectinload(self.model.parent_dashboards),
                selectinload(self.model.tags).joinedload(AssetTagLink.child_tag),
                selectinload(self.model.tags).joinedload(AssetTagLink.parent_tag),
                selectinload(self.model.tags).joinedload(AssetTagLink.tag_group),
                selectinload(self.model.statistics),
                selectinload(self.model.data_sharing),
                selectinload(self.model.ext_tags),
                selectinload(self.model.ext_owners),
                selectinload(self.model.asset_groups),
                selectinload(self.model.ext_connections).selectinload(
                    ExtConnection.ext_sources
                ),
                joinedload(self.model.property_sets)
                .joinedload(AssetPropertySet.property_set)
                .joinedload(PropertySet.property_links)
                .joinedload(PropertySetProperty.property)
                .joinedload(Property.attachments),
            )

            stmt = select(self.model).options(*loaders)

            # Build combined WHERE from logical_filters
            combined = None
            for f in logical_filters or []:
                col_sql = FIELD_MAP.get(f.key, (None))
                if col_sql is None or not f.text:
                    continue
                rx = self._wildcard_to_pg_regex(f.text)
                cond = col_sql.op("~*")(rx)  # case-insensitive regex

                op = (f.op or "and").lower()
                if combined is None:
                    combined = not_(cond) if op == "not" else cond
                else:
                    if op == "or":
                        combined = or_(combined, cond)
                    elif op == "not":
                        combined = and_(combined, not_(cond))
                    else:  # "and" / default
                        combined = and_(combined, cond)

            # Soft delete guard if you use it
            if hasattr(self.model, "is_deleted"):
                combined = (
                    self.model.is_deleted.is_(False)
                    if combined is None
                    else and_(combined, self.model.is_deleted.is_(False))
                )

            if combined is not None:
                stmt = stmt.where(combined)

            # Total count on filtered set
            count_stmt = select(func.count()).select_from(self.model)
            if combined is not None:
                count_stmt = count_stmt.where(combined)
            total = (await session.scalar(count_stmt)) or 0

            # Sorting
            if sort:
                sort_col = FIELD_MAP.get(sort, (None))
                if sort_col is not None:
                    stmt = stmt.order_by(
                        sort_col.asc()
                        if (order or "").lower() == "asc"
                        else sort_col.desc()
                    )

            # Pagination
            stmt = stmt.offset(int(from_)).limit(int(size))

            res = await session.execute(stmt)
            rows = res.scalars().all()
            return total, rows

    @staticmethod
    def _wildcard_to_pg_regex(pattern: str) -> str:
        p = re.escape(pattern).replace(r"\*", ".*").replace(r"\?", ".")
        if not p.startswith("^"):
            p = "^" + p
        if not p.endswith("$"):
            p = p + "$"
        return p

    def _common_loaders(self):
        return (
            selectinload(Asset.paths).joinedload(AssetPath.ancestor),
            selectinload(Asset.children),
            selectinload(Asset.parent_dashboards),
            selectinload(Asset.tags),
            selectinload(Asset.statistics),
            selectinload(Asset.data_sharing),
            selectinload(Asset.ext_tags),
            selectinload(Asset.ext_owners),
            selectinload(Asset.asset_groups),
            selectinload(Asset.ext_connections).selectinload(ExtConnection.ext_sources),
            joinedload(Asset.property_sets)
            .joinedload(AssetPropertySet.property_set)
            .joinedload(PropertySet.property_links)
            .joinedload(PropertySetProperty.property)
            .joinedload(Property.attachments),
        )

    async def _fetch_top_by_type(self, object_type: str) -> List[Asset]:
        async with self.session_factory() as session:
            stmt = (
                select(Asset)
                .where(Asset.object_type == object_type)
                .where(Asset.version == "latest")
            )
            if hasattr(Asset, "is_deleted"):
                stmt = stmt.where(Asset.is_deleted.is_(False))

            stmt = stmt.options(*self._common_loaders())

            res = await session.execute(stmt)
            return res.scalars().all()

    async def fetch_top_schemas(self) -> List[Asset]:
        return await self._fetch_top_by_type(Type.Schema.value)

    async def fetch_top_bi_groups(self) -> List[Asset]:
        return await self._fetch_top_by_type(Type.BiGroup.value)

    async def fetch_subtree_dfs(
        self,
        top_schema_id: str,
        batch_callback: Optional[Callable[[List[Asset]], None]] = None,
        project_fields: Optional[List[str]] = None,
    ) -> List[Asset]:
        """
        Depth-first-ish traversal using the closure table:
        - layer 2: direct children of top_schema
        - layer 3: grandchildren
        - layer 4: great-grandchildren
        - plus full parent chain for each layer-2 node

        If batch_callback is provided, it will be called for each batch:
          [layer4... then layer3... then parents..., and finally the layer2 node]
        Otherwise, batches are accumulated and returned.
        """
        all_results: List[Asset] = []
        async with self.session_factory() as session:
            parent_cache: Dict[str, Asset] = {}

            layer2 = await self._fetch_layer(
                session=session,
                top_schema_id=top_schema_id,
                layer=2,
                project_fields=project_fields,
            )

            for a2 in layer2:
                parents = await self._fetch_parents(
                    session=session,
                    asset=a2,
                    parent_map=parent_cache,
                )
                layer3 = await self.fetch_descendants(
                    session=session,
                    ancestor_id=a2.id,
                    min_depth=1,
                    max_depth=1,
                    project_fields=project_fields,
                )
                layer4 = await self.fetch_descendants(
                    session=session,
                    ancestor_id=a2.id,
                    min_depth=2,
                    max_depth=2,
                    project_fields=project_fields,
                )

                batch = list(layer4) + list(layer3) + list(parents) + [a2]

                if batch_callback:
                    maybe_coro = batch_callback(batch)
                    if inspect.isawaitable(maybe_coro):
                        await maybe_coro  # support async callbacks too
                else:
                    all_results.extend(batch)

        return all_results

    def _maybe_soft_delete(self, stmt):
        if hasattr(Asset, "is_deleted"):
            return stmt.where(Asset.is_deleted.is_(False))
        return stmt

    def _maybe_project(self, stmt, project_fields: Optional[List[str]]):
        """
        Keep returning Asset entities, just limit selected columns
        to those fields (plus PK) via load_only.
        """
        if not project_fields:
            return stmt
        cols = [getattr(Asset, f) for f in project_fields if hasattr(Asset, f)]
        if cols:
            # Always ensure PK is loaded
            if hasattr(Asset, "id") and Asset.id not in cols:
                cols.append(Asset.id)
            stmt = stmt.options(load_only(*cols))
        return stmt

    async def _fetch_layer(
        self,
        session: AsyncSession,
        top_schema_id: str,
        layer: int,
        project_fields: Optional[List[str]] = None,
    ) -> List[Asset]:
        """
        Return assets at specific depth relative to top_schema_id.
        layer=2 -> depth == 1 (direct children of top)
        """
        depth = layer - 1
        stmt = (
            select(Asset)
            .join(AssetPath, AssetPath.asset_id == Asset.id)
            .where(AssetPath.ancestor_id == top_schema_id)
            .where(AssetPath.depth == depth)
            .options(*self._common_loaders())
        )
        stmt = self._maybe_soft_delete(stmt)
        stmt = self._maybe_project(stmt, project_fields)

        res = await session.execute(stmt)
        return res.scalars().all()

    async def fetch_descendants(
        self,
        session: AsyncSession,
        ancestor_id: str,
        min_depth: int,
        max_depth: int,
        project_fields: Optional[List[str]] = None,
    ) -> List[Asset]:
        """
        Return descendants of ancestor_id with depth in [min_depth, max_depth].
        """
        stmt = (
            select(Asset)
            .join(AssetPath, AssetPath.asset_id == Asset.id)
            .where(AssetPath.ancestor_id == ancestor_id)
            .where(AssetPath.depth >= min_depth)
            .where(AssetPath.depth <= max_depth)
            .options(*self._common_loaders())
        )
        stmt = self._maybe_soft_delete(stmt)
        stmt = self._maybe_project(stmt, project_fields)

        res = await session.execute(stmt)
        return res.scalars().all()

    async def _fetch_parents(
        self,
        session: AsyncSession,
        asset: Asset,
        parent_map: Dict[str, Asset],
    ) -> List[Asset]:
        """
        Return ordered parent chain (root→…→direct parent) for the given asset,
        filling parent_map cache as needed.
        Assumes asset.paths are available (we load them in _common_loaders).
        """
        if not getattr(asset, "paths", None):
            return []

        # Ensure deterministic chain by path_order
        sorted_paths = sorted(asset.paths, key=lambda p: p.path_order)
        ancestor_ids = [p.ancestor_id for p in sorted_paths]

        missing = [aid for aid in ancestor_ids if aid not in parent_map]
        if missing:
            stmt = (
                select(Asset)
                .where(Asset.id.in_(missing))
                .options(*self._common_loaders())
            )
            stmt = self._maybe_soft_delete(stmt)
            res = await session.execute(stmt)
            for parent in res.scalars().all():
                parent_map[parent.id] = parent

        # Reconstruct ordered chain from cache
        return [parent_map[aid] for aid in ancestor_ids if aid in parent_map]

    async def soft_delete(self, id: str, user_id: str) -> Optional[Asset]:
        async with self.session_factory() as session:
            values = {}
            if hasattr(self.model, "is_deleted"):
                values["is_deleted"] = True
            if hasattr(self.model, "updated_by"):
                values["updated_by"] = user_id

            if not values:
                return await session.scalar(
                    select(self.model).where(self.model.id == id)
                )

            stmt = (
                sql_update(self.model)
                .where(self.model.id == id)
                .values(**values)
                .returning(self.model)  # PostgreSQL
            )
            res = await session.execute(stmt)
            obj = res.scalar_one_or_none()
            if obj is None:
                await session.rollback()
                return None
            await session.commit()
            return obj

    async def delete_by_ids(self, ids: List[str]) -> int:
        if not ids:
            return 0
        async with self.session_factory() as session:
            stmt = delete(self.model).where(self.model.id.in_(ids))
            res = await session.execute(stmt)
            await session.commit()
            return res.rowcount or 0
