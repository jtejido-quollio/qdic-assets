import re
from typing import List, Tuple
from app.domain.schemas.asset import (
    CommonAsset,
    TableStatistics,
    ColumnStatistics,
    Path,
    FullTag,
    TagGroup,
    TagCategory,
    Tag,
    PropertySet,
    Property,
    Attachment,
    DataSharing,
    ExtData,
    ExtOwner,
    ExtTag,
    Datasource,
)
from app.domain.schemas.search import (
    SearchRequest,
    SearchResponse,
    SimpleClause,
    SearchMode,
)
from app.domain.schemas.common import CommonResponse
from app.domain.repositories.asset import AssetRepository
from app.services.base import BaseService
from app.core.exceptions import BadRequestError


DEFAULT_ORDER = "asc"
DEFAULT_SIZE = 25


class SearchService(BaseService):
    def __init__(self, asset_repo: AssetRepository):
        self.asset_repo = asset_repo
        super().__init__(asset_repo)

    async def search_simple(self, req: SearchRequest) -> CommonResponse[SearchResponse]:
        clauses: List[SimpleClause] = []
        if req.query and req.query.clauses:
            clauses = req.query.clauses
        elif req.query and req.query.key and req.query.text:
            clauses = [SimpleClause(key=req.query.key, text=req.query.text, op="and")]

        total, rows = await self.asset_repo.search_simple(
            logical_filters=clauses,
            from_=req.from_,
            size=req.size or DEFAULT_SIZE,
            sort=req.sort,
            order=req.order or DEFAULT_ORDER,
        )
        data = [
            CommonAsset(
                global_id=asset.id,
                logical_name=getattr(asset, "logical_name", None),
                physical_name=getattr(asset, "physical_name", None),
                data_source_id=getattr(asset, "data_source_id", None),
                description=getattr(asset, "description", None),
                created_at=getattr(asset, "created_at", None),
                updated_at=getattr(asset, "updated_at", None),
                created_by=getattr(asset, "created_by", None),
                updated_by=getattr(asset, "updated_by", None),
                record_updated_at=getattr(asset, "record_updated_at", None),
                is_archived=getattr(asset, "is_archived", False),
                is_csv_imported=getattr(asset, "is_csv_imported", False),
                is_lost=getattr(asset, "is_lost", False),
                object_type=getattr(asset, "object_type", None),
                service_name=getattr(asset, "service_name", None),
                version=getattr(asset, "version", None),
                child_asset_ids=[c.id for c in getattr(asset, "children", [])],
                parent_dashboard_ids=[
                    p.id for p in getattr(asset, "parent_dashboards", [])
                ],
                statistics=(
                    TableStatistics(
                        count=getattr(asset.statistics, "stats_count", None),
                        size=getattr(asset.statistics, "stats_size", None),
                    )
                    if getattr(asset, "statistics", None)
                    else None
                ),
                column_stats=(
                    ColumnStatistics(
                        mean=getattr(asset.statistics, "stats_mean", None),
                        min=getattr(asset.statistics, "stats_min", None),
                        max=getattr(asset.statistics, "stats_max", None),
                        mode=getattr(asset.statistics, "stats_mode", None),
                        median=getattr(asset.statistics, "stats_median", None),
                        stddev=getattr(asset.statistics, "stats_stddev", None),
                        number_of_null=getattr(
                            asset.statistics, "stats_number_of_null", None
                        ),
                        number_of_unique=getattr(
                            asset.statistics, "stats_number_of_unique", None
                        ),
                    )
                    if getattr(asset, "statistics", None)
                    else None
                ),
                asset_type=getattr(asset, "asset_type", None),
                comment_on_ddl=getattr(asset, "comment_on_ddl", None),
                ddl_statement=getattr(asset, "ddl_statement", None),
                data_type=getattr(asset, "data_type", None),
                ordinal_position=getattr(asset, "ordinal_position", None),
                path=[
                    Path(
                        id=p.ancestor_id,
                        name=p.ancestor_name,
                        object_type=p.ancestor_type,
                        path_layer=p.path_layer,
                    )
                    for p in getattr(asset, "paths", [])
                ],
                tags=[
                    FullTag(
                        global_id=link.child_tag.id,
                        tag_group=TagGroup(
                            global_id=link.tag_group.id,
                            tag_group_name=link.tag_group.name,
                            tag_group_description=link.tag_group.description,
                            tag_group_color=link.tag_group.color,
                        ),
                        tag_category=TagCategory(
                            global_id=link.parent_tag.id,
                            tag_category_name=link.parent_tag.name,
                            tag_category_description=link.parent_tag.description,
                        ),
                        tag=Tag(
                            global_id=link.child_tag.id,
                            tag_name=link.child_tag.name,
                            tag_description=link.child_tag.description,
                        ),
                        is_manual=(getattr(link, "link_type", None) == "manual"),
                    )
                    for link in getattr(asset, "tags", [])
                ],
                property_sets=[
                    PropertySet(
                        id=aps.id,
                        property_set_id=aps.property_set.id,
                        property_set_title=aps.property_set.title,
                        is_activated=aps.property_set.is_activated,
                        order=aps.order,
                        properties=[
                            Property(
                                property_id=prop.id,
                                property_title=prop.title,
                                property_type=prop.type,
                                property_values=getattr(prop, "values", None),
                                options=getattr(prop, "options", None),
                                attachments=[
                                    Attachment(
                                        file_name=att.file_name,
                                        content_type=att.content_type,
                                        file_size=att.file_size,
                                        uploaded_at=att.uploaded_at,
                                        uploaded_by=att.uploaded_by,
                                    )
                                    for att in getattr(prop, "attachments", [])
                                ],
                            )
                            for prop in getattr(aps.property_set, "properties", [])
                        ],
                    )
                    for aps in getattr(asset, "property_sets", [])
                ],
                ext_access_count=getattr(asset, "ext_access_count", None),
                ext_name=getattr(asset, "ext_name", None),
                ext_description=getattr(asset, "ext_description", None),
                ext_url=getattr(asset, "ext_url", None),
                data_sharing=[
                    DataSharing(
                        global_id=s.id,
                        sharing_name=s.sharing_name,
                        physical_name=s.physical_name,
                        sharing_type=s.sharing_type,
                        error_reason=s.error_reason,
                    )
                    for s in getattr(asset, "data_sharing", [])
                ],
                ext_owners=[
                    ExtOwner(
                        ext_owner_id=o.id,
                        display_name=o.display_name,
                        email_address=o.email_address,
                    )
                    for o in getattr(asset, "ext_owners", [])
                ],
                ext_tags=[
                    ExtTag(
                        ext_tag_id=t.id,
                        ext_tag_name=t.ext_tag_name,
                        ext_tag_description=t.ext_tag_description,
                    )
                    for t in getattr(asset, "ext_tags", [])
                ],
                ext_data=[
                    ExtData(
                        ext_table_id=d.id,
                        possible_global_ids=getattr(d, "possible_global_ids", None),
                        ext_table_name=getattr(d, "ext_table_name", None),
                        ext_table_name_path=getattr(d, "ext_table_name_path", None),
                        ext_description=getattr(d, "ext_description", None),
                        connection_type=getattr(d, "ext_service_name", None),
                        datasources=[
                            Datasource(
                                id=s.id,
                                name=s.source_name,
                                type=s.source_type,
                            )
                            for s in getattr(d, "ext_sources", [])
                        ],
                    )
                    for d in getattr(asset, "ext_connections", [])
                ],
            )
            for asset in rows
        ]
        return CommonResponse(
            data=SearchResponse(
                total=total,
                number_of_results=len(data),
                size=req.size or DEFAULT_SIZE,
                from_=req.from_,
                data=[d.model_dump(by_alias=True) for d in data],
            )
        )
