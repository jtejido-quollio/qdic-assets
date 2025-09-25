from app.services.base import BaseService
from app.domain.repositories.asset import AssetRepository
from app.domain.repositories.event import EventRepository
from app.domain.schemas.asset import (
    Asset,
    TableStatistics,
    ColumnStatistics,
    Path,
    TagLink,
    PropertySet,
    Property,
    Attachment,
)
from app.core.exceptions import AssetNotFoundError, BadRequestError
from app.domain.schemas.common import CommonResponse
from app.domain.schemas.events import EventType, Operation
import logging

logger = logging.getLogger(__name__)


class AssetService(BaseService):
    def __init__(self, asset_repo: AssetRepository, event_repo: EventRepository):
        self.asset_repo = asset_repo
        self.event_repo = event_repo
        super().__init__(asset_repo)

    async def get_asset_details(self, asset_id: str) -> CommonResponse[Asset]:
        asset = await self.asset_repo.fetch_asset_with_all_nested_data(asset_id)
        if not asset:
            raise AssetNotFoundError(asset_id=asset_id)
        asset = Asset(
            id=asset.id,
            logical_name=asset.logical_name,
            physical_name=asset.physical_name,
            data_source_id=asset.data_source_id,
            description=asset.description,
            created_at=asset.created_at,
            updated_at=asset.updated_at,
            created_by=asset.created_by,
            updated_by=asset.updated_by,
            is_archived=asset.is_archived,
            is_csv_imported=asset.is_csv_imported,
            is_lost=asset.is_lost,
            object_type=asset.object_type,
            service_name=asset.service_name,
            sub_id=asset.version,
            child_asset_ids=[c.id for c in asset.children],
            statistics=(
                TableStatistics(
                    count=asset.statistics.stats_count,
                    size=asset.statistics.stats_size,
                )
                if asset.statistics
                else None
            ),
            column_stats=(
                ColumnStatistics(
                    mean=asset.statistics.stats_mean,
                    min=asset.statistics.stats_min,
                    max=asset.statistics.stats_max,
                    mode=asset.statistics.stats_mode,
                    median=asset.statistics.stats_median,
                    stddev=asset.statistics.stats_stddev,
                    number_of_null=asset.statistics.stats_number_of_null,
                    number_of_unique=asset.statistics.stats_number_of_unique,
                )
                if asset.statistics
                else None
            ),
            comment_on_ddl=asset.comment_on_ddl,
            ddl_statement=asset.ddl_statement,
            data_type=asset.data_type,
            ordinal_position=asset.ordinal_position,
            path=[
                Path(
                    id=p.ancestor_id,
                    name=p.ancestor_name,
                    object_type=p.ancestor_type,
                    path_layer=p.path_layer,
                )
                for p in asset.paths
            ],
            manual_tag_ids=[
                TagLink(
                    child_tag_id=link.child_tag_id,
                    parent_tag_id=link.parent_tag_id,
                    tag_group_id=link.tag_group_id,
                )
                for link in asset.tags
                if link.link_type == "manual"
            ],
            rule_tag_ids=[
                TagLink(
                    child_tag_id=link.child_tag_id,
                    parent_tag_id=link.parent_tag_id,
                    tag_group_id=link.tag_group_id,
                )
                for link in asset.tags
                if link.link_type == "rule"
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
                            property_values=prop.values,
                            options=prop.options,
                            attachments=[
                                Attachment(
                                    file_name=att.file_name,
                                    content_type=att.content_type,
                                    file_size=att.file_size,
                                    uploaded_at=att.uploaded_at,
                                    uploaded_by=att.uploaded_by,
                                )
                                for att in prop.attachments
                            ],
                        )
                        for prop in aps.property_set.properties
                    ],
                )
                for aps in asset.property_sets
            ],
            is_deleted=asset.is_deleted,
        )

        return CommonResponse(data=asset)

    async def delete_asset(
        self, asset_id: str, client_id: str, tenant_id: str
    ) -> CommonResponse[Asset]:
        logger.info(f"deleting asset id {asset_id}")
        message = "successfully deleted"

        asset = await self.asset_repo.get_by_id(asset_id)
        self._validate_asset_for_deletion(asset_id, asset)

        await self.asset_repo.soft_delete(asset_id, client_id)
        self._publish_delete_asset_event(client_id, tenant_id, asset_id)
        return message

    def _validate_asset_for_deletion(self, asset_id: str, asset: Asset):
        if not asset:
            logger.error(f"asset id {asset_id} does not exist")
            raise AssetNotFoundError(asset_id=asset_id)

        if asset.is_csv_imported and not asset.is_archived:
            message = "asset that is not archive cannot be deleted"
            logger.error(message)
            raise BadRequestError(detail=message)

        if not asset.is_lost:
            message = "asset that is not lost cannot be deleted"
            logger.error(message)
            raise BadRequestError(detail=message)

    def _publish_delete_asset_event(
        self, client_id: str, tenant_id: str, asset_id: str
    ):
        self.event_repo.create_event(
            asset_id,
            tenant_id,
            client_id,
            EventType.DELETE_ASSETS,
            Operation.DELETE_ASSETS,
        )

        # self.event_repo.create_event(
        #     asset_id,
        #     tenant_id,
        #     client_id,
        #     EventType.UPDATE_TAGS,
        #     Operation.DELETE_ASSETS,
        # )

        # self.event_repo.create_event(
        #     asset_id,
        #     tenant_id,
        #     client_id,
        #     EventType.UPDATE_USERS,
        #     Operation.DELETE_ASSETS,
        # )

        # self.event_repo.create_event(
        #     asset_id,
        #     tenant_id,
        #     client_id,
        #     EventType.DELETE_MISSING_COMMENTS,
        #     Operation.DELETE_ASSETS,
        # )
