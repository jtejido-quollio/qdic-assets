from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from app.domain.schemas.job import JobOverrideType
from app.domain.schemas.common import CommonSystem, OSCommonSystem


class Attachment(BaseModel):
    file_name: str
    content_type: str
    file_size: int
    uploaded_at: Optional[datetime]
    uploaded_by: Optional[str]


class Property(BaseModel):
    property_id: str
    property_title: str
    property_type: str
    property_values: Optional[list]
    options: Optional[dict]
    attachments: List[Attachment] = []


class PropertySet(BaseModel):
    id: str
    property_set_id: str
    property_set_title: str
    is_activated: bool
    order: int
    properties: List[Property]


class Path(BaseModel):
    id: str
    name: str
    object_type: str
    path_layer: str


class TagLink(BaseModel):
    child_tag_id: str
    parent_tag_id: Optional[str]
    tag_group_id: Optional[str]


class TableStatistics(BaseModel):
    count: Optional[int]
    size: Optional[float]


class ColumnStatistics(BaseModel):
    mean: Optional[str] = None
    min: Optional[str] = None
    max: Optional[str] = None
    mode: Optional[str] = None
    median: Optional[str] = None
    stddev: Optional[str] = None
    number_of_null: Optional[int]
    number_of_unique: Optional[int]


# this should be defunct as we have common asset,but we still use this for get asset route for some reason...
class Asset(BaseModel):
    id: str
    logical_name: Optional[str]
    physical_name: Optional[str]
    data_source_id: Optional[str]
    description: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    created_by: Optional[str]
    updated_by: Optional[str]
    is_archived: bool
    is_csv_imported: bool
    is_lost: bool
    object_type: Optional[str]
    service_name: Optional[str]
    sub_id: Optional[str]
    child_asset_ids: List[str]
    comment_on_ddl: Optional[str]
    ddl_statement: Optional[str]
    data_type: Optional[str]
    ordinal_position: Optional[int]
    path: List[Path]
    manual_tag_ids: List[TagLink]
    rule_tag_ids: List[TagLink]
    property_sets: List[PropertySet]
    statistics: Optional[TableStatistics]
    column_stats: Optional[ColumnStatistics]
    is_deleted: bool


class AssetPath(BaseModel):
    asset_id: str
    ancestor_id: str
    ancestor_name: str
    ancestor_type: str
    path_layer: str
    depth: int
    path_order: int
    path_layer: str


class AssetObjectType(BaseModel):
    object_type: Optional[str] = None


class TagGroup(BaseModel):
    global_id: str
    tag_group_name: Optional[str] = None
    tag_group_description: Optional[str] = None
    tag_group_color: Optional[str] = None
    is_archived: Optional[bool] = False
    is_deleted: Optional[bool] = False


class Tag(BaseModel):
    global_id: str
    tag_name: Optional[str] = None
    tag_description: Optional[str] = None
    is_archived: Optional[bool] = False
    is_deleted: Optional[bool] = False


class TagCategory(BaseModel):
    global_id: str
    tag_category_name: Optional[str] = None
    tag_category_description: Optional[str] = None
    is_archived: Optional[bool] = False
    is_deleted: Optional[bool] = False


class FullTag(BaseModel):
    global_id: str
    tag_group: Optional[TagGroup] = None
    tag_category: Optional[TagCategory] = None
    tag: Optional[Tag] = None
    is_manual: Optional[bool] = False


class DataSharing(BaseModel):
    global_id: str
    sharing_name: str
    physical_name: str
    sharing_type: str
    error_reason: str


class Datasource(BaseModel):
    id: str
    name: str
    type: str


class ExtData(BaseModel):
    possible_global_ids: List[str]
    ext_table_id: str
    ext_table_name: str
    ext_table_name_path: str
    ext_description: str
    connection_type: str
    datasources: List[Datasource]


class ExtTag(BaseModel):
    ext_tag_id: str
    ext_tag_description: str
    ext_tag_name: str


class ExtOwner(BaseModel):
    ext_owner_id: str
    email_address: str
    display_name: str


class ExtSource(BaseModel):
    source_id: str
    source_name: str
    source_type: str


class ExtConnection(BaseModel):
    possible_global_ids: List[str]
    ext_table_id: str
    ext_table_name: str
    ext_table_name_path: str
    ext_description: str
    ext_service_name: str
    ext_sources: List[ExtSource]


class AvroAssetProperty(BaseModel):
    property_id: str
    property_values: List[str]


class AvroAssetPropertySet(BaseModel):
    property_set_id: str
    properties: List[AvroAssetProperty]


class CsvCustomData(BaseModel):
    logical_name: str
    description: str
    manual_tags: List[str]
    owners: List[str]
    properties: List[AvroAssetPropertySet]


class AssetCreate(BaseModel):
    id: str
    tenant_id: Optional[str] = None
    physical_name: Optional[str] = None
    logical_name: Optional[str] = None
    service_name: Optional[str] = None
    description: Optional[str] = None
    override_logical_name: JobOverrideType = JobOverrideType.false
    comment_on_ddl: Optional[str] = None
    ddl_statement: Optional[str] = None
    asset_type: Optional[str] = None
    is_lost: bool = False
    is_csv_imported: bool = False
    is_archived: bool = False
    version: Optional[str] = "latest"
    data_type: Optional[str] = None
    ordinal_position: Optional[int] = None
    record_updated_at: Optional[str] = None
    ext_url: Optional[str] = None
    ext_access_count: Optional[int] = None
    ext_name: Optional[str] = None
    ext_description: Optional[str] = None
    data_sharing: Optional[List[DataSharing]] = None
    ext_tag: Optional[List[ExtTag]] = None
    ext_owner: Optional[List[ExtOwner]] = None
    ext_connection: Optional[List[ExtConnection]] = None
    stats_size: Optional[str] = None
    stats_count: Optional[str] = None
    stats_max: Optional[str] = None
    stats_min: Optional[str] = None
    stats_mean: Optional[str] = None
    stats_median: Optional[str] = None
    stats_mode: Optional[str] = None
    stats_stddev: Optional[str] = None
    stats_number_of_null: Optional[str] = None
    stats_number_of_unique: Optional[str] = None
    asset_group_ids: Optional[List[str]] = None
    csv_custom_data: Optional[CsvCustomData] = None


class CommonAsset(CommonSystem):
    logical_name: Optional[str] = None
    physical_name: Optional[str] = None
    data_source_id: Optional[str] = None
    description: Optional[str] = None
    record_updated_at: Optional[str] = None
    is_archived: bool
    is_csv_imported: bool
    is_lost: bool
    service_name: Optional[str] = None
    child_asset_ids: List[str] = Field(default_factory=list)
    parent_dashboard_ids: List[str] = Field(default_factory=list)
    comment_on_ddl: Optional[str] = None
    asset_type: Optional[str] = None
    ddl_statement: Optional[str] = None
    data_type: Optional[str] = None
    ordinal_position: Optional[int] = None
    path: List[Path] = Field(default_factory=list)
    tags: List[FullTag] = Field(default_factory=list)
    property_sets: List[PropertySet] = Field(default_factory=list)
    statistics: Optional[TableStatistics] = None
    column_stats: Optional[ColumnStatistics] = None
    ext_url: Optional[str] = None
    ext_access_count: Optional[int] = None
    ext_name: Optional[str] = None
    ext_description: Optional[str] = None
    data_sharing: List[DataSharing] = Field(default_factory=list)
    ext_tags: List[ExtTag] = Field(default_factory=list)
    ext_owners: List[ExtOwner] = Field(default_factory=list)
    ext_data: List[ExtData] = Field(default_factory=list)


class OSAsset(OSCommonSystem):
    logical_name: Optional[str] = None
    physical_name: Optional[str] = None
    data_source_id: Optional[str] = None
    description: Optional[str] = None
    record_updated_at: Optional[str] = None
    is_archived: bool
    is_csv_imported: bool
    is_lost: bool
    service_name: Optional[str] = None
    child_asset_ids: List[str] = Field(default_factory=list)
    parent_dashboard_ids: List[str] = Field(default_factory=list)
    comment_on_ddl: Optional[str] = None
    asset_type: Optional[str] = None
    ddl_statement: Optional[str] = None
    data_type: Optional[str] = None
    ordinal_position: Optional[int] = None
    path: List[Path] = Field(default_factory=list)
    tags: List[FullTag] = Field(default_factory=list)
    statistics: Optional[TableStatistics] = None
    column_stats: Optional[ColumnStatistics] = None
    ext_url: Optional[str] = None
    ext_access_count: Optional[int] = None
    ext_name: Optional[str] = None
    ext_description: Optional[str] = None
    data_sharing: List[DataSharing] = Field(default_factory=list)
    ext_tags: List[ExtTag] = Field(default_factory=list)
    ext_owners: List[ExtOwner] = Field(default_factory=list)
    ext_data: List[ExtData] = Field(default_factory=list)
    properties_property_details_with_context: Optional[List[str]] = Field(default=None)


class OSDoc(BaseModel):
    doc: OSAsset
