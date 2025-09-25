from enum import Enum
from typing import Dict, Optional


class OutboxStatus(str, Enum):
    pending = "pending"  # ready to publish
    publishing = "publishing"  # claimed by a worker
    published = "published"  # confirm acked by broker
    failed = "failed"  # last attempt failed
    dead = "dead"  # exceeded max attempts -> stop


class LineageType(str, Enum):
    upstream = "upstream"
    downstream = "downstream"
    ext_data_downstream = "downstream#extdata"
    upstream_column = "column#upstream"
    downstream_column = "column#downstream"


class Type(str, Enum):
    Schema = "schema"
    Table = "table"
    Column = "column"

    BiGroup = "bigroup"
    Dashboard = "dashboard"
    Sheet = "sheet"
    Etl = "etl"
    PropertySet = "property_set"
    Property = "property"
    Lineage = "lineage"
    DataOverview = "data_overview"
    AssetGroup = "asset_group"
    DataSource = "data_source"
    CommentThreadOn = "comment_thread_on"

    TagGroup = "tag_group"
    TagCategory = "tag_category"
    Tag = "tag"
    RuleSet = "rule_set"
    Rule = "rule"

    DraftItem = "drft"
    DraftMetadata = "drft_metadata"

    ImportLog = "import_log"
    ExportLog = "export_log"
    JobLog = "job_log"

    User = "user"
    UserGroup = "user_group"

    Event = "event"


class PrefixConstants(str, Enum):
    Schema = "schm-"
    Table = "tbl-"
    Column = "clmn-"
    BiGroup = "bgrp-"
    Dashboard = "dsbd-"
    Sheet = "sht-"
    Etl = "etl-"
    Lineage = "lng-"
    TagGroup = "tggp-"
    TagCategory = "tgct-"
    Tag = "tg-"
    RuleSet = "rlst-"
    Rule = "rl-"
    User = "usr-"
    UserGroup = "usgr-"
    PropertySet = "ppst-"
    AssetGroup = "asgp-"
    DataSource = "dsrc-"
    DraftMetadata = "drft-"

    # version unavailable / not partition key in original code
    ImportLog = "imlg-"
    ExportLog = "exlg-"
    JobLog = "jblg-"
    Property = "pp-"
    Event = "evnt-"


class PathLayer(str, Enum):
    Schema4 = "schema4"
    Schema3 = "schema3"
    BiGroup4 = "bigroup4"
    BiGroup3 = "bigroup3"


PREFIX_MAP: Dict[str, str] = {
    # Assets
    Type.Schema.value: PrefixConstants.Schema.value,
    Type.Table.value: PrefixConstants.Table.value,
    Type.Column.value: PrefixConstants.Column.value,
    Type.BiGroup.value: PrefixConstants.BiGroup.value,
    Type.Dashboard.value: PrefixConstants.Dashboard.value,
    Type.Sheet.value: PrefixConstants.Sheet.value,
    Type.Etl.value: PrefixConstants.Etl.value,
    Type.Lineage.value: PrefixConstants.Lineage.value,
    Type.PropertySet.value: PrefixConstants.PropertySet.value,
    Type.Property.value: PrefixConstants.Property.value,
    Type.AssetGroup.value: PrefixConstants.AssetGroup.value,
    Type.DataSource.value: PrefixConstants.DataSource.value,
    Type.Lineage.value: PrefixConstants.Lineage.value,
    # Tags
    Type.TagGroup.value: PrefixConstants.TagGroup.value,
    Type.TagCategory.value: PrefixConstants.TagCategory.value,
    Type.Tag.value: PrefixConstants.Tag.value,
    Type.RuleSet.value: PrefixConstants.RuleSet.value,
    Type.Rule.value: PrefixConstants.Rule.value,
    Type.DraftMetadata.value: PrefixConstants.DraftMetadata.value,
    # Logs
    Type.ImportLog.value: PrefixConstants.ImportLog.value,
    Type.ExportLog.value: PrefixConstants.ExportLog.value,
    Type.JobLog.value: PrefixConstants.JobLog.value,
    # Users
    Type.User.value: PrefixConstants.User.value,
    Type.UserGroup.value: PrefixConstants.UserGroup.value,
    Type.Event.value: PrefixConstants.Event.value,
}

OBJECT_TYPE_MAP: Dict[str, str] = {
    # Assets
    PrefixConstants.Schema.value: Type.Schema.value,
    PrefixConstants.Table.value: Type.Table.value,
    PrefixConstants.Column.value: Type.Column.value,
    PrefixConstants.BiGroup.value: Type.BiGroup.value,
    PrefixConstants.Dashboard.value: Type.Dashboard.value,
    PrefixConstants.Sheet.value: Type.Sheet.value,
    PrefixConstants.Etl.value: Type.Etl.value,
    PrefixConstants.Lineage.value: Type.Lineage.value,
    PrefixConstants.PropertySet.value: Type.PropertySet.value,
    PrefixConstants.Property.value: Type.Property.value,
    PrefixConstants.AssetGroup.value: Type.AssetGroup.value,
    PrefixConstants.DataSource.value: Type.DataSource.value,
    # Tags
    PrefixConstants.TagGroup.value: Type.TagGroup.value,
    PrefixConstants.TagCategory.value: Type.TagCategory.value,
    PrefixConstants.Tag.value: Type.Tag.value,
    PrefixConstants.RuleSet.value: Type.RuleSet.value,
    PrefixConstants.Rule.value: Type.Rule.value,
    PrefixConstants.DraftMetadata.value: Type.DraftMetadata.value,
    # Logs
    PrefixConstants.ImportLog.value: Type.ImportLog.value,
    PrefixConstants.ExportLog.value: Type.ExportLog.value,
    PrefixConstants.JobLog.value: Type.JobLog.value,
    # Users
    PrefixConstants.User.value: Type.User.value,
    PrefixConstants.UserGroup.value: Type.UserGroup.value,
    PrefixConstants.Event.value: Type.Event.value,
}


def lookup_prefix(t: str) -> Optional[str]:
    key = t.value if isinstance(t, Enum) else str(t)
    return PREFIX_MAP.get(key)


def lookup_object_type(id_: str) -> str:
    parts = id_.split("-")
    if not parts:  # empty string case
        return "undefined"
    prefix = f"{parts[0]}-"
    return OBJECT_TYPE_MAP.get(prefix, "undefined")


def get_object_type_prefix(object_type: str) -> str:
    return object_type.split("#", 1)[0]
