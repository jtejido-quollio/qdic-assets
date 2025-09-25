from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Any
import json
from datetime import datetime
from pydantic import BaseModel, ConfigDict


class EventType(str, Enum):
    # invoke dependency
    UPDATE_ASSETS = "UpdateAssets"
    UPDATE_TAGS = "UpdateTags"
    UPDATE_RULES = "UpdateRules"
    UPDATE_USERS = "UpdateUsers"
    UPDATE_CUSTOM_CATEGORIES = "UpdateCustomCategories"
    APPLY_RULE = "ApplyRule"
    DELETE_ASSETS = "DeleteAssets"
    EXPORT_DATA = "ExportData"
    DELETE_MISSING_COMMENTS = "DeleteMissingComments"
    DELETE_ALL_COMMENTS = "DeleteAllComments"
    UPDATE_BI_DATAS = "UpdateBiDatas"
    APPLY_RULE_BI_DATA = "ApplyRuleBiData"
    UPDATE_USER_GROUP = "UpdateUserGroup"
    UPDATE_USER_GROUP_PROPERTY_SETS = "UpdateUserGroupPropertySets"
    UPDATE_USER_GROUP_PROPERTY = "UpdateUserGroupProperty"
    DELETE_USER_GROUP = "DeleteUserGroup"
    UPDATE_WORKFLOW_SUBTASKS = "UpdateWorkflowSubtasks"
    DELETE_WORKFLOW_TASK_NOTIFICATIONS = "DeleteWorkflowTaskNotifcations"
    SET_WORKFLOW_TASK_STATUS_TO_CANCELED = (
        "SetWorkflowTaskStatusToCanceledByReferenceObject"
    )
    DELETE_TAG_WORKFLOW_TASK = "DeleteTagWorkflowTask"
    DELETE_TAG_CATEGORY_WORKFLOW_TASK = "DeleteTagCategoryWorkflowTask"
    UPDATE_TAG_WORKFLOW_TASK = "UpdateTagWorkflowTask"
    UPDATE_TAG_CATEGORY_WORKFLOW_TASK = "UpdateTagCategoryWorkflowTask"
    CREATE_TAG_WORKFLOW_TASK = "CreateTagWorkflowTask"
    CREATE_TAG_CATEGORY_WORKFLOW_TASK = "CreateTagCategoryWorkflowTask"
    UPDATE_ASSET_GROUP = "UpdateAssetGroup"
    DELETE_ASSET_GROUP = "DeleteAssetGroup"
    LIST_ASSET_GROUP_MEMBERS_TREE = "ListAssetGroupMembersTree"
    APPROVE_UPSERT_TAG_DRAFT = "ApproveUpsertTagDraft"
    APPROVE_DELETE_TAG_DRAFT = "ApproveDeleteTagDraft"
    APPROVE_UPSERT_TAG_CATEGORY_DRAFT = "ApproveUpsertTagCatgeoryDraft"
    APPROVE_DELETE_TAG_CATEGORY_DRAFT = "ApproveDeleteTagCategoryDraft"
    APPROVE_DELETE_TAG_DEPENDENT_DRAFT = "ApproveDeleteTagDepedentDraft"
    APPROVE_ASSET_DRAFT = "ApproveAssetDraft"
    REJECT_DRAFT = "RejectDraft"

    # bulk update
    BULK_ASSETS = "BulkAssets"
    BULK_TAGS = "BulkTags"
    BULK_RULES = "BulkRules"

    # schedule notification
    SEND_ALERT_NOTIFICATION = "SendAlertNotification"


class EventStatus(str, Enum):
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"


class Operation(str, Enum):
    # Internal operations
    UPDATE_METADATA = "OpUpdateMetadata"
    ASSET_UPDATE_PROPERTY = "OpAssetUpdateProperty"
    ASSET_UPDATE_TAG = "OpAssetUpdateTag"
    UPDATE_ASSET_DETAILS = "OpUpdateAssetDetails"
    DELETE_ASSETS = "OpDeleteAssets"
    UPDATE_RULE_SET = "OpUpdateRuleSet"
    DELETE_RULE_SET = "OpDeleteRuleSet"
    CREATE_RULE = "OpCreateRule"
    UPDATE_RULE = "OpUpdateRule"
    DELETE_RULE = "OpDeleteRule"
    APPLY_RULE = "OpApplyRule"
    DELETE_TAG_GROUP = "OpDeleteTagGroup"
    TAG_DELETE_TAG = "OpTagDeleteTag"
    UPDATE_TAG_CATEGORY = "OpUpdateTagCategory"
    DELETE_TAG_CATEGORY = "OpDeleteTagCategory"
    TAG_UPDATE_TAG = "OpTagUpdateTag"
    UPDATE_PROPERTY_SET = "OpUpdatePropertySet"
    CREATE_PROPERTY = "OpCreateProperty"
    DELETE_PROPERTY = "OpDeleteProperty"
    CREATE_PROPERTY_SET = "OpCreatePropertySet"
    DELETE_PROPERTY_SET = "OpDeletePropertySet"
    PROPERTY_UPDATE_PROPERTY = "OpPropertyUpdateProperty"
    DELETE_COMMENT = "OpDeleteComment"
    EXPORT_DATA = "OpExportData"
    CREATE_USER_GROUP = "OpCreateUserGroup"
    UPDATE_USER_GROUP = "OpUpdateUserGroup"
    DELETE_USER_GROUP = "OpDeleteUserGroup"
    UPDATE_PARENT_TAG = "OpUpdateParentTag"
    DELETE_PARENT_TAG = "OpDeleteParentTag"
    UPDATE_CHILD_TAG = "OpUpdateChildTag"
    DELETE_CHILD_TAG = "OpDeleteChildTag"
    ASSETS_BULK_UPDATE = "OpAssetsBulkUpdate"
    TAGS_BULK_UPDATE = "OpTagsBulkUpdate"
    RULES_BULK_UPDATE = "OpRulesBulkUpdate"
    BULK_ASSETS = "OpBulkAssets"
    BULK_TAGS = "OpBulkTags"
    BULK_RULES = "OpBulkRules"
    UPDATE_WORKFLOW_TASK = "OpUpdateWorkflowTask"
    DELETE_WORKFLOW_TASK = "OpDeleteWorkflowTask"
    SET_WORKFLOW_TASK_STATUS_TO_CANCELED = (
        "OpSetWorkflowTaskStatusToCanceledByReferenceObject"
    )
    DELETE_TAG_WORKFLOW_TASK = "OpDeleteTagWorkflowTask"
    DELETE_TAG_CATEGORY_WORKFLOW_TASK = "OpDeleteTagCategoryWorkflowTask"
    UPDATE_TAG_WORKFLOW_TASK = "OpUpdateTagWorkflowTask"
    UPDATE_TAG_CATEGORY_WORKFLOW_TASK = "OpUpdateTagCategoryWorkflowTask"
    CREATE_TAG_WORKFLOW_TASK = "OpCreateTagWorkflowTask"
    CREATE_TAG_CATEGORY_WORKFLOW_TASK = "OpCreateTagCategoryWorkflowTask"
    UPDATE_ASSET_GROUP = "OpUpdateAssetGroup"
    DELETE_ASSET_GROUP = "OpDeleteAssetGroup"
    LIST_ASSET_GROUP_MEMBERS_TREE = "OpListAssetGroupMembersTree"
    APPROVE_UPSERT_TAG_DRAFT = "OpApproveUpsertTagDraft"
    APPROVE_UPSERT_TAG_CATEGORY_DRAFT = "OpApproveUpsertTagCategoryDraft"
    APPROVE_DELETE_TAG_DRAFT = "OpApproveDeleteTagDraft"
    APPROVE_DELETE_TAG_CATEGORY_DRAFT = "OpApproveDeleteTagCategoryDraft"
    APPROVE_DELETE_TAG_DEPENDENT_DRAFT = "OpApproveDeleteTagDepedentDraft"
    APPROVE_ASSET_DRAFT = "OpApproveAssetDraft"
    REJECT_DRAFT = "OpRejectDraft"

    # External API operations
    EXT_DELETE_ASSETS = "OpExtDeleteAssets"
    EXT_UPDATE_METADATA = "OpExtUpdateMetadata"
    EXT_ASSET_UPDATE_TAG = "OpExtAssetUpdateTag"
    EXT_ASSET_UPDATE_PROPERTY = "OpExtAssetUpdateProperty"
    EXT_TAG_DELETE_TAG = "OpExtTagDeleteTag"
    EXT_UPDATE_PARENT_TAG = "OpExtUpdateParentTag"
    EXT_DELETE_PARENT_TAG = "OpExtDeleteParentTag"
    EXT_UPDATE_CHILD_TAG = "OpExtUpdateChildTag"
    EXT_DELETE_CHILD_TAG = "OpExtDeleteChildTag"


class Event(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    event_type: EventType
    body: str
    operation: Operation
    status: EventStatus = EventStatus.PENDING
    user_id: str = ""
    expires_at: datetime
    updated_by: str = ""
    created_at: datetime
    is_authorized: bool = True
    is_fast_track: bool = False
    retry_count: int = 0
    wait_time: Optional[int] = None
    error: Optional[str] = None
    is_dependency_resolved: bool = False
    completed_in_seconds: Optional[int] = None
    processed_at: Optional[datetime] = None
    receipt_handle: Optional[str] = None


@dataclass
class EventContext:
    event: Event
    context: Any = None

    @classmethod
    def from_debezium_message(cls, message_body: str) -> "EventContext":
        data = json.loads(message_body)
        after_data = data.get("after", {})
        created_at = cls._parse_timestamp(after_data.get("created_at"))
        expires_at = cls._parse_timestamp(after_data.get("expires_at"))
        event = Event(
            id=after_data.get("id", ""),
            event_type=after_data.get("event_type", ""),
            body=after_data.get("body", ""),
            status=EventStatus(after_data.get("status", EventStatus.PENDING)),
            user_id=after_data.get("user_id", ""),
            operation=Operation(after_data.get("operation", "")),
            expires_at=expires_at,
            updated_by=after_data.get("updated_by", ""),
            created_at=created_at,
            is_authorized=after_data.get("is_authorized", True),
            is_fast_track=after_data.get("is_fast_track", False),
            error=after_data.get("error"),
            retry_count=after_data.get("retry_count", 0),
            wait_time=after_data.get("wait_time"),
            is_dependency_resolved=after_data.get("is_dependency_resolved", False),
            completed_in_seconds=after_data.get("completed_in_seconds"),
            receipt_handle=after_data.get("receipt_handle"),
        )

        return cls(event=event)

    @staticmethod
    def _parse_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
        """Parse various timestamp formats from Debezium"""
        if not timestamp_str:
            return None

        try:
            # Try ISO format first
            if "T" in timestamp_str:
                return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            # Try Unix timestamp (milliseconds)
            elif timestamp_str.isdigit():
                return datetime.fromtimestamp(int(timestamp_str) / 1000)
            # Fallback to current time
            else:
                return datetime.now()
        except (ValueError, TypeError):
            return datetime.now()
