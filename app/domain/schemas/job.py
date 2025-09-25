from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, Any, Dict
from datetime import datetime
from enum import Enum


class DataSourceType(str, Enum):
    agent = "agent"
    csv = "csv"


class JobType(str, Enum):
    _import = "import"
    _export = "export"


class UpdateMode(str, Enum):
    full = "full"
    partial = "partial"


class JobStatus(str, Enum):
    created = "Created"
    failed = "Failed"
    running = "Running"
    success = "Success"
    stopped = "Stopped"


class ImportJobStatus(str, Enum):
    created = "Created"
    upload_complete = "UploadCompleted"
    started = "Started"
    completed = "Completed"
    stopped = "Stopped"
    failed = "Failed"
    pending = "Pending"
    success = "Success"


class ImportFileStatus(str, Enum):
    pending = "Pending"
    completed = "Completed"
    failed = "Failed"
    stopped = "Stopped"


class JobOverrideType(str, Enum):
    always = "always"
    new_asset = "only_new_asset"
    false = "false"


class JobCreateRequest(BaseModel):
    tenant_id: str
    object_name: str
    object_format: str = "csv"
    datasource_type: str
    datasource_name: str
    override_logical_name: str = "false"
    update_mode: UpdateMode = UpdateMode.full

    model_config = ConfigDict(from_attributes=True)


class ImportStatusUpdate(BaseModel):
    status: ImportJobStatus


class ImportJobResponse(BaseModel):
    object_name: str
    object_format: str
    datasource_type: str
    datasource_name: str
    update_mode: UpdateMode
    import_status: ImportJobStatus
    import_total_chunks: int
    import_processed_chunks: int

    model_config = ConfigDict(from_attributes=True)


class DataSourceMetadataResponseBody(BaseModel):
    user_id: str = Field(..., description="User ID who requested the import/export job")
    job_key: str = Field(..., description="The id of the log")
    service_name: str = Field(
        ..., description="Service name of a data source where an item is stored"
    )
    source_name: str = Field(
        ..., description="Name of the data source. Can be duplicated"
    )
    source_type: str = Field(..., description="Type of the data source")
    override_logical_name: str = Field(
        ..., description="Override logical name of assets"
    )


class SourceJobResponseBody(BaseModel):
    error_code: int = Field(..., description="Error code of the job")
    format_value: Dict[str, str] = Field(
        ..., description="Values to format error message template"
    )
    status: str = Field(..., description="The status of the import job")


class ImportJob(BaseModel):
    metadata: DataSourceMetadataResponseBody
    job: SourceJobResponseBody


class Job(BaseModel):
    id: str
    type: str
    status: str
    import_job: Optional[ImportJobResponse] = None

    model_config = ConfigDict(from_attributes=True)


class UploadStreaming(BaseModel):
    id: str
    status: ImportJobStatus


class ImportFileBase(BaseModel):
    import_job_id: str
    filename: str
    location: str
    status: ImportFileStatus = ImportFileStatus.pending
    file_hash: Optional[str] = None
    sync_markers: Optional[Dict[str, Any]] = None


class ImportFileCreate(ImportFileBase):
    pass


class ImportFileUpdate(BaseModel):
    status: Optional[ImportFileStatus] = None
    processed_at: Optional[datetime] = None
    uploaded_at: Optional[datetime] = None
    processed_assets: Optional[int] = None
    current_block: Optional[int] = None
    total_blocks: Optional[int] = None
    current_record_in_block: Optional[int] = None
    file_hash: Optional[str] = None
    sync_markers: Optional[Dict[str, Any]] = None


class ImportFileInDBBase(ImportFileBase):
    id: str
    created_at: datetime
    processed_at: Optional[datetime] = None
    uploaded_at: Optional[datetime] = None
    processed_assets: int = 0
    current_block: int = 0
    total_blocks: int = 0
    current_record_in_block: int = 0

    class Config:
        from_attributes = True


class ImportFile(ImportFileInDBBase):
    pass
