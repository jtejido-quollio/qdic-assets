from enum import Enum
from typing import List, Optional, Literal
from pydantic import BaseModel, Field
from app.domain.schemas.asset import CommonAsset
from pydantic import ConfigDict


class SearchMode(str, Enum):
    simple = "simple"  # PG regex / LIKE via repos
    querydsl = "querydsl"  # OpenSearch DSL
    nlq = "nlq"  # NLQ -> entities -> OpenSearch


# Optional boolean structure for "simple" mode
class SimpleClause(BaseModel):
    key: str  # e.g. "asset.name", "tag.name"
    text: str  # e.g. "TEST*"
    op: Literal["and", "or", "not"] = "and"


class SearchQuerySource(BaseModel):
    metadata_agent: Optional[bool] = None
    csv: Optional[bool] = None


class SearchQueryDataQuality(BaseModel):
    healthy: Optional[bool] = None
    anomaly: Optional[bool] = None


class SearchQueryAssetLightParams(BaseModel):
    # QDIC-compatible bits
    source: Optional[SearchQuerySource] = None
    data_quality: Optional[SearchQueryDataQuality] = None
    keyword: Optional[str] = None
    asset_type: List[str] = []
    user_ids: List[str] = []
    include_disabled: Optional[bool] = None
    user_group_ids: List[str] = []
    exclude_user_group_ids: List[str] = []
    asset_groups: List[str] = []
    service_name: List[str] = []
    custom_category_id: Optional[str] = None
    custom_category_ids: List[str] = []
    tags: List[str] = []

    # additions for SIMPLE mode
    key: Optional[str] = None
    text: Optional[str] = None
    clauses: Optional[List[SimpleClause]] = None


class SearchRequest(BaseModel):
    search_mode: SearchMode = Field(description="simple | querydsl | nlq")
    query: Optional[SearchQueryAssetLightParams] = None
    sort: Optional[str] = None
    order: Optional[Literal["asc", "desc"]] = "asc"
    size: Optional[int] = 25
    from_: int = Field(0, alias="from")


class SearchResponse(BaseModel):
    total: int
    number_of_results: int
    size: int
    from_: int = Field(alias="from")
    data: List[CommonAsset] = []

    model_config = ConfigDict(populate_by_name=True)
