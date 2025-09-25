from datetime import datetime
from typing import Generic, TypeVar, Optional, List

from pydantic import BaseModel, Field, AliasChoices, ConfigDict

T = TypeVar("T", bound=BaseModel)


class CommonResponse(BaseModel, Generic[T]):
    data: T


class CommonListResponse(BaseModel, Generic[T]):
    data: List[T]


class CommonSystem(BaseModel):
    global_id: str = Field(
        validation_alias=AliasChoices("_global_id", "global_id"),
        serialization_alias="_global_id",
    )
    version: str = Field(
        validation_alias=AliasChoices("_version", "version"),
        serialization_alias="_version",
    )
    object_type: str = Field(
        validation_alias=AliasChoices("_object_type", "object_type"),
        serialization_alias="_object_type",
    )
    created_at: datetime = Field(
        validation_alias=AliasChoices("_created_at", "created_at"),
        serialization_alias="_created_at",
    )
    updated_at: datetime = Field(
        validation_alias=AliasChoices("_updated_at", "updated_at"),
        serialization_alias="_updated_at",
    )
    created_by: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("_created_by", "created_by"),
        serialization_alias="_created_by",
    )
    updated_by: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("_updated_by", "updated_by"),
        serialization_alias="_updated_by",
    )

    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
    )


class OSCommonSystem(BaseModel):
    global_id: str
    version: str
    object_type: str
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
