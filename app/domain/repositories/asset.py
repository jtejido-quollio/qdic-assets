from typing import Optional, Protocol, Callable

from app.domain.repositories.base import Repository
from app.infrastructure.db.models.asset import Asset
from app.domain.schemas.asset import AssetCreate
from app.domain.schemas.asset import AssetObjectType
from app.domain.schemas.search import SimpleClause
from typing import List, Tuple


class AssetRepository(Repository, Protocol):
    async def find_or_create(self, id: str) -> Asset: ...

    async def get_by_id(self, id: str) -> Optional[Asset]: ...

    async def exists(self, id: str) -> bool: ...

    async def fetch_asset_with_all_nested_data(
        self, asset_id: str
    ) -> Optional[Asset]: ...

    async def update_object_type(
        self, id: str, schema: AssetObjectType
    ) -> Optional[Asset]: ...

    async def bulk_upsert(
        self, schemas: List[AssetCreate], default_asset_group_id: str, is_partial: bool
    ) -> List[Asset]: ...

    async def search_simple(
        self,
        logical_filters: List[SimpleClause],
        from_: int,
        size: int,
        sort: str | None,
        order: str,
    ) -> Tuple[int, List[Asset]]: ...

    async def fetch_top_schemas(self) -> List[Asset]: ...

    async def fetch_top_bi_groups(self) -> List[Asset]: ...

    async def fetch_subtree_dfs(
        self,
        top_schema_id: str,
        batch_callback: Optional[Callable[[List[Asset]], None]] = None,
        project_fields: Optional[List[str]] = None,
    ) -> List[Asset]: ...

    async def soft_delete(self, id: str, user_id: str) -> Optional[Asset]: ...

    async def delete_by_ids(self, ids: List[str]) -> int: ...
