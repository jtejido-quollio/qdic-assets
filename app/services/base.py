from typing import Any

from app.domain.repositories.base import Repository


class BaseService:
    def __init__(self, repository: Repository) -> None:
        self._repository = repository

    async def get_list(self, schema: Any, eager: bool = False) -> Any:
        return await self._repository.get_by_options(schema, eager)

    async def get_by_id(self, id: str, eager: bool = False) -> Any:
        return await self._repository.get_by_id(id, eager)

    async def add(self, schema: Any) -> Any:
        return await self._repository.create(schema)

    async def patch(self, id: str, schema: Any) -> Any:
        return await self._repository.update(id, schema)

    async def patch_attr(self, id: str, attr: str, value: Any) -> Any:
        return await self._repository.update_attr(id, attr, value)

    async def put_update(self, id: str, schema: Any) -> Any:
        return await self._repository.whole_update(id, schema)

    async def remove_by_id(self, id: str) -> Any:
        return await self._repository.delete_by_id(id)
