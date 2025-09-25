from __future__ import annotations

from typing import (
    Any,
    Callable,
    AsyncContextManager,
    Type,
    TypeAlias,
    Dict,
    List,
)
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import BadRequestError, NotFoundError
from app.infrastructure.db.models.base import Base  # your Declarative Base
from pydantic import BaseModel
from app.utils.query_builder import (
    dict_to_sqlalchemy_filter_options,
)  # assumed to return SQLAlchemy expressions

T: TypeAlias = Base
U: TypeAlias = BaseModel

PAGE = 1
PAGE_SIZE = 20
ORDERING = "-id"


class BaseRepository:
    def __init__(
        self,
        session_factory: Callable[[], AsyncContextManager[AsyncSession]],
        model: Type[T],
    ) -> None:
        self.session_factory = session_factory
        self.model = model

    # ---------- helpers ----------
    def _ordering_clause(self, ordering: str):
        # e.g. "-created_at" or "name"
        field = ordering[1:] if ordering.startswith("-") else ordering
        col = getattr(self.model, field)
        return col.desc() if ordering.startswith("-") else col.asc()

    def _eager_options(self, eager: bool):
        if not eager:
            return []
        # model.eagers: list[str] of relationship attribute names
        return [
            selectinload(getattr(self.model, rel))
            for rel in getattr(self.model, "eagers", [])
        ]

    # ---------- queries ----------
    async def get_by_options(self, schema: U, eager: bool = False) -> Dict[str, Any]:
        """Return items + paging metadata."""
        params = schema.model_dump(exclude_none=True)
        ordering: str = params.get("ordering", ORDERING)
        page: int | str = params.get("page", PAGE)
        page_size: int | str = params.get("page_size", PAGE_SIZE)

        where_clause = dict_to_sqlalchemy_filter_options(self.model, params)

        stmt = (
            select(self.model)
            .where(where_clause)
            .options(*self._eager_options(eager))
            .order_by(self._ordering_clause(ordering))
        )

        async with self.session_factory() as session:
            # total_count on the filtered set
            count_stmt = select(func.count()).select_from(
                select(self.model).where(where_clause).subquery()
            )
            total_count: int = await session.scalar(count_stmt) or 0

            if page_size != "all":
                # normalize to ints
                p = int(page)
                ps = int(page_size)
                stmt = stmt.limit(ps).offset((p - 1) * ps)

            result = await session.execute(stmt)
            items: List[T] = result.scalars().all()

        return {
            "founds": items,
            "search_options": {
                "page": page,
                "page_size": page_size,
                "ordering": ordering,
                "total_count": total_count,
            },
        }

    async def get_by_id(self, id: Any, eager: bool = False) -> T:
        stmt = (
            select(self.model)
            .where(self.model.id == id)
            .options(*self._eager_options(eager))
            .limit(1)
        )
        async with self.session_factory() as session:
            obj = (await session.execute(stmt)).scalars().first()
            if not obj:
                raise NotFoundError(detail=f"not found id : {id}")
            return obj

    async def create(self, schema: U) -> T:
        obj = self.model(**schema.model_dump())
        async with self.session_factory() as session:
            try:
                session.add(obj)
                await session.commit()
                await session.refresh(obj)
                return obj
            except IntegrityError as e:
                await session.rollback()
                raise BadRequestError(detail=str(e.orig))

    async def update(self, id: Any, schema: U) -> T:
        data = schema.model_dump(exclude_unset=True)
        async with self.session_factory() as session:
            # Option A: load, mutate, commit (keeps ORM events, defaults, etc.)
            obj = (
                (await session.execute(select(self.model).where(self.model.id == id)))
                .scalars()
                .first()
            )
            if not obj:
                raise NotFoundError(detail=f"not found id : {id}")
            for k, v in data.items():
                setattr(obj, k, v)
            await session.commit()
            await session.refresh(obj)
            return obj

    async def update_attr(self, id: Any, column: str, value: Any) -> T:
        return await self.update(
            id, type("Tmp", (BaseModel,), {"model_dump": lambda s: {column: value}})()
        )

    async def whole_update(self, id: Any, schema: U) -> T:
        # replaces all fields from schema (even Nones)
        data = schema.model_dump()
        async with self.session_factory() as session:
            obj = (
                (await session.execute(select(self.model).where(self.model.id == id)))
                .scalars()
                .first()
            )
            if not obj:
                raise NotFoundError(detail=f"not found id : {id}")
            for k, v in data.items():
                setattr(obj, k, v)
            await session.commit()
            await session.refresh(obj)
            return obj

    async def delete_by_id(self, id: Any) -> None:
        async with self.session_factory() as session:
            obj = (
                (await session.execute(select(self.model).where(self.model.id == id)))
                .scalars()
                .first()
            )
            if not obj:
                raise NotFoundError(detail=f"not found id : {id}")
            await session.delete(obj)
            await session.commit()
