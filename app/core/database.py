from __future__ import annotations

from typing import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, declared_attr
from app.core.config import settings


class Base(DeclarativeBase):
    @declared_attr.directive
    def __tablename__(cls) -> str:  # type: ignore[override]
        return cls.__name__.lower()


class Database:
    def __init__(self, db_url: str) -> None:
        self._engine = create_async_engine(
            self._ensure_async_url(db_url), pool_pre_ping=True
        )
        self._sessionmaker: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=self._engine,
            expire_on_commit=False,
            autoflush=False,
        )

    @staticmethod
    def _ensure_async_url(url: str) -> str:
        if url.startswith("postgresql+asyncpg") or "+asyncpg" in url:
            return url
        if url.startswith("postgresql+psycopg"):
            return url.replace("postgresql+psycopg", "postgresql+asyncpg", 1)
        if url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return url

    @property
    def engine(self):
        return self._engine

    @property
    def sessionmaker(self) -> async_sessionmaker[AsyncSession]:
        return self._sessionmaker

    async def create_database(self) -> None:
        """Create tables once (no Alembic)."""
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        async with self._sessionmaker() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
