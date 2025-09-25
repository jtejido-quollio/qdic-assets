from logging.config import fileConfig
import asyncio
from sqlalchemy import pool
from app.core.config import settings
from alembic import context
from app.infrastructure.db.models.base import Base
from app.infrastructure.db import models
from sqlalchemy.engine import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override URL from DATABASE_URL
if not config.get_main_option("sqlalchemy.url"):
    config.set_main_option("sqlalchemy.url", settings.DATABASE_URL)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def _run_sync_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online_async() -> None:
    engine = create_async_engine(settings.DATABASE_URL, poolclass=pool.NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(_run_sync_migrations)
    await engine.dispose()


def run_migrations_online_sync() -> None:
    engine = create_engine(settings.DATABASE_URL, poolclass=pool.NullPool)
    with engine.connect() as conn:
        _run_sync_migrations(conn)
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    if "+asyncpg" in settings.DATABASE_URL or settings.DATABASE_URL.startswith(
        "postgresql+asyncpg"
    ):
        asyncio.run(run_migrations_online_async())
    else:
        run_migrations_online_sync()
