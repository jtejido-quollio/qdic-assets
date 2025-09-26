"""Migration

Revision ID: 107631f875cb
Revises: 7dd8d518a253
Create Date: 2025-08-27 14:43:26.326572
"""

from typing import Sequence, Union
import os
import re

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "107631f875cb"
down_revision: Union[str, None] = "7dd8d518a253"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Strict identifier validator: letters/underscore, then letters/digits/underscore
_IDENT_RX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ident(name: str, envvar: str) -> str:
    """Validate Postgres identifier and return it quoted with double-quotes if needed.
    Here we just validate and return as-is (without quotes) because we’ll use format('%I', ...) server-side.
    """
    if not _IDENT_RX.match(name):
        raise ValueError(f"{envvar} contains invalid identifier: {name!r}")
    return name


def upgrade() -> None:
    # --- Read env with defaults ---
    user = _ident(os.getenv("REPLICATOR_USER", "replicator"), "REPLICATOR_USER")
    password = os.getenv("REPLICATOR_PASSWORD", "replicator_password")
    pubname = _ident(
        os.getenv("PUBLICATION_NAME", "events_publication"), "PUBLICATION_NAME"
    )

    # 1) Create/alter role with password (password as bind param; identifiers via EXECUTE format)
    op.execute(
        sa.text(
            """
DO $plpgsql$
DECLARE
    v_user text := :usr;
    v_pwd  text := :pwd;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_user) THEN
        EXECUTE format('CREATE ROLE %I WITH REPLICATION LOGIN PASSWORD %L', v_user, v_pwd);
    ELSE
        EXECUTE format('ALTER ROLE %I WITH REPLICATION LOGIN PASSWORD %L', v_user, v_pwd);
    END IF;
END
$plpgsql$;
"""
        ).bindparams(sa.bindparam("usr", user), sa.bindparam("pwd", password))
    )

    # 2) Grants on schema/tables/sequences (current objects)
    op.execute(
        sa.text(
            """
DO $plpgsql$
DECLARE
    v_user   text := :usr;
BEGIN
    EXECUTE format('GRANT USAGE ON SCHEMA public TO %I', v_user);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA public TO %I', v_user);
    EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %I', v_user);
END
$plpgsql$;
"""
        ).bindparams(sa.bindparam("usr", user))
    )

    # 3) Default privileges for future objects (must be run by the owner of future objects)
    op.execute(
        sa.text(
            """
DO $plpgsql$
DECLARE
    v_user   text := :usr;
BEGIN
    EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %I', v_user);
    EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO %I', v_user);
END
$plpgsql$;
"""
        ).bindparams(sa.bindparam("usr", user))
    )

    # 4) Create/own publication and add events table if present
    op.execute(
        sa.text(
            """
DO $plpgsql$
DECLARE
    v_pub    text := :pub;
    v_user   text := :usr;
    v_qualified regclass;
BEGIN
    -- Create publication if missing
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = v_pub) THEN
        EXECUTE format('CREATE PUBLICATION %I', v_pub);
    END IF;

    -- Ensure owner is replicator user
    PERFORM 1
    FROM pg_catalog.pg_publication p
    JOIN pg_catalog.pg_roles r ON r.oid = p.pubowner
    WHERE p.pubname = v_pub AND r.rolname = v_user;
    IF NOT FOUND THEN
        EXECUTE format('ALTER PUBLICATION %I OWNER TO %I', v_pub, v_user);
    END IF;

    -- Add table if it exists and not already in publication
    IF to_regclass('public.events') IS NOT NULL THEN
        PERFORM 1
        FROM pg_catalog.pg_publication_tables
        WHERE pubname = v_pub
          AND schemaname = 'public'
          AND tablename = 'events';
        IF NOT FOUND THEN
            EXECUTE format('ALTER PUBLICATION %I ADD TABLE public.events', v_pub);
        END IF;
    END IF;
END
$plpgsql$;
"""
        ).bindparams(
            sa.bindparam("pub", pubname),
            sa.bindparam("usr", user),
        )
    )


def downgrade() -> None:
    # Use same env vars for symmetry
    user = _ident(os.getenv("REPLICATOR_USER", "replicator"), "REPLICATOR_USER")
    pubname = _ident(
        os.getenv("PUBLICATION_NAME", "events_publication"), "PUBLICATION_NAME"
    )

    # Drop publication if exists
    op.execute(
        sa.text(
            """
DO $plpgsql$
DECLARE
    v_pub text := :pub;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = v_pub) THEN
        EXECUTE format('DROP PUBLICATION %I', v_pub);
    END IF;
END
$plpgsql$;
"""
        ).bindparams(sa.bindparam("pub", pubname))
    )

    # Revoke default privileges (best-effort)
    op.execute(
        sa.text(
            """
DO $plpgsql$
DECLARE
    v_user   text := :usr;
BEGIN
    -- These will no-op if not set
    EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM %I', v_user);
    EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE USAGE, SELECT ON SEQUENCES FROM %I', v_user);
END
$plpgsql$;
"""
        ).bindparams(sa.bindparam("usr", user))
    )

    # Revoke current-object grants (best-effort)
    op.execute(
        sa.text(
            """
DO $plpgsql$
DECLARE
    v_user   text := :usr;
BEGIN
    EXECUTE format('REVOKE USAGE ON SCHEMA public FROM %I', v_user);
    EXECUTE format('REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM %I', v_user);
    EXECUTE format('REVOKE USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public FROM %I', v_user);
END
$plpgsql$;
"""
        ).bindparams(sa.bindparam("usr", user))
    )

    # Drop role if exists
    op.execute(
        sa.text(
            """
DO $plpgsql$
DECLARE
    v_user text := :usr;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_user) THEN
        EXECUTE format('DROP ROLE %I', v_user);
    END IF;
END
$plpgsql$;
"""
        ).bindparams(sa.bindparam("usr", user))
    )
