"""Migration

Revision ID: 107631f875cb
Revises: 7dd8d518a253
Create Date: 2025-08-27 14:43:26.326572
"""

from typing import Sequence, Union
import os
import re
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "107631f875cb"
down_revision: Union[str, None] = "7dd8d518a253"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Strict identifier validator: letters/underscore, then letters/digits/underscore
_IDENT_RX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ident(name: str, envvar: str) -> str:
    """Validate Postgres identifier-ish token. We still pass it to format('%I') server-side."""
    if not _IDENT_RX.match(name):
        raise ValueError(f"{envvar} contains invalid identifier: {name!r}")
    return name


def _ql(s: str) -> str:
    """Quote-literal helper for embedding Python strings into DO blocks."""
    return s.replace("'", "''")


def upgrade() -> None:
    # --- Read env with defaults ---
    user = _ident(os.getenv("REPLICATOR_USER", "replicator"), "REPLICATOR_USER")
    password = os.getenv("REPLICATOR_PASSWORD", "replicator_password")
    pubname = _ident(
        os.getenv("PUBLICATION_NAME", "events_publication"), "PUBLICATION_NAME"
    )
    events_schema = _ident(os.getenv("EVENTS_SCHEMA", "public"), "EVENTS_SCHEMA")
    events_table = _ident(os.getenv("EVENTS_TABLE", "events"), "EVENTS_TABLE")

    # 1) Create/alter role (no bind params in DO; assign literals to vars and use EXECUTE format)
    op.execute(
        f"""
    DO $plpgsql$
    DECLARE
        v_user text := '{_ql(user)}';
        v_pwd  text := '{_ql(password)}';
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_user) THEN
            EXECUTE format('CREATE ROLE %I WITH REPLICATION LOGIN PASSWORD %L', v_user, v_pwd);
        ELSE
            EXECUTE format('ALTER ROLE %I WITH REPLICATION LOGIN PASSWORD %L', v_user, v_pwd);
        END IF;
    END
    $plpgsql$;
    """
    )

    # 2) Grants on schema/tables/sequences (current objects)
    op.execute(
        f"""
    DO $plpgsql$
    DECLARE
        v_user text := '{_ql(user)}';
    BEGIN
        EXECUTE format('GRANT USAGE ON SCHEMA public TO %I', v_user);
        EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA public TO %I', v_user);
        EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %I', v_user);
    END
    $plpgsql$;
    """
    )

    # 3) Default privileges for future objects
    op.execute(
        f"""
    DO $plpgsql$
    DECLARE
        v_user text := '{_ql(user)}';
    BEGIN
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %I', v_user);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO %I', v_user);
    END
    $plpgsql$;
    """
    )

    # 4) Publication (idempotent) and add events table if present
    op.execute(
        f"""
    DO $plpgsql$
    DECLARE
        v_pub  text := '{_ql(pubname)}';
        v_user text := '{_ql(user)}';
        v_sch  text := '{_ql(events_schema)}';
        v_tab  text := '{_ql(events_table)}';
        v_reg  regclass;
    BEGIN
        -- Create the publication if missing
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = v_pub) THEN
            EXECUTE format('CREATE PUBLICATION %I', v_pub);
        END IF;

        -- Ensure the publication is owned by the replicator user
        PERFORM 1
        FROM pg_catalog.pg_publication p
        JOIN pg_catalog.pg_roles r ON r.oid = p.pubowner
        WHERE p.pubname = v_pub AND r.rolname = v_user;
        IF NOT FOUND THEN
            EXECUTE format('ALTER PUBLICATION %I OWNER TO %I', v_pub, v_user);
        END IF;

        -- Add <schema>.<table> if it exists and is not already included
        v_reg := to_regclass(format('%I.%I', v_sch, v_tab));
        IF v_reg IS NOT NULL THEN
            PERFORM 1
            FROM pg_catalog.pg_publication_tables
            WHERE pubname = v_pub
              AND schemaname = v_sch
              AND tablename = v_tab;
            IF NOT FOUND THEN
                EXECUTE format('ALTER PUBLICATION %I ADD TABLE %I.%I', v_pub, v_sch, v_tab);
            END IF;
        END IF;
    END
    $plpgsql$;
    """
    )


def downgrade() -> None:
    user = _ident(os.getenv("REPLICATOR_USER", "replicator"), "REPLICATOR_USER")
    pubname = _ident(
        os.getenv("PUBLICATION_NAME", "events_publication"), "PUBLICATION_NAME"
    )

    # Drop publication if exists
    op.execute(
        f"""
    DO $plpgsql$
    DECLARE
        v_pub text := '{_ql(pubname)}';
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = v_pub) THEN
            EXECUTE format('DROP PUBLICATION %I', v_pub);
        END IF;
    END
    $plpgsql$;
    """
    )

    # Revoke default privileges (best-effort)
    op.execute(
        f"""
    DO $plpgsql$
    DECLARE
        v_user text := '{_ql(user)}';
    BEGIN
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM %I', v_user);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE USAGE, SELECT ON SEQUENCES FROM %I', v_user);
    END
    $plpgsql$;
    """
    )

    # Revoke current-object grants (best-effort)
    op.execute(
        f"""
    DO $plpgsql$
    DECLARE
        v_user text := '{_ql(user)}';
    BEGIN
        EXECUTE format('REVOKE USAGE ON SCHEMA public FROM %I', v_user);
        EXECUTE format('REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM %I', v_user);
        EXECUTE format('REVOKE USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public FROM %I', v_user);
    END
    $plpgsql$;
    """
    )

    # Drop role if exists
    op.execute(
        f"""
    DO $plpgsql$
    DECLARE
        v_user text := '{_ql(user)}';
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_user) THEN
            EXECUTE format('DROP ROLE %I', v_user);
        END IF;
    END
    $plpgsql$;
    """
    )
