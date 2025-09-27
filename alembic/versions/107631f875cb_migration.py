"""Migration

Revision ID: 107631f875cb
Revises: 7dd8d518a253
Create Date: 2025-08-27 14:43:26.326572
"""

from typing import Sequence, Union
import os
import re
from alembic import op

# Alembic identifiers
revision: str = "107631f875cb"
down_revision: Union[str, None] = "7dd8d518a253"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# identifier guard (loose; server-side we still use format('%I'))
_IDENT_RX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ident(x: str, env: str) -> str:
    if not _IDENT_RX.match(x):
        raise ValueError(f"{env} contains invalid identifier: {x!r}")
    return x


def _ql(s: str) -> str:
    # quote literal for embedding into DO block
    return s.replace("'", "''")


def upgrade() -> None:
    pubname = _ident(
        os.getenv("PUBLICATION_NAME", "events_publication"), "PUBLICATION_NAME"
    )

    # Create publication if missing; add <schema>.<table> if it exists
    op.execute(
        f"""
  DO $plpgsql$
  DECLARE
    v_pub text := '{_ql(pubname)}';
    v_sch text := 'public';
    v_tab text := 'events';
    v_reg regclass;
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = v_pub) THEN
      EXECUTE format('CREATE PUBLICATION %I', v_pub);
    END IF;

    v_reg := to_regclass(format('%I.%I', v_sch, v_tab));
    IF v_reg IS NOT NULL THEN
      PERFORM 1
      FROM pg_publication_tables
      WHERE pubname = v_pub AND schemaname = v_sch AND tablename = v_tab;

      IF NOT FOUND THEN
        EXECUTE format('ALTER PUBLICATION %I ADD TABLE %I.%I', v_pub, v_sch, v_tab);
      END IF;
    END IF;
  END
  $plpgsql$;
  """
    )


def downgrade() -> None:
    pubname = _ident(
        os.getenv("PUBLICATION_NAME", "events_publication"), "PUBLICATION_NAME"
    )

    # Drop the publication if it exists
    op.execute(
        f"""
  DO $plpgsql$
  DECLARE
    v_pub text := '{_ql(pubname)}';
  BEGIN
    IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = v_pub) THEN
      EXECUTE format('DROP PUBLICATION %I', v_pub);
    END IF;
  END
  $plpgsql$;
  """
    )
