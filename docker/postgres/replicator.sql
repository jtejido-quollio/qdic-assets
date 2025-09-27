-- Create/alter replicator role for local dev
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') THEN
    CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator_password';
  ELSE
    ALTER ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator_password';
  END IF;
END
$$;

-- Minimal grants (dev)
GRANT CONNECT ON DATABASE assets TO replicator;
GRANT USAGE ON SCHEMA public TO replicator;

-- Optional (not strictly needed for logical replication):
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;

-- Publication for Debezium (idempotent)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'events_publication') THEN
    CREATE PUBLICATION events_publication;
  END IF;

  -- Add table if it exists
  IF to_regclass('public.events') IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM pg_publication_tables
    WHERE pubname = 'events_publication' AND schemaname = 'public' AND tablename = 'events'
  ) THEN
    ALTER PUBLICATION events_publication ADD TABLE public.events;
  END IF;
END
$$;
