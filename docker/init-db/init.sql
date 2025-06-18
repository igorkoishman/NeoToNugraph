-- Create tables only if not exists
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    labels TEXT[],
    properties JSONB
);

CREATE TABLE IF NOT EXISTS relationships (
    id TEXT PRIMARY KEY,
    label TEXT,
    start_id TEXT,
    end_id TEXT,
    start_properties JSONB,
    end_properties JSONB
);

-- Create indexes if not exists (PostgreSQL >= 9.5 supports CREATE INDEX CONCURRENTLY IF NOT EXISTS from 13.0, but to be safe we do conditional check)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_nodes_id' AND n.nspname = 'public'
  ) THEN
    CREATE UNIQUE INDEX idx_nodes_id ON nodes(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_nodes_labels' AND n.nspname = 'public'
  ) THEN
    CREATE INDEX idx_nodes_labels ON nodes USING GIN(labels);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_nodes_properties' AND n.nspname = 'public'
  ) THEN
    CREATE INDEX idx_nodes_properties ON nodes USING GIN(properties);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_relationships_id' AND n.nspname = 'public'
  ) THEN
    CREATE UNIQUE INDEX idx_relationships_id ON relationships(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_relationships_start_id' AND n.nspname = 'public'
  ) THEN
    CREATE INDEX idx_relationships_start_id ON relationships(start_id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_relationships_end_id' AND n.nspname = 'public'
  ) THEN
    CREATE INDEX idx_relationships_end_id ON relationships(end_id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_relationships_label' AND n.nspname = 'public'
  ) THEN
    CREATE INDEX idx_relationships_label ON relationships(label);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_rel_start_guid' AND n.nspname = 'public'
  ) THEN
    CREATE INDEX idx_rel_start_guid ON relationships ((start_properties->>'guid'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_rel_end_guid' AND n.nspname = 'public'
  ) THEN
    CREATE INDEX idx_rel_end_guid ON relationships ((end_properties->>'guid'));
  END IF;
END$$;