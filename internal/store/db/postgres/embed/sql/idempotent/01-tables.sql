--
-- Copyright (C) 2024 IOTech Ltd
--
-- SPDX-License-Identifier: Apache-2.0

-- "<serviceKey>".store is used to save StoredObject as JSONB on failure
-- The table is used to store the data that failed to be processed by the application service
-- note that "<serviceKey>" is a placeholder to be replaced with the actual service key in the runtime
CREATE TABLE IF NOT EXISTS "<serviceKey>".store (
    id UUID PRIMARY KEY,
    created timestamp NOT NULL DEFAULT now(),
    content JSONB NOT NULL
);

ALTER TABLE "<serviceKey>".store ADD COLUMN IF NOT EXISTS retention_key text;
ALTER TABLE "<serviceKey>".store DROP COLUMN IF EXISTS stored_bytes;
CREATE INDEX IF NOT EXISTS store_created_id ON "<serviceKey>".store (created, id);
CREATE INDEX IF NOT EXISTS store_retention_created_id ON "<serviceKey>".store (retention_key, created, id);

-- Reclaim rotating queue tuples in the background. Avoid truncation's exclusive
-- table lock; reuse pages in place. PostgreSQL must have autovacuum enabled.
ALTER TABLE "<serviceKey>".store SET (
    autovacuum_enabled = true,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_cost_limit = 200,
    vacuum_truncate = false,
    toast.autovacuum_vacuum_threshold = 1000,
    toast.autovacuum_vacuum_scale_factor = 0.01,
    toast.vacuum_truncate = false
);
