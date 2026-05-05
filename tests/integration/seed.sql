-- Test fixture: schema + initial 500 rows on the source.
--
-- Columns are TEXT-typed because pg_dump/pg_restore + native logical
-- replication apply both handle text losslessly without coercion concerns;
-- the end-to-end flow is exercised regardless. Binary type fidelity is out
-- of scope for this integration suite.
DROP SCHEMA IF EXISTS app CASCADE;
CREATE SCHEMA app;
CREATE TABLE app.widgets (
    id   text PRIMARY KEY,
    name text NOT NULL,
    qty  text NOT NULL
);
INSERT INTO app.widgets (id, name, qty)
SELECT g::text, 'widget-' || g, (g * 2)::text
FROM generate_series(1, 500) g;

-- Second table with a real sequence-driven PK. Used by the
-- run_online_sequence_sync.sh integration test to verify that the
-- migrator advances the target's sequences at cutover (PostgreSQL
-- logical replication does NOT replay nextval(), so without the sync
-- the first post-cutover INSERT collides with a replicated row).
CREATE TABLE app.events (
    id   bigserial PRIMARY KEY,
    note text NOT NULL
);
INSERT INTO app.events (note)
SELECT 'event-' || g
FROM generate_series(1, 100) g;

-- Publication required for online mode. Library does not auto-create it.
DROP PUBLICATION IF EXISTS pg_migrator_pub;
CREATE PUBLICATION pg_migrator_pub FOR ALL TABLES;
