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

-- Publication required for online mode. Library does not auto-create it.
DROP PUBLICATION IF EXISTS pg_migrator_pub;
CREATE PUBLICATION pg_migrator_pub FOR ALL TABLES;
