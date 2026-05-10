# pg_dbmigrator Benchmark Results

## Test Environment

| Component | Details |
|-----------|---------|
| **Source** | Azure Database for PostgreSQL Flexible Server (PG 16) |
| **Target** | Azure Database for PostgreSQL Flexible Server (PG 18) |
| **pg_dump / pg_restore** | PostgreSQL 18.3 |
| **Parallel Jobs** | 8 |
| **Dump Compression** | zstd level 3 |

## Test Schema

Data is distributed across **8 tables** (`benchmark_data_01` .. `benchmark_data_08`) to leverage `pg_dump -j` / `pg_restore -j` parallelism — PostgreSQL cannot parallelize dump/restore within a single table.

```sql
CREATE UNLOGGED TABLE benchmark_data_XX (
    id BIGSERIAL,
    payload TEXT NOT NULL,          -- ~992 bytes (repeat(md5(random()::text), 31))
    created_at TIMESTAMP NOT NULL,
    val DOUBLE PRECISION NOT NULL
);

-- After all COPY data loading completes:
ALTER TABLE benchmark_data_XX SET LOGGED;

-- Then create indexes:
ALTER TABLE benchmark_data_XX ADD PRIMARY KEY (id);
CREATE INDEX idx_benchmark_data_XX_created_at ON benchmark_data_XX (created_at);
CREATE INDEX idx_benchmark_data_XX_val ON benchmark_data_XX (val);
```

Each row is approximately 1 KB. Rows are distributed evenly across all 8 tables. Data is seeded incrementally — after the first size, only the delta is added for subsequent sizes.

### Seed Optimizations

- **UNLOGGED tables**: Tables are created as `UNLOGGED` during bulk COPY to eliminate WAL overhead, then converted to `LOGGED` after all data is loaded.
- **COPY protocol**: Uses PostgreSQL COPY protocol instead of INSERT for minimal server-side CPU and WAL overhead.
- **Deferred indexes**: Primary key and secondary indexes are created only after all data loading and `SET LOGGED` are complete, avoiding index maintenance during bulk insert.
- **Parallel seeding**: All 8 tables are seeded concurrently using separate connections.
- **Index drop before incremental seed**: For incremental seeding (adding rows to existing tables), indexes are dropped before COPY and recreated afterward to avoid index maintenance overhead.

## Migration Mode

- **Online**: Snapshot via `pg_dump`/`pg_restore` with logical replication running in parallel. After restore completes, the subscription catches up remaining WAL, then cutover. Near-zero downtime.

## Results

| Target Size | Actual DB Size | Rows | Tables | Online (min) |
|-------------|---------------|------|--------|--------------|
| 10 GB | 11 GB | 10,000,000 | 8 | 4.0 |
| 50 GB | 57 GB | 50,000,000 | 8 | 25.3 |
| 100 GB | 114 GB | 100,000,000 | 8 | 49.1 |
| 200 GB | 228 GB | 200,000,000 | 8 | 105.2 |
| 300 GB | 342 GB | 300,000,000 | 8 | 161.1 |

## Throughput Analysis

| Size | Online Throughput |
|------|-------------------|
| 10 GB (11 GB actual) | 2.83 GB/min |
| 50 GB (57 GB actual) | 2.25 GB/min |
| 100 GB (114 GB actual) | 2.32 GB/min |
| 200 GB (228 GB actual) | 2.17 GB/min |
| 300 GB (342 GB actual) | 2.12 GB/min |

**Average online throughput (multi-table): ~2.3 GB/min (~140 GB/hr)**

## Observations

1. **Linear scaling**: Migration time scales approximately linearly with data size. Throughput remains consistent at ~2.1–2.8 GB/min across all tested volumes.

2. **Multi-table parallelism**: The 8-table layout enables effective `pg_dump -j 8` / `pg_restore -j 8` parallelism, achieving ~2.3 GB/min average throughput.

3. **Bottleneck is index creation**: At 200–300 GB, the PostData phase (rebuilding primary key + 2 secondary indexes per table) accounts for a significant portion of total migration time.

4. **Fast replication catchup**: The logical replication catchup phase is remarkably fast. WAL lag accumulated during the dump/restore phase is consumed rapidly, demonstrating effective batch replay.

5. **Cross-version compatibility**: PG 16 → PG 18 migration worked without issues using `--allow-restore-errors` flag, which handles minor incompatibilities in system catalog objects.

## Estimated Migration Times

Based on the benchmark results, estimated migration times for production workloads (online mode, multi-table):

| Database Size | Estimated Online Time |
|---------------|--------------------------------------|
| 500 GB | ~3.8 hours |
| 1 TB | ~7.5 hours |
| 2 TB | ~15 hours |

*Note: Actual times depend on table/index complexity, concurrent write load (online mode), network bandwidth, and server compute resources.*
