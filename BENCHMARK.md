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

```sql
CREATE TABLE benchmark_data (
    id BIGSERIAL PRIMARY KEY,
    payload TEXT NOT NULL,          -- ~992 bytes (repeat(md5(random()::text), 31))
    created_at TIMESTAMP DEFAULT now(),
    val DOUBLE PRECISION DEFAULT random()
);

CREATE INDEX idx_benchmark_created_at ON benchmark_data (created_at);
CREATE INDEX idx_benchmark_val ON benchmark_data (val);
```

Each row is approximately 1 KB. Data was generated in batches of 1,000,000 rows.

## Migration Modes

## Results

| Target Size | Actual DB Size | Rows | Offline (s) | Offline (min) | Online to CaughtUp (s) | Online (min) |
|-------------|---------------|------|-------------|---------------|------------------------|--------------|
| 10 GB | 12 GB | 10,000,000 | 322 | 5.4 | 252 | 4.2 |
| 50 GB | 58 GB | 50,000,000 | 1,597 | 26.6 | 1,486 | 24.8 |
| 100 GB | 115 GB | 100,000,000 | 3,083 | 51.4 | 3,182 | 53.0 |
| 200 GB | 231 GB | 200,000,000 | 6,308 | 105.1 | 7,260 | 121.0 |

## Throughput Analysis

| Size | Offline Throughput | Online Throughput |
|------|-------------------|-------------------|
| 10 GB (12 GB actual) | 2.23 GB/min | 2.86 GB/min |
| 50 GB (58 GB actual) | 2.18 GB/min | 2.34 GB/min |
| 100 GB (115 GB actual) | 2.24 GB/min | 2.17 GB/min |
| 200 GB (231 GB actual) | 2.20 GB/min | 1.91 GB/min |

**Average offline throughput: ~2.2 GB/min (~132 GB/hr)**

## Observations

1. **Linear scaling**: Migration time scales approximately linearly with data size. The offline throughput remains consistent at ~2.2 GB/min regardless of data volume.

2. **Online mode overhead**: For static data (no concurrent writes during migration), online mode adds ~15% overhead compared to offline mode at 200GB. This is due to the PostData phase taking longer while the subscription is being set up, plus the brief StreamApply catchup phase.

3. **Bottleneck is index creation**: At 200GB, the PostData phase (rebuilding primary key + 2 secondary indexes) accounts for ~50% of total time. For databases with many indexes, this ratio would be higher.

4. **Fast replication catchup**: The logical replication catchup phase is remarkably fast. For the 200GB test with no concurrent writes, it took only ~2 minutes to consume ~80GB of WAL lag — demonstrating effective batch replay.

5. **Cross-version compatibility**: PG 16 → PG 18 migration worked without issues using `--allow-restore-errors` flag, which handles minor incompatibilities in system catalog objects.

## Estimated Migration Times

Based on the benchmark results, estimated migration times for production workloads:

| Database Size | Estimated Offline Time | Estimated Online Time (to CaughtUp) |
|---------------|----------------------|--------------------------------------|
| 500 GB | ~4 hours | ~4.5 hours |
| 1 TB | ~8 hours | ~9 hours |
| 2 TB | ~16 hours | ~18 hours |

*Note: Actual times depend on table/index complexity, concurrent write load (online mode), network bandwidth, and server compute resources.*