# Ducklake Connector - Smoke Test Guide

**Status**: ✅ Catalog unit tests passing. Trino packaging/integration smoke is intentionally deferred.

## Current Test Coverage

### Automated Unit Tests (PASSING)
Run with: `mvn test`

**Results**: catalog tests passing
- ✅ Snapshot reading
- ✅ Schema discovery (`test_schema`)
- ✅ Table discovery (`simple_table`, `array_table`)
- ✅ Table metadata retrieval
- ✅ Parquet file discovery & metadata

The unit tests automatically generate a test catalog using DuckDB's Ducklake extension, then verify the connector can read it correctly.

## Quick Smoke Test

```bash
cd plugin/trino-ducklake

# Run all tests (auto-generates catalog if needed)
mvn test -Dtest=TestDucklakeCatalog

# Expected output:
# Tests run: N, Failures: 0, Errors: 0
# BUILD SUCCESS
```

## Test Data Generated

The tests create:
- **Database**: `target/test-catalog/catalog.db` (SQLite)
- **Schema**: `test_schema`
- **Tables**:
  - `simple_table`: INTEGER, VARCHAR, DOUBLE, BOOLEAN, DATE (5 rows)
  - `array_table`: INTEGER, VARCHAR, VARCHAR[], INTEGER (5 rows)
- **Parquet Files**: 2 files in `target/test-catalog/data/`

## Prerequisites

```bash
# Ensure Java 25 is active
java -version  # Should show 25.x.x

# Build Trino if needed (first time only)
cd /path/to/trino
mvn install -DskipTests -T1C
```

## Step 3: Run Unit Tests

```bash
# Run all tests
mvn test

# Run specific test
mvn test -Dtest=TestDucklakeCatalog
```

Expected: All tests pass

### Smoke Test Gauntlet (Unit Test Level)

The `TestDucklakeCatalog` class tests:

1. ✓ **getCurrentSnapshot** - Can read snapshot metadata
2. ✓ **listSchemas** - Can list schemas (expects "test_schema")
3. ✓ **listTables** - Can list tables in schema
4. ✓ **getTable** - Can retrieve table metadata
5. ✓ **getDataFiles** - Can discover Parquet data files
6. ✓ **getTableColumnsResolvesListType** - Reconstructs `list<...>` array type from nested metadata
7. ✓ **getDataFileIdsForPredicate** - Applies typed predicate pruning semantics

The `TestDucklakeSplitManager` class tests:

8. ✓ **getSplitsWithoutPredicate** - Returns the full split set when no constraints are pushed
9. ✓ **getSplitsPrunesByNumericStats** - Prunes splits using numeric file stats bounds
10. ✓ **getSplitsPrunesByDateStats** - Prunes splits using DATE bounds from tuple-domain constraints

## Step 4 (Deferred): Package the Plugin

```bash
mvn clean package -DskipTests
```

Expected: `target/trino-ducklake-480-SNAPSHOT.zip` created

## Step 5 (Deferred): Integration Test with Trino Server

### Setup Trino Server

```bash
# Download Trino server (or use existing installation)
# Extract the plugin
unzip target/trino-ducklake-480-SNAPSHOT.zip -d /path/to/trino-server/plugin/ducklake

# Configure catalog
cat > /path/to/trino-server/etc/catalog/ducklake.properties <<EOF
connector.name=ducklake
ducklake.catalog.database-url=jdbc:sqlite:/absolute/path/to/target/test-catalog/catalog.db
ducklake.data-path=/absolute/path/to/target/test-catalog
ducklake.catalog.max-connections=10
EOF

# Start Trino
/path/to/trino-server/bin/launcher start
```

### Run Smoke Test Queries

```bash
# Connect with Trino CLI
trino --catalog ducklake --schema test_schema
```

#### Test 1: SHOW SCHEMAS ✓

```sql
SHOW SCHEMAS;
```

Expected:
```
    Schema
--------------
 information_schema
 test_schema
(2 rows)
```

#### Test 2: SHOW TABLES ✓

```sql
SHOW TABLES FROM test_schema;
```

Expected:
```
  Table
-----------
 array_table
 simple_table
(2 rows)
```

#### Test 3: DESCRIBE table ✓

```sql
DESCRIBE test_schema.array_table;
```

Expected:
```
   Column      |      Type       | Extra | Comment
---------------+-----------------+-------+---------
 id            | integer         |       |
 product_name  | varchar         |       |
 tags          | array(varchar)  |       |
 quantity      | integer         |       |
(4 rows)
```

#### Test 4: SELECT count(*) ✓

```sql
SELECT count(*) FROM test_schema.simple_table;
```

Expected:
```
 _col0
-------
     5
(1 row)
```

#### Test 5: SELECT one_column LIMIT 10 ✓

```sql
SELECT product_name FROM test_schema.array_table ORDER BY id;
```

Expected:
```
 product_name
--------------
 Widget
 Gizmo
 Doohickey
 Thingamajig
 Whatchamacallit
(5 rows)
```

#### Test 6: SELECT with WHERE ✓

```sql
SELECT name, price
FROM test_schema.simple_table
WHERE price > 30.0
ORDER BY id;
```

Expected:
```
   name      | price
-------------+-------
 Product C   | 39.99
 Product D   | 49.99
 Product E   | 59.99
(3 rows)
```

#### Test 7: Compare with DuckDB ✓

Run the same queries in DuckDB and verify results match:

```bash
duckdb target/test-catalog/catalog.db
```

```sql
-- In DuckDB
SELECT name, price
FROM test_schema.simple_table
WHERE price > 30.0
ORDER BY id;
```

Compare row counts, values, and column types.

## Success Criteria

✓ All unit tests pass
✓ Plugin compiles without errors
✓ Catalog behavior matches DuckDB-generated metadata and files

Optional later milestones:
- Plugin packages successfully
- Trino server starts with Ducklake connector
- End-to-end SQL smoke queries return expected results

## Troubleshooting

### "Test catalog not found"

Run the catalog generator:
```bash
mvn exec:java -Dexec.mainClass="io.trino.plugin.ducklake.DucklakeCatalogGenerator"
```

### "Could not resolve dependencies"

Ensure Trino is fully built:
```bash
cd ../..
mvn clean install -DskipTests=false -Dmaven.test.skip.exec=true -T1C
```

### "DuckDB Ducklake extension not found"

Check DuckDB version supports Ducklake:
```bash
duckdb --version  # Should be 1.0+
```

Install Ducklake extension in DuckDB:
```sql
INSTALL ducklake;
LOAD ducklake;
```

### Connector fails to load

Check logs in `var/log/server.log` for stack traces.

Common issues:
- Wrong JDBC URL (must be absolute path)
- SQLite database doesn't exist
- Permission issues reading Parquet files

## Next Steps After Smoke Test Passes

1. Implement delete file handling (Phase 3)
2. Implement SQL predicate pushdown (Phase 4)
3. Add more comprehensive catalog test data
4. Run Trino integration smoke once read-path milestones are complete
5. Performance benchmarks

## Quick Validation Checklist

- [ ] Build completed successfully
- [ ] Test catalog generated with DuckDB
- [ ] Unit tests all pass
- [ ] Catalog predicates/columns behavior validated in unit tests

Deferred checklist:
- [ ] Plugin packages to ZIP
- [ ] Trino server loads connector
- [ ] SHOW SCHEMAS/SHOW TABLES/DESCRIBE/SELECT queries pass
- [ ] Results match DuckDB

Once all boxes checked: **Connector read path is working.**
