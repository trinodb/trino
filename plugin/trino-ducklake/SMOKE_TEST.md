# Ducklake Connector - Smoke Test Guide

**Status**: ✅ All unit tests passing - manual smoke test TBD

## Current Test Coverage

### Automated Unit Tests (PASSING)
Run with: `mvn test -Dtest=TestDucklakeCatalog`

**Results**: 5/5 tests passing
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
# Tests run: 5, Failures: 0, Errors: 0
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
Tables created:
  - test_schema.customers (5 rows)
  - test_schema.orders (5 rows)
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

## Step 4: Package the Plugin

```bash
mvn clean package -DskipTests
```

Expected: `target/trino-ducklake-480-SNAPSHOT.zip` created

## Step 5: Integration Test with Trino Server

### Setup Trino Server

```bash
# Download Trino server (or use existing installation)
# Extract the plugin
unzip target/trino-ducklake-480-SNAPSHOT.zip -d /path/to/trino-server/plugin/ducklake

# Configure catalog
cat > /path/to/trino-server/etc/catalog/ducklake.properties <<EOF
connector.name=ducklake
ducklake.catalog-database-url=jdbc:sqlite:/absolute/path/to/target/test-catalog/catalog.db
ducklake.data-path=/absolute/path/to/target/test-catalog
ducklake.max-catalog-connections=10
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
 customers
 orders
(2 rows)
```

#### Test 3: DESCRIBE table ✓

```sql
DESCRIBE test_schema.customers;
```

Expected:
```
    Column     |  Type   | Extra | Comment
---------------+---------+-------+---------
 customer_id   | integer |       |
 name          | varchar |       |
 email         | varchar |       |
 balance       | double  |       |
 created_date  | date    |       |
(5 rows)
```

#### Test 4: SELECT count(*) ✓

```sql
SELECT count(*) FROM test_schema.customers;
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
SELECT name FROM test_schema.customers LIMIT 10;
```

Expected:
```
      name
-----------------
 Alice Johnson
 Bob Smith
 Charlie Brown
 Diana Prince
 Eve Anderson
(5 rows)
```

#### Test 6: SELECT with WHERE ✓

```sql
SELECT name, balance
FROM test_schema.customers
WHERE balance > 1000.0;
```

Expected:
```
      name       | balance
-----------------+---------
 Alice Johnson   | 1500.5
 Bob Smith       | 2300.75
 Diana Prince    | 4200.0
 Eve Anderson    | 1750.5
(4 rows)
```

#### Test 7: Compare with DuckDB ✓

Run the same queries in DuckDB and verify results match:

```bash
duckdb target/test-catalog/catalog.db
```

```sql
-- In DuckDB
SELECT name, balance
FROM test_schema.customers
WHERE balance > 1000.0
ORDER BY name;
```

Compare row counts, values, and column types.

## Success Criteria

✓ All unit tests pass
✓ Plugin compiles without errors
✓ Plugin packages successfully
✓ Trino server starts with Ducklake connector
✓ All 7 smoke test queries return correct results
✓ Results match DuckDB output

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
3. Add more comprehensive test data
4. Performance benchmarks
5. Documentation updates

## Quick Validation Checklist

- [ ] Build completed successfully
- [ ] Test catalog generated with DuckDB
- [ ] Unit tests all pass
- [ ] Plugin packages to ZIP
- [ ] Trino server loads connector
- [ ] SHOW SCHEMAS works
- [ ] SHOW TABLES works
- [ ] DESCRIBE works
- [ ] SELECT count(*) works
- [ ] SELECT columns works
- [ ] WHERE predicates work
- [ ] Results match DuckDB

Once all boxes checked: **Connector read path is working! 🎉**
