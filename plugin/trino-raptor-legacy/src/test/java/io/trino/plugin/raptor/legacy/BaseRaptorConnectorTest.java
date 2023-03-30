/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.raptor.legacy;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import io.trino.Session;
import io.trino.spi.type.ArrayType;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.testng.services.Flaky;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.trino.plugin.raptor.legacy.RaptorColumnHandle.SHARD_UUID_COLUMN_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

public abstract class BaseRaptorConnectorTest
        extends BaseConnectorTest
{
    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT:
                return false;

            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
            case SUPPORTS_SET_COLUMN_TYPE:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_CREATE_VIEW:
                return true;

            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;

            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
                return true;

            case SUPPORTS_ROW_TYPE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Raptor connector does not support column default values");
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageContaining("Table was updated by a different transaction. Please retry the operation.");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessage("Unsupported type: char(3)");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchema()
    {
        // Raptor allows renaming to a schema it doesn't exist https://github.com/trinodb/trino/issues/11110
        String tableName = "test_rename_old_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String schemaName = "test_schema_" + randomNameSuffix();

        String renamedTable = "test_rename_new_" + randomNameSuffix();
        assertUpdate("ALTER TABLE " + tableName + " RENAME TO " + schemaName + "." + renamedTable);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertQuery("SELECT x FROM " + schemaName + "." + renamedTable, "VALUES 123");

        assertUpdate("DROP TABLE " + schemaName + "." + renamedTable);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertFalse(getQueryRunner().tableExists(Session.builder(getSession()).setSchema(schemaName).build(), renamedTable));
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("tinyint")
                || typeName.equals("real")
                || typeName.startsWith("decimal(")
                || typeName.equals("time")
                || typeName.equals("time(6)")
                || typeName.equals("timestamp(6)")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("timestamp(6) with time zone")
                || typeName.startsWith("char(")) {
            //TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("char(1)")) {
            //TODO this should either work or fail cleanly
            return Optional.empty();
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Test
    public void testCreateArrayTable()
    {
        assertUpdate("CREATE TABLE array_test AS SELECT ARRAY [1, 2, 3] AS c", 1);
        assertQuery("SELECT cardinality(c) FROM array_test", "SELECT 3");
        assertUpdate("DROP TABLE array_test");
    }

    @Test
    public void testMapTable()
    {
        assertUpdate("CREATE TABLE map_test AS SELECT MAP(ARRAY [1, 2, 3], ARRAY ['hi', 'bye', NULL]) AS c", 1);
        assertQuery("SELECT c[1] FROM map_test", "SELECT 'hi'");
        assertQuery("SELECT c[3] FROM map_test", "SELECT NULL");
        assertUpdate("DROP TABLE map_test");
    }

    @Test
    @Override
    public void testCreateViewSchemaNotFound()
    {
        // TODO (https://github.com/trinodb/trino/issues/11110) Raptor connector can create new views in a schema where it doesn't exist
        assertThatThrownBy(super::testCreateViewSchemaNotFound)
                .hasMessageContaining("Expected query to fail: CREATE VIEW test_schema_");
    }

    @Test
    public void testCreateTableViewAlreadyExists()
    {
        assertUpdate("CREATE VIEW view_already_exists AS SELECT 1 a");
        assertQueryFails("CREATE TABLE view_already_exists(a integer)", "View already exists: tpch.view_already_exists");
        assertQueryFails("CREATE TABLE View_Already_Exists(a integer)", "View already exists: tpch.view_already_exists");
        assertQueryFails("CREATE TABLE view_already_exists AS SELECT 1 a", "View already exists: tpch.view_already_exists");
        assertQueryFails("CREATE TABLE View_Already_Exists AS SELECT 1 a", "View already exists: tpch.view_already_exists");
        assertUpdate("DROP VIEW view_already_exists");
    }

    @Test
    public void testCreateViewTableAlreadyExists()
    {
        assertUpdate("CREATE TABLE table_already_exists (id integer)");
        assertQueryFails("CREATE VIEW table_already_exists AS SELECT 1 a", ".*Table already exists: 'raptor.tpch.table_already_exists'");
        assertQueryFails("CREATE VIEW Table_Already_Exists AS SELECT 1 a", ".*Table already exists: 'raptor.tpch.table_already_exists'");
        assertQueryFails("CREATE OR REPLACE VIEW table_already_exists AS SELECT 1 a", ".*Table already exists: 'raptor.tpch.table_already_exists'");
        assertQueryFails("CREATE OR REPLACE VIEW Table_Already_Exists AS SELECT 1 a", ".*Table already exists: 'raptor.tpch.table_already_exists'");
        assertUpdate("DROP TABLE table_already_exists");
    }

    @Test
    public void testInsertSelectDecimal()
    {
        assertUpdate("CREATE TABLE test_decimal(short_decimal DECIMAL(5,2), long_decimal DECIMAL(25,20))");
        assertUpdate("INSERT INTO test_decimal VALUES(DECIMAL '123.45', DECIMAL '12345.12345678901234567890')", "VALUES(1)");
        assertUpdate("INSERT INTO test_decimal VALUES(NULL, NULL)", "VALUES(1)");
        assertQuery("SELECT * FROM test_decimal", "VALUES (123.45, 12345.12345678901234567890), (NULL, NULL)");
        assertUpdate("DROP TABLE test_decimal");
    }

    @Test
    public void testShardUuidHiddenColumn()
    {
        assertUpdate("CREATE TABLE test_shard_uuid AS SELECT orderdate, orderkey FROM orders", "SELECT count(*) FROM orders");

        MaterializedResult actualResults = computeActual("SELECT *, \"$shard_uuid\" FROM test_shard_uuid");
        assertEquals(actualResults.getTypes(), ImmutableList.of(DATE, BIGINT, SHARD_UUID_COLUMN_TYPE));
        UUID arbitraryUuid = null;
        for (MaterializedRow row : actualResults.getMaterializedRows()) {
            Object uuid = row.getField(2);
            assertInstanceOf(uuid, String.class);
            arbitraryUuid = UUID.fromString((String) uuid);
        }
        assertNotNull(arbitraryUuid);

        actualResults = computeActual(format("SELECT * FROM test_shard_uuid where \"$shard_uuid\" = '%s'", arbitraryUuid));
        assertNotEquals(actualResults.getMaterializedRows().size(), 0);
        actualResults = computeActual("SELECT * FROM test_shard_uuid where \"$shard_uuid\" = 'foo'");
        assertEquals(actualResults.getMaterializedRows().size(), 0);
    }

    @Test
    public void testBucketNumberHiddenColumn()
    {
        assertUpdate("" +
                        "CREATE TABLE test_bucket_number " +
                        "WITH (bucket_count = 50, bucketed_on = ARRAY ['orderkey']) " +
                        "AS SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        MaterializedResult actualResults = computeActual("SELECT DISTINCT \"$bucket_number\" FROM test_bucket_number");
        assertEquals(actualResults.getTypes(), ImmutableList.of(INTEGER));
        Set<Object> actual = actualResults.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .collect(toSet());
        assertEquals(actual, IntStream.range(0, 50).boxed().collect(toSet()));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Column '\\$bucket_number' cannot be resolved")
    public void testNoBucketNumberHiddenColumn()
    {
        assertUpdate("CREATE TABLE test_no_bucket_number (test bigint)");
        computeActual("SELECT DISTINCT \"$bucket_number\" FROM test_no_bucket_number");
    }

    @Test
    public void testShardingByTemporalDateColumn()
    {
        // Make sure we have at least 2 different orderdate.
        assertEquals(computeActual("SELECT count(DISTINCT orderdate) >= 2 FROM orders WHERE orderdate < date '1992-02-08'").getOnlyValue(), true);

        assertUpdate("CREATE TABLE test_shard_temporal_date " +
                        "WITH (temporal_column = 'orderdate') AS " +
                        "SELECT orderdate, orderkey " +
                        "FROM orders " +
                        "WHERE orderdate < date '1992-02-08'",
                "SELECT count(*) " +
                        "FROM orders " +
                        "WHERE orderdate < date '1992-02-08'");

        MaterializedResult results = computeActual("SELECT orderdate, \"$shard_uuid\" FROM test_shard_temporal_date");

        // Each shard will only contain data of one date.
        SetMultimap<String, LocalDate> shardDateMap = HashMultimap.create();
        for (MaterializedRow row : results.getMaterializedRows()) {
            shardDateMap.put((String) row.getField(1), (LocalDate) row.getField(0));
        }

        for (Collection<LocalDate> dates : shardDateMap.asMap().values()) {
            assertEquals(dates.size(), 1);
        }

        // Make sure we have all the rows
        assertQuery("SELECT orderdate, orderkey FROM test_shard_temporal_date",
                "SELECT orderdate, orderkey FROM orders WHERE orderdate < date '1992-02-08'");
    }

    @Test
    public void testShardingByTemporalDateColumnBucketed()
    {
        // Make sure we have at least 2 different orderdate.
        assertEquals(computeActual("SELECT count(DISTINCT orderdate) >= 2 FROM orders WHERE orderdate < date '1992-02-08'").getOnlyValue(), true);

        assertUpdate("CREATE TABLE test_shard_temporal_date_bucketed " +
                        "WITH (temporal_column = 'orderdate', bucket_count = 10, bucketed_on = ARRAY ['orderkey']) AS " +
                        "SELECT orderdate, orderkey " +
                        "FROM orders " +
                        "WHERE orderdate < date '1992-02-08'",
                "SELECT count(*) " +
                        "FROM orders " +
                        "WHERE orderdate < date '1992-02-08'");

        MaterializedResult results = computeActual("SELECT orderdate, \"$shard_uuid\" FROM test_shard_temporal_date_bucketed");

        // Each shard will only contain data of one date.
        SetMultimap<String, LocalDate> shardDateMap = HashMultimap.create();
        for (MaterializedRow row : results.getMaterializedRows()) {
            shardDateMap.put((String) row.getField(1), (LocalDate) row.getField(0));
        }

        for (Collection<LocalDate> dates : shardDateMap.asMap().values()) {
            assertEquals(dates.size(), 1);
        }

        // Make sure we have all the rows
        assertQuery("SELECT orderdate, orderkey FROM test_shard_temporal_date_bucketed",
                "SELECT orderdate, orderkey FROM orders WHERE orderdate < date '1992-02-08'");
    }

    @Test
    public void testShardingByTemporalTimestampColumn()
    {
        assertUpdate("CREATE TABLE test_shard_temporal_timestamp(col1 BIGINT, col2 TIMESTAMP) WITH (temporal_column = 'col2')");

        int rows = 20;
        StringJoiner joiner = new StringJoiner(", ", "INSERT INTO test_shard_temporal_timestamp VALUES ", "");
        for (int i = 0; i < rows; i++) {
            joiner.add(format("(%s, TIMESTAMP '2016-08-08 01:00' + interval '%s' hour)", i, i * 4));
        }

        assertUpdate(joiner.toString(), format("VALUES(%s)", rows));

        MaterializedResult results = computeActual("SELECT cast(cast(col2 as DATE) as VARCHAR), \"$shard_uuid\" FROM test_shard_temporal_timestamp");
        assertEquals(results.getRowCount(), rows);

        // Each shard will only contain data of one date.
        SetMultimap<String, String> shardDateMap = HashMultimap.create();
        for (MaterializedRow row : results.getMaterializedRows()) {
            shardDateMap.put((String) row.getField(1), (String) row.getField(0));
        }

        for (Collection<String> dates : shardDateMap.asMap().values()) {
            assertEquals(dates.size(), 1);
        }

        // Ensure one shard can contain different timestamps from the same day
        assertLessThan(shardDateMap.size(), rows);
    }

    @Test
    public void testShardingByTemporalTimestampColumnBucketed()
    {
        assertUpdate("" +
                "CREATE TABLE test_shard_temporal_timestamp_bucketed(col1 BIGINT, col2 TIMESTAMP) " +
                "WITH (temporal_column = 'col2', bucket_count = 3, bucketed_on = ARRAY ['col1'])");

        int rows = 100;
        StringJoiner joiner = new StringJoiner(", ", "INSERT INTO test_shard_temporal_timestamp_bucketed VALUES ", "");
        for (int i = 0; i < rows; i++) {
            joiner.add(format("(%s, TIMESTAMP '2016-08-08 01:00' + interval '%s' hour)", i, i));
        }

        assertUpdate(joiner.toString(), format("VALUES(%s)", rows));

        MaterializedResult results = computeActual("" +
                "SELECT cast(cast(col2 as DATE) as VARCHAR), \"$shard_uuid\" " +
                "FROM test_shard_temporal_timestamp_bucketed");

        assertEquals(results.getRowCount(), rows);

        // Each shard will only contain data of one date.
        SetMultimap<String, String> shardDateMap = HashMultimap.create();
        for (MaterializedRow row : results.getMaterializedRows()) {
            shardDateMap.put((String) row.getField(1), (String) row.getField(0));
        }

        for (Collection<String> dates : shardDateMap.asMap().values()) {
            assertEquals(dates.size(), 1);
        }

        // Ensure one shard can contain different timestamps from the same day
        assertLessThan(shardDateMap.size(), rows);
    }

    @Test
    public void testTableProperties()
    {
        computeActual("CREATE TABLE test_table_properties_1 (foo BIGINT, bar BIGINT, ds DATE) WITH (ordering=array['foo','bar'], temporal_column='ds')");
        computeActual("CREATE TABLE test_table_properties_2 (foo BIGINT, bar BIGINT, ds DATE) WITH (ORDERING=array['foo','bar'], TEMPORAL_COLUMN='ds')");
    }

    @Test
    public void testShardsSystemTable()
    {
        assertQuery("" +
                        "SELECT table_schema, table_name, sum(row_count)\n" +
                        "FROM system.shards\n" +
                        "WHERE table_schema = 'tpch'\n" +
                        "  AND table_name IN ('orders', 'region')\n" +
                        "GROUP BY 1, 2",
                "" +
                        "SELECT 'tpch', 'orders', (SELECT count(*) FROM orders)\n" +
                        "UNION ALL\n" +
                        "SELECT 'tpch', 'region', (SELECT count(*) FROM region)");
    }

    @Test
    public void testShardsSystemTableWithTemporalColumn()
    {
        // Make sure we have rows in the selected range
        assertEquals(computeActual("SELECT count(*) >= 1 FROM orders WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-02-08'").getOnlyValue(), true);

        // Create a table that has DATE type temporal column
        assertUpdate("CREATE TABLE test_shards_system_table_date_temporal\n" +
                        "WITH (temporal_column = 'orderdate') AS\n" +
                        "SELECT orderdate, orderkey\n" +
                        "FROM orders\n" +
                        "WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-02-08'",
                "SELECT count(*)\n" +
                        "FROM orders\n" +
                        "WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-02-08'");

        // Create a table that has TIMESTAMP type temporal column
        assertUpdate("CREATE TABLE test_shards_system_table_timestamp_temporal\n" +
                        "WITH (temporal_column = 'ordertimestamp') AS\n" +
                        "SELECT CAST (orderdate AS TIMESTAMP) AS ordertimestamp, orderkey\n" +
                        "FROM test_shards_system_table_date_temporal",
                "SELECT count(*)\n" +
                        "FROM orders\n" +
                        "WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-02-08'");

        // For table with DATE type temporal column, min/max_timestamp columns must be null while min/max_date columns must not be null
        assertEquals(computeActual("" +
                "SELECT count(*)\n" +
                "FROM system.shards\n" +
                "WHERE table_schema = 'tpch'\n" +
                "AND table_name = 'test_shards_system_table_date_temporal'\n" +
                "AND NOT \n" +
                "(min_timestamp IS NULL AND max_timestamp IS NULL\n" +
                "AND min_date IS NOT NULL AND max_date IS NOT NULL)").getOnlyValue(), 0L);

        // For table with TIMESTAMP type temporal column, min/max_date columns must be null while min/max_timestamp columns must not be null
        assertEquals(computeActual("" +
                "SELECT count(*)\n" +
                "FROM system.shards\n" +
                "WHERE table_schema = 'tpch'\n" +
                "AND table_name = 'test_shards_system_table_timestamp_temporal'\n" +
                "AND NOT\n" +
                "(min_date IS NULL AND max_date IS NULL\n" +
                "AND min_timestamp IS NOT NULL AND max_timestamp IS NOT NULL)").getOnlyValue(), 0L);

        // Test date predicates in table with DATE temporal column
        assertQuery("" +
                        "SELECT table_schema, table_name, sum(row_count)\n" +
                        "FROM system.shards \n" +
                        "WHERE table_schema = 'tpch'\n" +
                        "AND table_name = 'test_shards_system_table_date_temporal'\n" +
                        "AND min_date >= date '1992-01-01'\n" +
                        "AND max_date <= date '1992-02-08'\n" +
                        "GROUP BY 1, 2",
                "" +
                        "SELECT 'tpch', 'test_shards_system_table_date_temporal',\n" +
                        "(SELECT count(*) FROM orders WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-02-08')");

        // Test timestamp predicates in table with TIMESTAMP temporal column
        assertQuery("" +
                        "SELECT table_schema, table_name, sum(row_count)\n" +
                        "FROM system.shards \n" +
                        "WHERE table_schema = 'tpch'\n" +
                        "AND table_name = 'test_shards_system_table_timestamp_temporal'\n" +
                        "AND min_timestamp >= timestamp '1992-01-01'\n" +
                        "AND max_timestamp <= timestamp '1992-02-08'\n" +
                        "GROUP BY 1, 2",
                "" +
                        "SELECT 'tpch', 'test_shards_system_table_timestamp_temporal',\n" +
                        "(SELECT count(*) FROM orders WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-02-08')");
    }

    @Test
    public void testColumnRangesSystemTable()
    {
        assertQuery("SELECT orderkey_min, orderkey_max, custkey_min, custkey_max, orderdate_min, orderdate_max FROM \"orders$column_ranges\"",
                "SELECT min(orderkey), max(orderkey), min(custkey), max(custkey), min(orderdate), max(orderdate) FROM orders");

        assertQuery("SELECT orderkey_min, orderkey_max FROM \"orders$column_ranges\"",
                "SELECT min(orderkey), max(orderkey) FROM orders");

        // No such table test
        assertQueryFails("SELECT * FROM \"no_table$column_ranges\"", ".*'raptor\\.tpch\\.no_table\\$column_ranges' does not exist.*");

        // No range column for DOUBLE, INTEGER or VARCHAR
        assertQueryFails("SELECT totalprice_min FROM \"orders$column_ranges\"", ".*Column 'totalprice_min' cannot be resolved.*");
        assertQueryFails("SELECT shippriority_min FROM \"orders$column_ranges\"", ".*Column 'shippriority_min' cannot be resolved.*");
        assertQueryFails("SELECT orderstatus_min FROM \"orders$column_ranges\"", ".*Column 'orderstatus_min' cannot be resolved.*");
        assertQueryFails("SELECT orderpriority_min FROM \"orders$column_ranges\"", ".*Column 'orderpriority_min' cannot be resolved.*");
        assertQueryFails("SELECT clerk_min FROM \"orders$column_ranges\"", ".*Column 'clerk_min' cannot be resolved.*");
        assertQueryFails("SELECT comment_min FROM \"orders$column_ranges\"", ".*Column 'comment_min' cannot be resolved.*");

        // Empty table
        assertUpdate("CREATE TABLE column_ranges_test (a BIGINT, b BIGINT)");
        assertQuery("SELECT a_min, a_max, b_min, b_max FROM \"column_ranges_test$column_ranges\"", "SELECT NULL, NULL, NULL, NULL");

        // Table with NULL values
        assertUpdate("INSERT INTO column_ranges_test VALUES (1, NULL)", 1);
        assertQuery("SELECT a_min, a_max, b_min, b_max FROM \"column_ranges_test$column_ranges\"", "SELECT 1, 1, NULL, NULL");
        assertUpdate("INSERT INTO column_ranges_test VALUES (NULL, 99)", 1);
        assertQuery("SELECT a_min, a_max, b_min, b_max FROM \"column_ranges_test$column_ranges\"", "SELECT 1, 1, 99, 99");
        assertUpdate("INSERT INTO column_ranges_test VALUES (50, 50)", 1);
        assertQuery("SELECT a_min, a_max, b_min, b_max FROM \"column_ranges_test$column_ranges\"", "SELECT 1, 50, 50, 99");

        // Drop table
        assertUpdate("DROP TABLE column_ranges_test");
        assertQueryFails("SELECT a_min, a_max, b_min, b_max FROM \"column_ranges_test$column_ranges\"",
                ".*'raptor\\.tpch\\.column_ranges_test\\$column_ranges' does not exist.*");
    }

    @Test
    @Flaky(
            issue = "https://github.com/trinodb/trino/issues/1977",
            match = "(?s)AssertionError.*query.*SELECT count\\(DISTINCT \"\\$shard_uuid\"\\) FROM orders_bucketed.*Actual rows.*\\[\\d\\d\\].*Expected rows.*\\[100\\]")
    public void testCreateBucketedTable()
    {
        assertUpdate("DROP TABLE IF EXISTS orders_bucketed");
        assertUpdate("" +
                        "CREATE TABLE orders_bucketed " +
                        "WITH (bucket_count = 50, bucketed_on = ARRAY ['orderkey']) " +
                        "AS SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM orders_bucketed", "SELECT * FROM orders");
        assertQuery("SELECT count(*) FROM orders_bucketed", "SELECT count(*) FROM orders");
        assertQuery("SELECT count(DISTINCT \"$shard_uuid\") FROM orders_bucketed", "SELECT 50");
        assertQuery("SELECT count(DISTINCT \"$bucket_number\") FROM orders_bucketed", "SELECT 50");

        assertUpdate("INSERT INTO orders_bucketed SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM orders_bucketed", "SELECT * FROM orders UNION ALL SELECT * FROM orders");
        assertQuery("SELECT count(*) FROM orders_bucketed", "SELECT count(*) * 2 FROM orders");
        assertQuery("SELECT count(DISTINCT \"$shard_uuid\") FROM orders_bucketed", "SELECT 50 * 2");
        assertQuery("SELECT count(DISTINCT \"$bucket_number\") FROM orders_bucketed", "SELECT 50");

        assertQuery("SELECT count(*) FROM orders_bucketed a JOIN orders_bucketed b USING (orderkey)", "SELECT count(*) * 4 FROM orders");

        assertUpdate("DELETE FROM orders_bucketed WHERE orderkey = 37", 2);
        assertQuery("SELECT count(*) FROM orders_bucketed", "SELECT (count(*) * 2) - 2 FROM orders");
        assertQuery("SELECT count(DISTINCT \"$shard_uuid\") FROM orders_bucketed", "SELECT 50 * 2");
        assertQuery("SELECT count(DISTINCT \"$bucket_number\") FROM orders_bucketed", "SELECT 50");

        assertUpdate("DROP TABLE orders_bucketed");
    }

    @Test
    public void testCreateBucketedTableLike()
    {
        assertUpdate("" +
                "CREATE TABLE orders_bucketed_original (" +
                "  orderkey bigint" +
                ", custkey bigint" +
                ") " +
                "WITH (bucket_count = 50, bucketed_on = ARRAY['orderkey'])");

        assertUpdate("" +
                "CREATE TABLE orders_bucketed_like (" +
                "  orderdate date" +
                ", LIKE orders_bucketed_original INCLUDING PROPERTIES" +
                ")");

        assertUpdate("INSERT INTO orders_bucketed_like SELECT orderdate, orderkey, custkey FROM orders", "SELECT count(*) FROM orders");
        assertUpdate("INSERT INTO orders_bucketed_like SELECT orderdate, orderkey, custkey FROM orders", "SELECT count(*) FROM orders");

        assertQuery("SELECT count(DISTINCT \"$shard_uuid\") FROM orders_bucketed_like", "SELECT 50 * 2");

        assertUpdate("DROP TABLE orders_bucketed_original");
        assertUpdate("DROP TABLE orders_bucketed_like");
    }

    @Test
    public void testBucketingMixedTypes()
    {
        assertUpdate("" +
                        "CREATE TABLE orders_bucketed_mixed " +
                        "WITH (bucket_count = 50, bucketed_on = ARRAY ['custkey', 'clerk', 'shippriority']) " +
                        "AS SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM orders_bucketed_mixed", "SELECT * FROM orders");
        assertQuery("SELECT count(*) FROM orders_bucketed_mixed", "SELECT count(*) FROM orders");
        assertQuery("SELECT count(DISTINCT \"$shard_uuid\") FROM orders_bucketed_mixed", "SELECT 50");
        assertQuery("SELECT count(DISTINCT \"$bucket_number\") FROM orders_bucketed_mixed", "SELECT 50");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        super.testShowCreateTable();

        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint,\n" +
                        "   c2 double,\n" +
                        "   \"c 3\" varchar,\n" +
                        "   \"c'4\" array(bigint),\n" +
                        "   c5 map(bigint, varchar),\n" +
                        "   c6 bigint,\n" +
                        "   c7 timestamp(3)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   bucket_count = 32,\n" +
                        "   bucketed_on = ARRAY['c1','c6'],\n" +
                        "   ordering = ARRAY['c6','c1'],\n" +
                        "   temporal_column = 'c7'\n" +
                        ")",
                getSession().getCatalog().get(), getSession().getSchema().get(), "test_show_create_table");
        assertUpdate(createTableSql);

        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_show_create_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        actualResult = computeActual("SHOW CREATE TABLE " + getSession().getSchema().get() + ".test_show_create_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        actualResult = computeActual("SHOW CREATE TABLE " + getSession().getCatalog().get() + "." + getSession().getSchema().get() + ".test_show_create_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        // With organization enabled
        createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint,\n" +
                        "   c2 double,\n" +
                        "   \"c 3\" varchar,\n" +
                        "   \"c'4\" array(bigint),\n" +
                        "   c5 map(bigint, varchar),\n" +
                        "   c6 bigint,\n" +
                        "   c7 timestamp(3)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   bucket_count = 32,\n" +
                        "   bucketed_on = ARRAY['c1','c6'],\n" +
                        "   ordering = ARRAY['c6','c1'],\n" +
                        "   organized = true\n" +
                        ")",
                getSession().getCatalog().get(), getSession().getSchema().get(), "test_show_create_table_organized");
        assertUpdate(createTableSql);

        actualResult = computeActual("SHOW CREATE TABLE test_show_create_table_organized");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        actualResult = computeActual("SHOW CREATE TABLE " + getSession().getSchema().get() + ".test_show_create_table_organized");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        actualResult = computeActual("SHOW CREATE TABLE " + getSession().getCatalog().get() + "." + getSession().getSchema().get() + ".test_show_create_table_organized");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   \"c\"\"1\" bigint,\n" +
                        "   c2 double,\n" +
                        "   \"c 3\" varchar,\n" +
                        "   \"c'4\" array(bigint),\n" +
                        "   c5 map(bigint, varchar)\n" +
                        ")",
                getSession().getCatalog().get(), getSession().getSchema().get(), "\"test_show_create_table\"\"2\"");
        assertUpdate(createTableSql);

        actualResult = computeActual("SHOW CREATE TABLE \"test_show_create_table\"\"2\"");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);
    }

    @Test
    @Override
    public void testCreateTableSchemaNotFound()
    {
        // TODO (https://github.com/trinodb/trino/issues/11110) Raptor connector can create new tables in a schema where it doesn't exist
        assertThatThrownBy(super::testCreateTableSchemaNotFound)
                .hasMessageContaining("Expected query to fail: CREATE TABLE test_schema_");
    }

    @Test
    @Override
    public void testCreateTableAsSelectSchemaNotFound()
    {
        // TODO (https://github.com/trinodb/trino/issues/11110) Raptor connector can create new tables in a schema where it doesn't exist
        assertThatThrownBy(super::testCreateTableAsSelectSchemaNotFound)
                .hasMessageContaining("Expected query to fail: CREATE TABLE test_schema_");
    }

    @Test
    public void testTablesSystemTable()
    {
        assertUpdate("" +
                "CREATE TABLE system_tables_test0 (c00 timestamp, c01 varchar, c02 double, c03 bigint, c04 bigint)");
        assertUpdate("" +
                "CREATE TABLE system_tables_test1 (c10 timestamp, c11 varchar, c12 double, c13 bigint, c14 bigint) " +
                "WITH (temporal_column = 'c10')");
        assertUpdate("" +
                "CREATE TABLE system_tables_test2 (c20 timestamp, c21 varchar, c22 double, c23 bigint, c24 bigint) " +
                "WITH (temporal_column = 'c20', ordering = ARRAY['c22', 'c21'])");
        assertUpdate("" +
                "CREATE TABLE system_tables_test3 (c30 timestamp, c31 varchar, c32 double, c33 bigint, c34 bigint) " +
                "WITH (temporal_column = 'c30', bucket_count = 40, bucketed_on = ARRAY ['c34', 'c33'])");
        assertUpdate("" +
                "CREATE TABLE system_tables_test4 (c40 timestamp, c41 varchar, c42 double, c43 bigint, c44 bigint) " +
                "WITH (temporal_column = 'c40', ordering = ARRAY['c41', 'c42'], distribution_name = 'test_distribution', bucket_count = 50, bucketed_on = ARRAY ['c43', 'c44'])");
        assertUpdate("" +
                "CREATE TABLE system_tables_test5 (c50 timestamp, c51 varchar, c52 double, c53 bigint, c54 bigint) " +
                "WITH (ordering = ARRAY['c51', 'c52'], distribution_name = 'test_distribution', bucket_count = 50, bucketed_on = ARRAY ['c53', 'c54'], organized = true)");

        MaterializedResult actualResults = computeActual("SELECT * FROM system.tables");
        assertEquals(
                actualResults.getTypes(),
                ImmutableList.builder()
                        .add(VARCHAR) // table_schema
                        .add(VARCHAR) // table_name
                        .add(VARCHAR) // temporal_column
                        .add(new ArrayType(VARCHAR)) // ordering_columns
                        .add(VARCHAR) // distribution_name
                        .add(BIGINT) // bucket_count
                        .add(new ArrayType(VARCHAR)) // bucket_columns
                        .add(BOOLEAN) // organized
                        .build());
        Map<String, MaterializedRow> map = actualResults.getMaterializedRows().stream()
                .filter(row -> ((String) row.getField(1)).startsWith("system_tables_test"))
                .collect(toImmutableMap(row -> ((String) row.getField(1)), identity()));
        assertEquals(map.size(), 6);
        assertEquals(
                map.get("system_tables_test0").getFields(),
                asList("tpch", "system_tables_test0", null, null, null, null, null, Boolean.FALSE));
        assertEquals(
                map.get("system_tables_test1").getFields(),
                asList("tpch", "system_tables_test1", "c10", null, null, null, null, Boolean.FALSE));
        assertEquals(
                map.get("system_tables_test2").getFields(),
                asList("tpch", "system_tables_test2", "c20", ImmutableList.of("c22", "c21"), null, null, null, Boolean.FALSE));
        assertEquals(
                map.get("system_tables_test3").getFields(),
                asList("tpch", "system_tables_test3", "c30", null, null, 40L, ImmutableList.of("c34", "c33"), Boolean.FALSE));
        assertEquals(
                map.get("system_tables_test4").getFields(),
                asList("tpch", "system_tables_test4", "c40", ImmutableList.of("c41", "c42"), "test_distribution", 50L, ImmutableList.of("c43", "c44"), Boolean.FALSE));
        assertEquals(
                map.get("system_tables_test5").getFields(),
                asList("tpch", "system_tables_test5", null, ImmutableList.of("c51", "c52"), "test_distribution", 50L, ImmutableList.of("c53", "c54"), Boolean.TRUE));

        actualResults = computeActual("SELECT * FROM system.tables WHERE table_schema = 'tpch'");
        long actualRowCount = actualResults.getMaterializedRows().stream()
                .filter(row -> ((String) row.getField(1)).startsWith("system_tables_test"))
                .count();
        assertEquals(actualRowCount, 6);

        actualResults = computeActual("SELECT * FROM system.tables WHERE table_name = 'system_tables_test3'");
        assertEquals(actualResults.getMaterializedRows().size(), 1);

        actualResults = computeActual("SELECT * FROM system.tables WHERE table_schema = 'tpch' and table_name = 'system_tables_test3'");
        assertEquals(actualResults.getMaterializedRows().size(), 1);

        actualResults = computeActual("" +
                "SELECT distribution_name, bucket_count, bucketing_columns, ordering_columns, temporal_column, organized " +
                "FROM system.tables " +
                "WHERE table_schema = 'tpch' and table_name = 'system_tables_test3'");
        assertEquals(actualResults.getTypes(), ImmutableList.of(VARCHAR, BIGINT, new ArrayType(VARCHAR), new ArrayType(VARCHAR), VARCHAR, BOOLEAN));
        assertEquals(actualResults.getMaterializedRows().size(), 1);

        assertUpdate("DROP TABLE system_tables_test0");
        assertUpdate("DROP TABLE system_tables_test1");
        assertUpdate("DROP TABLE system_tables_test2");
        assertUpdate("DROP TABLE system_tables_test3");
        assertUpdate("DROP TABLE system_tables_test4");
        assertUpdate("DROP TABLE system_tables_test5");

        assertEquals(computeActual("SELECT * FROM system.tables WHERE table_schema IN ('foo', 'bar')").getRowCount(), 0);
    }

    @SuppressWarnings("OverlyStrongTypeCast")
    @Test
    public void testTableStatsSystemTable()
    {
        // basic sanity tests
        assertQuery("" +
                        "SELECT table_schema, table_name, sum(row_count)\n" +
                        "FROM system.table_stats\n" +
                        "WHERE table_schema = 'tpch'\n" +
                        "  AND table_name IN ('orders', 'region')\n" +
                        "GROUP BY 1, 2",
                "" +
                        "SELECT 'tpch', 'orders', (SELECT count(*) FROM orders)\n" +
                        "UNION ALL\n" +
                        "SELECT 'tpch', 'region', (SELECT count(*) FROM region)");

        assertQuery("" +
                        "SELECT\n" +
                        "  bool_and(row_count >= shard_count)\n" +
                        ", bool_and(update_time >= create_time)\n" +
                        ", bool_and(table_version >= 1)\n" +
                        "FROM system.table_stats\n" +
                        "WHERE row_count > 0",
                "SELECT true, true, true");

        // create empty table
        assertUpdate("CREATE TABLE test_table_stats (x bigint)");

        @Language("SQL") String sql = "" +
                "SELECT create_time, update_time, table_version," +
                "  shard_count, row_count, uncompressed_size\n" +
                "FROM system.table_stats\n" +
                "WHERE table_schema = 'tpch'\n" +
                "  AND table_name = 'test_table_stats'";
        MaterializedRow row = getOnlyElement(computeActual(sql).getMaterializedRows());

        LocalDateTime createTime = (LocalDateTime) row.getField(0);
        LocalDateTime updateTime1 = (LocalDateTime) row.getField(1);
        assertEquals(createTime, updateTime1);

        assertEquals(row.getField(2), 1L);      // table_version
        assertEquals(row.getField(3), 0L);      // shard_count
        assertEquals(row.getField(4), 0L);      // row_count
        long size1 = (long) row.getField(5);    // uncompressed_size

        // insert
        assertUpdate("INSERT INTO test_table_stats VALUES (1), (2), (3), (4)", 4);
        row = getOnlyElement(computeActual(sql).getMaterializedRows());

        assertEquals(row.getField(0), createTime);
        LocalDateTime updateTime2 = (LocalDateTime) row.getField(1);
        assertLessThan(updateTime1, updateTime2);

        assertEquals(row.getField(2), 2L);                    // table_version
        assertGreaterThanOrEqual((Long) row.getField(3), 1L); // shard_count
        assertEquals(row.getField(4), 4L);                    // row_count
        long size2 = (long) row.getField(5);                  // uncompressed_size
        assertGreaterThan(size2, size1);

        // delete
        assertUpdate("DELETE FROM test_table_stats WHERE x IN (2, 4)", 2);
        row = getOnlyElement(computeActual(sql).getMaterializedRows());

        assertEquals(row.getField(0), createTime);
        LocalDateTime updateTime3 = (LocalDateTime) row.getField(1);
        assertLessThan(updateTime2, updateTime3);

        assertEquals(row.getField(2), 3L);                    // table_version
        assertGreaterThanOrEqual((Long) row.getField(3), 1L); // shard_count
        assertEquals(row.getField(4), 2L);                    // row_count
        long size3 = (long) row.getField(5);                  // uncompressed_Size
        assertLessThan(size3, size2);

        // add column
        assertUpdate("ALTER TABLE test_table_stats ADD COLUMN y bigint");
        row = getOnlyElement(computeActual(sql).getMaterializedRows());

        assertEquals(row.getField(0), createTime);
        assertLessThan(updateTime3, (LocalDateTime) row.getField(1));

        assertEquals(row.getField(2), 4L);      // table_version
        assertEquals(row.getField(4), 2L);      // row_count
        assertEquals(row.getField(5), size3);   // uncompressed_size

        // cleanup
        assertUpdate("DROP TABLE test_table_stats");
    }

    @Test
    public void testAlterTable()
    {
        assertUpdate("CREATE TABLE test_alter_table (c1 bigint, c2 bigint)");
        assertUpdate("INSERT INTO test_alter_table VALUES (1, 1), (1, 2), (1, 3), (1, 4)", 4);
        assertUpdate("INSERT INTO test_alter_table VALUES (11, 1), (11, 2)", 2);

        assertUpdate("ALTER TABLE test_alter_table ADD COLUMN c3 bigint");
        assertQueryFails("ALTER TABLE test_alter_table DROP COLUMN c3", "Cannot drop the column which has the largest column ID in the table");
        assertUpdate("INSERT INTO test_alter_table VALUES (2, 1, 1), (2, 2, 2), (2, 3, 3), (2, 4, 4)", 4);
        assertUpdate("INSERT INTO test_alter_table VALUES (22, 1, 1), (22, 2, 2), (22, 4, 4)", 3);

        // Do a partial delete on a shard that does not contain newly added column
        assertUpdate("DELETE FROM test_alter_table WHERE c1 = 1 and c2 = 1", 1);
        // Then drop a full shard that does not contain newly added column
        assertUpdate("DELETE FROM test_alter_table WHERE c1 = 11", 2);

        // Drop a column from middle of table
        assertUpdate("ALTER TABLE test_alter_table DROP COLUMN c2");
        assertUpdate("INSERT INTO test_alter_table VALUES (3, 1), (3, 2), (3, 3), (3, 4)", 4);

        // Do a partial delete on a shard that contains column already dropped
        assertUpdate("DELETE FROM test_alter_table WHERE c1 = 2 and c3 = 1", 1);
        // Then drop a full shard that contains column already dropped
        assertUpdate("DELETE FROM test_alter_table WHERE c1 = 22", 3);

        assertUpdate("DROP TABLE test_alter_table");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageContaining("Failed to perform metadata operation")
                .cause()
                .hasMessageMatching(
                        "(?s).*SQLIntegrityConstraintViolationException.*" +
                                "|.*Unique index or primary key violation.*" +
                                "|.*Deadlock found when trying to get lock; try restarting transaction.*");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessage("Failed to perform metadata operation");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessage("Failed to perform metadata operation");
    }

    @Test
    public void testMergeMultipleOperationsUnbucketed()
    {
        String targetTable = "merge_multiple_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, zipcode INT, spouse VARCHAR, address VARCHAR)", targetTable));
        testMergeMultipleOperationsInternal(targetTable, 32);
    }

    @Test
    public void testMergeMultipleOperationsBucketed()
    {
        String targetTable = "merge_multiple_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, zipcode INT, spouse VARCHAR, address VARCHAR)" +
                "   WITH (bucket_count=4, bucketed_on=ARRAY['customer'])", targetTable));
        testMergeMultipleOperationsInternal(targetTable, 32);
    }

    private void testMergeMultipleOperationsInternal(String targetTable, int targetCustomerCount)
    {
        String originalInsertFirstHalf = IntStream.range(1, targetCustomerCount / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 1000, 91000, intValue, intValue))
                .collect(joining(", "));
        String originalInsertSecondHalf = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 2000, 92000, intValue, intValue))
                .collect(joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s, %s", targetTable, originalInsertFirstHalf, originalInsertSecondHalf), targetCustomerCount - 1);

        String firstMergeSource = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 83000, intValue, intValue))
                .collect(joining(", "));

        @Language("SQL") String sql = format("MERGE INTO %s t USING (SELECT * FROM (VALUES %s)) AS s(customer, purchases, zipcode, spouse, address)", targetTable, firstMergeSource) +
                "    ON t.customer = s.customer" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases, zipcode = s.zipcode, spouse = s.spouse, address = s.address";
        assertUpdate(sql, targetCustomerCount / 2);

        assertQuery(
                "SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable,
                format("SELECT * FROM (VALUES %s, %s) AS v(customer, purchases, zipcode, spouse, address)", originalInsertFirstHalf, firstMergeSource));

        String nextInsert = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(joining(", "));
        assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s", targetTable, nextInsert), targetCustomerCount / 2);

        String secondMergeSource = IntStream.range(1, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(joining(", "));

        assertUpdate(format("MERGE INTO %s t USING (SELECT * FROM (VALUES %s)) AS s(customer, purchases, zipcode, spouse, address)", targetTable, secondMergeSource) +
                "    ON t.customer = s.customer" +
                "    WHEN MATCHED AND t.zipcode = 91000 THEN DELETE" +
                "    WHEN MATCHED AND s.zipcode = 85000 THEN UPDATE SET zipcode = 60000" +
                "    WHEN MATCHED THEN UPDATE SET zipcode = s.zipcode, spouse = s.spouse, address = s.address" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, zipcode, spouse, address) VALUES(s.customer, s.purchases, s.zipcode, s.spouse, s.address)",
                targetCustomerCount * 3 / 2 - 1);

        String updatedBeginning = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 60000, intValue, intValue))
                .collect(joining(", "));
        String updatedMiddle = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(joining(", "));
        String updatedEnd = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(joining(", "));

        assertQuery(
                "SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable,
                format("SELECT * FROM (VALUES %s, %s, %s) AS v(customer, purchases, zipcode, spouse, address)", updatedBeginning, updatedMiddle, updatedEnd));
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeSimpleQueryBucketed()
    {
        String targetTable = "merge_simple_target_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (bucket_count=7, bucketed_on=ARRAY['address'])", targetTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        @Language("SQL") String query = format("MERGE INTO %s t USING ", targetTable) +
                "(SELECT * FROM (VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville'))) AS s(customer, purchases, address)" +
                "    " +
                "ON (t.customer = s.customer)" +
                "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";
        assertUpdate(query, 4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");
    }

    @Test(dataProvider = "partitionedBucketedFailure")
    public void testMergeMultipleRowsMatchFails(String createTableSql)
    {
        String targetTable = "merge_all_matches_deleted_target_" + randomNameSuffix();
        assertUpdate(format(createTableSql, targetTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Antioch')", targetTable), 2);

        String sourceTable = "merge_all_matches_deleted_source_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Adelphi'), ('Aaron', 8, 'Ashland')", sourceTable), 2);

        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                "    WHEN MATCHED THEN UPDATE SET address = s.address"))
                .hasMessage("One MERGE target table row matched more than one source row");

        assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                "    WHEN MATCHED AND s.address = 'Adelphi' THEN UPDATE SET address = s.address",
                1);
        assertQuery("SELECT customer, purchases, address FROM " + targetTable, "VALUES ('Aaron', 5, 'Adelphi'), ('Bill', 7, 'Antioch')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @DataProvider
    public Object[][] partitionedBucketedFailure()
    {
        return new Object[][] {
                {"CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)"},
                {"CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (bucket_count = 3, bucketed_on = ARRAY['customer'])"},
                {"CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (bucket_count = 4, bucketed_on = ARRAY['address'])"},
                {"CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (bucket_count = 4, bucketed_on = ARRAY['address', 'purchases', 'customer'])"}};
    }

    @Test(dataProvider = "targetAndSourceWithDifferentBucketing")
    public void testMergeWithDifferentBucketing(String testDescription, String createTargetTableSql, String createSourceTableSql)
    {
        testMergeWithDifferentBucketingInternal(testDescription, createTargetTableSql, createSourceTableSql);
    }

    private void testMergeWithDifferentBucketingInternal(String testDescription, String createTargetTableSql, String createSourceTableSql)
    {
        String targetTable = format("%s_target_%s", testDescription, randomNameSuffix());
        assertUpdate(format(createTargetTableSql, targetTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        String sourceTable = format("%s_source_%s", testDescription, randomNameSuffix());
        assertUpdate(format(createSourceTableSql, sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

        @Language("SQL") String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

        assertUpdate(sql, 4);

        assertQuery("SELECT customer, purchases, address FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Ed', 7, 'Etherville'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @DataProvider
    public Object[][] targetAndSourceWithDifferentBucketing()
    {
        return new Object[][] {
                {
                        "target_and_source_with_different_bucketing_counts",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (bucket_count = 5, bucketed_on = ARRAY['customer'])",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (bucket_count = 3, bucketed_on = ARRAY['purchases', 'address'])",
                },
                {
                        "target_and_source_with_different_bucketing_columns",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (bucket_count = 3, bucketed_on = ARRAY['address'])",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (bucket_count = 3, bucketed_on = ARRAY['customer'])",
                },
                {
                        "target_flat_source_bucketed_by_customer",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (bucket_count = 3, bucketed_on = ARRAY['customer'])",
                },
                {
                        "target_bucketed_by_customer_source_flat",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (bucket_count = 3, bucketed_on = ARRAY['customer'])",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)",
                },
        };
    }

    @Test
    public void testMergeOverManySplits()
    {
        String targetTable = "merge_delete_select_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (orderkey bigint, custkey bigint, orderstatus varchar(1), totalprice double, orderdate date, orderpriority varchar(15), clerk varchar(15), shippriority integer, comment varchar(79))", targetTable));

        assertUpdate(format("INSERT INTO %s SELECT * FROM tpch.\"sf0.1\".orders", targetTable), 150000);

        @Language("SQL") String sql = format("MERGE INTO %s t USING (SELECT * FROM tpch.\"sf0.1\".orders) s ON (t.orderkey = s.orderkey)", targetTable) +
                " WHEN MATCHED AND mod(s.orderkey, 3) = 0 THEN UPDATE SET totalprice = t.totalprice + s.totalprice" +
                " WHEN MATCHED AND mod(s.orderkey, 3) = 1 THEN DELETE";

        assertUpdate(sql, 100_000);

        assertQuery(format("SELECT count(*) FROM %s t WHERE mod(t.orderkey, 3) = 1", targetTable), "VALUES (0)");

        assertUpdate("DROP TABLE " + targetTable);
    }
}
