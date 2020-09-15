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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.DoubleRange;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.QueryRunner;
import org.apache.iceberg.FileFormat;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.apache.iceberg.FileFormat.ORC;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public abstract class AbstractTestIcebergSmoke
        extends AbstractTestIntegrationSmokeTest
{
    private static final Pattern WITH_CLAUSE_EXTRACTER = Pattern.compile(".*(WITH\\s*\\([^)]*\\))\\s*$", Pattern.DOTALL);

    private final FileFormat format;

    protected AbstractTestIcebergSmoke(FileFormat format)
    {
        this.format = requireNonNull(format, "format is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of(), format);
    }

    @Test
    public void testShowCreateSchema()
    {
        assertThat(computeActual("SHOW CREATE SCHEMA tpch").getOnlyValue().toString())
                .matches("CREATE SCHEMA iceberg.tpch\n" +
                        "AUTHORIZATION USER user\n" +
                        "WITH \\(\n" +
                        "\\s+location = '.*/iceberg_data/tpch'\n" +
                        "\\)");
    }

    @Override
    @Test
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE iceberg.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar,\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar,\n" +
                        "   clerk varchar,\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '" + format.name() + "'\n" +
                        ")");
    }

    @Test
    public void testDecimal()
    {
        testDecimalWithPrecisionAndScale(1, 0);
        testDecimalWithPrecisionAndScale(8, 6);
        testDecimalWithPrecisionAndScale(9, 8);
        testDecimalWithPrecisionAndScale(10, 8);

        testDecimalWithPrecisionAndScale(18, 1);
        testDecimalWithPrecisionAndScale(18, 8);
        testDecimalWithPrecisionAndScale(18, 17);

        testDecimalWithPrecisionAndScale(17, 16);
        testDecimalWithPrecisionAndScale(18, 17);
        testDecimalWithPrecisionAndScale(24, 10);
        testDecimalWithPrecisionAndScale(30, 10);
        testDecimalWithPrecisionAndScale(37, 26);
        testDecimalWithPrecisionAndScale(38, 37);

        testDecimalWithPrecisionAndScale(38, 17);
        testDecimalWithPrecisionAndScale(38, 37);
    }

    private void testDecimalWithPrecisionAndScale(int precision, int scale)
    {
        checkArgument(precision >= 1 && precision <= 38, "Decimal precision (%s) must be between 1 and 38 inclusive", precision);
        checkArgument(scale < precision && scale >= 0, "Decimal scale (%s) must be less than the precision (%s) and non-negative", scale, precision);

        String decimalType = format("DECIMAL(%d,%d)", precision, scale);
        String beforeTheDecimalPoint = "12345678901234567890123456789012345678".substring(0, precision - scale);
        String afterTheDecimalPoint = "09876543210987654321098765432109876543".substring(0, scale);
        String decimalValue = format("%s.%s", beforeTheDecimalPoint, afterTheDecimalPoint);

        assertUpdate(format("CREATE TABLE test_iceberg_decimal (x %s)", decimalType));
        assertUpdate(format("INSERT INTO test_iceberg_decimal (x) VALUES (CAST('%s' AS %s))", decimalValue, decimalType), 1);
        assertQuery("SELECT * FROM test_iceberg_decimal", format("SELECT CAST('%s' AS %s)", decimalValue, decimalType));
        dropTable("test_iceberg_decimal");
    }

    @Test
    public void testTime()
    {
        testSelectOrPartitionedByTime(false);
    }

    @Test
    public void testPartitionedByTime()
    {
        testSelectOrPartitionedByTime(true);
    }

    private void testSelectOrPartitionedByTime(boolean partitioned)
    {
        String tableName = format("test_%s_by_time", partitioned ? "partitioned" : "selected");
        String partitioning = partitioned ? ", partitioning = ARRAY['x']" : "";
        assertUpdate(format("CREATE TABLE %s (x TIME(6), y BIGINT) WITH (format = '%s'%s)", tableName, format, partitioning));
        assertUpdate(format("INSERT INTO %s VALUES (TIME '10:12:34', 12345)", tableName), 1);
        assertQuery(format("SELECT COUNT(*) FROM %s", tableName), "SELECT 1");
        assertQuery(format("SELECT x FROM %s", tableName), "SELECT CAST('10:12:34' AS TIME)");
        assertUpdate(format("INSERT INTO %s VALUES (TIME '9:00:00', 67890)", tableName), 1);
        assertQuery(format("SELECT COUNT(*) FROM %s", tableName), "SELECT 2");
        assertQuery(format("SELECT x FROM %s WHERE x = TIME '10:12:34'", tableName), "SELECT CAST('10:12:34' AS TIME)");
        assertQuery(format("SELECT x FROM %s WHERE x = TIME '9:00:00'", tableName), "SELECT CAST('9:00:00' AS TIME)");
        assertQuery(format("SELECT x FROM %s WHERE y = 12345", tableName), "SELECT CAST('10:12:34' AS TIME)");
        assertQuery(format("SELECT x FROM %s WHERE y = 67890", tableName), "SELECT CAST('9:00:00' AS TIME)");
        dropTable(tableName);
    }

    @Test
    public void testPartitionByTimestamp()
    {
        testSelectOrPartitionedByTimestamp(true);
    }

    @Test
    public void testSelectByTimestamp()
    {
        testSelectOrPartitionedByTimestamp(false);
    }

    private void testSelectOrPartitionedByTimestamp(boolean partitioned)
    {
        String tableName = format("test_%s_by_timestamp", partitioned ? "partitioned" : "selected");
        assertUpdate(format("CREATE TABLE %s (_timestamp timestamp(6)) %s",
                tableName, partitioned ? "WITH (partitioning = ARRAY['_timestamp'])" : ""));
        @Language("SQL") String select1 = "SELECT TIMESTAMP '2017-05-01 10:12:34' _timestamp";
        @Language("SQL") String select2 = "SELECT TIMESTAMP '2017-10-01 10:12:34' _timestamp";
        @Language("SQL") String select3 = "SELECT TIMESTAMP '2018-05-01 10:12:34' _timestamp";
        assertUpdate(format("INSERT INTO %s %s", tableName, select1), 1);
        assertUpdate(format("INSERT INTO %s %s", tableName, select2), 1);
        assertUpdate(format("INSERT INTO %s %s", tableName, select3), 1);
        assertQuery(format("SELECT COUNT(*) from %s", tableName), "SELECT 3");

        assertQuery(format("SELECT * from %s WHERE _timestamp = TIMESTAMP '2017-05-01 10:12:34'", tableName), select1);
        assertQuery(format("SELECT * from %s WHERE _timestamp < TIMESTAMP '2017-06-01 10:12:34'", tableName), select1);
        assertQuery(format("SELECT * from %s WHERE _timestamp = TIMESTAMP '2017-10-01 10:12:34'", tableName), select2);
        assertQuery(format("SELECT * from %s WHERE _timestamp > TIMESTAMP '2017-06-01 10:12:34' AND _timestamp < TIMESTAMP '2018-05-01 10:12:34'", tableName), select2);
        assertQuery(format("SELECT * from %s WHERE _timestamp = TIMESTAMP '2018-05-01 10:12:34'", tableName), select3);
        assertQuery(format("SELECT * from %s WHERE _timestamp > TIMESTAMP '2018-01-01 10:12:34'", tableName), select3);
        dropTable(tableName);
    }

    @Test
    public void testCreatePartitionedTable()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table (" +
                "  _string VARCHAR" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ", _decimal_short DECIMAL(3,2)" +
                ", _decimal_long DECIMAL(30,10)" +
                ", _timestamp TIMESTAMP(6)" +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "partitioning = ARRAY[" +
                "  '_string'," +
                "  '_integer'," +
                "  '_bigint'," +
                "  '_boolean'," +
                "  '_real'," +
                "  '_double'," +
                "  '_decimal_short', " +
                "  '_decimal_long'," +
                "  '_timestamp'," +
                "  '_date']" +
                ")";

        assertUpdate(format(createTable, format));

        MaterializedResult result = computeActual("SELECT * FROM test_partitioned_table");
        assertEquals(result.getRowCount(), 0);

        @Language("SQL") String select = "" +
                "SELECT" +
                " 'foo' _string" +
                ", CAST(123 AS BIGINT) _bigint" +
                ", 456 _integer" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", CAST('2017-05-01 10:12:34' AS TIMESTAMP) _timestamp" +
                ", CAST('2017-05-01' AS DATE) _date";

        assertUpdate(format("INSERT INTO test_partitioned_table %s", select), 1);
        assertQuery("SELECT * FROM test_partitioned_table", select);

        @Language("SQL") String selectAgain = "" +
                "SELECT * FROM test_partitioned_table WHERE" +
                " 'foo' = _string" +
                " AND 456 = _integer" +
                " AND CAST(123 AS BIGINT) = _bigint" +
                " AND true = _boolean" +
                " AND CAST('3.14' AS DECIMAL(3,2)) = _decimal_short" +
                " AND CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = _decimal_long" +
                " AND CAST('2017-05-01 10:12:34' AS TIMESTAMP) = _timestamp" +
                " AND CAST('2017-05-01' AS DATE) = _date";
        assertQuery(selectAgain, select);

        dropTable("test_partitioned_table");
    }

    @Test
    public void testCreatePartitionedTableWithNestedTypes()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table_nested_type (" +
                "  _string VARCHAR" +
                ", _struct ROW(_field1 INT, _field2 VARCHAR)" +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "partitioning = ARRAY['_date']" +
                ")";

        assertUpdate(createTable);

        dropTable("test_partitioned_table_nested_type");
    }

    @Test
    public void testPartitionedTableWithNullValues()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table_with_null_values (" +
                "  _string VARCHAR" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ", _decimal_short DECIMAL(3,2)" +
                ", _decimal_long DECIMAL(30,10)" +
                ", _timestamp TIMESTAMP(6)" +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "partitioning = ARRAY[" +
                "  '_string'," +
                "  '_integer'," +
                "  '_bigint'," +
                "  '_boolean'," +
                "  '_real'," +
                "  '_double'," +
                "  '_decimal_short', " +
                "  '_decimal_long'," +
                "  '_timestamp'," +
                "  '_date']" +
                ")";

        assertUpdate(createTable);

        MaterializedResult result = computeActual("SELECT * from test_partitioned_table_with_null_values");
        assertEquals(result.getRowCount(), 0);

        @Language("SQL") String select = "" +
                "SELECT" +
                " null _string" +
                ", null _bigint" +
                ", null _integer" +
                ", null _real" +
                ", null _double" +
                ", null _boolean" +
                ", null _decimal_short" +
                ", null _decimal_long" +
                ", null _timestamp" +
                ", null _date";

        assertUpdate("INSERT INTO test_partitioned_table_with_null_values " + select, 1);
        assertQuery("SELECT * from test_partitioned_table_with_null_values", select);
        dropTable("test_partitioned_table_with_null_values");
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as " +
                "WITH (" +
                "partitioning = ARRAY['ORDER_STATUS', 'Ship_Priority', 'Bucket(order_key,9)']" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(createTable, "SELECT count(*) from orders");

        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   order_key bigint,\n" +
                        "   ship_priority integer,\n" +
                        "   order_status varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '%s',\n" +
                        "   partitioning = ARRAY['order_status','ship_priority','bucket(order_key, 9)']\n" +
                        ")",
                getSession().getCatalog().orElseThrow(),
                getSession().getSchema().orElseThrow(),
                "test_create_partitioned_table_as",
                format);

        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_create_partitioned_table_as");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        assertQuery("SELECT * from test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        dropTable("test_create_partitioned_table_as");
    }

    @Test
    public void testColumnComments()
    {
        assertUpdate("CREATE TABLE test_column_comments (_bigint BIGINT COMMENT 'test column comment')");
        assertQuery("SHOW COLUMNS FROM test_column_comments",
                "VALUES ('_bigint', 'bigint', '', 'test column comment')");

        dropTable("test_column_comments");
    }

    @Test
    public void testTableComments()
    {
        String createTableTemplate = "" +
                "CREATE TABLE iceberg.tpch.test_table_comments (\n" +
                "   _x bigint\n" +
                ")\n" +
                "COMMENT '%s'\n" +
                "WITH (\n" +
                format("   format = '%s'\n", format) +
                ")";
        @Language("SQL") String createTableSql = format(createTableTemplate, "test table comment", format);
        assertUpdate(createTableSql);
        MaterializedResult resultOfCreate = computeActual("SHOW CREATE TABLE test_table_comments");
        assertEquals(getOnlyElement(resultOfCreate.getOnlyColumnAsSet()), createTableSql);
        @Language("SQL") String showCreateTable = "SHOW CREATE TABLE test_table_comments";

        assertUpdate("COMMENT ON TABLE test_table_comments IS 'different test table comment'");
        MaterializedResult resultOfCommentChange = computeActual(showCreateTable);
        String afterChangeSql = format(createTableTemplate, "different test table comment", format);
        assertEquals(getOnlyElement(resultOfCommentChange.getOnlyColumnAsSet()), afterChangeSql);
        dropTable("iceberg.tpch.test_table_comments");

        String createTableWithoutComment = "" +
                "CREATE TABLE iceberg.tpch.test_table_comments (\n" +
                "   _x bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";
        assertUpdate(format(createTableWithoutComment, format));
        assertUpdate("COMMENT ON TABLE test_table_comments IS NULL");
        MaterializedResult resultOfRemovingComment = computeActual(showCreateTable);
        assertEquals(getOnlyElement(resultOfRemovingComment.getOnlyColumnAsSet()), format(createTableWithoutComment, format));

        dropTable("iceberg.tpch.test_table_comments");
    }

    // TODO: This test shows that column $snapshot_id doesn't exist at this time.  Decide if we should
    // add it and $snapshot_timestamp_ms
    @Test(enabled = false)
    public void testQueryBySnapshotId()
    {
        assertUpdate("CREATE TABLE test_query_by_snapshot (col0 INTEGER, col1 BIGINT)");
        assertUpdate("INSERT INTO test_query_by_snapshot (col0, col1) VALUES (123, CAST(987 AS BIGINT))", 1);
        long afterFirstInsertId = getLatestSnapshotId("test_query_by_snapshot");

        assertUpdate("INSERT INTO test_query_by_snapshot (col0, col1) VALUES (456, CAST(654 AS BIGINT))", 1);
        assertQuery("SELECT * FROM test_query_by_snapshot ORDER BY col0", "VALUES (123, CAST(987 AS BIGINT)), (456, CAST(654 AS BIGINT))");
        assertQuery("SELECT * FROM test_query_by_snapshot WHERE \"$snapshot_id\" = " + afterFirstInsertId, "VALUES (123, CAST(987 AS BIGINT))");

        dropTable("test_query_by_snapshot");
    }

    @Test
    public void testRollbackSnapshot()
    {
        assertUpdate("CREATE TABLE test_rollback (col0 INTEGER, col1 BIGINT)");
        long afterCreateTableId = getLatestSnapshotId("test_rollback");

        assertUpdate("INSERT INTO test_rollback (col0, col1) VALUES (123, CAST(987 AS BIGINT))", 1);
        long afterFirstInsertId = getLatestSnapshotId("test_rollback");

        assertUpdate("INSERT INTO test_rollback (col0, col1) VALUES (456, CAST(654 AS BIGINT))", 1);
        assertQuery("SELECT * FROM test_rollback ORDER BY col0", "VALUES (123, CAST(987 AS BIGINT)), (456, CAST(654 AS BIGINT))");

        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", afterFirstInsertId));
        assertQuery("SELECT * FROM test_rollback ORDER BY col0", "VALUES (123, CAST(987 AS BIGINT))");

        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", afterCreateTableId));
        assertEquals((long) computeActual("SELECT COUNT(*) FROM test_rollback").getOnlyValue(), 0);

        dropTable("test_rollback");
    }

    private long getLatestSnapshotId(String tableName)
    {
        return (long) computeActual(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC LIMIT 1", tableName))
                .getOnlyValue();
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        assertUpdate("CREATE TABLE test_not_null_table (c1 INTEGER, c2 INTEGER NOT NULL)");
        assertUpdate("INSERT INTO test_not_null_table (c2) VALUES (2)", 1);
        assertQuery("SELECT * FROM test_not_null_table", "VALUES (NULL, 2)");
        assertQueryFails("INSERT INTO test_not_null_table (c1) VALUES (1)", "NULL value not allowed for NOT NULL column: c2");
        dropTable("test_not_null_table");

        assertUpdate("CREATE TABLE test_commuted_not_null_table (a BIGINT, b BIGINT NOT NULL)");
        assertUpdate("INSERT INTO test_commuted_not_null_table (b) VALUES (2)", 1);
        assertQuery("SELECT * FROM test_commuted_not_null_table", "VALUES (NULL, 2)");
        assertQueryFails("INSERT INTO test_commuted_not_null_table (b, a) VALUES (NULL, 3)", "NULL value not allowed for NOT NULL column: b");
        dropTable("test_commuted_not_null_table");
    }

    @Test
    public void testSchemaEvolution()
    {
        assertUpdate("CREATE TABLE test_schema_evolution_drop_end (col0 INTEGER, col1 INTEGER, col2 INTEGER)");
        assertUpdate("INSERT INTO test_schema_evolution_drop_end VALUES (0, 1, 2)", 1);
        assertQuery("SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, 2)");
        assertUpdate("ALTER TABLE test_schema_evolution_drop_end DROP COLUMN col2");
        assertQuery("SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1)");
        assertUpdate("ALTER TABLE test_schema_evolution_drop_end ADD COLUMN col2 INTEGER");
        assertQuery("SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, NULL)");
        assertUpdate("INSERT INTO test_schema_evolution_drop_end VALUES (3, 4, 5)", 1);
        assertQuery("SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, NULL), (3, 4, 5)");
        dropTable("test_schema_evolution_drop_end");

        assertUpdate("CREATE TABLE test_schema_evolution_drop_middle (col0 INTEGER, col1 INTEGER, col2 INTEGER)");
        assertUpdate("INSERT INTO test_schema_evolution_drop_middle VALUES (0, 1, 2)", 1);
        assertQuery("SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 1, 2)");
        assertUpdate("ALTER TABLE test_schema_evolution_drop_middle DROP COLUMN col1");
        assertQuery("SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 2)");
        assertUpdate("ALTER TABLE test_schema_evolution_drop_middle ADD COLUMN col1 INTEGER");
        assertUpdate("INSERT INTO test_schema_evolution_drop_middle VALUES (3, 4, 5)", 1);
        assertQuery("SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 2, NULL), (3, 4, 5)");
        dropTable("test_schema_evolution_drop_middle");
    }

    @Test
    public void testCreateTableLike()
    {
        FileFormat otherFormat = format == PARQUET ? ORC : PARQUET;
        testCreateTableLikeForFormat(otherFormat);
    }

    private void testCreateTableLikeForFormat(FileFormat otherFormat)
    {
        assertUpdate(format("CREATE TABLE test_create_table_like_original (col1 INTEGER, aDate DATE) WITH(format = '%s', partitioning = ARRAY['aDate'])", format));
        assertEquals(getTablePropertiesString("test_create_table_like_original"), "WITH (\n" +
                format("   format = '%s',\n", format) +
                "   partitioning = ARRAY['adate']\n" +
                ")");

        assertUpdate("CREATE TABLE test_create_table_like_copy0 (LIKE test_create_table_like_original, col2 INTEGER)");
        assertUpdate("INSERT INTO test_create_table_like_copy0 (col1, aDate, col2) VALUES (1, CAST('1950-06-28' AS DATE), 3)", 1);
        assertQuery("SELECT * from test_create_table_like_copy0", "VALUES(1, CAST('1950-06-28' AS DATE), 3)");
        dropTable("test_create_table_like_copy0");

        assertUpdate("CREATE TABLE test_create_table_like_copy1 (LIKE test_create_table_like_original)");
        assertEquals(getTablePropertiesString("test_create_table_like_copy1"), "WITH (\n" +
                format("   format = '%s'\n)", format));
        dropTable("test_create_table_like_copy1");

        assertUpdate("CREATE TABLE test_create_table_like_copy2 (LIKE test_create_table_like_original EXCLUDING PROPERTIES)");
        assertEquals(getTablePropertiesString("test_create_table_like_copy2"), "WITH (\n" +
                format("   format = '%s'\n)", format));
        dropTable("test_create_table_like_copy2");

        assertUpdate("CREATE TABLE test_create_table_like_copy3 (LIKE test_create_table_like_original INCLUDING PROPERTIES)");
        assertEquals(getTablePropertiesString("test_create_table_like_copy3"), "WITH (\n" +
                format("   format = '%s',\n", format) +
                "   partitioning = ARRAY['adate']\n" +
                ")");
        dropTable("test_create_table_like_copy3");

        assertUpdate(format("CREATE TABLE test_create_table_like_copy4 (LIKE test_create_table_like_original INCLUDING PROPERTIES) WITH (format = '%s')", otherFormat));
        assertEquals(getTablePropertiesString("test_create_table_like_copy4"), "WITH (\n" +
                format("   format = '%s',\n", otherFormat) +
                "   partitioning = ARRAY['adate']\n" +
                ")");
        dropTable("test_create_table_like_copy4");

        dropTable("test_create_table_like_original");
    }

    private String getTablePropertiesString(String tableName)
    {
        MaterializedResult showCreateTable = computeActual("SHOW CREATE TABLE " + tableName);
        String createTable = (String) getOnlyElement(showCreateTable.getOnlyColumnAsSet());
        Matcher matcher = WITH_CLAUSE_EXTRACTER.matcher(createTable);
        return matcher.matches() ? matcher.group(1) : null;
    }

    @Test
    public void testPredicating()
    {
        assertUpdate("CREATE TABLE test_predicating_on_real (col REAL)");
        assertUpdate("INSERT INTO test_predicating_on_real VALUES 1.2", 1);
        assertQuery("SELECT * FROM test_predicating_on_real WHERE col = 1.2", "VALUES 1.2");
        dropTable("test_predicating_on_real");
    }

    @Test
    public void testHourTransform()
    {
        assertUpdate("CREATE TABLE test_hour_transform (d TIMESTAMP(6), b BIGINT) WITH (partitioning = ARRAY['hour(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(TIMESTAMP '1969-12-31 22:22:22.222222', 8)," +
                "(TIMESTAMP '1969-12-31 23:33:11.456789', 9)," +
                "(TIMESTAMP '1969-12-31 23:44:55.567890', 10)," +
                "(TIMESTAMP '1970-01-01 00:55:44.765432', 11)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456', 1)," +
                "(TIMESTAMP '2015-01-01 10:10:02.987654', 2)," +
                "(TIMESTAMP '2015-01-01 10:55:00.456789', 3)," +
                "(TIMESTAMP '2015-05-15 12:05:01.234567', 4)," +
                "(TIMESTAMP '2015-05-15 12:21:02.345678', 5)," +
                "(TIMESTAMP '2020-02-21 13:11:11.876543', 6)," +
                "(TIMESTAMP '2020-02-21 13:12:12.654321', 7)";
        assertUpdate("INSERT INTO test_hour_transform " + values, 11);
        assertQuery("SELECT * FROM test_hour_transform", values);

        @Language("SQL") String expected = "VALUES " +
                "(-1, 1, TIMESTAMP '1969-12-31 22:22:22.222222', TIMESTAMP '1969-12-31 22:22:22.222222', 8, 8), " +
                "(0, 3, TIMESTAMP '1969-12-31 23:33:11.456789', TIMESTAMP '1970-01-01 00:55:44.765432', 9, 11), " +
                "(394474, 3, TIMESTAMP '2015-01-01 10:01:23.123456', TIMESTAMP '2015-01-01 10:55:00.456789', 1, 3), " +
                "(397692, 2, TIMESTAMP '2015-05-15 12:05:01.234567', TIMESTAMP '2015-05-15 12:21:02.345678', 4, 5), " +
                "(439525, 2, TIMESTAMP '2020-02-21 13:11:11.876543', TIMESTAMP '2020-02-21 13:12:12.654321', 6, 7)";
        if (format == ORC) {
            expected = "VALUES " +
                    "(-1, 1, NULL, NULL, 8, 8), " +
                    "(0, 3, NULL, NULL, 9, 11), " +
                    "(394474, 3, NULL, NULL, 1, 3), " +
                    "(397692, 2, NULL, NULL, 4, 5), " +
                    "(439525, 2, NULL, NULL, 6, 7)";
        }

        assertQuery("SELECT d_hour, row_count, d.min, d.max, b.min, b.max FROM \"test_hour_transform$partitions\"", expected);

        System.out.println(computeActual("SELECT * FROM \"test_hour_transform$files\""));

        dropTable("test_hour_transform");
    }

    @Test
    public void testDayTransformDate()
    {
        assertUpdate("CREATE TABLE test_day_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['day(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(DATE '1969-01-01', 10), " +
                "(DATE '1969-12-31', 11), " +
                "(DATE '1970-01-01', 1), " +
                "(DATE '1970-03-04', 2), " +
                "(DATE '2015-01-01', 3), " +
                "(DATE '2015-01-13', 4), " +
                "(DATE '2015-01-13', 5), " +
                "(DATE '2015-05-15', 6), " +
                "(DATE '2015-05-15', 7), " +
                "(DATE '2020-02-21', 8), " +
                "(DATE '2020-02-21', 9)";
        assertUpdate("INSERT INTO test_day_transform_date " + values, 11);
        assertQuery("SELECT * FROM test_day_transform_date", values);

        assertQuery(
                "SELECT d_day, row_count, d.min, d.max, b.min, b.max FROM \"test_day_transform_date$partitions\"",
                "VALUES " +
                        "(DATE '1969-01-01', 1, DATE '1969-01-01', DATE '1969-01-01', 10, 10), " +
                        "(DATE '1969-12-31', 1, DATE '1969-12-31', DATE '1969-12-31', 11, 11), " +
                        "(DATE '1970-01-01', 1, DATE '1970-01-01', DATE '1970-01-01', 1, 1), " +
                        "(DATE '1970-03-04', 1, DATE '1970-03-04', DATE '1970-03-04', 2, 2), " +
                        "(DATE '2015-01-01', 1, DATE '2015-01-01', DATE '2015-01-01', 3, 3), " +
                        "(DATE '2015-01-13', 2, DATE '2015-01-13', DATE '2015-01-13', 4, 5), " +
                        "(DATE '2015-05-15', 2, DATE '2015-05-15', DATE '2015-05-15', 6, 7), " +
                        "(DATE '2020-02-21', 2, DATE '2020-02-21', DATE '2020-02-21', 8, 9)");

        dropTable("test_day_transform_date");
    }

    @Test
    public void testDayTransformTimestamp()
    {
        assertUpdate("CREATE TABLE test_day_transform_timestamp (d TIMESTAMP(6), b BIGINT) WITH (partitioning = ARRAY['day(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(TIMESTAMP '1969-12-25 15:13:12.876543', 8)," +
                "(TIMESTAMP '1969-12-30 18:47:33.345678', 9)," +
                "(TIMESTAMP '1969-12-31 00:00:00.000000', 10)," +
                "(TIMESTAMP '1969-12-31 05:06:07.234567', 11)," +
                "(TIMESTAMP '1970-01-01 12:03:08.456789', 12)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456', 1)," +
                "(TIMESTAMP '2015-01-01 11:10:02.987654', 2)," +
                "(TIMESTAMP '2015-01-01 12:55:00.456789', 3)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567', 4)," +
                "(TIMESTAMP '2015-05-15 14:21:02.345678', 5)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543', 6)," +
                "(TIMESTAMP '2020-02-21 16:12:12.654321', 7)";
        assertUpdate("INSERT INTO test_day_transform_timestamp " + values, 12);
        assertQuery("SELECT * FROM test_day_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(DATE '1969-12-26', 1, TIMESTAMP '1969-12-25 15:13:12.876543', TIMESTAMP '1969-12-25 15:13:12.876543', 8, 8), " +
                "(DATE '1969-12-31', 2, TIMESTAMP '1969-12-30 18:47:33.345678', TIMESTAMP '1969-12-31 00:00:00.000000', 9, 10), " +
                "(DATE '1970-01-01', 2, TIMESTAMP '1969-12-31 05:06:07.234567', TIMESTAMP '1970-01-01 12:03:08.456789', 11, 12), " +
                "(DATE '2015-01-01', 3, TIMESTAMP '2015-01-01 10:01:23.123456', TIMESTAMP '2015-01-01 12:55:00.456789', 1, 3), " +
                "(DATE '2015-05-15', 2, TIMESTAMP '2015-05-15 13:05:01.234567', TIMESTAMP '2015-05-15 14:21:02.345678', 4, 5), " +
                "(DATE '2020-02-21', 2, TIMESTAMP '2020-02-21 15:11:11.876543', TIMESTAMP '2020-02-21 16:12:12.654321', 6, 7)";
        if (format == ORC) {
            expected = "VALUES " +
                    "(DATE '1969-12-26', 1, NULL, NULL, 8, 8), " +
                    "(DATE '1969-12-31', 2, NULL, NULL, 9, 10), " +
                    "(DATE '1970-01-01', 2, NULL, NULL, 11, 12), " +
                    "(DATE '2015-01-01', 3, NULL, NULL, 1, 3), " +
                    "(DATE '2015-05-15', 2, NULL, NULL, 4, 5), " +
                    "(DATE '2020-02-21', 2, NULL, NULL, 6, 7)";
        }

        assertQuery("SELECT d_day, row_count, d.min, d.max, b.min, b.max FROM \"test_day_transform_timestamp$partitions\"", expected);

        dropTable("test_day_transform_timestamp");
    }

    @Test
    public void testMonthTransformDate()
    {
        assertUpdate("CREATE TABLE test_month_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['month(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(DATE '1969-11-13', 1)," +
                "(DATE '1969-12-01', 2)," +
                "(DATE '1969-12-02', 3)," +
                "(DATE '1969-12-31', 4)," +
                "(DATE '1970-01-01', 5), " +
                "(DATE '1970-05-13', 6), " +
                "(DATE '1970-12-31', 7), " +
                "(DATE '2020-01-01', 8), " +
                "(DATE '2020-06-16', 9), " +
                "(DATE '2020-06-28', 10), " +
                "(DATE '2020-06-06', 11), " +
                "(DATE '2020-07-18', 12), " +
                "(DATE '2020-07-28', 13), " +
                "(DATE '2020-12-31', 14)";
        assertUpdate("INSERT INTO test_month_transform_date " + values, 14);
        assertQuery("SELECT * FROM test_month_transform_date", values);

        assertQuery(
                "SELECT d_month, row_count, d.min, d.max, b.min, b.max FROM \"test_month_transform_date$partitions\"",
                "VALUES " +
                        "(-1, 2, DATE '1969-11-13', DATE '1969-12-01', 1, 2), " +
                        "(0, 3, DATE '1969-12-02', DATE '1970-01-01', 3, 5), " +
                        "(4, 1, DATE '1970-05-13', DATE '1970-05-13', 6, 6), " +
                        "(11, 1, DATE '1970-12-31', DATE '1970-12-31', 7, 7), " +
                        "(600, 1, DATE '2020-01-01', DATE '2020-01-01', 8, 8), " +
                        "(605, 3, DATE '2020-06-06', DATE '2020-06-28', 9, 11), " +
                        "(606, 2, DATE '2020-07-18', DATE '2020-07-28', 12, 13), " +
                        "(611, 1, DATE '2020-12-31', DATE '2020-12-31', 14, 14)");

        dropTable("test_month_transform_date");
    }

    @Test
    public void testMonthTransformTimestamp()
    {
        assertUpdate("CREATE TABLE test_month_transform_timestamp (d TIMESTAMP(6), b BIGINT) WITH (partitioning = ARRAY['month(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(TIMESTAMP '1969-11-15 15:13:12.876543', 8)," +
                "(TIMESTAMP '1969-11-19 18:47:33.345678', 9)," +
                "(TIMESTAMP '1969-12-01 00:00:00.000000', 10)," +
                "(TIMESTAMP '1969-12-01 05:06:07.234567', 11)," +
                "(TIMESTAMP '1970-01-01 12:03:08.456789', 12)," +
                "(TIMESTAMP '2015-01-01 10:01:23.123456', 1)," +
                "(TIMESTAMP '2015-01-01 11:10:02.987654', 2)," +
                "(TIMESTAMP '2015-01-01 12:55:00.456789', 3)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567', 4)," +
                "(TIMESTAMP '2015-05-15 14:21:02.345678', 5)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543', 6)," +
                "(TIMESTAMP '2020-02-21 16:12:12.654321', 7)";
        assertUpdate("INSERT INTO test_month_transform_timestamp " + values, 12);
        assertQuery("SELECT * FROM test_month_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(-1, 3, TIMESTAMP '1969-11-15 15:13:12.876543', TIMESTAMP '1969-12-01 00:00:00.000000', 8, 10), " +
                "(0, 2, TIMESTAMP '1969-12-01 05:06:07.234567', TIMESTAMP '1970-01-01 12:03:08.456789', 11, 12), " +
                "(540, 3, TIMESTAMP '2015-01-01 10:01:23.123456', TIMESTAMP '2015-01-01 12:55:00.456789', 1, 3), " +
                "(544, 2, TIMESTAMP '2015-05-15 13:05:01.234567', TIMESTAMP '2015-05-15 14:21:02.345678', 4, 5), " +
                "(601, 2, TIMESTAMP '2020-02-21 15:11:11.876543', TIMESTAMP '2020-02-21 16:12:12.654321', 6, 7)";
        if (format == ORC) {
            expected = "VALUES " +
                    "(-1, 3, NULL, NULL, 8, 10), " +
                    "(0, 2, NULL, NULL, 11, 12), " +
                    "(540, 3, NULL, NULL, 1, 3), " +
                    "(544, 2, NULL, NULL, 4, 5), " +
                    "(601, 2, NULL, NULL, 6, 7)";
        }

        assertQuery("SELECT d_month, row_count, d.min, d.max, b.min, b.max FROM \"test_month_transform_timestamp$partitions\"", expected);

        dropTable("test_month_transform_timestamp");
    }

    @Test
    public void testYearTransformDate()
    {
        assertUpdate("CREATE TABLE test_year_transform_date (d DATE, b BIGINT) WITH (partitioning = ARRAY['year(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(DATE '1968-10-13', 1), " +
                "(DATE '1969-01-01', 2), " +
                "(DATE '1969-03-15', 3), " +
                "(DATE '1970-01-01', 4), " +
                "(DATE '1970-03-05', 5), " +
                "(DATE '2015-01-01', 6), " +
                "(DATE '2015-06-16', 7), " +
                "(DATE '2015-07-28', 8), " +
                "(DATE '2016-05-15', 9), " +
                "(DATE '2016-06-06', 10), " +
                "(DATE '2020-02-21', 11), " +
                "(DATE '2020-11-10', 12)";
        assertUpdate("INSERT INTO test_year_transform_date " + values, 12);
        assertQuery("SELECT * FROM test_year_transform_date", values);

        assertQuery(
                "SELECT d_year, row_count, d.min, d.max, b.min, b.max FROM \"test_year_transform_date$partitions\"",
                "VALUES " +
                        "(-1, 2, DATE '1968-10-13', DATE '1969-01-01', 1, 2), " +
                        "(0, 3, DATE '1969-03-15', DATE '1970-03-05', 3, 5), " +
                        "(45, 3, DATE '2015-01-01', DATE '2015-07-28', 6, 8), " +
                        "(46, 2, DATE '2016-05-15', DATE '2016-06-06', 9, 10), " +
                        "(50, 2, DATE '2020-02-21', DATE '2020-11-10', 11, 12)");

        dropTable("test_year_transform_date");
    }

    @Test
    public void testYearTransformTimestamp()
    {
        assertUpdate("CREATE TABLE test_year_transform_timestamp (d TIMESTAMP(6), b BIGINT) WITH (partitioning = ARRAY['year(d)'])");

        @Language("SQL") String values = "VALUES " +
                "(TIMESTAMP '1968-03-15 15:13:12.876543', 1)," +
                "(TIMESTAMP '1968-11-19 18:47:33.345678', 2)," +
                "(TIMESTAMP '1969-01-01 00:00:00.000000', 3)," +
                "(TIMESTAMP '1969-01-01 05:06:07.234567', 4)," +
                "(TIMESTAMP '1970-01-18 12:03:08.456789', 5)," +
                "(TIMESTAMP '1970-03-14 10:01:23.123456', 6)," +
                "(TIMESTAMP '1970-08-19 11:10:02.987654', 7)," +
                "(TIMESTAMP '1970-12-31 12:55:00.456789', 8)," +
                "(TIMESTAMP '2015-05-15 13:05:01.234567', 9)," +
                "(TIMESTAMP '2015-09-15 14:21:02.345678', 10)," +
                "(TIMESTAMP '2020-02-21 15:11:11.876543', 11)," +
                "(TIMESTAMP '2020-08-21 16:12:12.654321', 12)";
        assertUpdate("INSERT INTO test_year_transform_timestamp " + values, 12);
        assertQuery("SELECT * FROM test_year_transform_timestamp", values);

        @Language("SQL") String expected = "VALUES " +
                "(-1, 3, TIMESTAMP '1968-03-15 15:13:12.876543', TIMESTAMP '1969-01-01 00:00:00.000000', 1, 3), " +
                "(0, 5, TIMESTAMP '1969-01-01 05:06:07.234567', TIMESTAMP '1970-12-31 12:55:00.456789', 4, 8), " +
                "(45, 2, TIMESTAMP '2015-05-15 13:05:01.234567', TIMESTAMP '2015-09-15 14:21:02.345678', 9, 10), " +
                "(50, 2, TIMESTAMP '2020-02-21 15:11:11.876543', TIMESTAMP '2020-08-21 16:12:12.654321', 11, 12)";
        if (format == ORC) {
            expected = "VALUES " +
                    "(-1, 3, NULL, NULL, 1, 3), " +
                    "(0, 5, NULL, NULL, 4, 8), " +
                    "(45, 2, NULL, NULL, 9, 10), " +
                    "(50, 2, NULL, NULL, 11, 12)";
        }

        assertQuery("SELECT d_year, row_count, d.min, d.max, b.min, b.max FROM \"test_year_transform_timestamp$partitions\"", expected);

        dropTable("test_year_transform_timestamp");
    }

    @Test
    public void testTruncateTransform()
    {
        String select = "SELECT d_trunc, row_count, d.min AS d_min, d.max AS d_max, b.min AS b_min, b.max AS b_max FROM \"test_truncate_transform$partitions\"";

        assertUpdate("CREATE TABLE test_truncate_transform (d VARCHAR, b BIGINT) WITH (partitioning = ARRAY['truncate(d, 2)'])");

        @Language("SQL") String insertSql = "INSERT INTO test_truncate_transform VALUES" +
                "('abcd', 1)," +
                "('abxy', 2)," +
                "('ab598', 3)," +
                "('mommy', 4)," +
                "('moscow', 5)," +
                "('Greece', 6)," +
                "('Grozny', 7)";
        assertUpdate(insertSql, 7);

        assertQuery("SELECT COUNT(*) FROM \"test_truncate_transform$partitions\"", "SELECT 3");

        assertQuery("SELECT b FROM test_truncate_transform WHERE substring(d, 1, 2) = 'ab'", "SELECT b FROM (VALUES (1), (2), (3)) AS t(b)");
        assertQuery(select + " WHERE d_trunc = 'ab'", "VALUES('ab', 3, 'ab598', 'abxy', 1, 3)");

        assertQuery("SELECT b FROM test_truncate_transform WHERE substring(d, 1, 2) = 'mo'", "SELECT b FROM (VALUES (4), (5)) AS t(b)");
        assertQuery(select + " WHERE d_trunc = 'mo'", "VALUES('mo', 2, 'mommy', 'moscow', 4, 5)");

        assertQuery("SELECT b FROM test_truncate_transform WHERE substring(d, 1, 2) = 'Gr'", "SELECT b FROM (VALUES (6), (7)) AS t(b)");
        assertQuery(select + " WHERE d_trunc = 'Gr'", "VALUES('Gr', 2, 'Greece', 'Grozny', 6, 7)");

        dropTable("test_truncate_transform");
    }

    @Test
    public void testBucketTransform()
    {
        String select = "SELECT d_bucket, row_count, d.min AS d_min, d.max AS d_max, b.min AS b_min, b.max AS b_max FROM \"test_bucket_transform$partitions\"";

        assertUpdate("CREATE TABLE test_bucket_transform (d VARCHAR, b BIGINT) WITH (partitioning = ARRAY['bucket(d, 2)'])");
        @Language("SQL") String insertSql = "INSERT INTO test_bucket_transform VALUES" +
                "('abcd', 1)," +
                "('abxy', 2)," +
                "('ab598', 3)," +
                "('mommy', 4)," +
                "('moscow', 5)," +
                "('Greece', 6)," +
                "('Grozny', 7)";
        assertUpdate(insertSql, 7);

        assertQuery("SELECT COUNT(*) FROM \"test_bucket_transform$partitions\"", "SELECT 2");

        assertQuery(select + " WHERE d_bucket = 0", "VALUES(0, 3, 'Grozny', 'mommy', 1, 7)");

        assertQuery(select + " WHERE d_bucket = 1", "VALUES(1, 4, 'Greece', 'moscow', 2, 6)");

        dropTable("test_bucket_transform");
    }

    @Test
    public void testMetadataDeleteSimple()
    {
        assertUpdate("CREATE TABLE test_metadata_delete_simple (col1 BIGINT, col2 BIGINT) WITH (partitioning = ARRAY['col1'])");
        assertUpdate("INSERT INTO test_metadata_delete_simple VALUES(1, 100), (1, 101), (1, 102), (2, 200), (2, 201), (3, 300)", 6);
        assertQueryFails(
                "DELETE FROM test_metadata_delete_simple WHERE col1 = 1 AND col2 > 101",
                "This connector only supports delete where one or more partitions are deleted entirely");
        assertQuery("SELECT sum(col2) FROM test_metadata_delete_simple", "SELECT 1004");
        assertQuery("SELECT count(*) FROM \"test_metadata_delete_simple$partitions\"", "SELECT 3");
        assertUpdate("DELETE FROM test_metadata_delete_simple WHERE col1 = 1");
        assertQuery("SELECT sum(col2) FROM test_metadata_delete_simple", "SELECT 701");
        assertQuery("SELECT count(*) FROM \"test_metadata_delete_simple$partitions\"", "SELECT 2");
        dropTable("test_metadata_delete_simple");
    }

    @Test
    public void testMetadataDelete()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_metadata_delete (" +
                "  orderkey BIGINT," +
                "  linenumber INTEGER," +
                "  linestatus VARCHAR" +
                ") " +
                "WITH (" +
                " partitioning = ARRAY[ 'linenumber', 'linestatus' ]" +
                ") ";

        assertUpdate(createTable);

        assertUpdate("" +
                        "INSERT INTO test_metadata_delete " +
                        "SELECT orderkey, linenumber, linestatus " +
                        "FROM tpch.tiny.lineitem",
                "SELECT count(*) FROM lineitem");

        assertQuery("SELECT COUNT(*) FROM \"test_metadata_delete$partitions\"", "SELECT 14");

        assertUpdate("DELETE FROM test_metadata_delete WHERE linestatus = 'F' AND linenumber = 3");
        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus <> 'F' or linenumber <> 3");
        assertQuery("SELECT count(*) FROM \"test_metadata_delete$partitions\"", "SELECT 13");

        assertUpdate("DELETE FROM test_metadata_delete WHERE linestatus='O'");
        assertQuery("SELECT count(*) FROM \"test_metadata_delete$partitions\"", "SELECT 6");
        assertQuery("SELECT * FROM test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus <> 'O' AND linenumber <> 3");

        assertQueryFails("DELETE FROM test_metadata_delete WHERE orderkey=1", "This connector only supports delete where one or more partitions are deleted entirely");

        dropTable("test_metadata_delete");
    }

    @Test
    public void testInSet()
    {
        testInSet(31);
        testInSet(35);
    }

    private void testInSet(int inCount)
    {
        String values = range(1, inCount + 1)
                .mapToObj(n -> format("(%s, %s)", n, n + 10))
                .collect(joining(", "));
        String inList = range(1, inCount + 1)
                .mapToObj(Integer::toString)
                .collect(joining(", "));

        assertUpdate("CREATE TABLE test_in_set (col1 INTEGER, col2 BIGINT)");
        assertUpdate(format("INSERT INTO test_in_set VALUES %s", values), inCount);
        // This proves that SELECTs with large IN phrases work correctly
        computeActual(format("SELECT col1 FROM test_in_set WHERE col1 IN (%s)", inList));
        dropTable("test_in_set");
    }

    @Test
    public void testBasicTableStatistics()
    {
        String tableName = format("iceberg.tpch.test_basic_%s_table_statistics", format.name().toLowerCase(ENGLISH));
        assertUpdate(format("CREATE TABLE %s (col REAL)", tableName));
        String insertStart = format("INSERT INTO %s", tableName);
        assertUpdate(insertStart + " VALUES -10", 1);
        assertUpdate(insertStart + " VALUES 100", 1);

        // SHOW STATS returns rows of the form: column_name, data_size, distinct_values_count, nulls_fractions, row_count, low_value, high_value

        MaterializedResult result = computeActual("SHOW STATS FOR " + tableName);
        MaterializedResult expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col", columnSizeForFormat(96.0), null, 0.0, null, "-10.0", "100.0")
                        .row(null, null, null, null, 2.0, null, null)
                        .build();
        assertEquals(result, expectedStatistics);

        assertUpdate(insertStart + " VALUES 200", 1);

        result = computeActual("SHOW STATS FOR " + tableName);
        expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col", columnSizeForFormat(144.0), null, 0.0, null, "-10.0", "200.0")
                        .row(null, null, null, null, 3.0, null, null)
                        .build();
        assertEquals(result, expectedStatistics);

        dropTable(tableName);
    }

    private Double columnSizeForFormat(double size)
    {
        return format == PARQUET ? size : null;
    }

    @Test
    public void testMultipleColumnTableStatistics()
    {
        String tableName = format("iceberg.tpch.test_multiple_%s_table_statistics", format.name().toLowerCase(ENGLISH));
        assertUpdate(format("CREATE TABLE %s (col1 REAL, col2 INTEGER, col3 DATE)", tableName));
        String insertStart = format("INSERT INTO %s", tableName);
        assertUpdate(insertStart + " VALUES (-10, -1, DATE '2019-06-28')", 1);
        assertUpdate(insertStart + " VALUES (100, 10, DATE '2020-01-01')", 1);

        MaterializedResult result = computeActual("SHOW STATS FOR " + tableName);

        MaterializedResult expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col1", columnSizeForFormat(96.0), null, 0.0, null, "-10.0", "100.0")
                        .row("col2", columnSizeForFormat(98.0), null, 0.0, null, "-1", "10")
                        .row("col3", columnSizeForFormat(102.0), null, 0.0, null, "2019-06-28", "2020-01-01")
                        .row(null, null, null, null, 2.0, null, null)
                        .build();
        assertEquals(result, expectedStatistics);

        assertUpdate(insertStart + " VALUES (200, 20, DATE '2020-06-28')", 1);
        result = computeActual("SHOW STATS FOR " + tableName);
        expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col1", columnSizeForFormat(144.0), null, 0.0, null, "-10.0", "200.0")
                        .row("col2", columnSizeForFormat(147), null, 0.0, null, "-1", "20")
                        .row("col3", columnSizeForFormat(153), null, 0.0, null, "2019-06-28", "2020-06-28")
                        .row(null, null, null, null, 3.0, null, null)
                        .build();
        assertEquals(result, expectedStatistics);

        assertUpdate(insertStart + " VALUES " + IntStream.rangeClosed(21, 25)
                .mapToObj(i -> format("(200, %d, DATE '2020-07-%d')", i, i))
                .collect(joining(", ")), 5);

        assertUpdate(insertStart + " VALUES " + IntStream.rangeClosed(26, 30)
                .mapToObj(i -> format("(NULL, %d, DATE '2020-06-%d')", i, i))
                .collect(joining(", ")), 5);

        result = computeActual("SHOW STATS FOR " + tableName);

        expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("col1", columnSizeForFormat(271.0), null, 5.0 / 13.0, null, "-10.0", "200.0")
                        .row("col2", columnSizeForFormat(251.0), null, 0.0, null, "-1", "30")
                        .row("col3", columnSizeForFormat(261), null, 0.0, null, "2019-06-28", "2020-07-25")
                        .row(null, null, null, null, 13.0, null, null)
                        .build();
        assertEquals(result, expectedStatistics);

        dropTable(tableName);
    }

    @Test
    public void testPartitionedTableStatistics()
    {
        assertUpdate("CREATE TABLE iceberg.tpch.test_partitioned_table_statistics (col1 REAL, col2 BIGINT) WITH (partitioning = ARRAY['col2'])");

        String insertStart = "INSERT INTO test_partitioned_table_statistics";
        assertUpdate(insertStart + " VALUES (-10, -1)", 1);
        assertUpdate(insertStart + " VALUES (100, 10)", 1);

        MaterializedResult result = computeActual("SHOW STATS FOR iceberg.tpch.test_partitioned_table_statistics");
        assertEquals(result.getRowCount(), 3);

        MaterializedRow row0 = result.getMaterializedRows().get(0);
        assertEquals(row0.getField(0), "col1");
        assertEquals(row0.getField(3), 0.0);
        assertEquals(row0.getField(5), "-10.0");
        assertEquals(row0.getField(6), "100.0");

        MaterializedRow row1 = result.getMaterializedRows().get(1);
        assertEquals(row1.getField(0), "col2");
        assertEquals(row1.getField(3), 0.0);
        assertEquals(row1.getField(5), "-1");
        assertEquals(row1.getField(6), "10");

        MaterializedRow row2 = result.getMaterializedRows().get(2);
        assertEquals(row2.getField(4), 2.0);

        assertUpdate(insertStart + " VALUES " + IntStream.rangeClosed(1, 5)
                .mapToObj(i -> format("(%d, 10)", i + 100))
                .collect(joining(", ")), 5);

        assertUpdate(insertStart + " VALUES " + IntStream.rangeClosed(6, 10)
                .mapToObj(i -> "(NULL, 10)")
                .collect(joining(", ")), 5);

        result = computeActual("SHOW STATS FOR iceberg.tpch.test_partitioned_table_statistics");
        assertEquals(result.getRowCount(), 3);
        row0 = result.getMaterializedRows().get(0);
        assertEquals(row0.getField(0), "col1");
        assertEquals(row0.getField(3), 5.0 / 12.0);
        assertEquals(row0.getField(5), "-10.0");
        assertEquals(row0.getField(6), "105.0");

        row1 = result.getMaterializedRows().get(1);
        assertEquals(row1.getField(0), "col2");
        assertEquals(row1.getField(3), 0.0);
        assertEquals(row1.getField(5), "-1");
        assertEquals(row1.getField(6), "10");

        row2 = result.getMaterializedRows().get(2);
        assertEquals(row2.getField(4), 12.0);

        assertUpdate(insertStart + " VALUES " + IntStream.rangeClosed(6, 10)
                .mapToObj(i -> "(100, NULL)")
                .collect(joining(", ")), 5);

        result = computeActual("SHOW STATS FOR iceberg.tpch.test_partitioned_table_statistics");
        row0 = result.getMaterializedRows().get(0);
        assertEquals(row0.getField(0), "col1");
        assertEquals(row0.getField(3), 5.0 / 17.0);
        assertEquals(row0.getField(5), "-10.0");
        assertEquals(row0.getField(6), "105.0");

        row1 = result.getMaterializedRows().get(1);
        assertEquals(row1.getField(0), "col2");
        assertEquals(row1.getField(3), 5.0 / 17.0);
        assertEquals(row1.getField(5), "-1");
        assertEquals(row1.getField(6), "10");

        row2 = result.getMaterializedRows().get(2);
        assertEquals(row2.getField(4), 17.0);

        dropTable("iceberg.tpch.test_partitioned_table_statistics");
    }

    @Test
    public void testStatisticsConstraints()
    {
        String tableName = "iceberg.tpch.test_simple_partitioned_table_statistics";
        assertUpdate("CREATE TABLE iceberg.tpch.test_simple_partitioned_table_statistics (col1 BIGINT, col2 BIGINT) WITH (partitioning = ARRAY['col1'])");

        String insertStart = "INSERT INTO iceberg.tpch.test_simple_partitioned_table_statistics";
        assertUpdate(insertStart + " VALUES (1, 101), (2, 102), (3, 103), (4, 104)", 4);
        TableStatistics tableStatistics = getTableStatistics(tableName, new Constraint(TupleDomain.all()));

        // TODO Change to use SHOW STATS FOR table_name when Iceberg applyFilter allows pushdown.
        // Then I can get rid of the helper methods and direct use of TableStatistics

        Predicate<Map<ColumnHandle, NullableValue>> predicate = new TestRelationalNumberPredicate("col1", 3, i -> i >= 0);
        IcebergColumnHandle col1Handle = getColumnHandleFromStatistics(tableStatistics, "col1");
        Constraint constraint = new Constraint(TupleDomain.all(), Optional.of(predicate), Optional.of(ImmutableSet.of(col1Handle)));
        tableStatistics = getTableStatistics(tableName, constraint);
        assertEquals(tableStatistics.getRowCount().getValue(), 2.0);
        ColumnStatistics columnStatistics = getStatisticsForColumn(tableStatistics, "col1");
        assertThat(columnStatistics.getRange()).hasValue(new DoubleRange(3, 4));

        // This shows that Predicate<ColumnHandle, NullableValue> only filters rows for partitioned columns.
        predicate = new TestRelationalNumberPredicate("col2", 102, i -> i >= 0);
        IcebergColumnHandle col2Handle = getColumnHandleFromStatistics(tableStatistics, "col2");
        tableStatistics = getTableStatistics(tableName, new Constraint(TupleDomain.all(), Optional.of(predicate), Optional.empty()));
        assertEquals(tableStatistics.getRowCount().getValue(), 4.0);
        columnStatistics = getStatisticsForColumn(tableStatistics, "col2");
        assertThat(columnStatistics.getRange()).hasValue(new DoubleRange(101, 104));

        dropTable(tableName);
    }

    private static class TestRelationalNumberPredicate
            implements Predicate<Map<ColumnHandle, NullableValue>>
    {
        private final String columnName;
        private final Number comparand;
        private final Predicate<Integer> comparePredicate;

        public TestRelationalNumberPredicate(String columnName, Number comparand, Predicate<Integer> comparePredicate)
        {
            this.columnName = columnName;
            this.comparand = comparand;
            this.comparePredicate = comparePredicate;
        }

        @Override
        public boolean test(Map<ColumnHandle, NullableValue> nullableValues)
        {
            for (Map.Entry<ColumnHandle, NullableValue> entry : nullableValues.entrySet()) {
                IcebergColumnHandle handle = (IcebergColumnHandle) entry.getKey();
                if (columnName.equals(handle.getName())) {
                    Object object = entry.getValue().getValue();
                    if (object instanceof Long) {
                        return comparePredicate.test(((Long) object).compareTo(comparand.longValue()));
                    }
                    if (object instanceof Double) {
                        return comparePredicate.test(((Double) object).compareTo(comparand.doubleValue()));
                    }
                    throw new IllegalArgumentException(format("NullableValue is neither Long or Double, but %s", object));
                }
            }
            return false;
        }
    }

    private ColumnStatistics getStatisticsForColumn(TableStatistics tableStatistics, String columnName)
    {
        for (Map.Entry<ColumnHandle, ColumnStatistics> entry : tableStatistics.getColumnStatistics().entrySet()) {
            IcebergColumnHandle handle = (IcebergColumnHandle) entry.getKey();
            if (handle.getName().equals(columnName)) {
                return checkColumnStatistics(entry.getValue());
            }
        }
        throw new IllegalArgumentException("TableStatistics did not contain column named " + columnName);
    }

    private static IcebergColumnHandle getColumnHandleFromStatistics(TableStatistics tableStatistics, String columnName)
    {
        for (ColumnHandle columnHandle : tableStatistics.getColumnStatistics().keySet()) {
            IcebergColumnHandle handle = (IcebergColumnHandle) columnHandle;
            if (handle.getName().equals(columnName)) {
                return handle;
            }
        }
        throw new IllegalArgumentException("TableStatistics did not contain column named " + columnName);
    }

    private ColumnStatistics checkColumnStatistics(ColumnStatistics statistics)
    {
        assertNotNull(statistics, "statistics is null");
        // Sadly, statistics.getDataSize().isUnknown() for columns in ORC files. See the TODO
        // in IcebergOrcFileWriter.
        if (format != ORC) {
            assertFalse(statistics.getDataSize().isUnknown());
        }
        assertFalse(statistics.getNullsFraction().isUnknown(), "statistics nulls fraction is unknown");
        assertFalse(statistics.getRange().isEmpty(), "statistics range is not present");
        return statistics;
    }

    private TableStatistics getTableStatistics(String tableName, Constraint constraint)
    {
        Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();
        QualifiedObjectName qualifiedName = QualifiedObjectName.valueOf(tableName);
        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(getSession(), session -> {
                    Optional<TableHandle> optionalHandle = metadata.getTableHandle(session, qualifiedName);
                    checkArgument(optionalHandle.isPresent(), "Could not create table handle for table %s", tableName);
                    return metadata.getTableStatistics(session, optionalHandle.get(), constraint);
                });
    }

    @Test
    public void testCreateNestedPartitionedTable()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_nested_table_1 (" +
                " bool BOOLEAN" +
                ", int INTEGER" +
                ", arr ARRAY(VARCHAR)" +
                ", big BIGINT" +
                ", rl REAL" +
                ", dbl DOUBLE" +
                ", mp MAP(INTEGER, VARCHAR)" +
                ", dec DECIMAL(5,2)" +
                ", vc VARCHAR" +
                ", vb VARBINARY" +
                ", ts TIMESTAMP(6)" +
                ", str ROW(id INTEGER , vc VARCHAR)" +
                ", dt DATE)" +
                " WITH (partitioning = ARRAY['int'])";

        assertUpdate(createTable);

        @Language("SQL") String insertSql = "INSERT INTO test_nested_table_1 " +
                " select true, 1, array['uno', 'dos', 'tres'], BIGINT '1', REAL '1.0', DOUBLE '1.0', map(array[1,2,3,4], array['ek','don','teen','char'])," +
                " CAST(1.0 as DECIMAL(5,2))," +
                " 'one', VARBINARY 'binary0/1values',\n" +
                " cast(current_timestamp as TIMESTAMP), (CAST(ROW(null, 'this is a random value') AS ROW(int, varchar))), current_date";

        assertUpdate(insertSql, 1);
        MaterializedResult result = computeActual("SELECT * from test_nested_table_1");
        assertEquals(result.getRowCount(), 1);

        dropTable("test_nested_table_1");

        @Language("SQL") String createTable2 = "" +
                "CREATE TABLE test_nested_table_2 (" +
                " int INTEGER" +
                ", arr ARRAY(ROW(id INTEGER, vc VARCHAR))" +
                ", big BIGINT" +
                ", rl REAL" +
                ", dbl DOUBLE" +
                ", mp MAP(INTEGER, ARRAY(VARCHAR))" +
                ", dec DECIMAL(5,2)" +
                ", str ROW(id INTEGER, vc VARCHAR, arr ARRAY(INTEGER))" +
                ", vc VARCHAR)" +
                " WITH (partitioning = ARRAY['int'])";

        assertUpdate(createTable2);

        insertSql = "INSERT INTO test_nested_table_2 " +
                " select 1, array[cast(row(1, null) as row(int, varchar)), cast(row(2, 'dos') as row(int, varchar))], BIGINT '1', REAL '1.0', DOUBLE '1.0', " +
                "map(array[1,2], array[array['ek', 'one'], array['don', 'do', 'two']]), CAST(1.0 as DECIMAL(5,2)), " +
                "CAST(ROW(1, 'this is a random value', null) AS ROW(int, varchar, array(int))), 'one'";

        assertUpdate(insertSql, 1);
        result = computeActual("SELECT * from test_nested_table_2");
        assertEquals(result.getRowCount(), 1);

        @Language("SQL") String createTable3 = "" +
                "CREATE TABLE test_nested_table_3 WITH (partitioning = ARRAY['int']) AS SELECT * FROM test_nested_table_2";

        assertUpdate(createTable3, 1);

        result = computeActual("SELECT * FROM test_nested_table_3");
        assertEquals(result.getRowCount(), 1);

        dropTable("test_nested_table_2");
        dropTable("test_nested_table_3");
    }

    private void dropTable(String table)
    {
        Session session = getSession();
        assertUpdate(session, "DROP TABLE " + table);
        assertFalse(getQueryRunner().tableExists(session, table));
    }
}
