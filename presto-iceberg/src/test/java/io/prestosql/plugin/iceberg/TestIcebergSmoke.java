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
import io.prestosql.Session;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.assertions.Assert;
import org.apache.iceberg.FileFormat;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestIcebergSmoke
        extends AbstractTestIntegrationSmokeTest
{
    private static final Pattern WITH_CLAUSE_EXTRACTER = Pattern.compile(".*(WITH\\s*\\([^)]*\\))\\s*$", Pattern.DOTALL);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of());
    }

    @Test
    public void testShowCreateSchema()
    {
        assertThat(computeActual("SHOW CREATE SCHEMA tpch").getOnlyValue().toString())
                .matches("CREATE SCHEMA iceberg.tpch\n" +
                        "AUTHORIZATION USER user\n" +
                        "WITH \\(\n" +
                        "   location = '.*/iceberg_data/tpch'\n" +
                        "\\)");
    }

    @Test
    @Override
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
        Assert.assertEquals(actualColumns, expectedColumns);
    }

    @Override
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
                        "   format = 'ORC'\n" +
                        ")");
    }

    @Test
    public void testDecimal()
    {
        for (int precision = 1; precision <= 38; precision++) {
            testDecimalWithPrecisionAndScale(precision, precision - 1);
        }

        for (int scale = 1; scale < 37; scale++) {
            testDecimalWithPrecisionAndScale(38, scale);
        }

        for (int scale = 1; scale < 17; scale++) {
            testDecimalWithPrecisionAndScale(18, scale);
        }
    }

    private void testDecimalWithPrecisionAndScale(int precision, int scale)
    {
        checkArgument(precision >= 1 && precision <= 38, "Decimal precision (%s) must be between 1 and 38 inclusive", precision);
        checkArgument(scale < precision && scale >= 0, "Decimal scale (%s) must be less than the precision (%s) and non-negative", scale, precision);

        String tableName = format("test_decimal_p%d_s%d", precision, scale);
        String decimalType = format("DECIMAL(%d,%d)", precision, scale);
        String beforeTheDecimalPoint = "12345678901234567890123456789012345678".substring(0, precision - scale);
        String afterTheDecimalPoint = "09876543210987654321098765432109876543".substring(0, scale);
        String decimalValue = format("%s.%s", beforeTheDecimalPoint, afterTheDecimalPoint);

        assertUpdate(format("CREATE TABLE %s (x %s)", tableName, decimalType));
        assertUpdate(format("INSERT INTO %s (x) VALUES (CAST('%s' AS %s))", tableName, decimalValue, decimalType), 1);
        assertQuery(format("SELECT * FROM %s", tableName), format("SELECT CAST('%s' AS %s)", decimalValue, decimalType));
        dropTable(getSession(), tableName);
    }

    @Test
    public void testTimestamp()
    {
        assertUpdate("CREATE TABLE test_timestamp (x timestamp)");
        assertUpdate("INSERT INTO test_timestamp VALUES (timestamp '2017-05-01 10:12:34')", 1);
        assertQuery("SELECT * FROM test_timestamp", "SELECT CAST('2017-05-01 10:12:34' AS TIMESTAMP)");
        dropTable(getSession(), "test_timestamp");
    }

    @Test
    public void testCreatePartitionedTable()
    {
        testWithAllFileFormats(this::testCreatePartitionedTable);
        testWithAllFileFormats(this::testCreatePartitionedTableWithNestedTypes);
        testWithAllFileFormats(this::testPartitionedTableWithNullValues);
    }

    private void testCreatePartitionedTable(Session session, FileFormat fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table (" +
                "  _string VARCHAR" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                returnSqlIfFormatSupportsDecimalsAndTimestamps(fileFormat, "" +
                        ", _decimal_short DECIMAL(3,2)" +
                        ", _decimal_long DECIMAL(30,10)" +
                        ", _timestamp TIMESTAMP") +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "format = '" + fileFormat + "', " +
                "partitioning = ARRAY[" +
                "  '_string'," +
                "  '_integer'," +
                "  '_bigint'," +
                "  '_boolean'," +
                "  '_real'," +
                "  '_double'," +
                returnSqlIfFormatSupportsDecimalsAndTimestamps(FileFormat.PARQUET, "" +
                        "  '_decimal_short', " +
                        "  '_decimal_long'," +
                        "  '_timestamp',") +
                "  '_date']" +
                ")";

        assertUpdate(session, createTable);

        MaterializedResult result = computeActual("SELECT * from test_partitioned_table");
        assertEquals(result.getRowCount(), 0);

        @Language("SQL") String select = "" +
                "SELECT" +
                " 'foo' _string" +
                ", CAST(123 AS BIGINT) _bigint" +
                ", 456 _integer" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                returnSqlIfFormatSupportsDecimalsAndTimestamps(fileFormat, "" +
                        ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                        ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                        ", CAST('2017-05-01 10:12:34' AS TIMESTAMP) _timestamp") +
                ", CAST('2017-05-01' AS DATE) _date";

        assertUpdate(session, "INSERT INTO test_partitioned_table " + select, 1);
        assertQuery(session, "SELECT * from test_partitioned_table", select);
        assertQuery(session, "" +
                        "SELECT * FROM test_partitioned_table WHERE" +
                        " 'foo' = _string" +
                        " AND 456 = _integer" +
                        " AND CAST(123 AS BIGINT) = _bigint" +
                        " AND true = _boolean" +
                        returnSqlIfFormatSupportsDecimalsAndTimestamps(fileFormat, "" +
                                " AND CAST('3.14' AS DECIMAL(3,2)) = _decimal_short" +
                                " AND CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = _decimal_long" +
                                " AND CAST('2017-05-01 10:12:34' AS TIMESTAMP) = _timestamp") +
                        " AND CAST('2017-05-01' AS DATE) = _date",
                select);

        dropTable(session, "test_partitioned_table");
    }

    private void testCreatePartitionedTableWithNestedTypes(Session session, FileFormat fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table_nested_type (" +
                "  _string VARCHAR" +
                ", _struct ROW(_field1 INT, _field2 VARCHAR)" +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "format = '" + fileFormat + "', " +
                "partitioning = ARRAY['_date']" +
                ")";

        assertUpdate(session, createTable);

        dropTable(session, "test_partitioned_table_nested_type");
    }

    private void testPartitionedTableWithNullValues(Session session, FileFormat fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table (" +
                "  _string VARCHAR" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                returnSqlIfFormatSupportsDecimalsAndTimestamps(fileFormat, "" +
                        ", _decimal_short DECIMAL(3,2)" +
                        ", _decimal_long DECIMAL(30,10)" +
                        ", _timestamp TIMESTAMP") +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "format = '" + fileFormat + "', " +
                "partitioning = ARRAY[" +
                "  '_string'," +
                "  '_integer'," +
                "  '_bigint'," +
                "  '_boolean'," +
                "  '_real'," +
                "  '_double'," +
                returnSqlIfFormatSupportsDecimalsAndTimestamps(fileFormat, "" +
                        "  '_decimal_short', " +
                        "  '_decimal_long'," +
                        "  '_timestamp',") +
                "  '_date']" +
                ")";

        assertUpdate(session, createTable);

        MaterializedResult result = computeActual("SELECT * from test_partitioned_table");
        assertEquals(result.getRowCount(), 0);

        @Language("SQL") String select = "" +
                "SELECT" +
                " null _string" +
                ", null _bigint" +
                ", null _integer" +
                ", null _real" +
                ", null _double" +
                ", null _boolean" +
                returnSqlIfFormatSupportsDecimalsAndTimestamps(fileFormat, "" +
                        ", null _decimal_short" +
                        ", null _decimal_long" +
                        ", null _timestamp") +
                ", null _date";

        assertUpdate(session, "INSERT INTO test_partitioned_table " + select, 1);
        assertQuery(session, "SELECT * from test_partitioned_table", select);
        dropTable(session, "test_partitioned_table");
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        testWithAllFileFormats(this::testCreatePartitionedTableAs);
    }

    private void testCreatePartitionedTableAs(Session session, FileFormat fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as " +
                "WITH (" +
                "format = '" + fileFormat + "', " +
                "partitioning = ARRAY['ORDER_STATUS', 'Ship_Priority', 'Bucket(order_key,9)']" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(session, createTable, "SELECT count(*) from orders");

        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   order_key bigint,\n" +
                        "   ship_priority integer,\n" +
                        "   order_status varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '" + fileFormat + "',\n" +
                        "   partitioning = ARRAY['order_status','ship_priority','bucket(order_key, 9)']\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_create_partitioned_table_as");

        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_create_partitioned_table_as");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

//        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * from test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        dropTable(session, "test_create_partitioned_table_as");
    }

    @Test
    public void testColumnComments()
    {
        Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_column_comments (_bigint BIGINT COMMENT 'test column comment')");

        assertQuery(session, "SHOW COLUMNS FROM test_column_comments",
                "VALUES ('_bigint', 'bigint', '', 'test column comment')");

        dropTable(session, "test_column_comments");
    }

    @Test
    public void testTableComments()
    {
        Session session = getSession();
        String createTableTemplate = "" +
                        "CREATE TABLE iceberg.tpch.test_table_comments (\n" +
                        "   _x bigint\n" +
                        ")\n" +
                        "COMMENT '%s'\n" +
                        "WITH (\n" +
                        "   format = 'ORC'\n" +
                        ")";
        String createTableSql = format(createTableTemplate, "test table comment");
        assertUpdate(createTableSql);
        MaterializedResult resultOfCreate = computeActual("SHOW CREATE TABLE test_table_comments");
        assertEquals(getOnlyElement(resultOfCreate.getOnlyColumnAsSet()), createTableSql);

        assertUpdate("COMMENT ON TABLE test_table_comments IS 'different test table comment'");
        MaterializedResult resultOfCommentChange = computeActual("SHOW CREATE TABLE test_table_comments");
        String afterChangeSql = format(createTableTemplate, "different test table comment");
        assertEquals(getOnlyElement(resultOfCommentChange.getOnlyColumnAsSet()), afterChangeSql);

        String createTableWithoutComment = "" +
                "CREATE TABLE iceberg.tpch.test_table_comments (\n" +
                "   _x bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";
        assertUpdate("COMMENT ON TABLE test_table_comments IS NULL");
        MaterializedResult resultOfRemovingComment = computeActual("SHOW CREATE TABLE test_table_comments");
        assertEquals(getOnlyElement(resultOfRemovingComment.getOnlyColumnAsSet()), createTableWithoutComment);

        dropTable(session, "test_table_comments");
    }

    @Test
    public void testRollbackSnapshot()
    {
        Session session = getSession();
        MaterializedResult result = computeActual("SHOW SCHEMAS FROM system");
        assertUpdate(session, "CREATE TABLE test_rollback (col0 INTEGER, col1 BIGINT)");
        long afterCreateTableId = getLatestSnapshotId();

        assertUpdate(session, "INSERT INTO test_rollback (col0, col1) VALUES (123, CAST(987 AS BIGINT))", 1);
        long afterFirstInsertId = getLatestSnapshotId();

        assertUpdate(session, "INSERT INTO test_rollback (col0, col1) VALUES (456, CAST(654 AS BIGINT))", 1);
        assertQuery(session, "SELECT * FROM test_rollback ORDER BY col0", "VALUES (123, CAST(987 AS BIGINT)), (456, CAST(654 AS BIGINT))");

        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", afterFirstInsertId));
        assertQuery(session, "SELECT * FROM test_rollback ORDER BY col0", "VALUES (123, CAST(987 AS BIGINT))");

        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", afterCreateTableId));
        assertEquals((long) computeActual(session, "SELECT COUNT(*) FROM test_rollback").getOnlyValue(), 0);

        dropTable(session, "test_rollback");
    }

    private long getLatestSnapshotId()
    {
        return (long) computeActual("SELECT snapshot_id FROM \"test_rollback$snapshots\" ORDER BY committed_at DESC LIMIT 1")
                .getOnlyValue();
    }

    @Test
    public void testSchemaEvolution()
    {
        // Schema evolution should be id based
        testWithAllFileFormats(this::testSchemaEvolution);
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        assertUpdate("CREATE TABLE test_not_null_table (c1 INTEGER, c2 INTEGER NOT NULL)");
        assertUpdate("INSERT INTO test_not_null_table (c2) VALUES (2)", 1);
        assertQuery("SELECT * FROM test_not_null_table", "VALUES (NULL, 2)");
        assertQueryFails("INSERT INTO test_not_null_table (c1) VALUES (1)", "NULL value not allowed for NOT NULL column: c2");
        assertUpdate("DROP TABLE IF EXISTS test_not_null_table");

        assertUpdate("CREATE TABLE test_commuted_not_null_table (a BIGINT, b BIGINT NOT NULL)");
        assertUpdate("INSERT INTO test_commuted_not_null_table (b) VALUES (2)", 1);
        assertQuery("SELECT * FROM test_commuted_not_null_table", "VALUES (NULL, 2)");
        assertQueryFails("INSERT INTO test_commuted_not_null_table (b, a) VALUES (NULL, 3)", "NULL value not allowed for NOT NULL column: b");
        assertUpdate("DROP TABLE IF EXISTS test_commuted_not_null_table");
    }

    private void testSchemaEvolution(Session session, FileFormat fileFormat)
    {
        assertUpdate(session, "CREATE TABLE test_schema_evolution_drop_end (col0 INTEGER, col1 INTEGER, col2 INTEGER) WITH (format = '" + fileFormat + "')");
        assertUpdate(session, "INSERT INTO test_schema_evolution_drop_end VALUES (0, 1, 2)", 1);
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, 2)");
        assertUpdate(session, "ALTER TABLE test_schema_evolution_drop_end DROP COLUMN col2");
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1)");
        assertUpdate(session, "ALTER TABLE test_schema_evolution_drop_end ADD COLUMN col2 INTEGER");
        assertUpdate(session, "INSERT INTO test_schema_evolution_drop_end VALUES (3, 4, 5)", 1);
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, NULL), (3, 4, 5)");
        dropTable(session, "test_schema_evolution_drop_end");

        assertUpdate(session, "CREATE TABLE test_schema_evolution_drop_middle (col0 INTEGER, col1 INTEGER, col2 INTEGER) WITH (format = '" + fileFormat + "')");
        assertUpdate(session, "INSERT INTO test_schema_evolution_drop_middle VALUES (0, 1, 2)", 1);
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 1, 2)");
        assertUpdate(session, "ALTER TABLE test_schema_evolution_drop_middle DROP COLUMN col1");
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 2)");
        assertUpdate(session, "ALTER TABLE test_schema_evolution_drop_middle ADD COLUMN col1 INTEGER");
        assertUpdate(session, "INSERT INTO test_schema_evolution_drop_middle VALUES (3, 4, 5)", 1);
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 2, NULL), (3, 4, 5)");
        dropTable(session, "test_schema_evolution_drop_middle");
    }

    @Test
    private void testCreateTableLike()
    {
        Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_create_table_like_original (col1 INTEGER, aDate DATE) WITH(format = 'PARQUET', partitioning = ARRAY['aDate'])");
        assertEquals(getTablePropertiesString("test_create_table_like_original"), "WITH (\n" +
                "   format = 'PARQUET',\n" +
                "   partitioning = ARRAY['adate']\n" +
                ")");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy0 (LIKE test_create_table_like_original, col2 INTEGER)");
        assertUpdate(session, "INSERT INTO test_create_table_like_copy0 (col1, aDate, col2) VALUES (1, CAST('1950-06-28' AS DATE), 3)", 1);
        assertQuery(session, "SELECT * from test_create_table_like_copy0", "VALUES(1, CAST('1950-06-28' AS DATE), 3)");
        dropTable(session, "test_create_table_like_copy0");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy1 (LIKE test_create_table_like_original)");
        assertEquals(getTablePropertiesString("test_create_table_like_copy1"), "WITH (\n" +
                "   format = 'ORC'\n" +
                ")");
        dropTable(session, "test_create_table_like_copy1");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy2 (LIKE test_create_table_like_original EXCLUDING PROPERTIES)");
        assertEquals(getTablePropertiesString("test_create_table_like_copy2"), "WITH (\n" +
                "   format = 'ORC'\n" +
                ")");
        dropTable(session, "test_create_table_like_copy2");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy3 (LIKE test_create_table_like_original INCLUDING PROPERTIES)");
        assertEquals(getTablePropertiesString("test_create_table_like_copy3"), "WITH (\n" +
                "   format = 'PARQUET',\n" +
                "   partitioning = ARRAY['adate']\n" +
                ")");
        dropTable(session, "test_create_table_like_copy3");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy4 (LIKE test_create_table_like_original INCLUDING PROPERTIES) WITH (format = 'ORC')");
        assertEquals(getTablePropertiesString("test_create_table_like_copy4"), "WITH (\n" +
                "   format = 'ORC',\n" +
                "   partitioning = ARRAY['adate']\n" +
                ")");
        dropTable(session, "test_create_table_like_copy4");

        dropTable(session, "test_create_table_like_original");
    }

    private String getTablePropertiesString(String tableName)
    {
        MaterializedResult showCreateTable = computeActual("SHOW CREATE TABLE " + tableName);
        String createTable = (String) getOnlyElement(showCreateTable.getOnlyColumnAsSet());
        Matcher matcher = WITH_CLAUSE_EXTRACTER.matcher(createTable);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        else {
            return null;
        }
    }

    @Test
    public void testPredicating()
    {
        testWithAllFileFormats(this::testPredicating);
    }

    private void testPredicating(Session session, FileFormat fileFormat)
    {
        assertUpdate(session, "CREATE TABLE test_predicating_on_real (col REAL) WITH (format = '" + fileFormat + "')");
        assertUpdate(session, "INSERT INTO test_predicating_on_real VALUES 1.2", 1);
        assertQuery(session, "SELECT * FROM test_predicating_on_real WHERE col = 1.2", "VALUES 1.2");
        dropTable(session, "test_predicating_on_real");
    }

    private void testWithAllFileFormats(BiConsumer<Session, FileFormat> test)
    {
        test.accept(getSession(), FileFormat.PARQUET);
        test.accept(getSession(), FileFormat.ORC);
    }

    // TODO: Remove and eliminate callers once we correctly handle Parquet decimals and timestamps
    private String returnSqlIfFormatSupportsDecimalsAndTimestamps(FileFormat fileFormat, String sql)
    {
        return fileFormat == FileFormat.ORC ? sql : "";
    }

    private void dropTable(Session session, String table)
    {
        assertUpdate(session, "DROP TABLE " + table);
        assertFalse(getQueryRunner().tableExists(session, table));
    }
}
