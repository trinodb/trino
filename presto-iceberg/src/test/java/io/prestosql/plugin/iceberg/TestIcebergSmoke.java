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
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.tests.AbstractTestIntegrationSmokeTest;
import org.apache.iceberg.FileFormat;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.prestosql.testing.MaterializedResult.DEFAULT_PRECISION;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestIcebergSmoke
        extends AbstractTestIntegrationSmokeTest
{
    public TestIcebergSmoke()
    {
        super(() -> createIcebergQueryRunner(ImmutableMap.of()));
    }

    @Override
    protected boolean isParameterizedVarcharSupported()
    {
        return false;
    }

    @Test
    public void testDecimal()
    {
        assertUpdate("CREATE TABLE test_decimal_short (x decimal(3,2))");
        assertQueryFails("INSERT INTO test_decimal_short VALUES (decimal '3.14')", "Writing to columns of type decimal not yet supported");
//        assertQuery("SELECT * FROM test_decimal_short", "SELECT CAST('3.14' AS DECIMAL(3,2))");
        dropTable(getSession(), "test_decimal_short");

        assertUpdate("CREATE TABLE test_decimal_long (x decimal(25,2))");
        assertQueryFails("INSERT INTO test_decimal_long VALUES (decimal '3.14')", "Writing to columns of type decimal not yet supported");
//        assertQuery("SELECT * FROM test_decimal_long", "SELECT CAST('3.14' AS DECIMAL(25,2))");
        dropTable(getSession(), "test_decimal_long");
    }

    @Test
    public void testTimestamp()
    {
        assertUpdate("CREATE TABLE test_timestamp (x timestamp)");
        assertQueryFails("INSERT INTO test_timestamp VALUES (timestamp '2017-05-01 10:12:34')", "Writing to columns of type timestamp not yet supported");
//        assertQuery("SELECT * FROM test_timestamp", "SELECT CAST('2017-05-01 10:12:34' AS TIMESTAMP)");
        dropTable(getSession(), "test_timestamp");
    }

    @Test
    public void testCreatePartitionedTable()
    {
        testWithAllFileFormats(this::testCreatePartitionedTable);
        testWithAllFileFormats(this::testCreatePartitionedTableWithNestedTypes);
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
//                ", _decimal_short DECIMAL(3,2)" +
//                ", _decimal_long DECIMAL(30,10)" +
//                ", _timestamp TIMESTAMP" +
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
//                "  '_decimal_short', " +
//                "  '_decimal_long'," +
//                "  '_timestamp'," +
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
//                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
//                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
//                ", CAST('2017-05-01 10:12:34' AS TIMESTAMP) _timestamp" +
                ", CAST('2017-05-01' AS DATE) _date";

        assertUpdate(session, "INSERT INTO test_partitioned_table " + select, 1);
        assertQuery(session, "SELECT * from test_partitioned_table", select);
        assertQuery(session, "" +
                        "SELECT * FROM test_partitioned_table WHERE" +
                        " 'foo' = _string" +
                        " AND 456 = _integer" +
                        " AND CAST(123 AS BIGINT) = _bigint" +
                        " AND true = _boolean" +
//                        " AND CAST('3.14' AS DECIMAL(3,2)) = _decimal_short" +
//                        " AND CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = _decimal_long" +
//                        " AND CAST('2017-05-01 10:12:34' AS TIMESTAMP) = _timestamp" +
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
                        "   format = 'PARQUET',\n" +
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
    public void testPartitionTableBasic()
    {
        testWithAllFileFormats(this::testPartitionTableBasic);
    }

    private void testPartitionTableBasic(Session session, FileFormat fileFormat)
    {
        assertUpdate(session, "CREATE TABLE test_partition_table_basic (_bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'], format = '" + fileFormat + "')");

        assertUpdate(session, "INSERT INTO test_partition_table_basic VALUES (0, CAST('2019-09-08' AS DATE)), (1, CAST('2019-09-09' AS DATE)), (2, CAST('2019-09-09' AS DATE))", 3);
        assertUpdate(session, "INSERT INTO test_partition_table_basic VALUES (3, CAST('2019-09-09' AS DATE)), (4, CAST('2019-09-10' AS DATE)), (5, CAST('2019-09-10' AS DATE))", 3);

        assertQuery(session, "SHOW COLUMNS FROM \"test_partition_table_basic$partitions\"",
                "VALUES ('_date', 'date', '', '')," +
                        "('row_count', 'bigint', '', '')," +
                        "('file_count', 'bigint', '', '')," +
                        "('total_size', 'bigint', '', '')," +
                        "('_bigint', 'row(min bigint, max bigint, null_count bigint)', '', '')");

        MaterializedResult result = computeActual("SELECT * from \"test_partition_table_basic$partitions\"");
        assertEquals(result.getRowCount(), 3);

        Map<LocalDate, MaterializedRow> rowsByPartition = result.getMaterializedRows().stream()
                .collect(toImmutableMap(row -> (LocalDate) row.getField(0), Function.identity()));

        // Test if row counts are computed correctly
        assertEquals(rowsByPartition.get(LocalDate.parse("2019-09-08")).getField(1), 1L);
        assertEquals(rowsByPartition.get(LocalDate.parse("2019-09-09")).getField(1), 3L);
        assertEquals(rowsByPartition.get(LocalDate.parse("2019-09-10")).getField(1), 2L);

        // Test if min/max values and null value count are computed correctly.
        assertEquals(rowsByPartition.get(LocalDate.parse("2019-09-08")).getField(4), new MaterializedRow(DEFAULT_PRECISION, 0L, 0L, 0L));
        assertEquals(rowsByPartition.get(LocalDate.parse("2019-09-09")).getField(4), new MaterializedRow(DEFAULT_PRECISION, 1L, 3L, 0L));
        assertEquals(rowsByPartition.get(LocalDate.parse("2019-09-10")).getField(4), new MaterializedRow(DEFAULT_PRECISION, 4L, 5L, 0L));

        dropTable(session, "test_partition_table_basic");
    }

    @Test
    public void testHistoryTableBasic()
    {
        testWithAllFileFormats(this::testHistoryTableBasic);
    }

    private void testHistoryTableBasic(Session session, FileFormat fileFormat)
    {
        assertUpdate(session, "CREATE TABLE test_history_table_basic (_string VARCHAR, _bigint BIGINT) WITH (format = '" + fileFormat + "')");

        assertUpdate(session, "INSERT INTO test_history_table_basic VALUES ('string0', 0), ('string1', 1)", 2);
        assertUpdate(session, "INSERT INTO test_history_table_basic VALUES ('string2', 2), ('string3', 3)", 2);

        assertQuery(session, "SHOW COLUMNS FROM \"test_history_table_basic$history\"",
                "VALUES ('made_current_at', 'timestamp with time zone', '', '')," +
                        "('snapshot_id', 'bigint', '', '')," +
                        "('parent_id', 'bigint', '', '')," +
                        "('is_current_ancestor', 'boolean', '', '')");

        // Test the number of history entries
        assertQuery("SELECT count(*) FROM \"test_history_table_basic$history\"", "VALUES 3");

        dropTable(session, "test_history_table_basic");
    }

    private void testWithAllFileFormats(BiConsumer<Session, FileFormat> test)
    {
        test.accept(getSession(), FileFormat.PARQUET);
    }

    private void dropTable(Session session, String table)
    {
        assertUpdate(session, "DROP TABLE " + table);
        assertFalse(getQueryRunner().tableExists(session, table));
    }
}
